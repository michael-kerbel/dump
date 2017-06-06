package util.dump;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.list.TLongList;


public class MultithreadedDumpReader<E> implements DumpInput<E>, Iterator<E> {

   private static final Logger LOG                   = LoggerFactory.getLogger(Dump.class);

   private static final int    BUFFER_SIZE           = 1 * 1024 * 1024;
   private static final int    BUFFER_REFILL_LIMIT   = 100 * 1024;

   InputStream                 _inputbuffer;
   byte[]                      _buffer               = new byte[BUFFER_SIZE];
   long                        _bufferedToPosition   = 0;
   private TLongList           _allPositions;
   private Dump                _dump;
   private AtomicInteger       _indexIterator        = new AtomicInteger(0);
   private long                _bufferDelta          = 0;
   private E[]                 _preloadedInstances;
   private AtomicBoolean       _waitingForPreloading = new AtomicBoolean(false);


   public MultithreadedDumpReader( Dump d, DumpIndex index ) throws IOException {
      _dump = d;
      _allPositions = index.getAllPositions();
      long maxElementSize = 0, lastPosition = 0;
      for ( long pos : _allPositions.toArray() ) {
         long elementSize = pos - lastPosition;
         maxElementSize = Math.max(maxElementSize, elementSize);
         lastPosition = pos;
      }
      if ( _buffer.length < maxElementSize ) {
         _buffer = new byte[(int)(maxElementSize * 2)];
      }
      _preloadedInstances = (E[])new Object[_allPositions.size()];
      _inputbuffer = new FileInputStream(_dump.getDumpFile()); // using a BufferedInputStream doesn't improve the situation! see: http://stackoverflow.com/questions/3122422/usage-of-bufferedinputstream

      new Preloader().start();
   }

   @Override
   public void close() throws IOException {
      _inputbuffer.close();
   }

   @Override
   public boolean hasNext() {
      boolean hasNext = _indexIterator.get() < _allPositions.size();
      if ( !hasNext ) {
         try {
            close();
         }
         catch ( IOException argh ) {
            LOG.error("Failed to close.", argh);
         }
      }
      return hasNext;
   }

   @Override
   public Iterator<E> iterator() {
      return this;
   }

   @Override
   public E next() {
      int i = _indexIterator.get();
      if ( i >= _allPositions.size() ) {
         throw new NoSuchElementException("end of dump reached");
      }

      E e = (E)null;
      e = _preloadedInstances[i];
      while ( e == null ) {
         _waitingForPreloading.set(true);
         synchronized ( _waitingForPreloading ) {
            try {
               _waitingForPreloading.wait();
            }
            catch ( InterruptedException argh ) {
               Thread.interrupted();
            }
         }
         e = _preloadedInstances[i];
      }
      _waitingForPreloading.set(false);
      _preloadedInstances[i] = null;
      _indexIterator.incrementAndGet();
      return e;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("remove() not supported by " + getClass().getSimpleName() + "!");
   }

   private int bufferIndexOf( long position ) {
      return (int)(position - _bufferDelta);
   }


   private static class OwnByteArrayInputStream extends ByteArrayInputStream {

      public OwnByteArrayInputStream( byte[] buf ) {
         super(buf);
      }

      public void setBuf( byte buf[], int offset, int length ) {
         this.buf = buf;
         this.pos = offset;
         this.count = Math.min(offset + length, buf.length);
         this.mark = offset;
      }
   }

   private class Preloader extends Thread {

      private static final int             UPPER_PRELOADER_CACHE_LIMIT = 30000;
      private static final int             LOWER_PRELOADER_CACHE_LIMIT = 10000;

      private AtomicInteger                _indexPreloadTasks          = new AtomicInteger(0);
      private Object[]                     _preloaderTasks;
      private AtomicInteger                _indexWorker                = new AtomicInteger(0);
      private AtomicBoolean                _waitingForCacheDepletion   = new AtomicBoolean(false);
      private AtomicBoolean                _waitingForPreloaderTask    = new AtomicBoolean(false);

      ThreadLocal<OwnByteArrayInputStream> _byteArrayInputs            = ThreadLocal.withInitial(() -> new OwnByteArrayInputStream(_buffer));

      ThreadLocal<ObjectInput>             _objectInputs               = ThreadLocal.withInitial(() -> {
                                                                          try {
                                                                             return _dump.getStreamProvider().createObjectInput(_byteArrayInputs.get());
                                                                          }
                                                                          catch ( IOException argh ) {
                                                                             throw new RuntimeException();
                                                                          }
                                                                       });


      public Preloader() {
         setName("Dump-Preloader-main");
         setDaemon(true);
      }

      @Override
      public void run() {
         _preloaderTasks = new Object[_allPositions.size()];

         for ( int i = 0, length = Runtime.getRuntime().availableProcessors(); i < length; i++ ) {
            new PreloadWorker("Dump-Preloader-" + i).start();
         }

         while ( hasNextPreload() ) {
            while ( _indexPreloadTasks.get() - _indexIterator.get() > UPPER_PRELOADER_CACHE_LIMIT ) {
               _waitingForCacheDepletion.set(true);
               synchronized ( _waitingForCacheDepletion ) {
                  try {
                     _waitingForCacheDepletion.wait();
                  }
                  catch ( InterruptedException argh ) {
                     Thread.interrupted();
                  }
               }
            }
            _waitingForCacheDepletion.set(false);

            int positionIndex = _indexPreloadTasks.getAndIncrement();
            long positionFrom = _allPositions.get(positionIndex);
            long positionTo = positionIndex + 1 < _allPositions.size() ? _allPositions.get(positionIndex + 1) : _dump.getDumpSize();

            int from = bufferIndexOf(positionFrom);
            int len = (int)(positionTo - positionFrom);

            _preloaderTasks[positionIndex] = new PreloaderTask(_buffer, from, len);

            if ( _waitingForPreloaderTask.get() ) {
               synchronized ( _waitingForPreloaderTask ) {
                  _waitingForPreloaderTask.notifyAll();
               }
            }
         }
      }

      private boolean hasNextPreload() {
         int positionIndex = _indexPreloadTasks.get();
         if ( _allPositions.size() <= positionIndex ) {
            return false;
         }
         long dumpPosition = positionIndex == 0 ? 0 : _allPositions.get(positionIndex);
         long nextDumpPosition = positionIndex + 1 >= _allPositions.size() ? 0 : _allPositions.get(positionIndex + 1);
         while ( (dumpPosition + BUFFER_REFILL_LIMIT > _bufferedToPosition || nextDumpPosition > _bufferedToPosition)
            && _bufferedToPosition < _dump.getDumpSize() ) {
            try {
               int oldBufferRest = (int)(_bufferedToPosition - dumpPosition);
               byte[] buffer = new byte[_buffer.length];
               System.arraycopy(_buffer, bufferIndexOf(dumpPosition), buffer, 0, oldBufferRest);
               _buffer = buffer;
               _bufferDelta = dumpPosition;
               int bytesRead = _inputbuffer.read(buffer, oldBufferRest, buffer.length - oldBufferRest);
               if ( bytesRead < 0 ) {
                  break;
               }
               _bufferedToPosition += bytesRead;

            }
            catch ( IOException argh ) {
               throw new RuntimeException(argh);
            }
         }

         return true;
      }


      private final class PreloaderTask {

         private byte[] _taskBuffer;
         private int    _from;
         private int    _len;


         public PreloaderTask( byte[] buffer, int from, int len ) {
            _taskBuffer = buffer;
            _from = from;
            _len = len;
         }

      }

      private final class PreloadWorker extends Thread {

         private PreloadWorker( String name ) {
            setName(name);
            setDaemon(true);
         }

         @Override
         public void run() {
            while ( _indexWorker.get() < _allPositions.size() ) {
               int positionIndex = _indexWorker.getAndIncrement();
               if ( positionIndex >= _allPositions.size() ) {
                  break;
               }
               PreloaderTask preloaderTask = (PreloaderTask)_preloaderTasks[positionIndex];
               while ( preloaderTask == null ) {
                  _waitingForPreloaderTask.set(true);
                  notifyIterator();
                  notifyPreloader();
                  synchronized ( _waitingForPreloaderTask ) {
                     try {
                        _waitingForPreloaderTask.wait(0, (int)(Math.random() * 100000));
                     }
                     catch ( InterruptedException argh ) {
                        Thread.interrupted();
                     }
                  }
                  preloaderTask = (PreloaderTask)_preloaderTasks[positionIndex];
               }
               _waitingForPreloaderTask.set(false);
               _preloaderTasks[positionIndex] = null;

               try {
                  ObjectInput oi = _objectInputs.get();
                  _byteArrayInputs.get().setBuf(preloaderTask._taskBuffer, preloaderTask._from, preloaderTask._len);
                  E instance = (E)oi.readObject();
                  _preloadedInstances[positionIndex] = instance;

               }
               catch ( Exception argh ) {
                  LOG.error("Failed to read instance " + positionIndex + " from dump", argh);
               }
            }
            notifyIterator();
         }

         private final void notifyIterator() {
            if ( _waitingForPreloading.get() ) {
               synchronized ( _waitingForPreloading ) {
                  _waitingForPreloading.notifyAll();
               }
            }
         }

         private final void notifyPreloader() {
            if ( _waitingForCacheDepletion.get() ) {
               int cacheSize = _indexPreloadTasks.get() - _indexIterator.get();
               if ( cacheSize < LOWER_PRELOADER_CACHE_LIMIT ) {
                  synchronized ( _waitingForCacheDepletion ) {
                     _waitingForCacheDepletion.notifyAll();
                  }
               }
            }
         }
      }

   }
}
