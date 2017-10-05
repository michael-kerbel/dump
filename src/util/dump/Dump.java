package util.dump;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.list.TByteList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import util.collections.SoftLRUCache;
import util.dump.UniqueIndex.DuplicateKeyException;
import util.dump.sort.InfiniteSorter;
import util.dump.stream.Compression;
import util.dump.stream.ObjectStreamProvider;
import util.dump.stream.SingleTypeObjectStreamProvider;
import util.io.IOUtils;
import util.time.StopWatch;


/**
 * A Dump is a persistent collection of objects. The objects are persisted to a local {@link File}.<p/>
 *
 * Creating and re-opening dumps is done in the same way: by using one of the constructors.<p/>
 *
 * Dump allows add, update and remove operations, iterating with the foreach construct, sorting and
 * adding {@link DumpIndex}es for fast lookup of data.<p/>
 *
 * To optimize speed, put only {@link Externalizable} objects of a single type into the dump.
 * The simplest way to do this, is to extend {@link ExternalizableBean} with your bean, and to use the
 * constructor {@link #Dump(Class, File)}. By using the other constructors you can provide a
 * {@link ObjectStreamProvider}, like {@link util.dump.stream.JavaObjectStreamProvider}, which allows you to put arbitrary
 * {@link java.io.Serializable} objects into the dump.<p/>
 *
 * <b>Beware</b>: Always close the dump properly after usage and before exiting the JVM.<p/>
 */
@SuppressWarnings({ "WeakerAccess", "unchecked", "unused" })
public class Dump<E> implements DumpInput<E> {

   private static final Logger _log = LoggerFactory.getLogger(Dump.class);

   // TODO add pack() and a threshold based repack mechanism which prunes deletions from dump and indexes
   // TODO add registry for Iterators and close all open iterators in close()
   // TODO [MKR 22.06.2009] improve memory performance of long key indexes by using two maps: int and long.

   public static final int              DEFAULT_CACHE_SIZE               = 10000;
   public static final int              DEFAULT_SORT_MAX_ITEMS_IN_MEMORY = 10000;
   public static final DumpAccessFlag[] DEFAULT_MODE                     = EnumSet.complementOf(EnumSet.of(DumpAccessFlag.shared))
         .toArray(new DumpAccessFlag[DumpAccessFlag.values().length - 1]);
   public static final DumpAccessFlag[] SHARED_MODE                      = EnumSet.allOf(DumpAccessFlag.class)
         .toArray(new DumpAccessFlag[DumpAccessFlag.values().length]);

   /** if the number of deleted elements exceeds the value of PRUNE_THRESHOLD, the dump is pruned during construction */
   public static final int PRUNE_THRESHOLD = 25000;

   private static final Set<String> OPENED_DUMPPATHS = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
   private static final Set<Dump>   OPENED_DUMPS     = Collections.newSetFromMap(new ConcurrentHashMap<Dump, Boolean>());


   static {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         for ( Dump openedDump : new HashSet<>(OPENED_DUMPS) ) {
            if ( openedDump._willBeClosedDuringShutdown ) {
               continue;
            }
            try {
               _log.error("Dump instance " + openedDump.getDumpFile()
                  + " was not closed correctly. This is a bug in its usage and might result in data loss! Closing it now...");
               openedDump.close();
            }
            catch ( IOException e ) {
               _log.error("Failed to close dump " + openedDump.getDumpFile(), e);
            }
         }
      }));
   }


   final Class          _beanClass;
   ObjectStreamProvider _streamProvider;
   final File           _dumpFile;
   File                 _deletionsFile;
   File                 _metaFile;
   Set<DumpIndex<E>>    _indexes = new HashSet<>();

   DumpWriter<E>                 _writer;
   DumpReader<E>                 _reader;
   PositionAwareOutputStream     _outputStream;
   RandomAccessFile              _raf;
   ResettableBufferedInputStream _resettableBufferedInputStream;
   DataOutputStream              _deletionsOutput;
   protected TLongSet            _deletedPositions = new TLongHashSet();

   /** The keys are positions in the dump file and the values are the bytes of the serialized item stored there.
    * Appended to these bytes is a space efficient encoding (see <code>longToBytes(long)</code>) of the next
    * item's position. */
   Map<Long, byte[]>             _cache;
   int                           _cacheSize;
   ObjectInput                   _cacheObjectInput;
   ResettableBufferedInputStream _cacheByteInput;
   Map<Long, byte[]>             _singleItemCache = new HashMap<>();
   AtomicInteger                 _cacheLookups    = new AtomicInteger(0);
   AtomicInteger                 _cacheHits       = new AtomicInteger(0);

   ByteArrayOutputStream _updateByteOutput;
   ObjectOutput          _updateOut;
   RandomAccessFile      _updateRaf;
   long                  _updateRafPosition;

   boolean _isClosed;

   ThreadLocal<Long> _nextItemPos = new LongThreadLocal();
   ThreadLocal<Long> _lastItemPos = new LongThreadLocal();

   /** incremented on each write operation */
   long _sequence = (long)(Math.random() * 1000000);

   final EnumSet<DumpAccessFlag> _mode;

   RandomAccessFile _metaRaf;
   FileLock         _dumpLock;

   boolean _willBeClosedDuringShutdown = false;

   String _instantiationDetails;


   /**
    * Constructs a new Dump with <code>beanClass</code> as instance class. If the dump already exists, it will be re-opened.<p/>
    * A {@link SingleTypeObjectStreamProvider} using <code>beanClass</code> is created for your convenience. This implies, that you can only
    * add objects of the exact type <code>beanClass</code>, i.e. successors of <code>beanClass</code> are not allowed. <p/>
    * A {@link SoftLRUCache} is used for caching with <code>DEFAULT_CACHE_SIZE</code> as size. <p/>
    * @param beanClass must implement {@link Externalizable} otherwise the {@link SingleTypeObjectStreamProvider} that is used will fail during runtime
    * @param dumpFile the dump file
    */
   public Dump( Class beanClass, File dumpFile ) {
      this(beanClass, new SingleTypeObjectStreamProvider(beanClass), dumpFile, DEFAULT_CACHE_SIZE, DEFAULT_MODE);
   }

   /**
    * same as {@link #Dump(Class, File)} but allows to set the compression algorithm to use.
    * @param compression the compression to use for the SingleTypeObjectStreamProvider, i.e. each bean is stored compressed using this algorithm  
    */
   public Dump( Class beanClass, File dumpFile, Compression compression ) {
      this(beanClass, new SingleTypeObjectStreamProvider(beanClass, compression), dumpFile, DEFAULT_CACHE_SIZE, DEFAULT_MODE);
   }

   /**
    * same as {@link #Dump(Class, File)} but allows to set the access modes.
    *
    * @see DumpAccessFlag
    */
   public Dump( Class beanClass, File dumpFile, DumpAccessFlag... mode ) {
      this(beanClass, new SingleTypeObjectStreamProvider(beanClass), dumpFile, DEFAULT_CACHE_SIZE, mode);
   }

   /**
    * Constructs a new Dump with <code>beanClass</code> as instance class. If the dump already exists, it will be re-opened.<p/>
    * A {@link SoftLRUCache} is used for caching with <code>cacheSize</code> as size. <p/>
    * @param cacheSize may only be greater 0 if you use a {@link SingleTypeObjectStreamProvider} as <code>streamProvider</code>.
    */
   public Dump( Class beanClass, ObjectStreamProvider streamProvider, File dumpFile, int cacheSize, @Nullable DumpAccessFlag... mode ) {
      _beanClass = beanClass;
      _streamProvider = streamProvider;
      _mode = EnumSet.copyOf(Arrays.asList(mode == null || mode.length == 0 ? DEFAULT_MODE : mode));
      _dumpFile = IOUtils.getCanonicalFileQuietly(dumpFile);
      _deletionsFile = new File(dumpFile.getPath() + ".deletions");
      _metaFile = new File(dumpFile.getPath() + ".meta");
      initInstantiationData();
      if ( OPENED_DUMPPATHS.contains(_dumpFile.getPath()) ) {
         String instantiationDetails = "";
         for ( Dump d : OPENED_DUMPS ) {
            if ( d.getDumpFile().equals(dumpFile) ) {
               instantiationDetails = d._instantiationDetails;
            }
         }
         throw new IllegalStateException("There is already an opened Dump instance for file " + _dumpFile
            + ". Having two instances is hazardous. Instantiation details of the older Dump:\n" + instantiationDetails);
      }
      OPENED_DUMPPATHS.add(_dumpFile.getPath());
      OPENED_DUMPS.add(this);
      _cacheSize = cacheSize;
      try {
         if ( !isReadonly() && !_mode.contains(DumpAccessFlag.shared) ) {
            // the lock is released as soon as the RandomAccessFile is closed (stackoverflow.com/questions/421833)
            acquireFileLock();
         }

         readDeletions();

         if ( shouldBePruned() ) {
            StopWatch t = new StopWatch();
            _log.info("need to prune {} deleted entries from {}", _deletedPositions.size(), _dumpFile);
            prune();
            _log.info("...pruned {} in {}", _dumpFile, t);
         }

         FileOutputStream fileOutputStream = new FileOutputStream(_dumpFile, true);
         BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream, DumpWriter.DEFAULT_BUFFER_SIZE);
         _outputStream = new PositionAwareOutputStream(bufferedOutputStream, _dumpFile.length());
         _writer = new DumpWriter<>(_outputStream, 0, _streamProvider);
         _raf = new RandomAccessFile(_dumpFile, "r");
         _updateRaf = new RandomAccessFile(_dumpFile, "rw");
         _reader = new DumpReader<>(_dumpFile, false, _streamProvider);

         initMeta();

         _updateByteOutput = new ByteArrayOutputStream(1024);
         _updateOut = _streamProvider.createObjectOutput(_updateByteOutput);
      }
      catch ( Exception argh ) {
         try {
            close();
         }
         catch ( IOException arghargh ) {
            _log.warn("Failed to close dump after failure in initialization.", arghargh);
         }
         throw new RuntimeException("Failed to initialize dump using dump file " + _dumpFile, argh);
      }
      if ( cacheSize > 0 ) {
         if ( !(streamProvider instanceof SingleTypeObjectStreamProvider) ) {
            throw new IllegalArgumentException("cacheSize may not be greater 0 when not using SingleTypeObjectStreamProvider.");
         }
         _cache = new SoftLRUCache(cacheSize); // no synchronization needed, since get(.) is synchronized
         _cacheByteInput = new ResettableBufferedInputStream((FileChannel)null, 0, false);
         try {
            _cacheObjectInput = streamProvider.createObjectInput(_cacheByteInput);
         }
         catch ( IOException argh ) {
            // ignore, cannot happen
         }
      }
   }

   /**
    * Appends the element o to the end of this Dump and its {@link DumpIndex}es.
    * @param o the element to add, may not be null
    */
   public void add( E o ) throws IOException {
      synchronized ( this ) {
         if ( !_mode.contains(DumpAccessFlag.add) ) {
            throw new AccessControlException("Add operation not allowed with current modes.");
         }
         assertOpen();
         for ( DumpIndex<E> index : _indexes ) {
            if ( index instanceof UniqueIndex && index.contains(((UniqueIndex)index).getKey(o))
               && !index.getIndexType().equals(GroupedIndex.class.getSimpleName()) ) {
               // check this before actually adding anything
               throw new DuplicateKeyException("Dump already contains an instance with the key " + ((UniqueIndex)index).getKey(o));
            }
         }
         long pos = _outputStream._n;
         _writer.write(o);
         _sequence++;
         for ( DumpIndex<E> index : _indexes ) {
            index.add(o, pos);
         }
      }
   }

   /**
    * Appends all of the elements in the specified Iterable to the end of this Dump, in the order that they are returned by the specified Iterable.
    * @param i the Iterable containing the elements to add, may not be null
    */
   public void addAll( Iterable<E> i ) throws IOException {
      for ( E e : i ) {
         add(e);
      }
   }

   /**
    * clear the accumulated hit rate of the cache
    * @see Dump#getCacheHitRate()
    */
   public void clearCacheHitRate() {
      _cacheLookups.set(0);
      _cacheHits.set(0);
   }

   /**
    * Closing a Dump is necessary in order to flush all corresponding streams. All registered {@link DumpIndex}es are closed as well.
    * No operations are allowed on a closed Dump instance.<p/>
    * <b>Failing to close the dump may result in data loss!</b>
    */
   @Override
   public void close() throws IOException {
      if ( _isClosed ) {
         return;
      }
      if ( _writer != null ) {
         _writer.flush();
         _writer.close();
      }
      if ( _raf != null ) {
         _raf.close();
      }
      // _resettableBufferedInputStream doesn't have to be closed, since it only closes the _raf
      if ( _reader != null ) {
         _reader.close();
      }
      if ( _deletionsOutput != null ) {
         _deletionsOutput.close();
         _deletionsOutput = null;
      }
      releaseFileLock();
      if ( _metaRaf != null ) {
         _metaRaf.close();
         _metaRaf = null;
      }
      if ( _updateOut != null ) {
         _updateOut.close();
      }
      if ( _updateRaf != null ) {
         _updateRaf.close();
      }
      writeMeta();
      if ( _metaRaf != null ) {
         _metaRaf.close();
         _metaRaf = null;
      }
      for ( DumpIndex index : new ArrayList<>(_indexes) ) {
         index.close();
      }
      OPENED_DUMPPATHS.remove(_dumpFile.getPath());
      OPENED_DUMPS.remove(this);
      _isClosed = true;
   }

   /**
    * Removes the element located at position <code>pos</code> from this dump.
    * Usually you use {@link Dump#deleteLast()} after iteration or index lookup of the element you want to delete.
    * But with {@link DumpIterator#getPosition()} or {@link DumpIndex#getAllPositions()} you could get access to the file position of
    * any element and thus use this method.
    * @return the deleted object
    * @throws RuntimeException if there is no element at <code>pos</code>
    */
   public E delete( long pos ) {
      if ( !_mode.contains(DumpAccessFlag.delete) ) {
         throw new AccessControlException("Delete operation not allowed with current modes.");
      }

      synchronized ( this ) {

         assertOpen();

         E e = get(pos);
         if ( e == null ) {
            throw new RuntimeException("Failed to delete item on position " + pos + ". There was no instance on that position - maybe it was already deleted?");
         }

         _deletedPositions.add(pos);
         if ( _cache != null ) {
            _cache.remove(pos);
         }

         try {
            // lazy open/create deletions file
            if ( _deletionsOutput == null ) {
               _deletionsOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_deletionsFile, true), DumpWriter.DEFAULT_BUFFER_SIZE));
            }
            _deletionsOutput.writeLong(pos);
         }
         catch ( IOException argh ) {
            throw new RuntimeException("Failed to add deletion to " + _deletionsFile, argh);
         }

         _sequence++;

         for ( DumpIndex index : _indexes ) {
            index.delete(e, pos);
         }

         return e;
      }
   }

   /**
    * Deletes the item which was acquired lastly from this dump instance by the current thread.
    * It doesn't play a role, in which way the last access to the dump took place: Whether via get(.),
    * iteration or index access. An exception is thrown, if the current thread did not acquire
    * anything from this dump yet.<p/>
    * <b>Beware</b>: If you do an unsuccessful index lookup (i.e. an index lookup which returns null),
    * there is no dump access. Calling deleteLast() after such an access would delete the "wrong"
    * item - the last one you successfully acquired.
    * @return the deleted object
    */
   public E deleteLast() {
      return delete(_lastItemPos.get());
   }

   /**
    * Flush any bytes in the buffer, just in case - this is cheap, if the buffer is empty.
    * If you want to ensure, that the dump files on disk are in a valid state without closing the dump, 
    * call {@link #flushMeta()} too. Indexes are also flushed.
    */
   public void flush() throws IOException {
      _outputStream.flush();
      for ( DumpIndex index : new ArrayList<>(_indexes) ) {
         index.flush();
      }
   }

   /**
    * Flushes deletions to disk and writes meta file. By calling this and {@link #flush()}, 
    * you can ensure a valid state on disk, without closing the dump. Of course this costs IO.
    * Indes metas are also flushed.
    */
   public void flushMeta() throws IOException {
      if ( _deletionsOutput != null ) {
         _deletionsOutput.flush();
      }
      writeMeta();
      for ( DumpIndex index : new ArrayList<>(_indexes) ) {
         index.flushMeta();
      }
   }

   /**
    * Retrieves the element located at position <code>pos</code>.
    * Changes to the returned instance are not reflected in the dump. You always get a fresh instance
    * when invoking this method. This method needs an IO operation, unless you specified a cache size greater 0 in the
    * constructor and the element at pos was retrieved recently.
    */
   @SuppressWarnings("UnnecessaryBoxing")
   public @Nullable E get( long pos ) {
      if ( !_mode.contains(DumpAccessFlag.read) ) {
         throw new AccessControlException("Get operation not allowed with current modes.");
      }

      synchronized ( this ) {

         assertOpen();

         boolean positionIsDeleted = _deletedPositions.contains(pos);
         if ( _cache != null ) {
            _cacheLookups.incrementAndGet();
            byte[] bytes = _cache.get(Long.valueOf(pos)); // ugly boxing of pos necessary here, since _cache is a regular Map - the cost of beauty is ugliness
            if ( bytes != null ) {
               _cacheHits.incrementAndGet();
               _lastItemPos.set(pos);
               _nextItemPos.set(getNextItemPos(bytes));
               if ( positionIsDeleted ) {
                  return null;
               }
               return readFromBytes(bytes);
            }
         }

         try {
            flush();

            if ( _resettableBufferedInputStream == null || _resettableBufferedInputStream._rafPos != pos ) {
               // only seek if we don't do sequential reads
               _raf.seek(pos);

               if ( _resettableBufferedInputStream == null ) {
                  _resettableBufferedInputStream = new ResettableBufferedInputStream(_raf.getChannel(), pos, true);
                  _resettableBufferedInputStream._lastElementBytes = new byte[1024];
                  _reader.reset(_resettableBufferedInputStream, 0, _streamProvider);
               } else {
                  _resettableBufferedInputStream.reset(_raf.getChannel(), pos);
               }
            }

            if ( _reader.hasNext() ) {
               E value = _reader.next();
               _nextItemPos.set(_resettableBufferedInputStream._rafPos);
               if ( _cache != null ) {
                  // we don't cache E instances to prevent the user from changing the cached instances
                  byte[] nextItemPos = longToBytes(_nextItemPos.get());
                  byte[] lastElementBytes = new byte[_resettableBufferedInputStream._lastElementBytesLength + nextItemPos.length];
                  System.arraycopy(_resettableBufferedInputStream._lastElementBytes, 0, lastElementBytes, 0,
                     _resettableBufferedInputStream._lastElementBytesLength);
                  appendNextItemPos(lastElementBytes, nextItemPos);
                  _cache.put(Long.valueOf(pos), lastElementBytes); // ugly boxing of pos necessary here, since _cache is a regular Map - the cost of beauty is ugliness
               }
               if ( _resettableBufferedInputStream._lastElementBytes.length > 64 * 1024 ) {
                  _resettableBufferedInputStream._lastElementBytes = new byte[1024];
               }
               _resettableBufferedInputStream._lastElementBytesLength = 0;
               _lastItemPos.set(pos);
               if ( positionIsDeleted ) {
                  return null;
               }
               return value;
            } else {
               // reset reader state if EOF
               _reader.next();
            }

            return null;
         }
         catch ( IOException argh ) {
            throw new RuntimeException("Failed to read from dump " + _dumpFile + " at position " + pos, argh);
         }
      }
   }

   /** 
    * @return the accumulated hit rate of the cache, value between 0 and 1, 0 if none used
    * @see Dump#clearCacheHitRate() 
    */
   public float getCacheHitRate() {
      int lookups = _cacheLookups.get();
      if ( lookups <= 0 ) {
         return 0f;
      }
      return _cacheHits.get() / (float)lookups;
   }

   /**
    * @return the absolute file where this Dump is persisted to.
    */
   public File getDumpFile() {
      return _dumpFile;
   }

   /**
    * Always returns the number of bytes contained in the dump, even if a flush operation is still missing
    * and the file does not contain all data yet. This method has no IO cost.
    * @return The current dump size in bytes.
    */
   public long getDumpSize() {
      return _outputStream._n;
   }

   public EnumSet<DumpAccessFlag> getMode() {
      return EnumSet.copyOf(_mode);
   }

   /**
    * @return the streamProvider specified during construction and used for creating serialization streams
    */
   public ObjectStreamProvider getStreamProvider() {
      return _streamProvider;
   }

   public boolean isClosed() {
      return _isClosed;
   }

   /**
    * Yields a DumpIterator with all (undeleted) elements in this dump.
    */
   @SuppressWarnings("resource")
   @Override
   public @Nonnull DumpIterator<E> iterator() {
      assertOpen();
      try {
         flush();
         return new DeletionAwareDumpReader(_dumpFile, _streamProvider).iterator();
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to create a DumpReader.", argh);
      }
   }

   public MultithreadedDumpReader<E> multithreadedIterator( DumpIndex index ) {
      try {
         return new MultithreadedDumpReader<>(this, index);
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to create a DumpReader.", argh);
      }
   }

   /**
    * Setter for the cache to use. If you are unhappy with the default SoftLRUCache (why should you!?), you can specify your own here.
    */
   public void setCache( @Nullable Map<Long, byte[]> cache ) {
      _cache = cache;
   }

   public void setWillBeClosedDuringShutdown( boolean willBeClosedDuringShutdown ) {
      _willBeClosedDuringShutdown = willBeClosedDuringShutdown;
   }

   /**
    * Sorts all elements in this Dump using the natural order of your Bean and {@link Dump#DEFAULT_SORT_MAX_ITEMS_IN_MEMORY} as parameter
    * for the underlying {@link InfiniteSorter}. <p/>
    * You use this for easily sorting and iterating all elements in this Dump:
    * <pre> for(E e: dump.sort()){
    *     // do something with e
    * }
    * </pre>
    * @throws Exception if your bean is not Comparable or sorting fails
    */
   public InfiniteSorter<E> sort() throws Exception {
      return sort(null, DEFAULT_SORT_MAX_ITEMS_IN_MEMORY);
   }

   /**
    * Sorts all elements in this Dump.
    * @param comparator If null, E must be Comparable and its natural order will be used
    * @param maxItemsInMemory The maximal number of items held in memory
    */
   public InfiniteSorter<E> sort( @Nullable Comparator<E> comparator, int maxItemsInMemory ) throws Exception {
      assertOpen();
      InfiniteSorter<E> sorter = new InfiniteSorter<>(maxItemsInMemory, _dumpFile.getParentFile());
      if ( comparator != null ) {
         sorter.setComparator(comparator);
      }
      if ( Externalizable.class.isAssignableFrom(_beanClass) ) {
         sorter.setObjectStreamProvider(new SingleTypeObjectStreamProvider(_beanClass));
      }
      sorter.addAll(this);
      return sorter;
   }

   /**
    * Replaces the element at position <code>pos</code> with <code>newItem</code>.<p/>
    *
    * Usually you use {@link Dump#updateLast(Object)} after iteration or index lookup of the element you want to update.
    * But with {@link DumpIterator#getPosition()} or {@link DumpIndex#getAllPositions()} you could get access to the file position of
    * any element and thus use this method. <p/>
    *
    * <b>BEWARE</b>: After updates an item might be situated at the end of the dump.
    * In other words: The sort order is not stable. <p/>
    *
    * If the length of the byte representation of the item changes, the old version of the item will be
    * deleted and the new one will be appended to the end.<p/>
    *
    * @return the old version of the updated element
    */
   public E update( long pos, E newItem ) throws IOException {
      synchronized ( this ) {

         assertOpen();

         try {
            if ( _cache == null ) {
               _singleItemCache.clear();
               _cache = _singleItemCache;
            }
            E oldItem = get(pos);
            if ( oldItem == null ) {
               throw new RuntimeException(
                  "Failed to delete item on position " + pos + ". There was no instance on that position - maybe it was already deleted?");
            }
            byte[] oldBytes = _cache.get(pos);

            _updateByteOutput.reset();
            _updateOut.writeObject(newItem);
            byte[] nb = _updateByteOutput.toByteArray();
            int suffixLength = oldBytes[oldBytes.length - 1] + 1;

            if ( oldBytes.length == nb.length + suffixLength ) {
               byte[] newBytes = new byte[nb.length + suffixLength];
               System.arraycopy(nb, 0, newBytes, 0, nb.length);
               System.arraycopy(oldBytes, nb.length, newBytes, nb.length, suffixLength);

               boolean indexesUpdatable = true;
               for ( DumpIndex index : _indexes ) {
                  indexesUpdatable &= index.isUpdatable(oldItem, newItem);
               }

               if ( indexesUpdatable ) {
                  if ( !_mode.contains(DumpAccessFlag.updateInPlace) ) {
                     throw new AccessControlException("Update in place operation not allowed with current modes.");
                  }
                  overwrite(pos, nb);
                  for ( DumpIndex index : _indexes ) {
                     index.update(pos, oldItem, newItem);
                  }
                  if ( _cache != null ) {
                     _cache.put(pos, newBytes);
                  }
                  return oldItem;
               }
            }
         }
         finally {
            if ( _cache == _singleItemCache ) {
               _cache = null;
            }
         }

         // overwriting was not possible -> delete old version and add new version
         if ( !_mode.contains(DumpAccessFlag.updateOutOfPlace) ) {
            throw new AccessControlException("Update out of place operation not allowed with current modes.");
         }

         E old = delete(pos);
         add(newItem);

         _sequence++;
         return old;
      }
   }

   /**
    * Batch update of all elements that are provided as argument.
    *
    * <b>Note:</b> this method is only efficient if the positions of the elements are consecutive
    *
    * @see #update(long, Object)
    */
   public void updateAll( Iterable<ElementAndPosition<E>> elements ) throws IOException {
      synchronized ( this ) {
         for ( ElementAndPosition<E> elementAndPosition : elements ) {
            update(elementAndPosition.getPosition(), elementAndPosition.getElement());
         }
      }
   }

   /**
    * Replaces the element which was acquired lastly from this dump instance by the current thread with <code>item</code>.<p/>
    *
    * It doesn't play a role, in which way the last access to the dump took place: Whether via get(.),
    * iteration or index access. An exception is thrown, if the current thread did not acquire
    * anything from this dump yet.<p/>
    *
    * <b>BEWARE</b>: If you do an unsuccessful index lookup (i.e. an index lookup which returns null),
    * there is no dump access. Calling updateLast(.) after such an access would update the "wrong"
    * item - the last one you successfully acquired.<p/>
    *
    * <b>BEWARE</b>: After updates an item might be situated at the end of the dump.
    * In other words: The sort order is not stable. <p/>
    *
    * If the length of the byte representation of the item changes, the old version of the item will be
    * deleted and the new one will be appended to the end.
    *
    * @return the old version of the updated object
    */
   public E updateLast( E item ) throws IOException {
      return update(_lastItemPos.get(), item);
   }

   /**
    * @return false if the dump was already locked
    */
   protected boolean acquireFileLock() {
      synchronized ( this ) {
         try {
            if ( _dumpLock != null ) {
               return false;
            }

            if ( !_metaFile.exists() || _metaFile.length() < 8 ) {
               writeMeta();
            }

            _dumpLock = getMetaRAF().getChannel().tryLock();
            if ( _dumpLock == null ) {
               _log.info("meta file " + _metaFile + " is already locked. waiting...");
               StopWatch time = new StopWatch();
               _dumpLock = _metaRaf.getChannel().lock();
               _log.info("got the lock on " + _metaFile + " after " + time);
            }
            return true;
         }
         catch ( Exception argh ) {
            throw new RuntimeException("unable to lock meta file " + _metaFile, argh);
         }
      }
   }

   protected void assertOpen() {
      if ( _isClosed ) {
         throw new RuntimeException("This dump instance (" + getDumpFile() + ") is already closed and cannot be used.");
      }
   }

   @Override
   protected void finalize() throws Throwable {
      if ( !_isClosed ) {
         _log.error("Dump instance " + getDumpFile() + " was not closed correctly. This is a bug in its usage and might result in data loss!");
      }
      close();
      super.finalize();
   }

   protected void prune() throws IOException {
      File prunedDumpFile = new File(_dumpFile.getAbsolutePath() + ".pruned");
      try {
         prunedDumpFile.deleteOnExit();
         DeletionAwareDumpReader input = new DeletionAwareDumpReader(_dumpFile, _streamProvider, _dumpFile.length());
         DumpWriter<E> out = new DumpWriter<>(prunedDumpFile, _streamProvider);
         out.writeAll(input);
         input.close();
         out.close();

         File dumpFileWithDeletions = new File(_dumpFile.getAbsolutePath() + ".withDeletions");
         dumpFileWithDeletions.delete();
         boolean success = _dumpFile.renameTo(dumpFileWithDeletions);
         if ( !success ) {
            throw new IOException("Failed to rename current dump " + _dumpFile + " to " + dumpFileWithDeletions);
         }

         success = prunedDumpFile.renameTo(_dumpFile);
         if ( !success ) {
            _dumpFile.delete();
            dumpFileWithDeletions.renameTo(_dumpFile);
            throw new IOException("Failed to rename pruned dump " + prunedDumpFile + " to " + _dumpFile);
         }

         dumpFileWithDeletions.delete();
         _deletionsFile.delete();
         _deletedPositions.clear();

         // force re-initialization of indexes!
         releaseFileLock();
         initMeta();
         _sequence++;
         writeMeta();
         acquireFileLock();
      }
      finally {
         prunedDumpFile.delete();
      }
   }

   /**
    * @return false if the dump wasn't locked
    */
   protected boolean releaseFileLock() {
      synchronized ( this ) {
         try {
            if ( _dumpLock == null ) {
               return false;
            }
            _dumpLock.release();
            _dumpLock = null;
            return true;
         }
         catch ( Exception argh ) {
            throw new RuntimeException("unable to unlock dump file " + _metaFile, argh);
         }
      }
   }

   protected boolean shouldBePruned() {
      return _deletedPositions.size() > PRUNE_THRESHOLD;
   }

   void addIndex( DumpIndex index ) {
      if ( !_mode.contains(DumpAccessFlag.indices) ) {
         throw new AccessControlException("Using indices is not allowed with current modes.");
      }

      assertOpen();
      _indexes.add(index);
   }

   void initMeta() throws IOException {
      if ( _metaFile.exists() && _metaFile.length() >= 8 ) {
         getMetaRAF().seek(0);
         _sequence = getMetaRAF().readLong();
      }
   }

   void overwrite( long pos, byte[] newBytes ) {
      try {
         if ( pos != _updateRafPosition ) {
            _updateRaf.seek(pos);
            _updateRafPosition = pos;
         }
         _updateRaf.write(newBytes);
         _updateRafPosition += newBytes.length;
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to update dump.", argh);
      }
   }

   void readDeletions() throws IOException {
      if ( _deletionsFile.exists() ) {
         if ( _deletionsFile.length() % 8 != 0 ) {
            throw new RuntimeException("Dump corrupted: " + _deletionsFile + " has unbalanced size.");
         }

         long dumpFileLength = _dumpFile.length();
         DataInputStream dataInputStream = null;
         try {
            dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(_deletionsFile), DumpReader.DEFAULT_BUFFER_SIZE));
            while ( true ) { // read until EOF
               long pos = dataInputStream.readLong();
               if ( pos < 0 || pos >= dumpFileLength ) {
                  throw new RuntimeException("Dump corrupted: " + _deletionsFile + " contains illegal data.");
               }
               _deletedPositions.add(pos);
            }
         }
         catch ( EOFException argh ) {
            // ignore
         }
         finally {
            if ( dataInputStream != null ) {
               dataInputStream.close();
            }
         }
      }
   }

   E readFromBytes( byte[] bytes ) {
      try {
         _cacheByteInput.buf = bytes;
         _cacheByteInput.count = bytes.length;
         _cacheByteInput.pos = 0;
         return (E)_cacheObjectInput.readObject();
      }
      catch ( Exception argh ) {
         if ( _cache != null ) {
            _cache.clear();
         }
         throw new RuntimeException("Failed to read from internal cache", argh);
      }
   }

   void removeIndex( DumpIndex index ) {
      _indexes.remove(index);
   }

   void writeMeta() throws IOException {
      getMetaRAF().seek(0);
      getMetaRAF().writeLong(_sequence);
   }

   private void appendNextItemPos( byte[] bytes, byte[] nextItemPos ) {
      int l = bytes.length;
      for ( int i = 1, length = nextItemPos.length; i <= length; i++ ) {
         bytes[l - i] = nextItemPos[length - i];
      }
   }

   private RandomAccessFile getMetaRAF() throws FileNotFoundException {
      synchronized ( this ) {
         if ( _metaRaf == null ) {
            _metaRaf = new RandomAccessFile(_metaFile, "rw");
         }
         return _metaRaf;
      }
   }

   private long getNextItemPos( byte[] bytes ) {
      byte n = bytes[bytes.length - 1];
      if ( n > 9 || n < 0 ) {
         throw new RuntimeException("invalid cache data.");
      }
      long l = 0L;
      for ( int i = bytes.length - n - 1, b = bytes.length - 1, j = 0; i < b; i++, j++ ) {
         l |= (bytes[i] & 0xff) << (j << 3);
      }
      return l;
   }

   private void initInstantiationData() {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      new Exception().printStackTrace(pw);
      _instantiationDetails = "instantiation time: " + new Date() + "\n" + "instantiation stack: " + sw;
   }

   private boolean isReadonly() {
      // are there any modes except read/indices
      EnumSet<DumpAccessFlag> writeModes = EnumSet.complementOf(EnumSet.of(DumpAccessFlag.read, DumpAccessFlag.indices));
      for ( DumpAccessFlag writeMode : writeModes ) {
         if ( _mode.contains(writeMode) ) {
            return false;
         }
      }
      return true;
   }

   private byte[] longToBytes( long l ) {
      // BEWARE Doesn't work with negative longs, which we don't need in this class. Would be easy to extend though.
      // TODO reuse bytes
      // TODO use a reusable byte[] instead of TByteArrayList
      TByteList bytes = new TByteArrayList(9);
      byte n = 0;
      while ( l != 0L ) {
         bytes.add((byte)l);
         l = l >>> 8;
         n++;
      }
      bytes.add(n);
      return bytes.toArray();
   }


   /**
    * This enum is used to control the access mode of a Dump instance.
    */
   public enum DumpAccessFlag {
      /** append new beans <b>at the end</b> of the dump  */
      add, //
      /** allow beans to be updated in case where the bean <b>length stays the same</b> */
      updateInPlace, //
      /** in contrast to {@link #updateInPlace} allow updates where the bean needs to be completely rewritten (ie. delete followed by an add) */
      updateOutOfPlace, //
      /** allow beans to be deleted from the dump */
      delete, //
      /** allow indices */
      indices, //
      /** get, iterate or sort elements */
      read, //
      /**
       * the dump is locked if it wasn't opened in read-only mode (see {@link Dump#isReadonly()})
       * this flag prevents this locking and enables shared access. But use it with care as shared write accesses are also allowed
       */
      shared, //
   }

   /**
    * the class implements a comparator to order the objects by their positions (ascending) in order to provide fast batch updates
    * @see Dump#updateAll(Iterable)
    */
   public static final class ElementAndPosition<D> implements Comparable<ElementAndPosition<D>> {

      private final D    _element;
      private final long _position;


      public ElementAndPosition( D element, long position ) {
         _element = element;
         _position = position;
      }

      @Override
      public int compareTo( @Nonnull ElementAndPosition<D> o ) {
         return Long.compare(_position, o._position);
      }

      public D getElement() {
         return _element;
      }

      public long getPosition() {
         return _position;
      }
   }

   public static abstract class PositionIteratorCallback<E> {

      public abstract void element( E o, long pos );
   }

   static class ResettableBufferedInputStream extends InputStream {

      private static int defaultBufferSize = 256 * 1024; // this is enough, see http://nadeausoftware.com/articles/2008/02/java_tip_how_read_files_quickly

      /**
       * The internal buffer array where the data is stored. When necessary,
       * it may be replaced by another array of
       * a different size.
       */
      protected volatile byte buf[];

      /**
       * The index one greater than the index of the last valid byte in
       * the buffer.
       * This value is always
       * in the range <code>0</code> through <code>buf.length</code>;
       * elements <code>buf[0]</code>  through <code>buf[count-1]
       * </code>contain buffered input data obtained
       * from the underlying  input stream.
       */
      protected int count;

      /**
       * The current position in the buffer. This is the index of the next
       * character to be read from the <code>buf</code> array.
       * <p>
       * This value is always in the range <code>0</code>
       * through <code>count</code>. If it is less
       * than <code>count</code>, then  <code>buf[pos]</code>
       * is the next byte to be supplied as input;
       * if it is equal to <code>count</code>, then
       * the  next <code>read</code> or <code>skip</code>
       * operation will require more bytes to be
       * read from the contained  input stream.
       *
       * @see     java.io.BufferedInputStream#buf
       */
      protected int pos;

      long _rafPos;

      byte[] _lastElementBytes       = null;
      int    _lastElementBytesLength = 0;

      boolean _suppressClose = false;

      ByteBuffer  _bb;
      FileChannel _ch;

      FileInputStream _fileInputStream;


      /**
       * Creates a <code>BufferedInputStream</code>
       * with the specified buffer size,
       * and saves its  argument, the input stream
       * <code>in</code>, for later use.  An internal
       * buffer array of length  <code>size</code>
       * is created and stored in <code>buf</code>.
       *
       * @param   ch     the underlying file channel
       * @param   size   the buffer size.
       * @exception IllegalArgumentException if size <= 0.
       */
      public ResettableBufferedInputStream( @Nullable FileChannel ch, int size, long rafPos, boolean suppressClose ) {
         init(ch, size, rafPos, suppressClose);
      }

      /**
       * Creates a <code>BufferedInputStream</code>
       * and saves its  argument, the input stream
       * <code>in</code>, for later use. An internal
       * buffer array is created and  stored in <code>buf</code>.
       *
       * @param   ch   the underlying file channel.
       */
      public ResettableBufferedInputStream( @Nullable FileChannel ch, long rafPos, boolean suppressClose ) {
         this(ch, defaultBufferSize, rafPos, suppressClose);
      }

      public ResettableBufferedInputStream( FileInputStream fileInputStream, long rafPos, boolean suppressClose ) {
         _fileInputStream = fileInputStream;
         init(_fileInputStream.getChannel(), defaultBufferSize, rafPos, suppressClose);
      }

      /**
       * Closes this input stream and releases any system resources
       * associated with the stream.
       * Once the stream has been closed, further read(), available(), reset(),
       * or skip() invocations will throw an IOException.
       * Closing a previously closed stream has no effect.
       *
       * @exception  IOException  if an I/O error occurs.
       */
      @Override
      public void close() throws IOException {
         if ( _lastElementBytes.length > 64 * 1024 ) {
            _lastElementBytes = new byte[1024];
         }
         _lastElementBytesLength = 0;

         if ( _suppressClose ) {
            return;
         }

         buf = null;
         _bb = null;

         FileChannel input = _ch;
         _ch = null;
         if ( input != null ) {
            input.close();
         }

         if ( _fileInputStream != null ) {
            _fileInputStream.close();
         }
      }

      /**
       * Tests if this input stream supports the <code>mark</code>
       * and <code>reset</code> methods. The <code>markSupported</code>
       * method of <code>BufferedInputStream</code> returns
       * <code>true</code>.
       *
       * @return  a <code>boolean</code> indicating if this stream type supports
       *          the <code>mark</code> and <code>reset</code> methods.
       * @see     java.io.InputStream#mark(int)
       * @see     java.io.InputStream#reset()
       */
      @Override
      public boolean markSupported() {
         return true;
      }

      /**
       * See
       * the general contract of the <code>read</code>
       * method of <code>InputStream</code>.
       *
       * @return     the next byte of data, or <code>-1</code> if the end of the
       *             stream is reached.
       * @exception  IOException  if this input stream has been closed by
       *              invoking its {@link #close()} method,
       *              or an I/O error occurs.
       * @see        java.io.FilterInputStream#in
       */
      @Override
      public/*synchronized*/int read() throws IOException {
         // we don't share instances of this class or synchronize access on a different level, so this method is not synchronized
         if ( pos >= count ) {
            fill();
            if ( pos >= count ) {
               return -1;
            }
         }
         _rafPos++;
         int b = getBufIfOpen()[pos++] & 0xff;
         if ( b >= 0 && _lastElementBytes != null ) {
            if ( _lastElementBytesLength + 1 >= _lastElementBytes.length ) {
               growLastElementBytes(1);
            }
            _lastElementBytes[_lastElementBytesLength] = (byte)b;
            _lastElementBytesLength++;
         }
         return b;
      }

      /**
       * Reads bytes from this byte-input stream into the specified byte array,
       * starting at the given offset.
       *
       * <p> This method implements the general contract of the corresponding
       * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
       * the <code>{@link InputStream}</code> class.  As an additional
       * convenience, it attempts to read as many bytes as possible by repeatedly
       * invoking the <code>read</code> method of the underlying stream.  This
       * iterated <code>read</code> continues until one of the following
       * conditions becomes true: <ul>
       *
       *   <li> The specified number of bytes have been read,
       *
       *   <li> The <code>read</code> method of the underlying stream returns
       *   <code>-1</code>, indicating end-of-file, or
       *
       *   <li> The <code>available</code> method of the underlying stream
       *   returns zero, indicating that further input requests would block.
       *
       * </ul> If the first <code>read</code> on the underlying stream returns
       * <code>-1</code> to indicate end-of-file then this method returns
       * <code>-1</code>.  Otherwise this method returns the number of bytes
       * actually read.
       *
       * <p> Subclasses of this class are encouraged, but not required, to
       * attempt to read as many bytes as possible in the same fashion.
       *
       * @param      b     destination buffer.
       * @param      off   offset at which to start storing bytes.
       * @param      len   maximum number of bytes to read.
       * @return     the number of bytes read, or <code>-1</code> if the end of
       *             the stream has been reached.
       * @exception  IOException  if this input stream has been closed by
       *              invoking its {@link #close()} method,
       *              or an I/O error occurs.
       */
      @Override
      public/*synchronized*/int read( @Nullable byte b[], int off, int len ) throws IOException {
         // we don't share instances of this class or synchronize access on a different level, so this method is not synchronized
         getBufIfOpen(); // Check for closed stream
         if ( (off | len | (off + len) | (b.length - (off + len))) < 0 ) {
            throw new IndexOutOfBoundsException();
         } else if ( len == 0 ) {
            return 0;
         }

         int n = read0(b, off, len);
         if ( n > 0 && _lastElementBytes != null ) {
            if ( _lastElementBytesLength + len >= _lastElementBytes.length ) {
               growLastElementBytes(len);
            }
            System.arraycopy(b, off, _lastElementBytes, _lastElementBytesLength, n);
            _lastElementBytesLength += n;
         }
         return n;
      }

      public synchronized void reset( FileChannel ch, long rafPos ) throws IOException {
         getBufIfOpen(); // Cause exception if closed
         if ( _ch != ch ) {
            if ( !_suppressClose ) {
               _ch.close();
            }
            _ch = ch;
         }
         pos = 0;
         count = 0;
         _rafPos = rafPos;
      }

      int read0( byte b[], int off, int len ) throws IOException {
         int n = 0;
         for ( ;; ) {
            int nread = read1(b, off + n, len - n);
            if ( nread <= 0 ) {
               int r = (n == 0) ? nread : n;
               _rafPos += r;
               return r;
            }
            n += nread;
            if ( n >= len ) {
               _rafPos += n;
               return n;
            }
            // if not closed but no bytes available, return
            ByteBuffer input = _bb;
            if ( input != null && !input.hasRemaining() ) {
               _rafPos += n;
               return n;
            }
         }
      }

      /**
       * Fills the buffer with more data, taking into account
       * shuffling and other tricks for dealing with marks.
       * Assumes that it is being called by a synchronized method.
       * This method also assumes that all data has already been read in,
       * hence pos > count.
       */
      private void fill() throws IOException {
         pos = 0; /* no mark: throw away the buffer */
         count = pos;

         _bb.clear();
         int n = _ch.read(_bb);
         if ( n > 0 ) {
            count = n + pos;
         }
      }

      /**
       * Check to make sure that buffer has not been nulled out due to
       * close; if not return it;
       */
      private byte[] getBufIfOpen() throws IOException {
         byte[] buffer = buf;
         if ( buffer == null ) {
            throw new IOException("Stream closed");
         }
         return buffer;
      }

      private void growLastElementBytes( int minGrowSize ) {
         // grow at least minGrowsize, at least a kilobyte and at least 10% of old size
         int newSize = Math.max(_lastElementBytes.length + 1024, _lastElementBytesLength + minGrowSize + 1);
         newSize = Math.max(newSize, _lastElementBytes.length + (int)(_lastElementBytes.length * 0.1f));
         byte[] nb = new byte[newSize];
         System.arraycopy(_lastElementBytes, 0, nb, 0, _lastElementBytes.length);
         _lastElementBytes = nb;
      }

      //      /**
      //       * Check to make sure that underlying input stream has not been
      //       * nulled out due to close; if not return it;
      //       */
      //      private InputStream getInIfOpen() throws IOException {
      //         InputStream input = _in;
      //         if ( input == null ) {
      //            throw new IOException("Stream closed");
      //         }
      //         return input;
      //      }

      private void init( @Nullable FileChannel ch, int size, long rafPos, boolean suppressClose ) {
         _ch = ch;

         if ( size <= 0 ) {
            throw new IllegalArgumentException("Buffer size <= 0");
         }
         buf = new byte[size];
         _bb = ByteBuffer.wrap(buf);
         _rafPos = rafPos;
         _suppressClose = suppressClose;
      }

      /**
       * Read characters into a portion of an array, reading from the underlying
       * stream at most once if necessary.
       */
      private int read1( byte[] b, int off, int len ) throws IOException {
         int avail = count - pos;
         if ( avail <= 0 ) {
            //            /* If the requested length is at least as large as the buffer, and
            //               if there is no mark/reset activity, do not bother to copy the
            //               bytes into the local buffer.  In this way buffered streams will
            //               cascade harmlessly. */
            //            if ( len >= getBufIfOpen().length ) {
            //               return getInIfOpen().read(b, off, len);
            //            }
            fill();
            avail = count - pos;
            if ( avail <= 0 ) {
               return -1;
            }
         }
         int cnt = (avail < len) ? avail : len;
         System.arraycopy(getBufIfOpen(), pos, b, off, cnt);
         pos += cnt;
         return cnt;
      }
   }

   class DeletionAwareDumpReader extends DumpReader<E>implements DumpIterator<E> {

      ResettableBufferedInputStream _positionAwareInputStream;
      long                          _lastPos;
      long                          _maxPos;
      FileInputStream               _in;


      public DeletionAwareDumpReader( File dumpFile, ObjectStreamProvider streamProvider ) throws IOException {
         this(dumpFile, streamProvider, _outputStream._n);
      }

      private DeletionAwareDumpReader( File dumpFile, ObjectStreamProvider streamProvider, long maxPos ) throws IOException {
         super(new ResettableBufferedInputStream(new FileInputStream(_dumpFile), 0, false), 0, streamProvider);
         if ( !_mode.contains(DumpAccessFlag.read) ) {
            throw new AccessControlException("Read operation not allowed with current modes.");
         }
         _positionAwareInputStream = (ResettableBufferedInputStream)_primitiveInputStream;
         _positionAwareInputStream._lastElementBytes = new byte[1024];
         _maxPos = maxPos;
      }

      @Override
      public long getPosition() {
         return _lastPos;
      }

      @Override
      public boolean hasNext() {
         synchronized ( Dump.this ) {
            long pos = -1;
            boolean hasNext = false;
            do {
               _positionAwareInputStream._lastElementBytesLength = 0;
               pos = _positionAwareInputStream._rafPos;
               hasNext = _maxPos > pos && super.hasNext();
            }
            while ( hasNext && _deletedPositions.contains(pos) && super.next() != null ); // condition 'super.next() != null' is just to move the Iterator on
            _lastPos = pos;

            _nextItemPos.set(_positionAwareInputStream._rafPos);
            if ( hasNext && _cache != null ) {
               // we don't cache E instances to prevent the user from changing the cached instances
               byte[] nextItemPos = longToBytes(_nextItemPos.get());
               byte[] lastElementBytes = new byte[_positionAwareInputStream._lastElementBytesLength + nextItemPos.length];
               System.arraycopy(_positionAwareInputStream._lastElementBytes, 0, lastElementBytes, 0, _positionAwareInputStream._lastElementBytesLength);
               appendNextItemPos(lastElementBytes, nextItemPos);
               _cache.put(Long.valueOf(pos), lastElementBytes); // ugly boxing of pos necessary here, since _cache is a regular Map - the cost of beauty is ugliness
            }
            if ( !hasNext ) {
               super.closeStreams(true);
            }

            _positionAwareInputStream._lastElementBytesLength = 0;

            return hasNext;
         }
      }

      @Override
      public @Nonnull DumpIterator<E> iterator() {
         return this;
      }

      @Override
      public E next() {
         _lastItemPos.set(_lastPos);
         return super.next();
      }
   }

   class PositionAwareOutputStream extends OutputStream {

      long _n;

      private final OutputStream _out;


      public PositionAwareOutputStream( OutputStream out, long n ) {
         _out = out;
         _n = n;
      }

      @Override
      public void close() throws IOException {
         _out.close();
      }

      @Override
      public void flush() throws IOException {
         _out.flush();
      }

      @Override
      public void write( byte[] b ) throws IOException {
         _out.write(b);
         _n += b.length;
      }

      @Override
      public void write( byte[] b, int off, int len ) throws IOException {
         _out.write(b, off, len);
         _n += len;
      }

      @Override
      public void write( int b ) throws IOException {
         _out.write(b);
         _n++;
      }
   }

   private final class LongThreadLocal extends ThreadLocal<Long> {

      @Override
      protected Long initialValue() {
         return -1L;
      }
   }
}
