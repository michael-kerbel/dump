package util.dump.sort;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import util.dump.Dump;
import util.dump.DumpInput;
import util.dump.DumpReader;
import util.dump.DumpWriter;
import util.dump.stream.ObjectStreamProvider;


/**
 * <p>The (almost) infinite sorter is intended to receive a set of Objects of the same
 * type and return them sorted. This sort is guaranteed to be stable: equal elements will
 * not be reordered as a result of the sort.</p>
 *
 * <p>This implementation basically is not limited by the memory available in the JVM,
 * it will swap to the hard disk in order get its job done. This means, that the limit
 * of items to sort is given by the capacity of your hard drive. Of course there is
 * an exception to this rule, your available RAM may prove insufficient if you configure
 * the Sorter to keep too many objects in memory.</p>
 *
 * <p>You can limit the memory usage by specifying the maximal number of items to be placed in memory.
 *
 * <p>The temporary files will be deleted as soon as they are no longer needed, at the latest
 * during JVM shutdown.</p>
 *
 * <p>A newly constructed InfiniteSorter instance will have the following defaults:</p>
 *
 * <ul>
 * <li>Maximal Items in memory: see static constant <code>DEFAULT_MAX_ITEMS_IN_MEMORY</code></li>
 * <li>Object comparator: uses natural order from Es <code>Comparable</code> interface implementation.
 * <li>Temporary Folder: Uses a TempFileProvider with default settings
 * </ul>
 *
 */
@SuppressWarnings("unused")
public class InfiniteSorter<E> implements Iterable<E> {

   // static internal values
   private static final String SERIALIZED_SEGMENT_PREFIX   = "seg.";
   /**
    * This constant documents the default maximal amount of items to be kept in
    * memory before proceeding with swapping to the specified <code>File</code> folder
    * object or by default to the temporary OS directory.
    */
   public static final int     DEFAULT_MAX_ITEMS_IN_MEMORY = 100_000;

   // private members for public configuration
   private Comparator<E>        _comparator            = null;
   // private temporary files provider
   private TempFileProvider     _tempFileProvider;
   // private control members
   private int                  _totalBufferedElements = 0;                 // *Total* number of items buffered
   private List<E>              _memoryBuffer          = new ArrayList<>(); // memory items buffer
   private List<File>           _segmentFiles          = null;
   private ObjectStreamProvider _objectStreamProvider  = null;
   private int                  _maxItemsInMemory;


   public InfiniteSorter() {
      init(DEFAULT_MAX_ITEMS_IN_MEMORY, null, TempFileProvider.DEFAULT_PROVIDER);
   }

   public InfiniteSorter( File tempDir ) {
      init(DEFAULT_MAX_ITEMS_IN_MEMORY, null, new TempFileProvider(tempDir));
   }

   public InfiniteSorter( int maxItemsInMemory ) {
      init(maxItemsInMemory, null, TempFileProvider.DEFAULT_PROVIDER);
   }

   public InfiniteSorter( int maxItemsInMemory, File tempDir ) {
      init(maxItemsInMemory, null, new TempFileProvider(tempDir));
   }

   public InfiniteSorter( int maxItemsInMemory, File tempDir, ObjectStreamProvider objectStreamProvider, Comparator<E> comparator ) {
      init(maxItemsInMemory, comparator, new TempFileProvider(tempDir));
      this._objectStreamProvider = objectStreamProvider;
   }

   public InfiniteSorter( int maxItemsInMemory, File tempDir, ObjectStreamProvider objectStreamProvider ) {
      init(maxItemsInMemory, null, new TempFileProvider(tempDir));
      this._objectStreamProvider = objectStreamProvider;
   }

   public void add( E e ) throws IOException {

      // writes the new item to memory
      _memoryBuffer.add(e);

      // counts the total buffer size
      _totalBufferedElements++;

      if ( _memoryBuffer.size() == _maxItemsInMemory ) {
         flush();
      }
   }

   public void addAll( Iterable<E> entries ) throws Exception {
      for ( E e : entries ) {
         add(e);
      }
   }

   public void addSortedSegment( Dump<E> dump ) {
      addSortedSegment(dump.getDumpFile());
   }

   public void addSortedSegment( File dumpFile ) {
      if ( _segmentFiles == null ) {
         _segmentFiles = new ArrayList<>();
      }
      if ( dumpFile == null || !dumpFile.isFile() ) {
         throw new IllegalArgumentException("dumpFile argument not valid: " + dumpFile);
      }
      _segmentFiles.add(dumpFile);
   }

   /** Writes everything from the memory buffer to disk. */
   public void flush() throws IOException {
      try {
         if ( _memoryBuffer.size() > 0 ) {
            _memoryBuffer.sort(_comparator);
            dumpToDump(_memoryBuffer);
            _memoryBuffer.clear();
         }
      }
      catch ( Exception e ) {
         if ( e instanceof IOException ) {
            throw (IOException)e;
         }
         throw new IOException(e.getMessage());
      }
   }

   /**
    * @return Number of elements in buffer. Please notice that this is not the number of items currently in memory but the total amount of buffered items.
    */
   public int getBufferSize() {
      return _totalBufferedElements;
   }

   public Comparator getComparator() {
      return _comparator;
   }

   public DumpInput<E> getSortedElements() throws Exception {

      DumpInput<E> finalStream;

      if ( _segmentFiles == null ) {
         // all buffered items are still in memory, no swapping needed
         _memoryBuffer.sort(_comparator);
         finalStream = new ListInput<>(_memoryBuffer);

      } else {
         // flush items in memory, if necessary
         flush();

         // merge from HD
         finalStream = mergeDumps();
      }

      // prepares the object for the next sort process
      startFromScratch();

      // and returns the result of the current sort process
      return finalStream;
   }

   public TempFileProvider getTempFileProvider() {
      return _tempFileProvider;
   }

   public @Nonnull Iterator<E> iterator() {
      try {
         return getSortedElements().iterator();
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }
   }

   public void setComparator( Comparator<E> comparator ) {
      this._comparator = comparator;
   }

   public void setObjectStreamProvider( ObjectStreamProvider objectStreamProvider ) {
      this._objectStreamProvider = objectStreamProvider;
   }

   public void setTempFileProvider( TempFileProvider tempFileProvider ) {
      this._tempFileProvider = tempFileProvider;
   }

   // receives a type safe input containing sorted items and dumps them to a temporary file.
   private void dumpToDump( List<E> sortedElements ) throws Exception {

      if ( _segmentFiles == null ) {
         _segmentFiles = new ArrayList<>();
      }

      // gets a new temporary file and dumps the sorted data into it
      File dumpFile = _tempFileProvider.getNextTemporaryFile(_tempFileProvider.getFileSubPrefix() + SERIALIZED_SEGMENT_PREFIX);
      DumpWriter<E> dump = new DumpWriter<>(dumpFile, _objectStreamProvider);
      for ( E e : sortedElements ) {
         dump.write(e);
      }
      dump.close();

      _segmentFiles.add(dumpFile);
   }

   private List<DumpInput<E>> getSegments() throws IOException {
      List<DumpInput<E>> streamsbuffer = new ArrayList<>();

      for ( File f : _segmentFiles ) {
         streamsbuffer.add(new DumpReader<>(f, true, _objectStreamProvider));
      }
      return streamsbuffer;
   }

   // init the class
   private void init( int maxItemsInMemory, Comparator<E> comparator, TempFileProvider tempFileProvider ) {
      _maxItemsInMemory = maxItemsInMemory;
      if ( _maxItemsInMemory < 1 ) {
         throw new IllegalArgumentException("maxItemsInMemory must be positive: " + maxItemsInMemory);
      }

      this._comparator = comparator;
      this._tempFileProvider = tempFileProvider;

      startFromScratch();
   }

   private DumpInput<E> mergeDumps() throws Exception {
      List<DumpInput<E>> streamsbuffer = getSegments();
      _segmentFiles = null;
      return new SortedInputMerger<>(streamsbuffer, _comparator);

   }

   // inits the object in order to get started from scratch
   private void startFromScratch() {

      this._totalBufferedElements = 0;
      this._memoryBuffer = new ArrayList<>();

      this._segmentFiles = null;
   }

}
