package util.dump.sort;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
 * <p>This implementation is not limited by the memory available in the JVM,
 * it will swap the data to the hard disk in order get its job done. This means, that the limit
 * of elements to sort is determined by the capacity of your hard drive, unless you configure
 * the Sorter to keep too many objects in memory.</p>
 *
 * <p>The sorting works by keeping a limited number of elements in memory, sorting them once
 * that limit is reached and writing this list into a small Dump into the temp folder. Once
 * all elements have been added, these dumps are merged: the sorter opens all dumps and reads
 * a single element of each dump. It writes the smallest to the result stream and reads the
 * next element from its dump. This is repeated until there are no elements left.</p>
 *
 * <p>You can limit the memory usage by specifying the maximal number of elements to be placed in memory
 * and the bufferSize for the IO streams during merging.</p>
 *
 * <p>The temporary files will be deleted as soon as they are no longer needed, at the latest
 * during JVM shutdown.</p>
 *
 * <p>A newly constructed InfiniteSorter instance will have the following defaults:</p>
 *
 * <ul>
 * <li>Maximal elements in memory: see {@link InfiniteSorter#DEFAULT_MAX_ELEMENTS_IN_MEMORY}</li>
 * <li>Buffer size for streams during merging: see {@link InfiniteSorter#DEFAULT_BUFFER_SIZE}</li>
 * <li>Object comparator: uses natural order from Es <code>Comparable</code> interface implementation.</li>
 * <li>Temporary Folder: Uses a TempFileProvider with default settings</li>
 * <li>ObjectStreamProvider: An ExternalizableObjectStreamProvider</li>
 * </ul>
 *
 * <p>In most cases you will want to set a SingleTypeObjectStreamProvider matching your element's type
 * to improve performance.</p>
 */
@SuppressWarnings("unused")
public class InfiniteSorter<E> implements Iterable<E> {

   // static internal values
   private static final String SERIALIZED_SEGMENT_PREFIX      = "seg.";
   /**
    * The maximal amount of elements to be kept memory by default,
    * before proceeding with swapping to the specified <code>File</code> folder
    * object or by default to the temporary OS directory.
    */
   public static final int     DEFAULT_MAX_ELEMENTS_IN_MEMORY = 100_000;
   public static final int     DEFAULT_BUFFER_SIZE            = 262144;

   private Comparator<E>        _comparator            = null;
   private TempFileProvider     _tempFileProvider;
   private int                  _totalBufferedElements = 0;                  // *Total* number of elements buffered
   private List<E>              _memoryBuffer          = new ArrayList<>();  // memory elements buffer
   private List<File>           _segmentFiles          = null;
   private List<Dump<E>>        _segmentDumps          = null;
   private ObjectStreamProvider _objectStreamProvider  = null;
   private int                  _maxElementsInMemory;
   // the buffer size of the sorted segmentFiles during merging
   private int                  _bufferSize            = DEFAULT_BUFFER_SIZE;


   public InfiniteSorter() {
      init(DEFAULT_MAX_ELEMENTS_IN_MEMORY, -1, null, TempFileProvider.DEFAULT_PROVIDER);
   }

   public InfiniteSorter( @Nonnull File tempDir ) {
      init(DEFAULT_MAX_ELEMENTS_IN_MEMORY, -1, null, new TempFileProvider(tempDir));
   }

   public InfiniteSorter( int maxElementsInMemory ) {
      init(maxElementsInMemory, -1, null, TempFileProvider.DEFAULT_PROVIDER);
   }

   public InfiniteSorter( int maxElementsInMemory, int bufferSize ) {
      init(maxElementsInMemory, bufferSize, null, TempFileProvider.DEFAULT_PROVIDER);
   }

   public InfiniteSorter( int maxElementsInMemory, int bufferSize, @Nonnull File tempDir ) {
      init(maxElementsInMemory, -1, null, new TempFileProvider(tempDir));
   }

   public InfiniteSorter( int maxElementsInMemory, int bufferSize, @Nonnull File tempDir, @Nullable ObjectStreamProvider objectStreamProvider,
         @Nullable Comparator<E> comparator ) {
      init(maxElementsInMemory, bufferSize, comparator, new TempFileProvider(tempDir));
      this._objectStreamProvider = objectStreamProvider;
   }

   public void add( E e ) throws IOException {

      // writes the new element to memory
      _memoryBuffer.add(e);

      // counts the total buffer size
      _totalBufferedElements++;

      if ( _memoryBuffer.size() == _maxElementsInMemory ) {
         flush();
      }
   }

   public void addAll( Iterable<E> entries ) throws Exception {
      for ( E e : entries ) {
         add(e);
      }
   }

   public void addSortedSegment( Dump<E> dump ) {
      if ( _segmentDumps == null ) {
         _segmentDumps = new ArrayList<>();
      }
      if ( dump == null || dump.isClosed() ) {
         throw new IllegalArgumentException("dump argument not valid: " + dump);
      }
      _segmentDumps.add(dump);
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
    * @return Number of elements in buffer. Please notice that this is not the number of elements currently in memory but the total amount of buffered elements.
    */
   public int getBufferSize() {
      return _totalBufferedElements;
   }

   public Comparator getComparator() {
      return _comparator;
   }

   public DumpInput<E> getSortedElements() throws Exception {

      DumpInput<E> finalStream;

      if ( _segmentFiles == null && _segmentDumps == null ) {
         // all buffered elements are still in memory, no swapping needed
         _memoryBuffer.sort(_comparator);
         finalStream = new ListInput<>(_memoryBuffer);

      } else {
         // flush elements in memory, if necessary
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

   public void setComparator( @Nullable Comparator<E> comparator ) {
      this._comparator = comparator;
   }

   public void setObjectStreamProvider( @Nullable ObjectStreamProvider objectStreamProvider ) {
      this._objectStreamProvider = objectStreamProvider;
   }

   public void setTempFileProvider( TempFileProvider tempFileProvider ) {
      this._tempFileProvider = tempFileProvider;
   }

   // receives a type safe input containing sorted elements and dumps them to a temporary file.
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

      if ( _segmentFiles != null )
         for ( File f : _segmentFiles ) {
            streamsbuffer.add(new DumpReader<>(f, true, _bufferSize, _objectStreamProvider));
         }
      if ( _segmentDumps != null )
         for ( Dump<E> d : _segmentDumps ) {
            streamsbuffer.add(d.getDumpReader());
         }
      return streamsbuffer;
   }

   // init the class
   private void init( int maxElementsInMemory, int bufferSize, Comparator<E> comparator, TempFileProvider tempFileProvider ) {
      _maxElementsInMemory = maxElementsInMemory;
      if ( _maxElementsInMemory < 1 ) {
         throw new IllegalArgumentException("maxElementsInMemory must be positive: " + maxElementsInMemory);
      }

      this._comparator = comparator;
      this._tempFileProvider = tempFileProvider;
      if ( bufferSize > 0 )
         this._bufferSize = bufferSize;

      startFromScratch();
   }

   private DumpInput<E> mergeDumps() throws Exception {
      List<DumpInput<E>> streamsbuffer = getSegments();
      _segmentFiles = null;
      _segmentDumps = null;
      return new SortedInputMerger<>(streamsbuffer, _comparator);

   }

   // inits the object in order to get started from scratch
   private void startFromScratch() {

      this._totalBufferedElements = 0;
      this._memoryBuffer = new ArrayList<>();

      this._segmentFiles = null;
   }

}
