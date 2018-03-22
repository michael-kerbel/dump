package util.dump.sort;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import util.dump.Dump;
import util.dump.DumpInput;
import util.dump.DumpReader;
import util.dump.DumpWriter;
import util.dump.stream.ObjectStreamProvider;


/**
 *
 * <p>The (almost) infinite sorter is intended to receive a set of Objects of the same
 * type and return them sorted by using its natural <code>Comparable</code> interface or
 * the provided <code>Comparator</code> implementation.</p>
 *
 * <p>This sort is guaranteed to be stable: equal elements will not be reordered as a
 * result of the sort.</p>
 *
 * <p>This implementation basically is not limited by the memory available in your System,
 * it will swap to the hard disk in order get its job done. This means, that the limit
 * of items to sort is given by the capacity of your hard drive. Of course there is
 * an exception to this rule, your available RAM may prove insufficient only in the case
 * you configure the Sorter to keep to many objects in memory and this way generating
 * an out of memory error.</p>
 *
 * <p>You can limit the memory usage by specifying the maximal number of items and leaf merge
 * streams to by placed in memory. Please notice that this implementation clusters the streams,
 * this means that you may maximally generate <code>(leafstreams * 2) - 1</code> streams in memory.</p>
 *
 * <p>The temporal files will be deleted as long they are no longer needed, you may enforce this
 * by additionally configuring the object to tell the Java VM to delete the files on exit,
 * this is behaviour is by default not active, you should use the corresponding setter to <code>true</code>
 * in order to activate this functionality.</p>
 *
 * <p>By default, a new constructed infinite sorter object will habe the following default configuration:</p>
 *
 * <ul>
 * <li>Maximal Items in memory: see static constant <code>DEFAULT_MAX_ITEMS_IN_MEMORY</code></li>
 * <li>Maximal leaf streams in memory:  see static constant <code>DEFAULT_MAX_LEAF_STREAMS_IN_MEMORY</code></li>
 * <li>Object comparator: uses default <code>Comparable</code> interface.
 * <li>Temporal Folder: Uses default temporal file provider from Global object
 * </ul>
 *
 */
public class InfiniteSorter<E> implements Iterable<E> {

   // static internal values
   private static final String  SERIALIZED_SEGMENT_PREFIX   = "seg.";

   // private members for public configuration
   private Comparator<E>        comparator                  = null;

   // private temporal files provider
   private TempFileProvider     tempFileProvider;

   // private control members
   private int                  totalBufferedElements       = 0;                 // *Total* number of items buffered
   private List<E>              memoryBuffer                = new ArrayList<E>(); // memory items buffer

   private List<File>           segmentFiles                = null;

   private ObjectStreamProvider objectStreamProvider        = null;

   /**
    * This constant documents the default maximal amount of items to be kept in
    * memory before proceeding with swapping to the specified <code>File</code> folder
    * object or by default to the temporal OS directory.
    */
   public static final int      DEFAULT_MAX_ITEMS_IN_MEMORY = 100000;

   private int                  _maxItemsInMemory;

   public InfiniteSorter() {
      init(DEFAULT_MAX_ITEMS_IN_MEMORY, null, TempFileProvider.DEFAULT_PROVIDER);
   }

   public InfiniteSorter( File tempDir ) {
      this(new TempFileProvider(tempDir));
   }

   public InfiniteSorter( int maxItemsInMemory ) {
      init(maxItemsInMemory, null, TempFileProvider.DEFAULT_PROVIDER);
   }

   public InfiniteSorter( int maxItemsInMemory, File tempDir ) {
      init(maxItemsInMemory, null, new TempFileProvider(tempDir));
   }

   public InfiniteSorter( int maxItemsInMemory, File tempDir, ObjectStreamProvider objectStreamProvider ) {
      init(maxItemsInMemory, null, new TempFileProvider(tempDir));
      this.objectStreamProvider = objectStreamProvider;
   }

   public InfiniteSorter( int maxItemsInMemory, TempFileProvider tempFileProvider ) {
      init(maxItemsInMemory, null, tempFileProvider);
   }

   public InfiniteSorter( TempFileProvider tempFileProvider ) {
      init(DEFAULT_MAX_ITEMS_IN_MEMORY, null, tempFileProvider);
   }

   public void add( E objectToWrite ) throws IOException {

      // writes the new item to memory
      memoryBuffer.add(objectToWrite);

      // counts the total buffer size
      totalBufferedElements++;

      if ( memoryBuffer.size() == _maxItemsInMemory ) {
         flush();
      }
   }

   public void addAll( Iterable<E> inputelements ) throws Exception {
      for ( E e : inputelements ) {
         add(e);
      }
   }

   /**
    * Has no effect. Interface compatibility
    */
   public void close() {}

   public void flush() throws IOException {

      try {
         if ( memoryBuffer.size() > 0 ) {
            Collections.sort(memoryBuffer, comparator);
            dumpToDump(memoryBuffer);
            memoryBuffer.clear();
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
      return totalBufferedElements;
   }

   public Comparator getComparator() {
      return comparator;
   }

   public DumpInput<E> getSortedElements() throws Exception {

      DumpInput<E> finalStream = null;

      if ( segmentFiles == null ) {
         // all buffered items are still in memory, no swapping needed
         Collections.sort(memoryBuffer, comparator);
         finalStream = new ListInput<E>(memoryBuffer);

      } else {
         // default case: items are buffered and segments are stored as temporal files

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
      return tempFileProvider;
   }

   public Iterator<E> iterator() {
      try {
         return getSortedElements().iterator();
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }
   }

   public void setComparator( Comparator<E> comparator ) {
      this.comparator = comparator;
   }

   public void setObjectStreamProvider( ObjectStreamProvider objectStreamProvider ) {
      this.objectStreamProvider = objectStreamProvider;
   }

   public void setTempFileProvider( TempFileProvider tempFileProvider ) {
      this.tempFileProvider = tempFileProvider;
   }

   public void addSortedSegment( Dump<E> dump ) {
      addSortedSegment(dump.getDumpFile());
   }

   public void addSortedSegment( File dumpFile ) {
      if ( segmentFiles == null ) {
         segmentFiles = new ArrayList<File>();
      }
      if ( dumpFile == null || !dumpFile.isFile() ) {
         throw new IllegalArgumentException("dumpFile argument not valid: " + dumpFile);
      }
      segmentFiles.add(dumpFile);
   }

   private List<DumpInput<E>> getSegments() throws IOException {
      List<DumpInput<E>> streamsbuffer = new ArrayList<DumpInput<E>>();

      for ( File f : segmentFiles ) {
         streamsbuffer.add(new DumpReader<E>(f, true, objectStreamProvider));
      }
      return streamsbuffer;
   }

   // receives a type safe input containing sorted items and dumps them to a temporal file.
   private void dumpToDump( List<E> sortedElements ) throws Exception {

      if ( segmentFiles == null ) {
         segmentFiles = new ArrayList<File>();
      }

      // gets a new temporal file and dumps the sorted data into it
      File dumpFile = tempFileProvider.getNextTemporalFile(tempFileProvider.getFileSubPrefix() + SERIALIZED_SEGMENT_PREFIX);
      DumpWriter<E> dump = new DumpWriter<E>(dumpFile, objectStreamProvider);
      for ( E e : sortedElements ) {
         dump.write(e);
      }
      dump.close();

      segmentFiles.add(dumpFile);
   }

   // init the class
   private void init( int maxItemsInMemory, Comparator<E> comparator, TempFileProvider tempFileProvider ) {
      _maxItemsInMemory = maxItemsInMemory;
      if ( _maxItemsInMemory < 1 ) {
         throw new IllegalArgumentException("maxItemsInMemory must be positive: " + maxItemsInMemory);
      }

      this.comparator = comparator;
      this.tempFileProvider = tempFileProvider;

      startFromScratch();
   }

   private DumpInput<E> mergeDumps() throws Exception {
      List<DumpInput<E>> streamsbuffer = getSegments();
      segmentFiles = null;
      return new SortedInputMerger<E>(streamsbuffer, comparator);

   }

   // inits the object in order to get started from scratch
   private void startFromScratch() {

      this.totalBufferedElements = 0;
      this.memoryBuffer = new ArrayList<E>();

      this.segmentFiles = null;
   }

}
