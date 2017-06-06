package util.dump.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import util.dump.DumpInput;


/**
 * This class receives a set of already sorted <code>DumpInput</code> objects and
 * streams a merged version of them by using the <code>Comparable</code> interface or the
 * equivalent provided <code>Comparator</code>.
 */
class SortedInputMerger<E> implements DumpInput<E>, Iterator<E> {

   private DumpInput<E>  dualChannelA   = null;                    // first channel for dual stream mode (using comparator)

   private Iterator<E>   iteratorA      = null;
   private E             lastA          = (E)null;
   private DumpInput<E>  dualChannelB   = null;                    // second channel for dual stream mode (using comparator)
   private Iterator<E>   iteratorB      = null;
   private E             lastB          = (E)null;
   private Iterator<E>   singleIterator = null;                    // channel for single stream mode
   private E             lastSingle     = (E)null;
   private E             nextElement    = (E)null;                 // next element to be returned
   private boolean       nextPrepared   = false;
   private Comparator<E> comparator     = null;                    // reference to the provided Comparator
   private MergerMode    mergerMode     = MergerMode.uninitialized; // indicates the current merger status


   /**
    * Constructs a merger using the given collection of <code>DumpInput</code> objetcs,
    * the class will create internally a <code>Comparator</code> object based on the
    * current implementation of <code>Comparable</code>
    *
    * @param inputstreams Colection (List) of <code>DumpInput</code> objects to be merged
    * @throws Exception
    */
   public SortedInputMerger( List<DumpInput<E>> inputstreams ) throws Exception {
      init(inputstreams, null);
   }

   /**
    * Constructs a merger using the given collection of <code>DumpInput</code> objetcs,
    * the class will merge the streams by using the provided Comparator
    *
    * @param inputstreams Colection (List) of <code>DumpInput</code> objects to be merged
    * @param comparator used to determine the item ordering
    * @throws Exception
    */
   public SortedInputMerger( List<DumpInput<E>> inputstreams, Comparator<E> comparator ) throws Exception {
      super();
      init(inputstreams, comparator);
   }

   /**
    * Closes the stream
    */
   @Override
   public void close() throws IOException {

      this.nextElement = (E)null;

      if ( dualChannelA != null ) {
         dualChannelA.close();
      }

      if ( dualChannelB != null ) {
         dualChannelB.close();
      }
   }

   private int compare( E a, E b ) {
      if ( comparator == null ) {
         if ( !(a instanceof Comparable) ) {
            throw new IllegalArgumentException(a + " isn't Comparable and no comparator is set");
         }
         if ( !(b instanceof Comparable) ) {
            throw new IllegalArgumentException(a + " isn't Comparable and no comparator is set");
         }

         return ((Comparable)a).compareTo(b);
      }

      return comparator.compare(a, b);
   }

   @Override
   public boolean hasNext() {
      if ( nextPrepared ) {
         return nextElement != null;
      }

      try {
         switch ( mergerMode ) {

         case dualStream:
            // checks which object from the channels has priority
            int c = compare(lastA, lastB);
            if ( c > 0 ) {
               // return channel B
               this.nextElement = lastB;
               nextPrepared = true;

               // if channelB is empty then it goes into transition for single channelA
               if ( !hasNextB() ) {
                  singleIterator = iteratorA;
                  lastSingle = lastA;
                  mergerMode = MergerMode.transition;
               }

            } else {

               // return channel A
               this.nextElement = lastA;
               nextPrepared = true;

               // if channelA is empty then it goes into transition for single channelB
               if ( !hasNextA() ) {
                  singleIterator = iteratorB;
                  lastSingle = lastB;
                  mergerMode = MergerMode.transition;
               }

            }

            return true;

         case singleStream:
            if ( hasNextSingle() ) {
               // single stream has data
               this.nextElement = lastSingle;
               nextPrepared = true;
               return true;
            }
            // EOF reached change status and inform the user
            mergerMode = MergerMode.concluded;
            this.nextElement = (E)null;
            nextPrepared = true;
            return false;

         case transition:

            // returns the remaining item from the dualChannel operation
            mergerMode = MergerMode.singleStream;
            this.nextElement = lastSingle;
            nextPrepared = true;
            return true;

         case concluded:
            this.nextElement = (E)null;
            nextPrepared = true;
            return false;

         case uninitialized:
            throw new Exception("the SortedInputMerger is still uninitalized, this status is unexpected.");

         }

         throw new Exception("Unexpected merger status");
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }

   }

   @Override
   public Iterator<E> iterator() {
      return this;
   }

   @Override
   public E next() {
      nextPrepared = false;
      if ( nextElement == null ) {
         throw new NoSuchElementException();
      }
      return nextElement;
   }

   @Override
   public void remove() {}

   @Override
   protected void finalize() throws Throwable {
      close();
      super.finalize();
   }

   // used to generate the initial merge channels A and B
   private DumpInput<E> getMergerForList( List<DumpInput<E>> sublist ) throws Exception {

      int listSize = sublist.size();

      // list in empty, return a dummy NULL input stream
      if ( listSize == 0 ) {
         return new ListInput<E>(new ArrayList<E>());
      }

      // single element, returns the elemnt itself (it supports already the expected interface)
      if ( listSize == 1 ) {
         return sublist.get(0);
      }

      // recusrivelly splits the list into a new merger
      return new SortedInputMerger<E>(sublist, this.comparator);

   }

   private boolean hasNextA() {
      boolean hasNext = iteratorA.hasNext();
      if ( hasNext ) {
         E next = iteratorA.next();
         if ( lastA != null && compare(lastA, next) > 0 ) {
            throw new IllegalStateException("underlying stream not sorted!");
         }
         lastA = next;
      } else {
         lastA = (E)null;
      }
      return hasNext;
   }

   private boolean hasNextB() {
      boolean hasNext = iteratorB.hasNext();
      if ( hasNext ) {
         E next = iteratorB.next();
         if ( lastB != null && compare(lastB, next) > 0 ) {
            throw new IllegalStateException("underlying stream not sorted!");
         }
         lastB = next;
      } else {
         lastB = (E)null;
      }
      return hasNext;
   }

   private boolean hasNextSingle() {
      boolean hasNext = singleIterator.hasNext();
      if ( hasNext ) {
         lastSingle = singleIterator.next();
      } else {
         lastSingle = (E)null;
      }
      return hasNext;
   }

   private void init( List<DumpInput<E>> inputstreams, Comparator<E> comparator ) throws Exception {

      // retains a reference to the comparator
      this.comparator = comparator;

      // splits the streams, notice that the distribution of stream items
      // tends to send more elements to the dualChannelA.
      int numberOfStreams = inputstreams.size();
      int streamsForChannelA = (int)Math.ceil((float)numberOfStreams / 2);

      // generates the two basic channels
      dualChannelA = getMergerForList(inputstreams.subList(0, streamsForChannelA));
      dualChannelB = getMergerForList(inputstreams.subList(streamsForChannelA, numberOfStreams));

      iteratorA = dualChannelA.iterator();
      iteratorB = dualChannelB.iterator();

      // init streams
      if ( hasNextA() ) {

         if ( hasNextB() ) {
            // channelA and channelB contains data and are ready for operation
            mergerMode = MergerMode.dualStream;
         } else {
            // channelA has data but channelB is empty, goes into transition (channelA contains already data)
            singleIterator = iteratorA;
            mergerMode = MergerMode.transition;
            lastSingle = lastA;
         }

      } else {
         // channelA is empty, so switch to single channel mode and delegate channelB the task
         mergerMode = MergerMode.singleStream;
         singleIterator = iteratorB;
      }

   }


   // private enumartion used to document the merger status
   private enum MergerMode {
      /*
       * uninitialized: initial status, channels are not yed assigned nor initialized
       * dualstream: channelA and channelB are active and with elements ready to be comapred.
       * transition: one of both channelA or channelB reached EOF, the other channel has still an element to be returned before continuing reading from it.
       * singleStream: only one channel is active, normal interface opeartion according to DumpInput.
       * concluded: no more data left, channels are empty.
       */
      uninitialized, dualStream, transition, singleStream, concluded;
   }

}
