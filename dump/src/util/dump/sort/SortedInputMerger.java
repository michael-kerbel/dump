package util.dump.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import util.dump.DumpInput;
import util.dump.DumpReader;


/**
 * This class receives a set of already sorted <code>DumpInput</code> objects and
 * streams a merged version of them by using the <code>Comparable</code> interface or the
 * equivalent provided <code>Comparator</code>.
 */
class SortedInputMerger<E> implements DumpInput<E>, Iterator<E> {

   private DumpInput<E> dualChannelA = null; // first channel for dual stream mode (using comparator)

   private Iterator<E>   iteratorA      = null;
   private E             lastA          = (E)null;
   private DumpInput<E>  dualChannelB   = null;                     // second channel for dual stream mode (using comparator)
   private Iterator<E>   iteratorB      = null;
   private E             lastB          = (E)null;
   private Iterator<E>   singleIterator = null;                     // channel for single stream mode
   private E             lastSingle     = (E)null;
   private E             nextElement    = (E)null;                  // next element to be returned
   private boolean       nextPrepared   = false;
   private Comparator<E> comparator     = null;                     // reference to the provided Comparator
   private MergerMode    mergerMode     = MergerMode.uninitialized; // indicates the current merger status


   /**
    * Constructs a merger using the given collection of <code>DumpInput</code> objects,
    * the class will merge the streams by using the provided Comparator
    *
    * @param inputStreams Collection (List) of <code>DumpInput</code> objects to be merged
    * @param comparator used to determine the item ordering, will use natural ordering when null
    */
   public SortedInputMerger( List<DumpInput<E>> inputStreams, @Nullable Comparator<E> comparator ) throws Exception {
      super();
      init(inputStreams, comparator);
   }

   /**
    * Closes the stream
    */
   @Override
   public void close() throws IOException {

      nextElement = null;

      if ( dualChannelA != null ) {
         dualChannelA.close();
      }

      if ( dualChannelB != null ) {
         dualChannelB.close();
      }
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
               nextElement = lastB;
               nextPrepared = true;

               // if channelB is empty then it goes into transition for single channelA
               if ( !hasNextB() ) {
                  singleIterator = iteratorA;
                  lastSingle = lastA;
                  mergerMode = MergerMode.transition;
               }

            } else {

               // return channel A
               nextElement = lastA;
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
               nextElement = lastSingle;
               nextPrepared = true;
               return true;
            }
            // EOF reached change status and inform the user
            mergerMode = MergerMode.concluded;
            nextElement = null;
            nextPrepared = true;
            return false;

         case transition:

            // returns the remaining item from the dualChannel operation
            mergerMode = MergerMode.singleStream;
            nextElement = lastSingle;
            nextPrepared = true;
            return true;

         case concluded:
            nextElement = null;
            nextPrepared = true;
            return false;

         case uninitialized:
            throw new Exception("the SortedInputMerger is still uninitialized, this status is unexpected.");

         }

         throw new Exception("Unexpected merger status");
      }
      catch ( Exception e ) {
         throw new RuntimeException(e);
      }

   }

   @Override
   public @Nonnull Iterator<E> iterator() {
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

   private int compare( E a, E b ) {
      if ( comparator == null ) {
         if ( !(a instanceof Comparable) ) {
            throw new IllegalArgumentException(a + " isn't Comparable and no comparator is set");
         }
         if ( !(b instanceof Comparable) ) {
            throw new IllegalArgumentException(a + " isn't Comparable and no comparator is set");
         }

         //noinspection unchecked
         return ((Comparable)a).compareTo(b);
      }

      return comparator.compare(a, b);
   }

   // used to generate the initial merge channels A and B
   private DumpInput<E> getMergerForList( List<DumpInput<E>> sublist ) throws Exception {

      int listSize = sublist.size();

      // list in empty, return a dummy NULL input stream
      if ( listSize == 0 ) {
         return new ListInput<>(new ArrayList<>());
      }

      // single element, returns the element itself (it supports already the expected interface)
      if ( listSize == 1 ) {
         return sublist.get(0);
      }

      // recursively splits the list into a new merger
      return new SortedInputMerger<>(sublist, comparator);

   }

   @SuppressWarnings("Duplicates")
   private boolean hasNextA() {
      boolean hasNext = iteratorA.hasNext();
      if ( hasNext ) {
         E next = iteratorA.next();
         if ( lastA != null && compare(lastA, next) > 0 ) {
            if ( dualChannelA instanceof DumpReader ) {
               throw new IllegalStateException("underlying stream " + ((DumpReader)dualChannelA).getSourceFile() + " not sorted! " + lastA + " > " + next);
            }
            throw new IllegalStateException("underlying stream not sorted!");
         }
         lastA = next;
      } else {
         lastA = null;
      }
      return hasNext;
   }

   @SuppressWarnings("Duplicates")
   private boolean hasNextB() {
      boolean hasNext = iteratorB.hasNext();
      if ( hasNext ) {
         E next = iteratorB.next();
         if ( lastB != null && compare(lastB, next) > 0 ) {
            if ( dualChannelB instanceof DumpReader ) {
               throw new IllegalStateException("underlying stream " + ((DumpReader)dualChannelB).getSourceFile() + " not sorted! " + lastB + " > " + next);
            }
            throw new IllegalStateException("underlying stream not sorted!");
         }
         lastB = next;
      } else {
         lastB = null;
      }
      return hasNext;
   }

   private boolean hasNextSingle() {
      boolean hasNext = singleIterator.hasNext();
      if ( hasNext ) {
         lastSingle = singleIterator.next();
      } else {
         lastSingle = null;
      }
      return hasNext;
   }

   private void init( List<DumpInput<E>> inputStreams, Comparator<E> comparator ) throws Exception {

      // retains a reference to the comparator
      this.comparator = comparator;

      // splits the streams, notice that the distribution of stream items
      // tends to send more elements to the dualChannelA.
      int numberOfStreams = inputStreams.size();
      int streamsForChannelA = (int)Math.ceil((float)numberOfStreams / 2);

      // generates the two basic channels
      dualChannelA = getMergerForList(inputStreams.subList(0, streamsForChannelA));
      dualChannelB = getMergerForList(inputStreams.subList(streamsForChannelA, numberOfStreams));

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


   // private enumeration used to document the merger status
   private enum MergerMode


   {
      /*
       * uninitialized: initial status, channels are not yed assigned nor initialized
       * dualStream: channelA and channelB are active and with elements ready to be compared.
       * transition: one of both channelA or channelB reached EOF, the other channel has still an element to be returned before continuing reading from it.
       * singleStream: only one channel is active, normal interface operation according to DumpInput.
       * concluded: no more data left, channels are empty.
       */
      uninitialized, dualStream, transition, singleStream, concluded
   }

}
