package util.dump;

import junit.framework.Assert;

import org.junit.Test;


public class FindFirstTest {

   int[] TEST_INTS1 = new int[] { 1, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7 };
   int[] TEST_INTS2 = new int[] { 0, 1, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 5, 6, 7 };

   @Test
   public void test() {
      for ( int low = 0; low < 2; low++ ) {
         for ( int high = 2; high < 10; high++ ) {
            Assert.assertEquals("TEST_INTS1", 2, findFirst(TEST_INTS1, 3, low, high));
         }
      }
      for ( int low = 0; low < 3; low++ ) {
         for ( int high = 3; high < 11; high++ ) {
            Assert.assertEquals("TEST_INTS2", 3, findFirst(TEST_INTS2, 3, low, high));
         }
      }
   }

   private int findFirst( int[] keys, int key, int low, int high ) {
      int mid = -1;
      int midVal;
      while ( low <= high ) {
         mid = (low + high) >>> 1;
         midVal = keys[mid];

         if ( midVal < key )
            low = mid + 1;
         else if ( midVal == key ) high = mid - 1;
      }
      //
      //      if ( mid == low ) {
      //         System.err.println("mid==low");
      //         System.err.println("keys[low]==" + keys[low]);
      //      } else if ( mid == high ) {
      //         System.err.println("mid==high");
      //         System.err.println("keys[high]==" + keys[high]);
      //      }

      return low;
   }

}
