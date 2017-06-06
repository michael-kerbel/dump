package util.dump;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import util.dump.sort.InfiniteSorter;
import util.dump.stream.ObjectStreamProvider;
import util.dump.stream.SingleTypeObjectStreamProvider;


public class InfiniteSorterTest {

   @Test
   public void testSorterInMemory() throws IOException {
      InfiniteSorter<Bean> infiniteSorter = new InfiniteSorter<Bean>(1000);
      for ( int i = 100; i > 0; i-- ) {
         Bean bean = new Bean(i);
         infiniteSorter.add(bean);
      }

      int n = 0;
      for ( Bean bean : infiniteSorter ) {
         n++;
         Assert.assertEquals(n, bean._id);
      }

      Assert.assertEquals(n, 100);
   }

   @Test
   public void testSorterOnDisk() throws IOException {
      ObjectStreamProvider p = new SingleTypeObjectStreamProvider<Bean>(Bean.class);
      InfiniteSorter<Bean> infiniteSorter = new InfiniteSorter<Bean>(1000000);
      infiniteSorter.setObjectStreamProvider(p);
      for ( int i = 10000000; i > 0; i-- ) {
         infiniteSorter.add(new Bean(i));
      }

      int n = 0;
      for ( Bean bean : infiniteSorter ) {
         n++;
         Assert.assertEquals(n, bean._id);
      }

      Assert.assertEquals(n, 10000000);
   }


   public static class Bean implements ExternalizableBean, Comparable<Bean> {

      @externalize(1)
      private long _id;


      public Bean() {}

      public Bean( long id ) {
         _id = id;
      }

      @Override
      public int compareTo( Bean o ) {
         return (_id < o._id ? -1 : (_id == o._id ? 0 : 1));
      }
   }

}
