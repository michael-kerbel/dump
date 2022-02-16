package util.dump;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import util.dump.sort.InfiniteSorter;
import util.dump.stream.ObjectStreamProvider;
import util.dump.stream.SingleTypeObjectStreamProvider;


public class InfiniteSorterTest {

   @Test
   public void testSorterInMemory() throws IOException {
      InfiniteSorter<Bean> infiniteSorter = new InfiniteSorter<>(1000);
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
      ObjectStreamProvider p = new SingleTypeObjectStreamProvider<>(Bean.class);
      InfiniteSorter<Bean> infiniteSorter = new InfiniteSorter<>(1_000_000);
      infiniteSorter.setObjectStreamProvider(p);
      for ( int i = 10_000_000; i > 0; i-- ) {
         infiniteSorter.add(new Bean(i));
      }

      int n = 0;
      for ( Bean bean : infiniteSorter ) {
         n++;
         Assert.assertEquals(n, bean._id);
      }

      Assert.assertEquals(n, 10000000);
   }

   @Test
   public void testSorterWithSortedSegment() throws IOException {
      ObjectStreamProvider p = new SingleTypeObjectStreamProvider<>(Bean.class);
      InfiniteSorter<Bean> infiniteSorter = new InfiniteSorter<>(1_000_000);
      infiniteSorter.setObjectStreamProvider(p);
      for ( int i = 10_000_000; i > 0; i-- ) {
         infiniteSorter.add(new Bean(i));
      }
      File dumpFile = File.createTempFile("InfiniteSorterTest", ".dmp");
      try(Dump<Bean> sortedDump = new Dump<>(Bean.class, dumpFile)) {
         DumpUtils.deleteDumpFilesOnExit(sortedDump);
         for ( int i = 10_000_001; i <= 11_000_000; i ++ ) {
            sortedDump.add(new Bean(i));
         }
         infiniteSorter.addSortedSegment(sortedDump);

         int n = 0;
         for ( Bean bean : infiniteSorter ) {
            n++;
            Assert.assertEquals(n, bean._id);
         }

         Assert.assertEquals( 11_000_000, n);
      }
   }


   public static class Bean implements ExternalizableBean, Comparable<Bean> {

      @externalize(1)
      private long _id;


      public Bean() {}

      public Bean( long id ) {
         _id = id;
      }

      @Override
      public int compareTo( @Nonnull Bean o ) {
         return (Long.compare(_id, o._id));
      }
   }

}
