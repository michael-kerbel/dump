package util.dump;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class CombinedIndexTest {

   private static final String DUMP_FILENAME = "DumpTest.dmp";
   private static final int    DUMP_SIZE     = 100000;
   private static final int    READ_NUMBER   = 10000;
   private static final int    BEAN_SIZE     = 10;

   private Random              _random;


   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = new File(".").listFiles(new FileFilter() {

         @Override
         public boolean accept( File f ) {
            return f.getName().startsWith("DumpTest.");
         }
      });
      for ( File df : dumpFile ) {
         if ( !df.delete() ) {
            System.out.println("Failed to delete old dump file " + df);
         }
      }
   }

   @Before
   public void initRandom() {
      long seed = System.currentTimeMillis();
      //      seed = 1234133095531L;
      _random = new Random(seed);
      System.out.println("Seed used for this DumpTest run: " + seed);
   }

   @After
   public void printMemory() {
      System.gc();
      long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
      System.out.println(mem / (1024 * 1024) + " MB used after test run");
   }

   @Test
   public void test() throws NoSuchFieldException, IOException {
      File dumpFile = new File(DUMP_FILENAME);

      Dump<Bean> dump = new Dump<Bean>(Bean.class, dumpFile);

      GroupIndex<Bean> indexLong = new GroupIndex<Bean>(dump, "_groupLong");
      GroupIndex<Bean> indexString = new GroupIndex<Bean>(dump, "_groupString");
      GroupIndex<Bean> indexLongObject = new GroupIndex<Bean>(dump, "_groupLongObject");

      CombinedIndex<Bean> combinedIndex = new CombinedIndex<Bean>(indexLong, indexString, indexLongObject);

      StringBuilder sb = new StringBuilder("-");
      for ( int i = 0; i < BEAN_SIZE - 15; i++ ) { // 15 is an estimation for the size of the Bean instance without this padding
         sb.append('0');
      }

      long t = System.currentTimeMillis();
      for ( int i = 24; i < DUMP_SIZE + 24; i++ ) {
         dump.add(new Bean(i, i + sb.toString()));
      }
      System.out.println("Written " + DUMP_SIZE + " instances to dump. Needed " + (System.currentTimeMillis() - t) / (float)DUMP_SIZE + " ms/instance.");

      t = System.currentTimeMillis();
      for ( int j = 0; j < READ_NUMBER; j++ ) {
         long id = (long)_random.nextInt(DUMP_SIZE) + 24;
         int n = 0;
         for ( Bean bean : combinedIndex.lookup(id / 12, "" + (id / 4), id / 24) ) {
            Assert.assertNotNull("no Bean for index " + id, bean);
            Assert.assertEquals(id / 12, bean._groupLong);
            Assert.assertEquals("" + (id / 4), bean._groupString);
            Assert.assertEquals(Long.valueOf(id / 24), bean._groupLongObject);
            n++;
         }
         Assert.assertEquals("wrong number of elements in group " + id, 4, n);
      }
      System.out.println("Read " + READ_NUMBER + " groups from dump. Needed " + (System.currentTimeMillis() - t) / (float)READ_NUMBER + " ms/group.");

      dump.close();

      System.out.println("Closing and re-opening dump");

      dump = new Dump<Bean>(Bean.class, dumpFile);

      indexLong = new GroupIndex<Bean>(dump, "_groupLong");
      indexString = new GroupIndex<Bean>(dump, "_groupString");
      indexLongObject = new GroupIndex<Bean>(dump, "_groupLongObject");

      combinedIndex = new CombinedIndex<Bean>(indexLong, indexString, indexLongObject);

      t = System.currentTimeMillis();
      for ( int j = 0; j < READ_NUMBER; j++ ) {
         long id = (long)_random.nextInt(DUMP_SIZE) + 24;
         int n = 0;
         for ( Bean bean : combinedIndex.lookup(id / 12, "" + (id / 4), id / 24) ) {
            Assert.assertNotNull("no Bean for index " + id, bean);
            Assert.assertEquals(id / 12, bean._groupLong);
            Assert.assertEquals("" + (id / 4), bean._groupString);
            Assert.assertEquals(Long.valueOf(id / 24), bean._groupLongObject);
            n++;
         }
         Assert.assertEquals("wrong number of elements in group " + id, 4, n);
      }
      System.out.println("Read " + READ_NUMBER + " groups from dump. Needed " + (System.currentTimeMillis() - t) / (float)READ_NUMBER + " ms/group.");

      int n = 0;
      for ( Bean bean : dump ) {
         if ( bean._groupLongObject % 2 == 0 ) {
            dump.deleteLast();
            n++;
         }
      }

      System.out.println("deleted " + n + " items from dump");

      t = System.currentTimeMillis();
      for ( int j = 0; j < READ_NUMBER; j++ ) {
         long id = (long)_random.nextInt(DUMP_SIZE) + 24;
         n = 0;
         for ( Bean bean : combinedIndex.lookup(id / 12, "" + (id / 4), id / 24) ) {
            Assert.assertNotNull("no Bean for index " + id, bean);
            Assert.assertEquals(id / 12, bean._groupLong);
            Assert.assertEquals("" + (id / 4), bean._groupString);
            Assert.assertEquals(Long.valueOf(id / 24), bean._groupLongObject);
            n++;
         }
         Assert.assertEquals("wrong number of elements in group " + id, id / 24 % 2 == 0 ? 0 : 4, n);
      }
      System.out.println("Read " + READ_NUMBER + " groups from dump. Needed " + (System.currentTimeMillis() - t) / (float)READ_NUMBER + " ms/group.");

      dump.close();
   }


   public static class Bean implements ExternalizableBean {

      @externalize(1)
      private long   _groupLong;
      @externalize(3)
      private String _groupString;
      @externalize(4)
      private Long   _groupLongObject;


      //      @externalize(10)
      //      private String _data;

      public Bean() {
         // for Externalization
      }

      public Bean( int id, String data ) {
         _groupLong = id / 12;
         _groupString = "" + (id / 4);
         _groupLongObject = (long)(id / 24);
         //         _data = data;
      }
   }
}
