package util.dump;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.security.AccessControlException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import util.dump.Dump.DumpAccessFlag;
import util.dump.Dump.ElementAndPosition;
import util.dump.ExternalizableBeanTest.TestBeanPadding;


public class DumpTest {

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
         try {
            Files.delete(df.toPath());
         }
         catch ( IOException e ) {
            e.printStackTrace();
         }
      }
   }

   @Test(expected = AccessControlException.class)
   public void testAddIndexWithoutAccessRight() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile, DumpAccessFlag.add, DumpAccessFlag.delete)) {
         dump.add(new Bean(1));
         dump.delete(0);
         dump.add(new Bean(1));
         new UniqueIndex<>(dump, "_id");
      }
   }

   @Test(expected = AccessControlException.class)
   public void testAddWithoutAccessRight() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile, DumpAccessFlag.read)) {
         dump.add(new Bean(1));
      }
   }

   @Test(expected = AccessControlException.class)
   public void testDeleteWithoutAccessRight() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile, DumpAccessFlag.add)) {
         dump.add(new Bean(1));
         dump.delete(0);
      }
   }

   @Test
   public void testGetWithoutAccessRight() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile, DumpAccessFlag.add);

      dump.add(new Bean(1));
      dump.add(new Bean(2));
      dump.add(new Bean(3));

      try {
         dump.get(0);
         Assert.fail();
      }
      catch ( AccessControlException e ) {}
      finally {
         dump.close();
      }
   }

   @Test
   public void testInPlaceUpdateWithPadding() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<TestBeanPadding> dump = new Dump<>(TestBeanPadding.class, dumpFile)) {
         TestBeanPadding bean = new TestBeanPadding();
         bean._data = new byte[100];
         dump.add(bean);
         assertThat(dump.getDumpSize()).as("unexpected dump size before update").isEqualTo(1000);
         bean._data = new byte[200];
         dump.update(0, bean);
         assertThat(dump.getDumpSize()).as("unexpected dump size after update").isEqualTo(1000);
      }
   }

   @Test
   public void testIterateEmptyDump() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {
         for ( Bean bean : dump ) {
            Assert.assertTrue("Element returned during iteration of empty dump", false);
            Assert.assertNotNull("Element returned during iteration of empty dump", bean);
         }
      }
   }

   @Test
   public void testPruning() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {
         for ( int i = 0; i < Dump.PRUNE_THRESHOLD * 3; i++ ) {
            dump.add(new Bean(i));
         }

         for ( Bean bean : dump ) {
            if ( bean._id % 2 == 0 ) {
               dump.deleteLast();
            }
         }
      }

      long dumpFileLengthBeforePruning = dumpFile.length();

      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {
         Assert.assertEquals(0, dump._deletedPositions.size());
         dump.close();

         Assert.assertEquals(0, dump._deletionsFile.length());
         assertThat(dumpFile.length()).isLessThanOrEqualTo((dumpFileLengthBeforePruning / 2) + 1);
      }
   }

   @Test
   public void testReOpening() throws IOException, NoSuchFieldException {
      File dumpFile = new File("DumpTest.dmp");
      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
      UniqueIndex<Bean> index = new UniqueIndex<>(dump, "_id");
      for ( int j = 0; j < 100000; j++ ) {
         dump.add(new Bean(j));
      }
      Bean bean = index.lookup(50000);
      Assert.assertNotNull("Bean added after reopening not found using index", bean);
      Assert.assertEquals("wrong index for bean retrieved using index", 50000, bean._id);

      int i = 0;
      for ( Bean b : dump ) {
         Assert.assertEquals("Wrong index during iteration", i++, b._id);
      }
      Assert.assertEquals("Iterated wrong number of elements", 100000, i);

      dump.close();

      dump = new Dump<>(Bean.class, dumpFile);
      index = new UniqueIndex<>(dump, "_id");
      for ( int j = 100000; j < 200000; j++ ) {
         dump.add(new Bean(j));
      }

      bean = index.lookup(150000);
      Assert.assertNotNull("Bean added after reopening not found using index", bean);
      Assert.assertEquals("wrong index for bean retrieved using index", 150000, bean._id);

      i = 0;
      for ( Bean b : dump ) {
         if ( i % 2 == 0 ) {
            dump.deleteLast();
         }
         Assert.assertEquals("Wrong index during iteration", i++, b._id);
      }
      Assert.assertEquals("Iterated wrong number of elements", 200000, i);

      /* test deletions */
      i = 0;
      for ( Bean b : dump ) {
         Assert.assertEquals("Wrong index during iteration", i * 2 + 1, b._id);
         i++;
      }
      Assert.assertEquals("Iterated wrong number of elements", 100000, i);

      dump.close();

      dump = new Dump<>(Bean.class, dumpFile);

      /* test deletions after re-opening*/
      i = 0;
      for ( Bean b : dump ) {
         Assert.assertEquals("Wrong index during iteration", i * 2 + 1, b._id);
         i++;
      }
      Assert.assertEquals("Iterated wrong number of elements", 100000, i);

      dump.close();
   }

   @Test
   public void testUpdateAll() throws IOException {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {
         for ( int j = 0; j < 100000; j++ ) {
            dump.add(new Bean(j));
         }
         final SortedSet<ElementAndPosition<Bean>> updates = new TreeSet<>();
         DumpIterator<Bean> iterator = dump.iterator();
         while ( iterator.hasNext() ) {
            Bean b = iterator.next();
            if ( b._id % 100 == 0 ) {
               b._id = -b._id;
               updates.add(new Dump.ElementAndPosition<>(b, iterator.getPosition()));
            }
         }

         dump.updateAll(updates);

         int numUpdates = 0;
         for ( Bean b : dump ) {
            if ( b._id % 100 == 0 ) {
               numUpdates++;
               Assert.assertTrue("updated wrong bean", b._id <= 0);
            } else {
               Assert.assertTrue("updated wrong bean", b._id >= 0);
            }
         }
         Assert.assertEquals("Wrong number of updates", numUpdates, updates.size());
      }
   }

   @Test(expected = AccessControlException.class)
   public void testUpdateInPlaceWithoutAccessRight() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile, DumpAccessFlag.add, DumpAccessFlag.delete)) {
         dump.add(new Bean(1));
         dump.delete(0);
         dump.add(new Bean(1));
         dump.update(0, new Bean(2));
      }
   }


   public static class Bean implements ExternalizableBean {

      @externalize(1)
      int _id;


      public Bean() {}

      public Bean( int id ) {
         _id = id;
      }
   }
}
