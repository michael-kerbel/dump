package util.dump;

import static org.assertj.core.api.Assertions.assertThat;
import static util.dump.ExternalizableBean.OnIncompatibleVersion.DeleteDump;
import static util.dump.ExternalizableBean.OnIncompatibleVersion.RewriteDump;

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
import util.dump.ExternalizableBean.externalizationVersion;
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
      catch ( AccessControlException e ) {
      }
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
         bean._data[0] = 1;
         dump.add(bean);
         bean._data[0] = 2;
         dump.add(bean);
         dump.flush();
         assertThat(dump.getDumpSize()).as("unexpected dump size before update").isEqualTo(2000);
         bean._data = new byte[200];
         bean._data[0] = 3;
         dump.update(0, bean);
         dump.flush();
         assertThat(dump.getDumpSize()).as("unexpected dump size after update").isEqualTo(2000);
         assertThat(dump.get(0)._data[0]).as("unexpected data after update").isEqualTo((byte)3);
         assertThat(dump.get(1000)._data[0]).as("unexpected data after update").isEqualTo((byte)2);
      }
   }

   @Test
   public void testIterateEmptyDump() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {
         for ( Bean bean : dump ) {
            Assert.fail("Element returned during iteration of empty dump");
            Assert.assertNotNull("Element returned during iteration of empty dump", bean);
         }
      }
   }

   @Test
   public void testMetaValue() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
      dump.setMetaValue("test", "test");
      assertThat(dump.getMetaValue("test")).isEqualTo("test");
      dump.close();
      dump = new Dump<>(Bean.class, dumpFile);
      assertThat(dump.getMetaValue("test")).isEqualTo("test");
      dump.close();
   }

   @Test
   public void testOutOfPlaceUpdateWithPadding() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<TestBeanPadding> dump = new Dump<>(TestBeanPadding.class, dumpFile)) {
         TestBeanPadding bean = new TestBeanPadding();
         bean._data = new byte[100];
         bean._data[0] = 1;
         dump.add(bean);
         bean._data[0] = 2;
         dump.add(bean);
         dump.flush();
         assertThat(dump.getDumpSize()).as("unexpected dump size before update").isEqualTo(2000);
         bean._data = new byte[1500];
         bean._data[0] = 3;
         dump.update(0, bean);
         dump.flush();
         assertThat(dump.getDumpSize()).as("unexpected dump size after update").isEqualTo(4000);
         assertThat(dump.get(2000)._data[0]).as("unexpected data after update").isEqualTo((byte)3);
         assertThat(dump.get(1000)._data[0]).as("unexpected data after update").isEqualTo((byte)2);
      }

      try (Dump<TestBeanPadding> dump = new Dump<>(TestBeanPadding.class, dumpFile)) {
         int n = 0;
         for ( TestBeanPadding b : dump ) {
            n++;
            if ( n == 1 ) {
               assertThat(b._data[0]).as("unexpected data after re-open").isEqualTo((byte)2);
            }
            if ( n == 2 ) {
               assertThat(b._data[0]).as("unexpected data after re-open").isEqualTo((byte)3);
            }
         }
         assertThat(n).as("unexpected size after re-open").isEqualTo(2);
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

   @Test
   public void testUpdateWithPadding() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      try (Dump<TestBeanPadding> dump = new Dump<>(TestBeanPadding.class, dumpFile)) {
         TestBeanPadding bean = new TestBeanPadding();
         bean._data = new byte[986];
         bean._data[0] = 1;
         dump.add(bean);
         dump.flush();
         assertThat(dump.getDumpSize()).as("unexpected dump size before update").isEqualTo(1000);

         for ( int i = 986; i < 1000; i++ ) {
            for ( TestBeanPadding b : dump ) {
               b._data = new byte[i];
               b._data[0] = (byte)(i - 988);
               dump.updateLast(b);
               dump.flush();
            }
         }
      }

      try (Dump<TestBeanPadding> dump = new Dump<>(TestBeanPadding.class, dumpFile)) {
         int n = 0;
         for ( TestBeanPadding b : dump ) {
            n++;
            if ( n == 1 ) {
               assertThat(b._data[0]).as("unexpected data after re-open").isEqualTo((byte)11);
            }
         }
         assertThat(n).as("unexpected size after re-open").isEqualTo(1);
      }
   }

   @Test
   public void testVersionUpdate() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
      dump.add(new Bean(1));
      dump.close();

      Dump<BeanVersion2> v2Dump = new Dump<>(BeanVersion2.class, dumpFile);
      File oldDumpFile = new File("DumpTest.dmp.version0");
      assertThat(oldDumpFile).as("Dump was not renamed after version upgrade").exists();
      int n = 0;
      for ( BeanVersion2 b : v2Dump ) {
         n++;
      }
      assertThat(n).as("Dump was not renamed after version upgrade").isEqualTo(0);
      v2Dump.add(new BeanVersion2(1));
      v2Dump.close();

      Dump<BeanVersion3> v3Dump = new Dump<>(BeanVersion3.class, dumpFile);
      oldDumpFile = new File("DumpTest.dmp.version2");
      assertThat(oldDumpFile).as("Dump was not renamed after version upgrade").exists();
      n = 0;
      for ( BeanVersion3 b : v3Dump ) {
         n++;
      }
      assertThat(n).as("Dump was not renamed after version upgrade").isEqualTo(0);
      v3Dump.add(new BeanVersion3(1));
      v3Dump.close();

      Dump<BeanVersion4> v4Dump = new Dump<>(BeanVersion4.class, dumpFile);
      n = 0;
      for ( BeanVersion4 b : v4Dump ) {
         n++;
      }
      assertThat(n).as("Dump was not deleted after version upgrade").isEqualTo(0);
      v4Dump.add(new BeanVersion4(1));
      v4Dump.close();

      Dump<BeanVersion5> v5Dump = new Dump<>(BeanVersion5.class, dumpFile);
      n = 0;
      for ( BeanVersion5 b : v5Dump ) {
         n++;
      }
      assertThat(n).as("Dump did not retain contents during version upgrade").isEqualTo(1);
      assertThat(v5Dump.getMetaValue("externalizationVersion")).as("Dump was not rewritten after version upgrade").isEqualTo("5");
      v5Dump.close();
   }

   @Test
   public void testVersionUpdateWithoutPreexistingFiles() throws Exception {
      File dumpFile = new File("DumpTest.dmp");
      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
      dump.close();
      dumpFile.delete();

      Dump<BeanVersion2> v2Dump = new Dump<>(BeanVersion2.class, dumpFile);
      File oldDumpFile = new File("DumpTest.dmp.version0");
      assertThat(oldDumpFile).as("Dump was renamed after version upgrade on nonexistent file").doesNotExist();
      v2Dump.add(new BeanVersion2(1));
      v2Dump.close();
      dumpFile.delete();

      Dump<BeanVersion3> v3Dump = new Dump<>(BeanVersion3.class, dumpFile);
      oldDumpFile = new File("DumpTest.dmp.version2");
      assertThat(oldDumpFile).as("Dump was renamed after version upgrade on nonexistent file").doesNotExist();
      v3Dump.close();
      dumpFile.delete();

      Dump<BeanVersion4> v4Dump = new Dump<>(BeanVersion4.class, dumpFile);
      v4Dump.close();
      dumpFile.delete();

      Dump<BeanVersion5> v5Dump = new Dump<>(BeanVersion5.class, dumpFile);
      v5Dump.close();
      dumpFile.delete();
   }

   public static class Bean implements ExternalizableBean {

      @externalize(1)
      int _id;

      public Bean() {}

      public Bean( int id ) {
         _id = id;
      }
   }


   @externalizationVersion(version = 2)
   public static class BeanVersion2 implements ExternalizableBean {

      @externalize(1)
      int _id;

      public BeanVersion2() {}

      public BeanVersion2( int id ) {
         _id = id;
      }
   }


   @externalizationVersion(version = 3)
   public static class BeanVersion3 implements ExternalizableBean {

      @externalize(1)
      int _id;

      public BeanVersion3() {}

      public BeanVersion3( int id ) {
         _id = id;
      }
   }


   @externalizationVersion(version = 4, onIncompatibleVersion = DeleteDump)
   public static class BeanVersion4 implements ExternalizableBean {

      @externalize(1)
      int _id;

      public BeanVersion4() {}

      public BeanVersion4( int id ) {
         _id = id;
      }
   }


   @externalizationVersion(version = 5, onIncompatibleVersion = RewriteDump)
   public static class BeanVersion5 implements ExternalizableBean {

      @externalize(1)
      int _id;

      public BeanVersion5() {}

      public BeanVersion5( int id ) {
         _id = id;
      }
   }
}
