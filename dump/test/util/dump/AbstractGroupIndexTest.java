package util.dump;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.assertj.core.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import junit.framework.Assert;
import util.dump.reflection.FieldAccessor;
import util.dump.reflection.FieldFieldAccessor;
import util.dump.reflection.Reflection;


@RunWith(Parameterized.class)
abstract public class AbstractGroupIndexTest {

   protected static final String DUMP_FILENAME = "DumpTest.dmp";
   protected static final int    READ_NUMBER   = 1000;
   protected static final int    BEAN_SIZE     = 10;
   protected static       File   _tmpdir;

   @Parameters
   public static Collection<Object[]> getDumpSizesToTestFor() {
      List<Object[]> parameters = new ArrayList<>();
      parameters.add(Arrays.array(10));
      parameters.add(Arrays.array(500));
      parameters.add(Arrays.array(10000));
      return parameters;
   }

   @BeforeClass
   public static void setUpTmpdir() throws IOException {
      _tmpdir = new File("target", "tmp");
      _tmpdir.mkdirs();
      if ( !_tmpdir.isDirectory() ) {
         throw new IOException("unable to create temporary directory: " + _tmpdir.getAbsolutePath());
      }
      System.setProperty("java.io.tmpdir", _tmpdir.getAbsolutePath());
   }

   protected       Random _random;
   protected final int    _dumpSize;

   public AbstractGroupIndexTest( int dumpSize ) {
      _dumpSize = dumpSize;
   }

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = _tmpdir.listFiles(new FileFilter() {

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
      // seed = 1255625802808L;
      _random = new Random(seed);
      System.out.println("Seed used for this DumpTest run: " + seed);
   }

   @After
   public void printMemory() {
      System.gc();
      long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
      System.out.println(mem / (1024 * 1024) + " MB used.");
   }

   protected Dump<Bean> prepareDump( int numGroups ) throws Exception {
      File dumpFile = new File(_tmpdir, DUMP_FILENAME);

      StringBuilder sb = new StringBuilder("-");
      for ( int i = 0; i < BEAN_SIZE - 15; i++ ) { // 15 is an estimation for the size of the Bean instance without this padding
         sb.append('0');
      }

      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
      for ( int i = 0; i < numGroups; i++ ) {
         dump.add(new Bean(i * 10, i + sb.toString()));
      }

      // reopen dump
      dump.close();
      dump = new Dump<>(Bean.class, dumpFile);

      return dump;
   }

   protected void testIndex( String fieldName, TestConfiguration config ) throws Exception {

      testLateOpenIndex(fieldName, config);

      deleteOldTestDumps();

      File dumpFile = new File(_tmpdir, DUMP_FILENAME);

      /* create dump and index */
      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
      try {
         Field field = Reflection.getField(Bean.class, fieldName);
         FieldFieldAccessor fieldAccessor = new FieldFieldAccessor(field);
         NonUniqueIndex index = config.createIndex(dump, fieldAccessor);

         fillDump(dump);

         validateNumKeys(dump, index);

         testLookup(config, field, index, 10);

         dump.close();

         System.out.println("Closing and re-opening dump");

         dump = new Dump<>(Bean.class, dumpFile);
         index = config.createIndex(dump, fieldAccessor);

         validateNumKeys(dump, index);

         testLookup(config, field, index, 10);

         /* test lookup of non-existing key */
         int n = 0;
         Object k = config.createKey(_dumpSize / 10 + 100);
         for ( Bean bean : (Iterable<Bean>)index.lookup(k) ) {
            n++;
            Assert.assertNull("Found a Bean for index which should be out of range", bean);
         }
         Assert.assertEquals(0, n);

         /* iterate dump and delete half of it */
         long t = System.currentTimeMillis();
         int id = 10;
         int deletions = 0;
         for ( Bean bean : dump ) {
            Assert.assertEquals(config.createKey(id / 10), fieldAccessor.get(bean));
            Assert.assertTrue("unexpected bean data", bean._data.startsWith("" + id));
            if ( id % 2 == 0 ) {
               Bean deleted = dump.deleteLast();
               Assert.assertEquals("deleted bean != iterated bean", deleted, bean);
               deletions++;
            }
            id++;
         }
         System.out.println("Iterated the whole dump. Deleted " + deletions + " items. Needed " + (System.currentTimeMillis() - t) + " ms.");

         validateNumKeys(dump, index);

         /* lookup and assert deletions */
         testLookup(config, field, index, 5); // only half as many elements per group expected

         /* iterate dump and update beans */
         t = System.currentTimeMillis();
         id = 11;
         int updates = 0;
         for ( Bean bean : dump ) {
            Assert.assertEquals(config.createKey(id / 10), field.get(bean));
            Assert.assertTrue("unexpected bean data", bean._data.startsWith("" + id));
            if ( config.isUpdatable() ) {
               if ( id % 3 == 0 ) {
                  /* update without changing externalization size of bean */
                  long oldDumpSize = dump._outputStream._n;
                  Bean updatedBean = new Bean(-id, bean._data);
                  Bean oldVersion = dump.updateLast(updatedBean);
                  Assert.assertEquals("old bean != iterated bean", oldVersion, bean);
                  Assert.assertEquals("dump has grown, even though the update was overwrite compatible", oldDumpSize, dump._outputStream._n);
                  updates++;
               } else {
                  /* update and change externalization size of bean */
                  Bean updatedBean = new Bean(id, bean._data.replaceFirst("-", "++"));
                  Bean oldVersion = dump.updateLast(updatedBean);
                  Assert.assertEquals("old bean != iterated bean", oldVersion, bean);
                  updates++;
               }
            }
            id += 2;
         }
         System.out.println("Iterated the whole dump. Updated " + updates + " items. Needed " + (System.currentTimeMillis() - t) + " ms.");

         validateNumKeys(dump, index);

         if ( config.isUpdatable() ) {
            /* lookup and assert updates */
            testLookupAfterUpdates(config, field, index);

            dump.close();

            System.out.println("Closing and re-opening dump");

            dump = new Dump<>(Bean.class, dumpFile);
            index = config.createIndex(dump, fieldAccessor);

            testLookupAfterUpdates(config, field, index);
         }

         dump.close();

         /* delete index meta file to invalidate the index */
         File[] metaFiles = _tmpdir.listFiles(new FileFilter() {

            @Override
            public boolean accept( File f ) {
               return f.getName().startsWith("DumpTest.") && f.getName().endsWith("meta");
            }
         });
         for ( File df : metaFiles ) {
            Assert.assertTrue("Failed to delete meta file " + df, df.delete());
         }
         /* re-open, enforcing the index to be re-created */
         dump = new Dump<>(Bean.class, dumpFile);
         index = config.createIndex(dump, fieldAccessor);

         /* after having re-created the index, repeat last test */
         if ( config.isUpdatable() ) {
            testLookupAfterUpdates(config, field, index);
         } else {
            testLookup(config, field, index, 5);
         }
      }
      finally {
         dump.close();
      }
   }

   protected void testLookup( TestConfiguration config, Field field, NonUniqueIndex index, int expectedGroupNumber ) throws Exception {
      printMemory();

      long t;
      t = System.currentTimeMillis();
      for ( int j = 0; j < READ_NUMBER; j++ ) {
         int id = _random.nextInt(_dumpSize / 10) + 1;
         Object k = config.createKey(id);
         int n = 0;
         for ( Bean bean : (Iterable<Bean>)index.lookup(k) ) {
            Assert.assertNotNull("no Bean for index " + k, bean);
            Assert.assertEquals(k, field.get(bean));
            assertThat(bean._data).as("unexpected bean data").startsWith("" + id);
            n++;
         }

         assertThat(n).as("wrong number of elements in group " + k).isEqualTo(expectedGroupNumber);
      }
      System.out.println("Read " + READ_NUMBER + " groups from dump. Needed " + (System.currentTimeMillis() - t) / (float)READ_NUMBER + " ms/group.");

      printMemory();
   }

   protected void testLookupAfterUpdates( TestConfiguration config, Field field, NonUniqueIndex index ) throws Exception {
      long t;
      Object k;
      int id;
      t = System.currentTimeMillis();
      for ( int j = 0; j < READ_NUMBER; j++ ) {
         id = _random.nextInt(_dumpSize / 10) + 1;
         if ( _random.nextBoolean() ) {
            id = -id;
         }
         k = config.createKey(id);
         for ( Bean bean : (Iterable<Bean>)index.lookup(k) ) {
            int oid = Integer.parseInt(bean._data.substring(0, Math.max(bean._data.indexOf('-', 1), bean._data.indexOf('+', 1))));
            if ( oid % 2 == 0 ) {
               Assert.assertNull("deleted Bean with index " + oid + " is still accessable", bean);
            } else if ( id < 0 ) {
               Assert.assertNotNull("no Bean for index " + id, bean);
               Assert.assertEquals(k, field.get(bean));
               Assert.assertTrue("bean data wrong: oid=" + oid + ", data=" + bean._data, bean._data.startsWith("" + (-id)));
               Assert.assertTrue("wrong bean", oid % 3 == 0);
            } else {
               Assert.assertNotNull("no Bean for index " + oid, bean);
               Assert.assertEquals(k, field.get(bean));
               Assert.assertTrue(bean._data.startsWith(oid + "++"));
               Assert.assertTrue("wrong bean", oid % 3 != 0);
            }
         }
      }
      System.out.println("Read " + READ_NUMBER + " instances from dump. Needed " + (System.currentTimeMillis() - t) / (float)READ_NUMBER + " ms/instance.");
   }

   private void fillDump( Dump<Bean> dump ) throws IOException {
      StringBuilder sb = new StringBuilder("-");
      for ( int i = 0; i < BEAN_SIZE - 15; i++ ) { // 15 is an estimation for the size of the Bean instance without this padding
         sb.append('0');
      }

      /* add some elements */
      long t = System.currentTimeMillis();
      for ( int i = 10; i < _dumpSize + 10; i++ ) {
         dump.add(new Bean(i, i + sb.toString()));
      }

      System.out.println("Written " + _dumpSize + " instances to dump. Needed " + (System.currentTimeMillis() - t) / (float)_dumpSize + " ms/instance.");
   }

   private void testLateOpenIndex( String fieldName, TestConfiguration config ) throws Exception {
      File dumpFile = new File(_tmpdir, DUMP_FILENAME);

      /* create dump and index */
      Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
      Field field = Reflection.getField(Bean.class, fieldName);

      fillDump(dump);
      try {
         FieldFieldAccessor fieldAccessor = new FieldFieldAccessor(field);
         NonUniqueIndex index = config.createIndex(dump, fieldAccessor);
         testLookup(config, field, index, 10);
      }
      finally {
         dump.close();
      }
   }

   private void validateNumKeys( Dump<Bean> dump, NonUniqueIndex index ) {
      // count keys
      TIntSet keys = new TIntHashSet();
      for ( Bean bean : dump ) {
         keys.add(bean._groupInt);
      }

      // test num keys
      if ( index instanceof DumpIndex<?> ) {
         int numKeys = ((DumpIndex)index).getNumKeys();
         assertThat(numKeys).isEqualTo(keys.size());
      }
   }

   public static class Bean implements ExternalizableBean {

      @externalize(1)
      long             _groupLong;
      @externalize(2)
      int              _groupInt;
      @externalize(3)
      String           _groupString;
      @externalize(4)
      Long             _groupLongObject;
      @externalize(5)
      ExternalizableId _groupExternalizable;
      @externalize(10)
      String           _data;

      public Bean() {
         // for Externalization
      }

      public Bean( int id, String data ) {
         id = id / 10;
         _groupLong = id;
         _groupInt = id;
         _groupString = (id < 0 ? "" : "+") + id;
         _groupLongObject = (long)id;
         _groupExternalizable = new ExternalizableId(id);
         _data = data;
      }

      @Override
      public boolean equals( Object obj ) {
         if ( this == obj ) {
            return true;
         }
         if ( obj == null ) {
            return false;
         }
         Bean other = (Bean)obj;
         if ( _data == null ) {
            if ( other._data != null ) {
               return false;
            }
         } else if ( !_data.equals(other._data) ) {
            return false;
         }
         if ( _groupExternalizable == null ) {
            if ( other._groupExternalizable != null ) {
               return false;
            }
         } else if ( !_groupExternalizable.equals(other._groupExternalizable) ) {
            return false;
         }
         if ( _groupInt != other._groupInt ) {
            return false;
         }
         if ( _groupLong != other._groupLong ) {
            return false;
         }
         if ( _groupLongObject == null ) {
            if ( other._groupLongObject != null ) {
               return false;
            }
         } else if ( !_groupLongObject.equals(other._groupLongObject) ) {
            return false;
         }
         if ( _groupString == null ) {
            if ( other._groupString != null ) {
               return false;
            }
         } else if ( !_groupString.equals(other._groupString) ) {
            return false;
         }
         return true;
      }

      @Override
      public int hashCode() {
         throw new UnsupportedOperationException("hashCode() not needed.");
      }
   }


   public static class ExternalizableId implements ExternalizableBean {

      @externalize(1)
      long _id;

      public ExternalizableId() {
         // for Externalization
      }

      public ExternalizableId( long id ) {
         _id = id;
      }

      @Override
      public boolean equals( Object obj ) {
         if ( this == obj ) {
            return true;
         }
         if ( obj == null ) {
            return false;
         }
         if ( getClass() != obj.getClass() ) {
            return false;
         }
         ExternalizableId other = (ExternalizableId)obj;
         if ( _id != other._id ) {
            return false;
         }
         return true;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + (int)(_id ^ (_id >>> 32));
         return result;
      }
   }


   protected static abstract class TestConfiguration {

      public abstract NonUniqueIndex createIndex( Dump dump, FieldAccessor fieldAccessor );

      public abstract Object createKey( int id );

      /** not all indexes support updates (GroupedIndex!) */
      public boolean isUpdatable() {
         return true;
      }
   }

}
