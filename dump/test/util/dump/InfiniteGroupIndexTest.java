package util.dump;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Test;

import util.dump.reflection.FieldAccessor;


public class InfiniteGroupIndexTest extends AbstractGroupIndexTest {

   public InfiniteGroupIndexTest( int dumpSize ) {
      super(dumpSize);
   }

   @Test
   public void testExternalizableKeyIndex() throws Exception {
      testIndex("_groupExternalizable", new InfiniteGroupIndexConfig() {

         @Override
         public Object createKey( int id ) {
            return new ExternalizableId(id);
         }
      });
   }

   @Test
   public void testExternalizableKeyIndexWithCache() throws Exception {
      testIndex("_groupExternalizable", new InfiniteGroupIndexWithCacheConfig() {

         @Override
         public Object createKey( int id ) {
            return new ExternalizableId(id);
         }
      });
   }

   @Test
   public void testGetNumKeysExternalizable() throws Exception {
      int numKeys = 5;
      Dump<Bean> dump = prepareDump(numKeys);
      try {
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<>(dump, "_groupExternalizable");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup(new ExternalizableId(1)).iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused") Bean bean : intIndex.lookup(new ExternalizableId(1)) ) {
            dump.deleteLast();
         }

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys - 1);
      }
      finally {
         dump.close();
      }
   }

   @Test
   public void testGetNumKeysInt() throws Exception {
      int numKeys = 5;
      Dump<Bean> dump = prepareDump(numKeys);
      try {
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<>(dump, "_groupInt");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup(1).iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused") Bean bean : intIndex.lookup(1) ) {
            dump.deleteLast();
         }

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys - 1);
      }
      finally {
         dump.close();
      }
   }

   @Test
   public void testGetNumKeysLong() throws Exception {
      int numKeys = 5;
      Dump<Bean> dump = prepareDump(numKeys);
      try {
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<>(dump, "_groupLong");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup(1l).iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused") Bean bean : intIndex.lookup(1l) ) {
            dump.deleteLast();
         }

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys - 1);
      }
      finally {
         dump.close();
      }
   }

   @Test
   public void testGetNumKeysString() throws Exception {
      int numKeys = 5;
      Dump<Bean> dump = prepareDump(numKeys);
      try {
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<>(dump, "_groupString");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup("+1").iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused") Bean bean : intIndex.lookup("+1") ) {
            dump.deleteLast();
         }

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys - 1);
      }
      finally {
         dump.close();
      }
   }

   @Test
   public void testIntKeyIndex() throws Exception {
      testIndex("_groupInt", new InfiniteGroupIndexConfig() {

         @Override
         public Object createKey( int id ) {
            return Integer.valueOf(id);
         }
      });
   }

   @Test
   public void testIntKeyIndexWithCache() throws Exception {
      testIndex("_groupInt", new InfiniteGroupIndexWithCacheConfig() {

         @Override
         public Object createKey( int id ) {
            return Integer.valueOf(id);
         }
      });
   }

   @Test
   public void testLongKeyIndex() throws Exception {
      testIndex("_groupLong", new InfiniteGroupIndexConfig() {

         @Override
         public Object createKey( int id ) {
            return Long.valueOf(id);
         }
      });
   }

   @Test
   public void testLongKeyWithCacheIndex() throws Exception {
      testIndex("_groupLong", new InfiniteGroupIndexWithCacheConfig() {

         @Override
         public Object createKey( int id ) {
            return Long.valueOf(id);
         }
      });
   }

   @Test
   public void testRangeLookup() throws Exception {

      File dumpFile = new File(_tmpdir, DUMP_FILENAME);
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile)) {
         for ( int i = _dumpSize * 2; i > 0; i -= 2 ) {
            dump.add(new Bean(i * 10, i + "----"));
         }

         InfiniteGroupIndex<Bean> longIndex = new InfiniteGroupIndex<>(dump, "_groupLong");
         testRangeLookup(dump, longIndex, _dumpSize * 2 / 4, _dumpSize * 2 / 2);
         testRangeLookup(dump, longIndex, (_dumpSize * 2 / 4) - 1, (_dumpSize * 2 / 2) + 1);
         testRangeLookup(dump, longIndex, -1, _dumpSize * 4);
      }
   }

   @Test
   public void testStringKeyIndex() throws Exception {
      testIndex("_groupString", new InfiniteGroupIndexConfig() {

         @Override
         public Object createKey( int id ) {
            return (id < 0 ? "" : "+") + id;
         }
      });
   }

   @Test
   public void testStringKeyWithCacheIndex() throws Exception {
      testIndex("_groupString", new InfiniteGroupIndexWithCacheConfig() {

         @Override
         public Object createKey( int id ) {
            return (id < 0 ? "" : "+") + id;
         }
      });
   }

   private void testRangeLookup( Dump<Bean> dump, InfiniteGroupIndex<Bean> index, int lowerKey, int upperKey ) {
      Iterable<Bean> beans = index.rangeLookup(lowerKey, upperKey);
      int numberFoundByIndex = 0;
      for ( Bean bean : beans ) {
         assertThat(bean._groupLong).as("bean id not in expected range").isGreaterThanOrEqualTo(lowerKey);
         assertThat(bean._groupLong).as("bean id not in expected range").isLessThan(upperKey);
         numberFoundByIndex++;
      }
      int numberInDump = 0;
      for ( Bean bean : dump ) {
         if ( bean._groupLong >= lowerKey && bean._groupLong < upperKey ) {
            numberInDump++;
         }
      }
      assertThat(numberFoundByIndex).as("wrong number of elements found").isEqualTo(numberInDump);
   }

   public abstract static class InfiniteGroupIndexConfig extends TestConfiguration {

      @Override
      public NonUniqueIndex createIndex( Dump dump, FieldAccessor fieldAccessor ) {
         return new InfiniteGroupIndex<Bean>(dump, fieldAccessor, 2500);
      }
   }


   public abstract static class InfiniteGroupIndexWithCacheConfig extends TestConfiguration {

      @Override
      public NonUniqueIndex createIndex( Dump dump, FieldAccessor fieldAccessor ) {
         InfiniteGroupIndex<Bean> infiniteGroupIndex = new InfiniteGroupIndex<Bean>(dump, fieldAccessor, 2500);
         infiniteGroupIndex.setLRUCacheSize(1000);
         return infiniteGroupIndex;
      }
   }

}
