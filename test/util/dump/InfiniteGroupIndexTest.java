package util.dump;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import util.reflection.FieldAccessor;


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
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<Bean>(dump, "_groupExternalizable");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup(new ExternalizableId(1)).iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused")
         Bean bean : intIndex.lookup(new ExternalizableId(1)) ) {
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
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<Bean>(dump, "_groupInt");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup(1).iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused")
         Bean bean : intIndex.lookup(1) ) {
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
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<Bean>(dump, "_groupLong");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup(1l).iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused")
         Bean bean : intIndex.lookup(1l) ) {
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
         InfiniteGroupIndex<Bean> intIndex = new InfiniteGroupIndex<Bean>(dump, "_groupString");
         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         assertThat(intIndex.lookup("+1").iterator().hasNext()).isTrue();

         dump.add(new Bean(10, "foo"));

         assertThat(intIndex.getNumKeys()).isEqualTo(numKeys);

         for ( @SuppressWarnings("unused")
         Bean bean : intIndex.lookup("+1") ) {
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
