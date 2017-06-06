package util.dump;

import static org.fest.assertions.Assertions.assertThat;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import org.junit.Test;

import util.dump.GroupIndex.Positions;
import util.reflection.FieldAccessor;


public class GroupIndexTest extends AbstractGroupIndexTest {

   public GroupIndexTest( int dumpSize ) {
      super(dumpSize);
   }

   @Test
   public void testExternalizableKeyIndex() throws Exception {
      testIndex("_groupExternalizable", new GroupIndexTestConfig() {

         @Override
         public Object createKey( int id ) {
            return new ExternalizableId(id);
         }
      });
   }

   @Test
   public void testGetNumKeysInt() throws Exception {
      final int NUM_EXPECTED_GROUPS = 5;
      Dump<Bean> dump = prepareDump(NUM_EXPECTED_GROUPS);
      try {
         GroupIndex<Bean> intIndex = new GroupIndex<Bean>(dump, "_groupInt");
         assertThat(intIndex.getNumKeys()).isEqualTo(NUM_EXPECTED_GROUPS);

         int numDeletions = 0;
         for ( @SuppressWarnings("unused")
         Bean bean : intIndex.lookup(1) ) {
            dump.deleteLast();
            numDeletions++;
         }
         assertThat(numDeletions).isPositive();

         assertThat(intIndex.getNumKeys()).isEqualTo(NUM_EXPECTED_GROUPS - 1);
      }
      finally {
         dump.close();
      }
   }

   @Test
   public void testIntKeyIndex() throws Exception {
      testIndex("_groupInt", new GroupIndexTestConfig() {

         @Override
         public Object createKey( int id ) {
            return Integer.valueOf(id);
         }
      });
   }

   @Test
   public void testLongKeyIndex() throws Exception {
      testIndex("_groupLong", new GroupIndexTestConfig() {

         @Override
         public Object createKey( int id ) {
            return Long.valueOf(id);
         }
      });
   }

   @Test
   public void testRemovePosition() throws Exception {

      for ( int x = 0; x < 100; x++ ) {

         long valueToDelete = 0;

         TLongSet before = new TLongHashSet();

         int size = _random.nextInt(10000);
         Positions array = new Positions();
         for ( int i = 0; i < size; i++ ) {
            // we need a set of values (no duplicates!)
            long value;
            do {
               value = _random.nextLong();
            }
            while ( before.contains(value) );
            before.add(value);

            if ( _random.nextFloat() < 1 / 10000.0 ) {
               valueToDelete = value;
            }
            array.add(value);
         }

         assertThat(before.size()).isEqualTo(array.size());

         Positions newArray = GroupIndex.removePosition(array, valueToDelete);

         TLongSet after = new TLongHashSet(newArray);

         before.remove(valueToDelete);
         assertThat(before).isEqualTo(after);
      }
   }

   @Test
   public void testStringKeyIndex() throws Exception {
      testIndex("_groupString", new GroupIndexTestConfig() {

         @Override
         public Object createKey( int id ) {
            return (id < 0 ? "" : "+") + id;
         }
      });
   }


   public abstract static class GroupIndexTestConfig extends TestConfiguration {

      @Override
      public NonUniqueIndex createIndex( Dump dump, FieldAccessor fieldAccessor ) {
         return new GroupIndex<Bean>(dump, fieldAccessor);
      }
   }
}
