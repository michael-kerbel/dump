package util.dump;

import java.util.Iterator;
import java.util.NoSuchElementException;

import util.dump.UniqueIndex.DuplicateKeyException;
import util.dump.reflection.FieldAccessor;


/**
 * Provides an index for usage with {@link Dump} instances which is optimized for storing items with non-unique, consecutive keys.
 * The items having the same keys must be added en bloc. The benefit of this constraint is less memory consumption compared with a 
 * {@link GroupIndex}: The memory used is about 16 bytes * number of groups * 120% overhead for indexes with <code>long</code> keys.<p/> 
 * The index key can be of any type and must be accessable with a {@link FieldAccessor}, i.e. either a Field or a Method 
 * of your bean <code>E</code> must return the key.<p/>   
 * The indexed field should be either int, long, String or Externalizable for good initialization and writing performance.<p/>
 * <b>Beware</b>: Your key instances are used in a HashMap, so you probably want to implement <nobr><code>hashCode()</code></nobr> and 
 * <nobr><code>equals()</code></nobr> accordingly, if you use a custom key instance (i.e. not <code>int</code>, <code>long</code>, 
 * {@link String}, any {@link Number}, ...)<p/>
 */
public class GroupedIndex<E> implements NonUniqueIndex<E> {

   private MyUniqueIndex _index;

   private E             _lastKey;
   private int           _lastKeyNumber;
   private long          _lastKeyPos;


   public GroupedIndex( Dump<E> dump, FieldAccessor fieldAccessor ) {
      _index = new MyUniqueIndex(dump, fieldAccessor);
   }

   public GroupedIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      _index = new MyUniqueIndex(dump, fieldName);
   }

   public void add( E o, long pos ) {
      if ( _index._fieldIsInt ) {
         int key = _index.getIntKey(o);
         if ( _lastKey != null && _index.getIntKey(_lastKey) == key ) {
            _lastKeyNumber++;
         } else if ( _index._lookupInt.containsKey(key) ) {
            throw new DuplicateKeyException("Dump already contains a group with the key " + key);
         } else {
            _lastKey = o;
            _lastKeyNumber = 1;
            _lastKeyPos = pos;
            _index.superAdd(_lastKey, _lastKeyPos);
         }
      } else if ( _index._fieldIsLong ) {
         long key = _index.getLongKey(o);
         if ( _lastKey != null && _index.getLongKey(_lastKey) == key ) {
            _lastKeyNumber++;
         } else if ( _index._lookupLong.containsKey(key) ) {
            throw new DuplicateKeyException("Dump already contains a group with the key " + key);
         } else {
            _lastKey = o;
            _lastKeyNumber = 1;
            _lastKeyPos = pos;
            _index.superAdd(_lastKey, _lastKeyPos);
         }
      } else {
         Object key = _index.getObjectKey(o);
         if ( _lastKey != null && _index.getObjectKey(_lastKey).equals(key) ) {
            _lastKeyNumber++;
         } else if ( _index._lookupObject.containsKey(key) ) {
            throw new DuplicateKeyException("Dump already contains a group with the key " + key);
         } else {
            _lastKey = o;
            _lastKeyNumber = 1;
            _lastKeyPos = pos;
            _index.superAdd(_lastKey, _lastKeyPos);
         }
      }
   }

   public synchronized boolean contains( int key ) {
      // TODO doesn't check if all elements in the group are deleted!
      return _index.contains(key);
   }

   public synchronized boolean contains( long key ) {
      // TODO doesn't check if all elements in the group are deleted!
      return _index.contains(key);
   }

   public synchronized boolean contains( Object key ) {
      // TODO doesn't check if all elements in the group are deleted!
      return _index.contains(key);
   }

   @Override
   public synchronized Iterable<E> lookup( int key ) {
      if ( !_index._fieldIsInt ) {
         throw new IllegalArgumentException("The type of the used key class of this index is " + _index._fieldAccessor.getType()
            + ". Please use the appropriate lookup(.) method.");
      }
      if ( _index._lookupInt.containsKey(key) ) {
         return new GroupIterable(key, _index._lookupInt.get(key));
      }
      return new GroupIterable(key, -1);
   }

   @Override
   public synchronized Iterable<E> lookup( long key ) {
      if ( !_index._fieldIsLong ) {
         throw new IllegalArgumentException("The type of the used key class of this index is " + _index._fieldAccessor.getType()
            + ". Please use the appropriate lookup(.) method.");
      }
      if ( _index._lookupLong.containsKey(key) ) {
         return new GroupIterable(key, _index._lookupLong.get(key));
      }
      return new GroupIterable(key, -1);
   }

   @Override
   public synchronized Iterable<E> lookup( Object key ) {
      if ( (_index._fieldIsLong || _index._fieldIsLongObject) && key instanceof Long ) {
         return lookup(((Long)key).longValue());
      }
      if ( (_index._fieldIsInt || _index._fieldIsIntObject) && key instanceof Integer ) {
         return lookup(((Integer)key).intValue());
      }
      if ( _index._fieldIsLong || _index._fieldIsInt ) {
         throw new IllegalArgumentException("The type of the used key class of this index is " + _index._fieldAccessor.getType()
            + ". Please use the appropriate lookup(.) method.");
      }
      if ( _index._lookupObject.containsKey(key) ) {
         return new GroupIterable(key, _index._lookupObject.get(key));
      }
      return new GroupIterable(key, -1);
   }


   private final class GroupIterable implements Iterable<E> {

      private final long   _pos;
      private final Object _key;


      public GroupIterable( Object key, long pos ) {
         _key = key;
         _pos = pos;
      }

      @Override
      public Iterator<E> iterator() {
         return new GroupIterator(_key, _pos);
      }
   }

   private final class GroupIterator implements Iterator<E> {

      private final Object _key;
      private long         _pos;
      private E            _e;


      private GroupIterator( Object key, long pos ) {
         _key = key;
         _pos = pos;
         findNextUndeleted(); // sets _e if an undeleted instance exists
      }

      @Override
      public boolean hasNext() {
         return _pos >= 0 && _e != null;
      }

      @Override
      public E next() {
         if ( _e == null ) {
            throw new NoSuchElementException();
         }

         synchronized ( _index._dump ) { // we have to synchronize because of the access on _nextItemPos
            _e = _index._dump.get(_pos);
            _pos = _index._dump._nextItemPos.get();
         }

         E e = _e;
         findNextUndeleted();
         return e;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      private void findNextUndeleted() {
         if ( _pos < 0 ) {
            return;
         }

         synchronized ( _index._dump ) {
            _e = _index._dump.get(_pos);
            while ( _e == null ) { // we have to synchronize because of the access on _nextItemPos
               _pos = _index._dump._nextItemPos.get();
               if ( _pos >= _index._dump._outputStream._n ) {
                  break;
               }
               _e = _index._dump.get(_pos);
            }
         }

         if ( _e != null && !_index.getObjectKey(_e).equals(_key) ) {
            _e = (E)null;
         }
      }
   }

   private class MyUniqueIndex extends UniqueIndex<E> {

      public MyUniqueIndex( Dump<E> dump, FieldAccessor fieldAccessor ) {
         super(dump, fieldAccessor);
      }

      public MyUniqueIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
         super(dump, fieldName);
      }

      @Override
      public void add( E o, long pos ) {
         if ( _index == null ) {
            /* This is the case during initFromDump(.) which is invoked from the constructor of GroupedIndex. */
            _index = this;
         }
         GroupedIndex.this.add(o, pos);
      }

      public void superAdd( E o, long pos ) {
         super.add(o, pos);
      }

      @Override
      protected String getIndexType() {
         return GroupedIndex.class.getSimpleName();
      }

      @Override
      void delete( E o, long pos ) {
         // don't delete the whole group!
      }

      @Override
      void update( long pos, E oldItem, E newItem ) {
         if ( !isUpdatable(oldItem, newItem) ) {
            throw new UnsupportedOperationException("GroupedIndex doesn't support updates.");
         }
      }
   }

}
