package util.dump;

import java.util.Iterator;
import java.util.NoSuchElementException;


public class CombinedIndex<E> {

   private GroupIndex<E>[] _indexes;

   public CombinedIndex( GroupIndex<E>... indexes ) {
      if ( indexes == null ) {
         throw new IllegalArgumentException("The indexes of an CombinedIndex may not be null.");
      }
      if ( indexes.length < 2 ) {
         throw new IllegalArgumentException("You need at least two indexes for a CombinedIndex.");
      }
      _indexes = new GroupIndex[indexes.length];
      System.arraycopy(indexes, 0, _indexes, 0, indexes.length);
      for ( int i = 1, length = indexes.length; i < length; i++ ) {
         if ( indexes[0]._dump != indexes[i]._dump ) {
            throw new IllegalArgumentException("All indexes for a CombinedIndex must belong to the same dump. " + indexes[0]._dump.getDumpFile() + " != "
               + indexes[i]._dump.getDumpFile());
         }
      }
   }

   public synchronized Iterable<E> lookup( Object... keys ) {
      if ( keys == null ) {
         throw new IllegalArgumentException("Keys for CombinedIndex.lookup(keys) may not be null");
      }
      if ( keys.length != _indexes.length ) {
         throw new IllegalArgumentException("The number of keys for CombinedIndex.lookup(keys) must match the number of GroupIndexes of this CombinedIndex, "
            + keys.length + " != " + _indexes.length);
      }

      long[][] positions = new long[keys.length][];
      for ( int i = 0, length = keys.length; i < length; i++ ) {
         positions[i] = getPositions(_indexes[i], keys[i]);
      }

      return new CombinedGroupIterable(positions);
   }

   protected long[] getPositions( GroupIndex<E> groupIndex, Object key ) {
      if ( groupIndex._fieldIsInt ) {
         if ( !(key instanceof Integer) ) {
            throw new IllegalArgumentException("The type of the key used in lookup doesn't match the GroupIndexes key type, " + key.getClass() + " != "
               + groupIndex._fieldAccessor.getType());
         }
         int k = (Integer)key;
         return groupIndex.getPositions(k);
      }
      if ( groupIndex._fieldIsLong ) {
         if ( !(key instanceof Long) ) {
            throw new IllegalArgumentException("The type of the key used in lookup doesn't match the GroupIndexes key type, " + key.getClass() + " != "
               + groupIndex._fieldAccessor.getType());
         }
         long k = (Long)key;
         return groupIndex.getPositions(k);
      }
      return groupIndex.getPositions(key);
   }

   private final class CombinedGroupIterable implements Iterable<E> {

      private final long[][] _pos;

      public CombinedGroupIterable( long[][] pos ) {
         _pos = pos;
      }

      public Iterator<E> iterator() {
         return new CombinedGroupIterator(_pos);
      }
   }

   private final class CombinedGroupIterator implements Iterator<E> {

      private final long[][] _pos;
      private final int[]    _i;
      private boolean        _searchOnHasNext = true; // this is not thread safe in the least...
      private boolean        _hasNext         = false;

      private CombinedGroupIterator( long[][] pos ) {
         _pos = pos;
         _i = new int[pos.length];
         _i[0] = -1;
      }

      public boolean hasNext() {
         if ( _searchOnHasNext ) {
            boolean allArraysEqual = false;
            while ( !allArraysEqual || _i[0] == -1 || _indexes[0]._dump._deletedPositions.contains(_pos[0][_i[0]]) ) {
               allArraysEqual = true;
               _i[0] = _i[0] + 1;
               if ( _i[0] >= _pos[0].length ) {
                  _hasNext = false;
                  _searchOnHasNext = false;
                  return _hasNext;
               }
               // the following works, because _pos[i] are expected to be sorted 
               long pos0 = _pos[0][_i[0]];
               for ( int i = 1, length = _pos.length; i < length; i++ ) {
                  while ( _i[i] < _pos[i].length && _pos[i][_i[i]] < pos0 ) {
                     _i[i] = _i[i] + 1;
                  }
                  if ( _i[i] >= _pos[i].length ) {
                     _hasNext = false;
                     _searchOnHasNext = false;
                     return _hasNext;
                  }
                  if ( _pos[i][_i[i]] > pos0 ) {
                     allArraysEqual = false;
                  }
               }
            }
            _hasNext = true;
            _searchOnHasNext = false;
         }
         return _hasNext;
      }

      public E next() {
         if ( !_hasNext ) {
            throw new NoSuchElementException();
         }
         _searchOnHasNext = true;
         return _indexes[0]._dump.get(_pos[0][_i[0]]);
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }
   }

}
