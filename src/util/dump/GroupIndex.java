package util.dump;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.procedure.TLongObjectProcedure;
import util.dump.stream.ExternalizableObjectInputStream;
import util.dump.stream.SingleTypeObjectInputStream;
import util.reflection.FieldAccessor;


/**
 * Allows lookup of non-unique keys, i.e. a lookup returns an Iterable instead of a single element.
 * If the key is null, the value is not stored in the index, so null is not treated as key. This 
 * behaviour allows sparse indexes.  
 */
public class GroupIndex<E> extends DumpIndex<E>implements NonUniqueIndex<E> {

   protected static Positions removePosition( Positions positions, long pos ) {

      if ( positions == null ) {
         return new Positions();
      }
      int length = positions.size();
      if ( length == 0 ) {
         return positions;
      }

      if ( length == 1 ) {
         return pos == positions.get(0) ? new Positions() : positions;
      }

      int idx = length - 1;
      for ( ; idx >= 0; idx-- ) {
         if ( positions.getQuick(idx) == pos ) {
            break;
         }
      }

      if ( idx < 0 ) {
         // no element found
         return positions;
      }

      positions.remove(idx, 1);

      return positions;
   }


   protected Map<Object, Positions>    _lookupObject;
   protected TLongObjectMap<Positions> _lookupLong;
   protected TIntObjectMap<Positions>  _lookupInt;


   public GroupIndex( Dump<E> dump, FieldAccessor fieldAccessor ) {
      super(dump, fieldAccessor);
      init();
   }

   public GroupIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      super(dump, fieldName);
      init();
   }

   GroupIndex( Dump<E> dump, FieldAccessor fieldAccessor, File lookupFile ) {
      super(dump, fieldAccessor, lookupFile);
      init();
   }

   @Override
   public void add( E o, long pos ) {
      try {
         if ( _fieldIsInt ) {
            int key = getIntKey(o);
            Positions positions = _lookupInt.get(key);
            positions = addPosition(positions, pos);
            _lookupInt.put(key, positions);
            _lookupOutputStream.writeInt(key);
         } else if ( _fieldIsLong ) {
            long key = getLongKey(o);
            Positions positions = _lookupLong.get(key);
            positions = addPosition(positions, pos);
            _lookupLong.put(key, positions);
            _lookupOutputStream.writeLong(key);
         } else {
            Object key = getObjectKey(o);
            if ( key == null ) {
               return;
            }
            Positions positions = _lookupObject.get(key);
            positions = addPosition(positions, pos);
            _lookupObject.put(key, positions);
            if ( _fieldIsString ) {
               _lookupOutputStream.writeUTF(key.toString());
            } else {
               ((ObjectOutput)_lookupOutputStream).writeObject(key);
            }
         }

         _lookupOutputStream.writeLong(pos);
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to add key to index " + getLookupFile(), argh);
      }
   }

   @Override
   public boolean contains( int key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsInt ) {
            throw new IllegalArgumentException(
               "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate contains(.) method.");
         }
         Positions pos = _lookupInt.get(key);
         ensureSorting(pos);
         return contains(pos);
      }
   }

   @Override
   public boolean contains( long key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsLong ) {
            throw new IllegalArgumentException(
               "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate contains(.) method.");
         }
         Positions pos = _lookupLong.get(key);
         ensureSorting(pos);
         return contains(pos);
      }
   }

   @Override
   public boolean contains( Object key ) {
      synchronized ( _dump ) {
         if ( (_fieldIsLong || _fieldIsLongObject) && key instanceof Long ) {
            return contains(((Long)key).longValue());
         }
         if ( (_fieldIsInt || _fieldIsIntObject) && key instanceof Integer ) {
            return contains(((Integer)key).intValue());
         }
         if ( _fieldIsLong || _fieldIsInt ) {
            throw new IllegalArgumentException(
               "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate contains(.) method.");
         }
         Positions pos = _lookupObject.get(key);
         ensureSorting(pos);
         return contains(pos);
      }
   }

   @Override
   public TLongList getAllPositions() {
      TLongList pos = new TLongArrayList(100000);
      Collection<Positions> c = _fieldIsInt ? _lookupInt.valueCollection() : (_fieldIsLong ? _lookupLong.valueCollection() : _lookupObject.values());
      for ( Positions p : c ) {
         ensureSorting(p);
         for ( int i = 0, length = p.size(); i < length; i++ ) {
            long pp = p.get(i);
            if ( !_dump._deletedPositions.contains(pp) ) {
               pos.add(pp);
            }
         }
      }
      pos.sort();
      return pos;
   }

   @Override
   public int getNumKeys() {
      if ( _lookupObject != null ) {
         return _lookupObject.size();
      }
      if ( _lookupLong != null ) {
         return _lookupLong.size();
      }
      if ( _lookupInt != null ) {
         return _lookupInt.size();
      }
      throw new IllegalStateException("weird, all lookup maps are null");
   }

   public int getNumValues( long key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return countLivePositions(pos);
      }
   }

   public int getNumValues( Object key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return countLivePositions(pos);
      }
   }

   public int getNumValues( int key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return countLivePositions(pos);
      }
   }

   @Override
   public Iterable<E> lookup( int key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return new GroupIterable(pos);
      }
   }

   @Override
   public Iterable<E> lookup( long key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return new GroupIterable(pos);
      }
   }

   @Override
   public Iterable<E> lookup( Object key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return new GroupIterable(pos);
      }
   }

   @Override
   protected String getIndexType() {
      return GroupIndex.class.getSimpleName();
   }

   /**
    * @return <b>BEWARE</b>: the position may contain deleted dump positions!
    */
   protected long[] getPositions( int key ) {
      if ( !_fieldIsInt ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
      }
      Positions pos = _lookupInt.get(key);
      if ( pos == null ) {
         return new long[0];
      }
      ensureSorting(pos);
      return pos.toArray();
   }

   /**
    * @return <b>BEWARE</b>: the position may contain deleted dump positions!
    */
   protected long[] getPositions( long key ) {
      if ( !_fieldIsLong ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
      }
      Positions pos = _lookupLong.get(key);
      if ( pos == null ) {
         return new long[0];
      }
      ensureSorting(pos);
      return pos.toArray();
   }

   /**
    * @return <b>BEWARE</b>: the position may contain deleted dump positions!
    */
   protected long[] getPositions( Object key ) {
      if ( (_fieldIsLong || _fieldIsLongObject) && key instanceof Long ) {
         return getPositions(((Long)key).longValue());
      }
      if ( (_fieldIsInt || _fieldIsIntObject) && key instanceof Integer ) {
         return getPositions(((Integer)key).intValue());
      }
      if ( _fieldIsLong || _fieldIsInt ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
      }
      Positions pos = _lookupObject.get(key);
      if ( pos == null ) {
         return new long[0];
      }
      ensureSorting(pos);
      return pos.toArray();
   }

   @Override
   protected void initFromDump() {
      super.initFromDump();

      try {
         closeAndDeleteUpdatesOutput();
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to delete updates file " + getUpdatesFile(), argh);
      }
   }

   @Override
   protected void initLookupMap() {
      if ( _fieldIsInt ) {
         _lookupInt = new TIntObjectHashMap<>();
      } else if ( _fieldIsLong ) {
         _lookupLong = new TLongObjectHashMap<>();
      } else {
         _lookupObject = new HashMap<>();
      }
   }

@Override
   protected void load() {
      if ( !getLookupFile().exists() || getLookupFile().length() == 0 ) {
         return;
      }

      DataInputStream updatesInput = null;
      TLongIntMap positionsToIgnore = new TLongIntHashMap();
      try {
         if ( getUpdatesFile().exists() ) {
            if ( getUpdatesFile().length() % 8 != 0 ) {
               throw new RuntimeException("Index corrupted: " + getUpdatesFile() + " has unbalanced size.");
            }
            try {
               updatesInput = new DataInputStream(new BufferedInputStream(new FileInputStream(getUpdatesFile()), DumpReader.DEFAULT_BUFFER_SIZE));
               long pos;
               while ( (pos = readNextPosition(updatesInput)) != -1 ) {
                  positionsToIgnore.adjustOrPutValue(pos, 1, 1);
               }
            }
            catch ( FileNotFoundException argh ) {
               // since we do a getUpdatesFile().exists() this is most unlikely
               throw new RuntimeException("Failed read updates from " + getUpdatesFile(), argh);
            }
         }

         boolean mayEOF = true;
         if ( _fieldIsInt ) {
            TIntObjectMap<Positions> dynamicLookupInt = new TIntObjectHashMap<>(10000);
            DataInputStream in = null;
            try {
               in = new DataInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
               while ( true ) {
                  int key = in.readInt();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     Positions positions = dynamicLookupInt.get(key);
                     if ( positions == null ) {
                        positions = new Positions();
                        dynamicLookupInt.put(key, positions);
                     }
                     positions.add(pos);
                  }
               }
            }
            catch ( EOFException argh ) {
               if ( !mayEOF ) {
                  throw new RuntimeException("Failed to read lookup from " + getLookupFile() + ", file is unbalanced - unexpected EoF", argh);
               }
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to read lookup from " + getLookupFile(), argh);
            }
            finally {
               if ( in != null ) {
                  try {
                     in.close();
                  }
                  catch ( IOException argh ) {
                     throw new RuntimeException("Failed to close input stream.", argh);
                  }
               }
            }
            // optimize memory consumption of lookup
            final TIntObjectMap<Positions> lookupInt = new TIntObjectHashMap<>(Math.max(1000, dynamicLookupInt.size()));
            dynamicLookupInt.forEachEntry(new TIntObjectProcedure<Positions>() {

               @Override
               public boolean execute( int key, Positions positions ) {
                  positions.sort();
                  lookupInt.put(key, positions);
                  return true;
               }
            });
            _lookupInt = lookupInt;

         } else if ( _fieldIsLong ) {
            TLongObjectMap<Positions> dynamicLookupLong = new TLongObjectHashMap<>(10000);
            DataInputStream in = null;
            try {
               in = new DataInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
               while ( true ) {
                  long key = in.readLong();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     Positions positions = dynamicLookupLong.get(key);
                     if ( positions == null ) {
                        positions = new Positions();
                        dynamicLookupLong.put(key, positions);
                     }
                     positions.add(pos);
                  }
               }
            }
            catch ( EOFException argh ) {
               if ( !mayEOF ) {
                  throw new RuntimeException("Failed to read lookup from " + getLookupFile() + ", file is unbalanced - unexpected EoF", argh);
               }
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to read lookup from " + getLookupFile(), argh);
            }
            finally {
               if ( in != null ) {
                  try {
                     in.close();
                  }
                  catch ( IOException argh ) {
                     throw new RuntimeException("Failed to close input stream.", argh);
                  }
               }
            }
            // optimize memory consumption of lookup
            final TLongObjectMap<Positions> lookupLong = new TLongObjectHashMap<>(Math.max(1000, dynamicLookupLong.size()));
            dynamicLookupLong.forEachEntry(new TLongObjectProcedure<Positions>() {

               @Override
               public boolean execute( long key, Positions positions ) {
                  positions.sort();
                  lookupLong.put(key, positions);
                  return true;
               }
            });
            _lookupLong = lookupLong;

         } else if ( _fieldIsString ) {
            HashMap<Object, Positions> lookupObject = new HashMap<>(10000);
            DataInputStream in = null;
            try {
               in = new DataInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
               while ( true ) {
                  String key = in.readUTF();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     Positions positions = lookupObject.get(key);
                     if ( positions == null ) {
                        positions = new Positions();
                        lookupObject.put(key, positions);
                     }
                     positions.add(pos);
                  }
               }
            }
            catch ( EOFException argh ) {
               if ( !mayEOF ) {
                  throw new RuntimeException("Failed to read lookup from " + getLookupFile() + ", file is unbalanced - unexpected EoF", argh);
               }
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to read lookup from " + getLookupFile(), argh);
            }
            finally {
               if ( in != null ) {
                  try {
                     in.close();
                  }
                  catch ( IOException argh ) {
                     throw new RuntimeException("Failed to close input stream.", argh);
                  }
               }
            }
            // optimize memory consumption of lookup
            _lookupObject = new HashMap<>((int)(lookupObject.size() / 0.75f) + 10); // 0.75f is the default for HashMap
            for ( Iterator<Map.Entry<Object, Positions>> iterator = lookupObject.entrySet().iterator(); iterator.hasNext(); ) {
               Map.Entry<Object, Positions> e = iterator.next();
               Object key = e.getKey();
               Positions positions = e.getValue();
               positions.sort();
               _lookupObject.put(key, positions);
               iterator.remove(); // for gc
            }

         } else {
            HashMap<Object, Positions> lookupObject = new HashMap<>(10000);
            ObjectInput in = null;
            try {
               if ( _fieldIsExternalizable ) {
                  in = new SingleTypeObjectInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())), _fieldAccessor.getType());
               } else {
                  in = new ExternalizableObjectInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
               }
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to initialize dump index with lookup file " + getLookupFile(), argh);
            }
            try {
               while ( true ) {
                  Object key = in.readObject();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     Positions positions = lookupObject.get(key);
                     if ( positions == null ) {
                        positions = new Positions();
                        lookupObject.put(key, positions);
                     }
                     positions.add(pos);
                  }
               }
            }
            catch ( EOFException argh ) {
               if ( !mayEOF ) {
                  throw new RuntimeException("Failed to read lookup from " + getLookupFile() + ", file is unbalanced - unexpected EoF", argh);
               }
            }
            catch ( ClassNotFoundException | IOException argh ) {
               throw new RuntimeException("Failed to read lookup from " + getLookupFile(), argh);
            }
            finally {
               try {
                  in.close();
               }
               catch ( IOException argh ) {
                  throw new RuntimeException("Failed to close input stream.", argh);
               }
            }
            // optimize memory consumption of lookup
            _lookupObject = new HashMap<>((int)(lookupObject.size() / 0.75f) + 10); // 0.75f is the default for HashMap
            for ( Iterator<Map.Entry<Object, Positions>> iterator = lookupObject.entrySet().iterator(); iterator.hasNext(); ) {
               Map.Entry<Object, Positions> e = iterator.next();
               Object key = e.getKey();
               Positions positions = e.getValue();
               positions.sort();
               _lookupObject.put(key, positions);
               iterator.remove(); // for gc
            }
         }
      }
      finally {
         if ( updatesInput != null ) {
            try {
               updatesInput.close();
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to close updates stream.", argh);
            }
         }
      }
   }

      protected long readNextPosition( DataInputStream updatesInput ) {
      if ( updatesInput == null ) {
         return -1;
      }
      try {
         return updatesInput.readLong();
      }
      catch ( EOFException argh ) {
         return -1;
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to read updates from " + getUpdatesFile(), argh);
      }
   };

   @Override
   void delete( E o, long pos ) {
      if ( _fieldIsInt ) {
         int key = getIntKey(o);
         Positions positions = _lookupInt.get(key);
         positions = removePosition(positions, pos);
         if ( positions.size() == 0 ) {
            _lookupInt.remove(key);
         }
      } else if ( _fieldIsLong ) {
         long key = getLongKey(o);
         Positions positions = _lookupLong.get(key);
         positions = removePosition(positions, pos);
         if ( positions.size() == 0 ) {
            _lookupLong.remove(key);
         }
      } else {
         Object key = getObjectKey(o);
         if ( key == null ) {
            return;
         }
         Positions positions = _lookupObject.get(key);
         positions = removePosition(positions, pos);
         if ( positions.size() == 0 ) {
            _lookupObject.remove(key);
         }
      }
   }

   @Override
   boolean isUpdatable( E oldItem, E newItem ) {
      return true;
   }

   @Override
   void update( long pos, E oldItem, E newItem ) {
      boolean noChange = super.isUpdatable(oldItem, newItem);
      if ( noChange ) {
         return;
      }
      delete(oldItem, pos); // remove from memory

      try {
         // we add this position to the stream of ignored positions used during load()
         getUpdatesOutput().writeLong(pos);
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to append to updates file " + getUpdatesFile(), argh);
      }

      add(newItem, pos);
      /* This position is now twice in the index on disk, under different keys.
       * This is handled during load() using getUpdatesFile() */
   }

   private Positions addPosition( Positions positions, long pos ) {
      if ( positions == null ) {
         positions = new Positions();
      }
      positions.add(pos);
      return positions;
   }

   private boolean contains( Positions pos ) {
      if ( pos != null ) {
         for ( int i = 0, length = pos.size(); i < length; i++ ) {
            if ( !_dump._deletedPositions.contains(pos.getQuick(i)) ) {
               return true;
            }
         }
      }
      return false;
   }

   private int countLivePositions( long[] pos ) {
      int n = 0;
      for ( int i = 0, length = pos.length; i < length; i++ ) {
         if ( !_dump._deletedPositions.contains(pos[i]) )
            n++;
      }
      return n;
   }

   private void ensureSorting( Positions pos ) {
      if ( pos == null || pos.size() <= 1 ) {
         return;
      }
      if ( !pos.isSorted() ) {
         pos.sort();
      }
   }


   static class Positions extends TLongArrayList {

      private boolean _sorted;


      @Override
      public boolean add( long val ) {
         _sorted = false;
         return super.add(val);
      }

      public boolean isSorted() {
         return _sorted;
      }

      @Override
      public void sort() {
         _sorted = true;
         super.sort();
      }
   }

   private final class GroupIterable implements Iterable<E> {

      private final long[] _pos;


      public GroupIterable( long[] pos ) {
         _pos = pos;
      }

      @Override
      public Iterator<E> iterator() {
         return new GroupIterator(_pos);
      }

   }

   private final class GroupIterator implements Iterator<E> {

      private final long[] _pos;
      int                  _i = 0;


      private GroupIterator( long[] pos ) {
         _pos = pos;
         while ( _i < _pos.length && _dump._deletedPositions.contains(_pos[_i]) ) {
            _i++;
         }
      }

      @Override
      public boolean hasNext() {
         return _i < _pos.length;
      }

      @Override
      public E next() {
         if ( _i >= _pos.length ) {
            throw new NoSuchElementException();
         }
         E e = _dump.get(_pos[_i]);
         do {
            _i++;
         }
         while ( _i < _pos.length && _dump._deletedPositions.contains(_pos[_i]) );
         return e;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }
}
