package util.dump;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import gnu.trove.TLongCollection;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import util.dump.stream.ExternalizableObjectInputStream;
import util.dump.stream.SingleTypeObjectInputStream;
import util.reflection.FieldAccessor;


public class UniqueIndex<E> extends DumpIndex<E> {

   protected TObjectLongMap _lookupObject;
   protected TLongLongMap   _lookupLong;
   protected TIntLongMap    _lookupInt;


   public UniqueIndex( Dump<E> dump, FieldAccessor fieldAccessor ) {
      super(dump, fieldAccessor);
      init();
   }

   public UniqueIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      super(dump, fieldName);
      init();
   }

   @Override
   public void add( E o, long pos ) {
      try {
         if ( _fieldIsInt ) {
            int key = getIntKey(o);
            if ( _lookupInt.containsKey(key) ) {
               throw new DuplicateKeyException("Dump already contains an instance with the key " + key);
            }
            _lookupInt.put(key, pos);
            _lookupOutputStream.writeInt(key);
         } else if ( _fieldIsLong ) {
            long key = getLongKey(o);
            if ( _lookupLong.containsKey(key) ) {
               throw new DuplicateKeyException("Dump already contains an instance with the key " + key);
            }
            _lookupLong.put(key, pos);
            _lookupOutputStream.writeLong(key);
         } else {
            Object key = getObjectKey(o);
            if ( _lookupObject.containsKey(key) ) {
               throw new DuplicateKeyException("Dump already contains an instance with the key " + key);
            }
            _lookupObject.put(key, pos);
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
         return _lookupInt.containsKey(key) && !_dump._deletedPositions.contains(_lookupInt.get(key));
      }
   }

   @Override
   public boolean contains( long key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsLong ) {
            throw new IllegalArgumentException(
               "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate contains(.) method.");
         }
         return _lookupLong.containsKey(key) && !_dump._deletedPositions.contains(_lookupLong.get(key));
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
         return _lookupObject.containsKey(key) && !_dump._deletedPositions.contains(_lookupObject.get(key));
      }
   }

   public int[] getAllIntKeys() {
      return _lookupInt.keys();
   }

   public long[] getAllLongKeys() {
      return _lookupLong.keys();
   }

   public Object[] getAllObjectKeys() {
      return _lookupObject.keys();
   }

   @Override
   public TLongList getAllPositions() {
      TLongList pos = new TLongArrayList(100000, 10000);
      TLongCollection c = _fieldIsInt ? _lookupInt.valueCollection() : (_fieldIsLong ? _lookupLong.valueCollection() : _lookupObject.valueCollection());
      for ( TLongIterator iterator = c.iterator(); iterator.hasNext(); ) {
         long p = iterator.next();
         if ( !_dump._deletedPositions.contains(p) ) {
            pos.add(p);
         }
      }
      pos.sort();
      return pos;
   }

   public Object getKey( E o ) {
      if ( _fieldIsInt ) {
         return getIntKey(o);
      }
      if ( _fieldIsLong ) {
         return getLongKey(o);
      }
      return getObjectKey(o);
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

   public E lookup( int key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsInt ) {
            throw new IllegalArgumentException(
               "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return (E)null;
         }
         return _dump.get(pos);
      }
   }

   public E lookup( long key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsLong ) {
            throw new IllegalArgumentException(
               "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return (E)null;
         }
         return _dump.get(pos);
      }
   }

   public E lookup( Object key ) {
      synchronized ( _dump ) {
         if ( (_fieldIsLong || _fieldIsLongObject) && key instanceof Long ) {
            return lookup(((Long)key).longValue());
         }
         if ( (_fieldIsInt || _fieldIsIntObject) && key instanceof Integer ) {
            return lookup(((Integer)key).intValue());
         }
         if ( _fieldIsLong || _fieldIsInt ) {
            throw new IllegalArgumentException(
               "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return (E)null;
         }
         return _dump.get(pos);
      }
   }

   protected void cachePayload( long pos, Object payload ) {}

   @Override
   protected String getIndexType() {
      return UniqueIndex.class.getSimpleName();
   }

   protected long getPosition( int key ) {
      if ( !_fieldIsInt ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate getPosition(.) method.");
      }
      if ( !_lookupInt.containsKey(key) ) {
         return -1;
      }
      return _lookupInt.get(key);
   }

   protected long getPosition( long key ) {
      if ( !_fieldIsLong ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate getPosition(.) method.");
      }
      if ( !_lookupLong.containsKey(key) ) {
         return -1;
      }
      return _lookupLong.get(key);
   }

   protected long getPosition( Object key ) {
      if ( (_fieldIsLong || _fieldIsLongObject) && key instanceof Long ) {
         return getPosition(((Long)key).longValue());
      }
      if ( (_fieldIsInt || _fieldIsIntObject) && key instanceof Integer ) {
         return getPosition(((Integer)key).intValue());
      }
      if ( _fieldIsLong || _fieldIsInt ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate getPosition(.) method.");
      }
      if ( !_lookupObject.containsKey(key) ) {
         return -1;
      }
      return _lookupObject.get(key);
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
         _lookupInt = new TIntLongHashMap();
      } else if ( _fieldIsLong ) {
         _lookupLong = new TLongLongHashMap();
      } else {
         _lookupObject = new TObjectLongHashMap();
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
               // since we do a _updatesFile.exists() this is most unlikely
               throw new RuntimeException("Failed read updates from " + getUpdatesFile(), argh);
            }
         }

         boolean mayEOF = true;
         if ( _fieldIsInt ) {
            int size = (int)(getLookupFile().length() / (4 + 8));
            size = Math.max(10000, size);
            _lookupInt = new TIntLongHashMap(size);
            DataInputStream in = null;
            try {
               in = new DataInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
               while ( true ) {
                  Object payload = readPayload(in);
                  if(payload != null)
                     mayEOF = false;
                  int key = in.readInt();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( _lookupInt.containsKey(key) ) {
                     throw new DuplicateKeyException("index lookup " + getLookupFile() + " is broken - contains non unique key " + key);
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     _lookupInt.put(key, pos);
                     cachePayload(pos, payload);
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
         } else if ( _fieldIsLong ) {
            int size = (int)(getLookupFile().length() / (8 + 8));
            size = Math.max(10000, size);
            _lookupLong = new TLongLongHashMap(size);
            DataInputStream in = null;
            try {
               in = new DataInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
               while ( true ) {
                  Object payload = readPayload(in);
                  if(payload != null)
                     mayEOF = false;
                  long key = in.readLong();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( _lookupLong.containsKey(key) ) {
                     throw new DuplicateKeyException("index lookup " + getLookupFile() + " is broken - contains non unique key " + key);
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     _lookupLong.put(key, pos);
                     cachePayload(pos, payload);
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
         } else if ( _fieldIsString ) {
            int size = (int)(getLookupFile().length() / (10 + 8)); // let's assume an average length of the String keys of 10 bytes
            size = Math.max(10000, size);
            _lookupObject = new TObjectLongHashMap(size);
            DataInputStream in = null;
            try {
               in = new DataInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
               while ( true ) {
                  Object payload = readPayload(in);
                  if(payload != null)
                     mayEOF = false;
                  String key = in.readUTF();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( _lookupObject.containsKey(key) ) {
                     throw new DuplicateKeyException("index lookup " + getLookupFile() + " is broken - contains non unique key " + key);
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     _lookupObject.put(key, pos);
                     cachePayload(pos, payload);
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
         } else {
            int size = (int)(getLookupFile().length() / (20 + 8)); // let's assume an average length of the keys of 20 bytes
            size = Math.max(10000, size);
            _lookupObject = new TObjectLongHashMap(size);
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
                  Object payload = readPayload(in);
                  if(payload != null)
                     mayEOF = false;
                  Object key = in.readObject();
                  mayEOF = false;
                  long pos = in.readLong();
                  mayEOF = true;
                  if ( positionsToIgnore.get(pos) > 0 ) {
                     positionsToIgnore.adjustValue(pos, -1);
                     continue;
                  }
                  if ( _lookupObject.containsKey(key) ) {
                     throw new DuplicateKeyException("index lookup " + getLookupFile() + " is broken - contains non unique key " + key);
                  }
                  if ( !_dump._deletedPositions.contains(pos) ) {
                     _lookupObject.put(key, pos);
                     cachePayload(pos, payload);
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
   }

   protected Object readPayload( DataInput in ) throws IOException {
      return null;
   }

   @Override
   void delete( E o, long pos ) {
      if ( _fieldIsInt ) {
         int key = getIntKey(o);
         long p = _lookupInt.get(key);
         if ( p == pos ) {
            _lookupInt.remove(key);
         }
      } else if ( _fieldIsLong ) {
         long key = getLongKey(o);
         long p = _lookupLong.get(key);
         if ( p == pos ) {
            _lookupLong.remove(key);
         }
      } else {
         Object key = getObjectKey(o);
         long p = _lookupObject.get(key);
         if ( p == pos ) {
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
   };


   /**
    * This Exception is thrown, when trying to add a non-unique index-value to a dump.
    */
   public static class DuplicateKeyException extends RuntimeException {

      private static final long serialVersionUID = -7959993269514169802L;


      public DuplicateKeyException( String message ) {
         super(message);
      }
   }

}
