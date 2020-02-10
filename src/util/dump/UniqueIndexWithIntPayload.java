package util.dump;

import java.io.DataInput;
import java.io.IOException;
import java.util.function.ToIntFunction;

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import util.reflection.FieldAccessor;


/**
 * This extends UniqueIndex with the capability to directly lookup an int that is calculated from E without the
 * need to load E itself. Not only does it reduce IO when doing this type of access, it also externalizes its state
 * so that this index is usable directly after instantiation.
 */
public class UniqueIndexWithIntPayload<E> extends UniqueIndex<E> {

   protected TLongIntMap _posToPayload;

   protected ToIntFunction<E> _payloadProvider;

   public UniqueIndexWithIntPayload( Dump<E> dump, FieldAccessor fieldAccessor, ToIntFunction<E> payloadProvider ) {
      super(dump, fieldAccessor);
      _payloadProvider = payloadProvider;
   }

   public UniqueIndexWithIntPayload( Dump<E> dump, String fieldName, ToIntFunction<E> payloadProvider ) throws NoSuchFieldException {
      super(dump, fieldName);
      _payloadProvider = payloadProvider;
   }

   @Override
   public void add( E o, long pos ) {
      synchronized ( _dump ) {
         int payload = _payloadProvider.applyAsInt(o);
         try {
            _lookupOutputStream.writeInt(payload);
         }
         catch ( IOException e ) {
            throw new RuntimeException("Failed to add key to index " + getLookupFile(), e);
         }
         _posToPayload.put(pos, payload);
      }

      super.add(o, pos);
   }

   public long lookupPayload( int key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsInt ) {
            throw new IllegalArgumentException(
                  "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookupPayload(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return 0;
         }
         return _posToPayload.get(pos);
      }
   }

   public long lookupPayload( long key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsLong ) {
            throw new IllegalArgumentException(
                  "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookupPayload(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return 0;
         }
         return _posToPayload.get(pos);
      }
   }

   public long lookupPayload( Object key ) {
      synchronized ( _dump ) {
         if ( (_fieldIsLong || _fieldIsLongObject) && key instanceof Long ) {
            return lookupPayload(((Long)key).longValue());
         }
         if ( (_fieldIsInt || _fieldIsIntObject) && key instanceof Integer ) {
            return lookupPayload(((Integer)key).intValue());
         }
         if ( _fieldIsLong || _fieldIsInt ) {
            throw new IllegalArgumentException(
                  "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookupPayload(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return 0;
         }
         return _posToPayload.get(pos);
      }
   }

   @Override
   protected void cachePayload( long pos, Object payload ) {
      _posToPayload.put(pos, (Integer)payload);
   }

   @Override
   protected String getIndexType() {
      return UniqueIndexWithIntPayload.class.getSimpleName();
   }

   @Override
   protected void initLookupMap() {
      super.initLookupMap();
      _posToPayload = new TLongIntHashMap();
   }

   @Override
   protected Object readPayload( DataInput in ) throws IOException {
      return in.readInt();
   }

   @Override
   void delete( E o, long pos ) {
      synchronized ( _dump ) {
         if ( _fieldIsInt ) {
            int key = getIntKey(o);
            long p = _lookupInt.get(key);
            if ( p == pos ) {
               _posToPayload.remove(pos);
            }
         } else if ( _fieldIsLong ) {
            long key = getLongKey(o);
            long p = _lookupLong.get(key);
            if ( p == pos ) {
               _posToPayload.remove(pos);
            }
         } else {
            Object key = getObjectKey(o);
            long p = _lookupObject.get(key);
            if ( p == pos ) {
               _posToPayload.remove(pos);
            }
         }
      }

      super.delete(o, pos);
   }

   @Override
   void update( long pos, E oldItem, E newItem ) {
      super.update(pos, oldItem, newItem);

      if ( lookupPayload(getKey(newItem)) != _payloadProvider.applyAsInt(newItem) ) {
         delete(oldItem, pos);

         try {
            // we add this position to the stream of ignored positions used during load()
            getUpdatesOutput().writeLong(pos);
         }
         catch ( IOException argh ) {
            throw new RuntimeException("Failed to append to updates file " + getUpdatesFile(), argh);
         }

         add(newItem, pos);
      }
   }
}
