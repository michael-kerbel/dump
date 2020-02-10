package util.dump;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import util.reflection.FieldAccessor;


/**
 * This extends UniqueIndex with the capability to directly lookup an Object that is calculated from E without the
 * need to load E itself. Not only does it reduce IO when doing this type of access, it also externalizes its state
 * so that this index is usable directly after instantiation.
 */
public class UniqueIndexWithObjectPayload<E, P> extends UniqueIndex<E> {

   protected TLongObjectMap<P> _posToPayload;

   protected Function<E, P> _payloadProvider;

   protected BiConsumer<DataOutput, P> _payloadWriter;
   protected Function<DataInput, P>    _payloadReader;

   public UniqueIndexWithObjectPayload( Dump<E> dump, FieldAccessor fieldAccessor, Function<E, P> payloadProvider, BiConsumer<DataOutput, P> payloadWriter,
         Function<DataInput, P> payloadReader ) {
      super(dump, fieldAccessor);
      _payloadProvider = payloadProvider;
      _payloadWriter = payloadWriter;
      _payloadReader = payloadReader;
   }

   public UniqueIndexWithObjectPayload( Dump<E> dump, String fieldName, Function<E, P> payloadProvider, Function<DataInput, P> payloadReader )
         throws NoSuchFieldException {
      super(dump, fieldName);
      _payloadProvider = payloadProvider;
      _payloadReader = payloadReader;
   }

   @Override
   public void add( E o, long pos ) {
      synchronized ( _dump ) {
         P payload = _payloadProvider.apply(o);
         try {
            _payloadWriter.accept(_lookupOutputStream, payload);
         }
         catch ( Exception e ) {
            throw new RuntimeException("Failed to add key to index " + getLookupFile(), e);
         }
         _posToPayload.put(pos, payload);
      }

      super.add(o, pos);
   }

   public P lookupPayload( int key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsInt ) {
            throw new IllegalArgumentException(
                  "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookupPayload(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return null;
         }
         return _posToPayload.get(pos);
      }
   }

   public P lookupPayload( long key ) {
      synchronized ( _dump ) {
         if ( !_fieldIsLong ) {
            throw new IllegalArgumentException(
                  "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookupPayload(.) method.");
         }
         long pos = getPosition(key);
         if ( pos < 0 ) {
            return null;
         }
         return _posToPayload.get(pos);
      }
   }

   public P lookupPayload( Object key ) {
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
            return null;
         }
         return _posToPayload.get(pos);
      }
   }

   @Override
   protected void cachePayload( long pos, Object payload ) {
      _posToPayload.put(pos, (P)payload);
   }

   @Override
   protected String getIndexType() {
      return UniqueIndexWithObjectPayload.class.getSimpleName();
   }

   @Override
   protected void initLookupMap() {
      super.initLookupMap();
      _posToPayload = new TLongObjectHashMap<>();
   }

   @Override
   protected P readPayload( DataInput in ) throws IOException {
      return _payloadReader.apply(in);
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

      if ( lookupPayload(getKey(newItem)) != _payloadProvider.apply(newItem) ) {
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
