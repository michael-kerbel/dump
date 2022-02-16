package util.dump.cache;

import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


public class SoftLRUCache<K, V> extends LRUCache {

   // TODO in order to make the generics work well we would have to use an LRUCache as delegate instead of extending it

   public SoftLRUCache( int capacity ) {
      super(capacity);
   }

   public SoftLRUCache( int capacity, float loadFactor ) {
      super(capacity, loadFactor);
   }

   @Override
   public boolean containsKey( Object key ) {
      return get(key) != null;
   }

   @Override
   public boolean containsValue( Object value ) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Set entrySet() {
      // TODO this yields a Set containing keys in SoftReferences...
      return super.entrySet();
   }

   @Override
   public V get( Object key ) {
      SoftReference<V> ref = (SoftReference<V>)super.get(key);
      if ( ref == null ) {
         return (V)null;
      }
      V o = ref.get();
      if ( o == null ) {
         remove(key);
         return (V)null;
      }
      return o;
   }

   @Override
   public Set keySet() {
      // TODO this yields a Set containing SoftReferences...
      return super.keySet();
   }

   @Override
   public Object put( Object key, Object value ) {
      SoftReference<V> ref = new SoftReference<V>((V)value);
      SoftReference<V> oldValue = (SoftReference<V>)super.put(key, ref);
      return oldValue != null ? oldValue.get() : null;
   }

   @Override
   public void putAll( Map m ) {
      for ( Object o : m.entrySet() ) {
         Map.Entry e = (Map.Entry)o;
         put(e.getKey(), e.getValue());
      }
   }

   @Override
   public Collection values() {
      return super.values();
   }

}
