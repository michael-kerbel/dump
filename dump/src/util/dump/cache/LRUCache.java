package util.dump.cache;

import java.util.LinkedHashMap;
import java.util.Map;


public class LRUCache<K, V> extends LinkedHashMap<K, V> implements java.io.Serializable {

   private final int _capacity;


   public LRUCache( int capacity ) {
      super(capacity, 0.75f, true);
      _capacity = capacity;
   }

   public LRUCache( int capacity, float loadFactor ) {
      super(capacity, loadFactor, true);
      _capacity = capacity;
   }

   @Override
   protected boolean removeEldestEntry( Map.Entry eldest ) {
      return size() > _capacity;
   }
}