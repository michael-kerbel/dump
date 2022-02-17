package util.dump;


public interface NonUniqueIndex<E> {

   /**
    * BEWARE: While this method is synchronized, the iteration afterwards is not! You have to synchronize using
    * the dump as monitor while iterating the Iterable, otherwise you might miss values in the iteration or get
    * null values.
    */
   Iterable<E> lookup( int key );

   /**
    * BEWARE: While this method is synchronized, the iteration afterwards is not! You have to synchronize using
    * the dump as monitor while iterating the Iterable, otherwise you might miss values in the iteration or get
    * null values.
    */
   Iterable<E> lookup( long key );

   /**
    * BEWARE: While this method is synchronized, the iteration afterwards is not! You have to synchronize using
    * the dump as monitor while iterating the Iterable, otherwise you might miss values in the iteration or get
    * null values.
    */
   Iterable<E> lookup( Object key );

}