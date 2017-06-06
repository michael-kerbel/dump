package util.dump;


public interface NonUniqueIndex<E> {

   public Iterable<E> lookup( int key );

   public Iterable<E> lookup( long key );

   public Iterable<E> lookup( Object key );

}