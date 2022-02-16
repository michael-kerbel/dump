package util.dump.stream;

import java.io.Externalizable;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;


/**
 * The SingleTypeObjectStreamProvider uses very performant implementations for ObjectInput and ObjectOutput.<br><br>
 *
 * The drawbacks are:<br>
 *  - only {@link Externalizable} instances may be used<br>
 *  - the {@link Externalizable}s may not use readObject() during readExternal() or writeObject(o) during writeExternal()<br><br>
 *  - if you put an instance twice into the dump, you will have two instances after deserialization in memory<br><br>
 *
 * This ObjectStreamProvider can compress the streams using Gzip, Snappy, LZ4 or ZStd. Use the appropriate constructor with a CompressionType.
 * Use compression only if you have limited storage space on your server, an IO bottleneck on your server, or if you access the dumps via
 * network and have a network bottleneck.
 *
 * @see JavaObjectStreamProvider
 * @see ExternalizableObjectStreamProvider
 */
public class SingleTypeObjectStreamProvider<E extends Externalizable> implements ObjectStreamProvider {

   private final Class<E>  _class;
   private ByteArrayPacker _compressionType = null;
   private byte[]          _dict;


   public SingleTypeObjectStreamProvider( Class<E> c ) {
      _class = c;
   }

   public SingleTypeObjectStreamProvider( Class<E> c, ByteArrayPacker compressionType ) {
      this(c, compressionType, null, null);
   }

   public SingleTypeObjectStreamProvider( Class<E> c, ByteArrayPacker compressionType, Iterable<E> dictInputProvider, byte[] dict ) {
      _class = c;
      _compressionType = compressionType;
      if ( dict != null && dict.length > 0 ) {
         _dict = dict;
      } else if ( dictInputProvider != null ) {
         _dict = compressionType.initDictionary(dictInputProvider, new SingleTypeObjectStreamProvider<E>(c));
      }
   }

   @Override
   public ObjectInput createObjectInput( InputStream in ) {
      return new SingleTypeObjectInputStream<E>(in, _class, _compressionType, _dict);
   }

   @Override
   public ObjectOutput createObjectOutput( OutputStream out ) {
      return new SingleTypeObjectOutputStream<E>(out, _class, _compressionType, _dict);
   }

   @Override
   public byte[] getStaticCompressionDictionary() {
      return _dict;
   }
}
