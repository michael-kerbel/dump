package util.dump.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;


/**
 * The ExternalizableObjectStreamProvider uses high-performance implementations for ObjectInput and ObjectOutput.<br><br>
 * 
 * If the instance that is to be serialized is {@link Externalizable}, the storage of the instance is more efficient than with JavaObjectStreamProvider.
 * Also some basic types are supported for efficient externalization: String, Date, UUID, Integer, Double, Float, Long.
 * Any other instances which are only {@link Serializable} are stored the same way as with JavaObjectStreamProvider.<br><br>
 * 
 * <b>Beware</b>: if you put an instance twice into the dump, you will have two instances after deserialization in memory.<br><br>
 * 
 * This ObjectStreamProvider can compress the streams using java.util.zip.Deflater. Use the appropriate constructor with values between 1 and 9.
 * Using values higher than 6 degrades performance too much to be of use. Using 1 is often the most sensible approach. Use compression only if you have 
 * limitted storage space on your server, an IO bottleneck on your server, or if you access the dumps via network and have a network bottleneck. 
 * 
 * @see JavaObjectStreamProvider 
 * @see ExternalizableObjectStreamProvider
 */
public class ExternalizableObjectStreamProvider implements ObjectStreamProvider {

   private Compression _compressionType = null;
   private byte[]      _dict;


   public ExternalizableObjectStreamProvider() {}

   public ExternalizableObjectStreamProvider( Compression compressionType ) {
      _compressionType = compressionType;
   }

   public ExternalizableObjectStreamProvider( Compression compressionType, Iterable dictInputProvider, byte[] dict ) {
      _compressionType = compressionType;
      if ( dict != null && dict.length > 0 ) {
         _dict = dict;
      } else if ( dictInputProvider != null ) {
         //noinspection unchecked
         _dict = compressionType.initDictionary(dictInputProvider, new ExternalizableObjectStreamProvider());
      }
   }

   @Override
   public ObjectInput createObjectInput( InputStream in ) throws IOException {
      return new ExternalizableObjectInputStream(in, _compressionType, _dict);
   }

   @Override
   public ObjectOutput createObjectOutput( OutputStream out ) throws IOException {
      return new ExternalizableObjectOutputStream(out, _compressionType, _dict);
   }

   @Override
   public byte[] getStaticCompressionDictionary() {
      return _dict;
   }


   enum InstanceType {
      Object(0), Externalizable(1), String(2), Date(3), UUID(4), Integer(5), Double(6), Float(7), Long(8);

      static InstanceType[] LOOKUP = new InstanceType[255];


      static {
         for ( InstanceType it : values() ) {
            LOOKUP[it.getId()] = it;
         }
      }

      static InstanceType forId( byte id ) {
         return LOOKUP[id];
      }


      private final byte _id;


      private InstanceType( int id ) {
         _id = (byte)id;
      }

      public byte getId() {
         return _id;
      }
   }
}
