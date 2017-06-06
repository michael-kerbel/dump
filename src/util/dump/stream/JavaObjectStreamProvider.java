package util.dump.stream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;


/**
 * The JavaObjectStreamProvider uses java's standard implementation for ObjectInput and ObjectOutput: ObjectInputStream and ObjectOutputStream.<br><br>
 * 
 * While this provides you with many advanced features (such as ensuring that an Object when serialized twice is stored only once), these features 
 * come at a high price: serialization is slow and memory consuming.<br><br>
 * 
 * This ObjectStreamProvider can compress the streams using java.util.zip.Deflater. Use the appropriate constructor with values between 1 and 9.
 * Using values higher than 6 degrades performance too much to be of use. Using 1 is often the most sensible approach. Use compression only if you have 
 * limitted storage space on your server, an IO bottleneck on your server, or if you access the dumps via network and have a network bottleneck. 
 * 
 * @see ExternalizableObjectStreamProvider
 * @see SingleTypeObjectStreamProvider
 */
public class JavaObjectStreamProvider implements ObjectStreamProvider {

   private final int _compression;

   public JavaObjectStreamProvider() {
      _compression = 0;
   }

   /**
    * @param compression if set to a value > 0 the input and output streams are wrapped with GZip compression
    * @see java.util.zip.Deflater
    */
   public JavaObjectStreamProvider( int compression ) {
      _compression = compression;
   }

   public ObjectInput createObjectInput( InputStream in ) throws IOException {
      if ( _compression > 0 ) {
         in = new GZIPInputStream(in);
      }
      return new ObjectInputStream(in);
   }

   public ObjectOutput createObjectOutput( OutputStream out ) throws IOException {
      if ( _compression > 0 ) {
         out = new BufferedOutputStream(new ConfigurableGZIPOutputStream(out, _compression));
      }
      return new ObjectOutputStream(out);
   }
}
