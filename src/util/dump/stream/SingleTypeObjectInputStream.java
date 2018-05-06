package util.dump.stream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;

import util.io.IOUtils;


public class SingleTypeObjectInputStream<E extends Externalizable> extends DataInputStream implements ObjectInput {

   public static <T extends Externalizable> T readSingleInstance( Class<T> instanceClass, byte[] bytes ) {
      ByteArrayInputStream bytesInput = new ByteArrayInputStream(bytes);

      SingleTypeObjectInputStream in = new SingleTypeObjectInputStream(bytesInput, instanceClass);
      try {
         return (T)in.readObject();
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }
      finally {
         IOUtils.close(in);
      }
   }


   private final Class          _class;
   private Compression          _compressionType              = null;
   private byte[]               _dict;
   private ByteArrayInputStream _compressionByteBuffer        = null;
   private InputStream          _originalIn                   = null;
   private byte[]               _reusableCompressedBytesArray = null;
   private byte[]               _reusableUncompressBytesArray = null;


   public SingleTypeObjectInputStream( InputStream in, Class c ) {
      super(in);
      _class = c;
   }

   public SingleTypeObjectInputStream( InputStream in, Class c, Compression compressionType ) {
      this(in, c, compressionType, null);
   }

   public SingleTypeObjectInputStream( InputStream in, Class c, Compression compressionType, byte[] dict ) {
      this(in, c);
      _compressionType = compressionType;
      _dict = dict;
      _reusableUncompressBytesArray = new byte[8192];
      _reusableCompressedBytesArray = new byte[8192];
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      boolean restore = false;
      try {
         if ( _compressionType != null && _originalIn == null ) {
            _originalIn = in;
            restore = true;
            boolean compressed = readBoolean();
            if ( compressed ) {
               int length = readShort() & 0xffff;
               if ( length == 0xffff ) {
                  length = readInt();
               }
               if ( _reusableCompressedBytesArray.length < length )
                  _reusableCompressedBytesArray = new byte[length];
               readFully(_reusableCompressedBytesArray, 0, length);
               _reusableUncompressBytesArray = _compressionType.uncompress(_reusableCompressedBytesArray, length, _reusableUncompressBytesArray, _dict);

               _compressionByteBuffer = new ByteArrayInputStream(_reusableUncompressBytesArray);
               in = _compressionByteBuffer;
            }
         }

         Object obj = _class.newInstance();
         ((Externalizable)obj).readExternal(this);
         return obj;
      }
      catch ( IllegalAccessException e ) {
         throw new RuntimeException("Failed to instantiate " + _class, e);
      }
      catch ( InstantiationException e ) {
         throw new RuntimeException("Failed to instantiate " + _class, e);
      }
      finally {
         if ( restore ) {
            in = _originalIn;
            _originalIn = null;
            _compressionByteBuffer = null;
         }
         if ( _reusableCompressedBytesArray != null && _reusableCompressedBytesArray.length > 128 * 1024 ) {
            _reusableCompressedBytesArray = new byte[8192];
         }
         if ( _reusableUncompressBytesArray != null && _reusableUncompressBytesArray.length > 128 * 1024 ) {
            _reusableUncompressBytesArray = new byte[8192];
            _compressionByteBuffer = new ByteArrayInputStream(_reusableUncompressBytesArray);
         }
      }
   }

}
