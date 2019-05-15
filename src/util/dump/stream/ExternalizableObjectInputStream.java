package util.dump.stream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import util.dump.stream.ExternalizableObjectStreamProvider.InstanceType;
import util.io.IOUtils;


public class ExternalizableObjectInputStream extends DataInputStream implements ObjectInput {

   public static <T extends Externalizable> T readSingleInstance( Class<T> instanceClass, byte[] bytes ) {
      ByteArrayInputStream bytesInput = new ByteArrayInputStream(bytes);

      ExternalizableObjectInputStream in = null;
      try {
         in = new ExternalizableObjectInputStream(bytesInput);
         return (T)in.readObject();
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }
      finally {
         IOUtils.close(in);
      }
   }


   private Map<String, Class> _classes = new HashMap<String, Class>();

   private ObjectInputStream    _objectInputStream;
   private Compression          _compressionType              = null;
   private byte[]               _dict;
   private ByteArrayInputStream _compressionByteBuffer        = null;
   private InputStream          _originalIn                   = null;
   private ObjectInputStream    _originalObjectInputStream;
   private byte[]               _reusableCompressedBytesArray = null;
   private byte[]               _reusableUncompressBytesArray = null;


   public ExternalizableObjectInputStream( InputStream in ) throws IOException {
      super(in);
      _objectInputStream = new NoHeaderObjectInputStream(in);
   }

   public ExternalizableObjectInputStream( InputStream in, Compression compressionType ) throws IOException {
      this(in, compressionType, null);
   }

   public ExternalizableObjectInputStream( InputStream in, Compression compressionType, byte[] dict ) throws IOException {
      this(in);
      _compressionType = compressionType;
      _dict = dict;
      _reusableCompressedBytesArray = new byte[8192];
   }

   @Override
   public void close() throws IOException {
      super.close();
      if ( _objectInputStream != null )
         _objectInputStream.close();
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      boolean isNotNull = readBoolean();
      if ( isNotNull ) {
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
                  _reusableUncompressBytesArray = _compressionType.unpack(_reusableCompressedBytesArray, length, _reusableUncompressBytesArray, _dict);

                  _compressionByteBuffer = new ByteArrayInputStream(_reusableUncompressBytesArray);
                  in = _compressionByteBuffer;
                  _originalObjectInputStream = _objectInputStream;
                  _objectInputStream = new NoHeaderObjectInputStream(in);
               }
            }

            byte instanceTypeId = readByte();
            InstanceType instanceType = InstanceType.forId(instanceTypeId);
            switch ( instanceType ) {
            case Externalizable:
               String className = readUTF();
               try {
                  Object obj = getClass(className).newInstance();
                  ((Externalizable)obj).readExternal(this);
                  return obj;
               }
               catch ( IllegalAccessException e ) {
                  throw new RuntimeException("Failed to instantiate " + className, e);
               }
               catch ( InstantiationException e ) {
                  throw new RuntimeException("Failed to instantiate " + className, e);
               }
            case String:
               return readUTF();
            case Date:
               return new Date(readLong());
            case UUID:
               return new UUID(readLong(), readLong());
            case Integer:
               return Integer.valueOf(readInt());
            case Double:
               return Double.valueOf(readDouble());
            case Float:
               return Float.valueOf(readFloat());
            case Long:
               return Long.valueOf(readLong());
            default:
               return _objectInputStream.readObject();
            }
         }
         finally {
            if ( restore ) {
               in = _originalIn;
               _originalIn = null;
               _compressionByteBuffer = null;
               _objectInputStream = _originalObjectInputStream;
               _originalObjectInputStream = null;
            }
         }
      } else {
         return null;
      }
   }

   private Class getClass( String className ) throws ClassNotFoundException {
      Class c = _classes.get(className);
      if ( c == null ) {
         c = Class.forName(className);
         _classes.put(className, c);
      }
      return c;
   }


   private final class NoHeaderObjectInputStream extends ObjectInputStream {

      private NoHeaderObjectInputStream( InputStream arg0 ) throws IOException {
         super(arg0);
      }

      @Override
      protected void readStreamHeader() throws IOException, StreamCorruptedException {
         // do nothing
      }
   }

}
