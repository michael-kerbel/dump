package util.dump.stream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;

import util.io.IOUtils;


public class SingleTypeObjectOutputStream<E extends Externalizable> extends DataOutputStream implements ObjectOutput {

   public static byte[] writeSingleInstance( Externalizable e ) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      SingleTypeObjectOutputStream out = new SingleTypeObjectOutputStream<Externalizable>(bytes, e.getClass());
      try {
         e.writeExternal(out);
      }
      catch ( IOException argh ) {
         throw new RuntimeException(argh);
      }
      finally {
         IOUtils.close(out);
      }

      return bytes.toByteArray();
   }


   private final Class<?>            _class;
   private Compression               _compressionType            = null;
   private FastByteArrayOutputStream _compressionByteBuffer      = null;
   private OutputStream              _originalOut                = null;
   private byte[]                    _reusableCompressBytesArray = null;


   public SingleTypeObjectOutputStream( OutputStream out, Class<?> c ) {
      super(out);
      _class = c;
   }

   public SingleTypeObjectOutputStream( OutputStream out, Class<?> c, Compression compressionType ) {
      this(out, c);
      _compressionType = compressionType;
      _compressionByteBuffer = new FastByteArrayOutputStream();
      _reusableCompressBytesArray = new byte[8192];
   }

   @Override
   public void writeObject( Object obj ) throws IOException {
      if ( obj == null ) {
         throw new IOException("Object is null");
      }

      Class<?> objClass = obj.getClass();

      if ( !(obj instanceof Externalizable) ) {
         throw new IOException("Object with class " + objClass.getName() + " is not Externalizable");
      }
      if ( objClass != _class ) {
         throw new IOException("Object has wrong class: " + objClass);
      }

      boolean restore = false;
      if ( _compressionType != null && _originalOut == null ) {
         restore = true;
         _originalOut = out;
         _compressionByteBuffer.reset();
         out = _compressionByteBuffer;
      }

      ((Externalizable)obj).writeExternal(this);

      if ( restore ) {
         byte[] bytes = _compressionByteBuffer.getBuf();
         int bytesLength = _compressionByteBuffer.size();
         _reusableCompressBytesArray = _compressionType.compress(bytes, bytesLength, _reusableCompressBytesArray);
         int compressedLength = _reusableCompressBytesArray.length;
         if ( _compressionType == Compression.Snappy || _compressionType == Compression.LZ4 ) {
            compressedLength = (((_reusableCompressBytesArray[0] & 0xff) << 24) + ((_reusableCompressBytesArray[1] & 0xff) << 16)
               + ((_reusableCompressBytesArray[2] & 0xff) << 8) + ((_reusableCompressBytesArray[3] & 0xff) << 0));
         }
         out = _originalOut;

         if ( compressedLength + 6 < bytesLength ) {
            out.write(1);

            if ( compressedLength >= 65535 ) {
               out.write(0xff);
               out.write(0xff);
               out.write((compressedLength >>> 24) & 0xFF);
               out.write((compressedLength >>> 16) & 0xFF);
            }
            out.write((compressedLength >>> 8) & 0xFF);
            out.write((compressedLength >>> 0) & 0xFF);

            if ( _compressionType == Compression.Snappy || _compressionType == Compression.LZ4 ) {
               out.write(_reusableCompressBytesArray, 4, compressedLength);
            } else {
               out.write(_reusableCompressBytesArray);
            }
         } else {
            out.write(0);
            out.write(bytes, 0, bytesLength);
         }
         _originalOut = null;

         if ( _reusableCompressBytesArray != null && _reusableCompressBytesArray.length > 128 * 1024 ) {
            _reusableCompressBytesArray = new byte[8192];
            _compressionByteBuffer = new FastByteArrayOutputStream();
         }
      }
   }


   static class FastByteArrayOutputStream extends ByteArrayOutputStream {

      public byte[] getBuf() {
         return buf;
      }
   }
}
