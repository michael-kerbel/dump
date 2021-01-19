package util.dump.stream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;


public class SingleTypeObjectOutputStream<E extends Externalizable> extends DataOutputStream implements ObjectOutput, CompressingObjectOutputStream {

   public static byte[] writeSingleInstance( Externalizable e ) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      try (SingleTypeObjectOutputStream out = new SingleTypeObjectOutputStream(bytes, e.getClass())) {
         e.writeExternal(out);
      }
      catch ( IOException argh ) {
         throw new RuntimeException(argh);
      }

      return bytes.toByteArray();
   }

   private final Class<E>                  _class;
   private       ByteArrayPacker           _compressionType            = null;
   private       FastByteArrayOutputStream _compressionByteBuffer      = null;
   private       OutputStream              _originalOut                = null;
   private       byte[]                    _reusableCompressBytesArray = null;
   private       byte[]                    _dict;

   public SingleTypeObjectOutputStream( OutputStream out, Class<E> c ) {
      super(out);
      _class = c;
   }

   public SingleTypeObjectOutputStream( OutputStream out, Class<E> c, ByteArrayPacker compressionType ) {
      this(out, c, compressionType, null);
   }

   public SingleTypeObjectOutputStream( OutputStream out, Class<E> c, ByteArrayPacker compressionType, byte[] dict ) {
      this(out, c);
      _compressionType = compressionType;
      _dict = dict;
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
         out = _originalOut;
         _originalOut = null;

         compress(out, _compressionType, _compressionByteBuffer, _reusableCompressBytesArray, _dict);

         if ( _reusableCompressBytesArray != null && _reusableCompressBytesArray.length > 128 * 1024 ) {
            _reusableCompressBytesArray = new byte[8192];
         }
         if ( _compressionByteBuffer.getBuf().length > 128 * 1024 ) {
            _compressionByteBuffer = new FastByteArrayOutputStream();
         }
      }
   }

}
