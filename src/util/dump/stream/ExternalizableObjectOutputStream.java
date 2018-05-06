package util.dump.stream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.UUID;

import util.dump.stream.ExternalizableObjectStreamProvider.InstanceType;
import util.io.IOUtils;


public class ExternalizableObjectOutputStream extends DataOutputStream implements ObjectOutput, CompressingObjectOutputStream {

   @SuppressWarnings("unused")
   public static byte[] writeSingleInstance( Externalizable e ) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();

      ExternalizableObjectOutputStream out = null;
      try {
         out = new ExternalizableObjectOutputStream(bytes);
         out.writeObject(e);
      }
      catch ( IOException argh ) {
         throw new RuntimeException(argh);
      }
      finally {
         IOUtils.close(out);
      }

      return bytes.toByteArray();
   }


   private ObjectOutputStream _objectOutputStream;

   private boolean                   _resetPending               = false;
   private Compression               _compressionType            = null;
   private FastByteArrayOutputStream _compressionByteBuffer      = null;
   private OutputStream              _originalOut                = null;
   private ObjectOutputStream        _originalObjectOutputStream = null;
   private byte[]                    _reusableCompressBytesArray = null;
   private byte[]                    _dict;


   public ExternalizableObjectOutputStream( OutputStream out ) throws IOException {
      super(out);
      _objectOutputStream = new NoHeaderObjectOutputStream(out);
   }

   public ExternalizableObjectOutputStream( OutputStream out, Compression compressionType ) throws IOException {
      this(out, compressionType, null);
   }

   public ExternalizableObjectOutputStream( OutputStream out, Compression compressionType, byte[] dict ) throws IOException {
      this(out);
      _compressionType = compressionType;
      _compressionByteBuffer = new FastByteArrayOutputStream();
      _reusableCompressBytesArray = new byte[8192];
      if ( dict != null && dict.length > 0 ) {
         _dict = dict;
      }
   }

   @Override
   public void close() throws IOException {
      super.close();
      _objectOutputStream.close();
   }

   @Override
   public void flush() throws IOException {
      super.flush();
      _objectOutputStream.flush();
   }

   public void reset() {
      _resetPending = true;
   }

   @Override
   public void writeObject( Object obj ) throws IOException {

      writeBoolean(obj != null);
      if ( obj != null ) {
         boolean compress = false;
         if ( _compressionType != null && _originalOut == null ) {
            compress = true;
            _originalOut = out;
            _compressionByteBuffer.reset();
            out = _compressionByteBuffer;
            _originalObjectOutputStream = _objectOutputStream;
            _objectOutputStream = new NoHeaderObjectOutputStream(out);
         }

         if ( obj instanceof Externalizable ) {
            writeByte(InstanceType.Externalizable.getId());
            writeUTF(obj.getClass().getName());
            ((Externalizable)obj).writeExternal(this);
         } else if ( obj instanceof String ) {
            writeByte(InstanceType.String.getId());
            writeUTF((String)obj);
         } else if ( obj instanceof Date ) {
            writeByte(InstanceType.Date.getId());
            writeLong(((Date)obj).getTime());
         } else if ( obj instanceof UUID ) {
            writeByte(InstanceType.UUID.getId());
            writeLong(((UUID)obj).getMostSignificantBits());
            writeLong(((UUID)obj).getLeastSignificantBits());
         } else if ( obj instanceof Integer ) {
            writeByte(InstanceType.Integer.getId());
            writeInt((Integer)obj);
         } else if ( obj instanceof Double ) {
            writeByte(InstanceType.Double.getId());
            writeDouble((Double)obj);
         } else if ( obj instanceof Float ) {
            writeByte(InstanceType.Float.getId());
            writeFloat((Float)obj);
         } else if ( obj instanceof Long ) {
            writeByte(InstanceType.Long.getId());
            writeLong((Long)obj);
         } else {
            writeByte(InstanceType.Object.getId());
            if ( _resetPending ) {
               _objectOutputStream.reset();
               _resetPending = false;
            }
            _objectOutputStream.writeObject(obj);
         }

         if ( compress ) {
            out = _originalOut;
            _originalOut = null;

            compress(out, _compressionType, _compressionByteBuffer, _reusableCompressBytesArray, _dict);

            _objectOutputStream = _originalObjectOutputStream;
            _originalObjectOutputStream = null;
         }
      }
   }


   private final class NoHeaderObjectOutputStream extends ObjectOutputStream {

      private NoHeaderObjectOutputStream( OutputStream out ) throws IOException {
         super(out);
      }

      @Override
      protected void writeStreamHeader() {
         // do nothing
      }
   }
}
