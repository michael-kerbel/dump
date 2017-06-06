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
import util.dump.stream.SingleTypeObjectOutputStream.FastByteArrayOutputStream;
import util.io.IOUtils;


public class ExternalizableObjectOutputStream extends DataOutputStream implements ObjectOutput {

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


   private ObjectOutputStream        _objectOutputStream;

   private boolean                   _resetPending               = false;
   private Compression               _compressionType            = null;
   private FastByteArrayOutputStream _compressionByteBuffer      = null;
   private OutputStream              _originalOut                = null;
   private ObjectOutputStream        _originalObjectOutputStream = null;
   private byte[]                    _reusableCompressBytesArray = null;


   public ExternalizableObjectOutputStream( OutputStream out ) throws IOException {
      super(out);
      _objectOutputStream = new NoHeaderObjectOutputStream(out);
   }

   public ExternalizableObjectOutputStream( OutputStream out, Compression compressionType ) throws IOException {
      this(out);
      _compressionType = compressionType;
      _compressionByteBuffer = new FastByteArrayOutputStream();
      _reusableCompressBytesArray = new byte[8192];
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
            byte[] bytes = _compressionByteBuffer.getBuf();
            int bytesLength = _compressionByteBuffer.size();
            _reusableCompressBytesArray = _compressionType.compress(bytes, bytesLength, _reusableCompressBytesArray);
            int compressedLength = _reusableCompressBytesArray.length;
            if ( _compressionType == Compression.Snappy || _compressionType == Compression.LZ4 ) {
               compressedLength = (((_reusableCompressBytesArray[0] & 0xff) << 24) + ((_reusableCompressBytesArray[1] & 0xff) << 16)
                  + ((_reusableCompressBytesArray[2] & 0xff) << 8) + ((_reusableCompressBytesArray[3] & 0xff) << 0));
            }
            out = _originalOut;

            if ( compressedLength + 6 < bytes.length ) {
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
               out.write(bytes);
            }
            _originalOut = null;
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
      protected void writeStreamHeader() throws IOException {
         // do nothing
      }
   }
}
