package util.dump.stream;

import java.io.IOException;
import java.io.OutputStream;


public interface CompressingObjectOutputStream {

   default void compress( OutputStream out, Compression _compressionType, FastByteArrayOutputStream _compressionByteBuffer, byte[] _reusableCompressBytesArray,
         byte[] _dict ) throws IOException {
      byte[] bytes = _compressionByteBuffer.getBuf();
      int bytesLength = _compressionByteBuffer.size();
      _reusableCompressBytesArray = _compressionType.compress(bytes, bytesLength, _reusableCompressBytesArray, _dict);
      int compressedLength = _reusableCompressBytesArray.length;
      if ( _compressionType.isCompressedSizeInFirstFourBytes() ) {
         compressedLength = (((_reusableCompressBytesArray[0] & 0xff) << 24) + ((_reusableCompressBytesArray[1] & 0xff) << 16)
            + ((_reusableCompressBytesArray[2] & 0xff) << 8) + ((_reusableCompressBytesArray[3] & 0xff) << 0));
      }

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

         if ( _compressionType.isCompressedSizeInFirstFourBytes() ) {
            out.write(_reusableCompressBytesArray, 4, compressedLength);
         } else {
            out.write(_reusableCompressBytesArray);
         }
      } else {
         out.write(0);
         out.write(bytes, 0, bytesLength);
      }
   }

}
