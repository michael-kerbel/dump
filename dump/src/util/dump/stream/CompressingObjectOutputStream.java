package util.dump.stream;

import java.io.IOException;
import java.io.OutputStream;


public interface CompressingObjectOutputStream {

   default void compress( OutputStream out, ByteArrayPacker compressionType, FastByteArrayOutputStream compressionByteBuffer, byte[] reusableCompressBytesArray,
         byte[] _dict ) throws IOException {
      byte[] bytes = compressionByteBuffer.getBuf();
      int bytesLength = compressionByteBuffer.size();
      reusableCompressBytesArray = compressionType.pack(bytes, bytesLength, reusableCompressBytesArray, _dict);
      int compressedLength = reusableCompressBytesArray.length;
      if ( compressionType.isPackedSizeInFirstFourBytes() ) {
         compressedLength = (((reusableCompressBytesArray[0] & 0xff) << 24) + ((reusableCompressBytesArray[1] & 0xff) << 16)
            + ((reusableCompressBytesArray[2] & 0xff) << 8) + (reusableCompressBytesArray[3] & 0xff));
      }

      if ( compressionType.isAlwaysPack() || compressedLength + 6 < bytesLength ) {
         out.write(1);

         if ( compressedLength >= 65535 ) {
            out.write(0xff);
            out.write(0xff);
            out.write((compressedLength >>> 24) & 0xFF);
            out.write((compressedLength >>> 16) & 0xFF);
         }
         out.write((compressedLength >>> 8) & 0xFF);
         out.write(compressedLength & 0xFF);

         if ( compressionType.isPackedSizeInFirstFourBytes() ) {
            out.write(reusableCompressBytesArray, 4, compressedLength);
         } else {
            out.write(reusableCompressBytesArray);
         }
      } else {
         out.write(0);
         out.write(bytes, 0, bytesLength);
      }
   }

}
