package util.dump.stream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nullable;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import util.io.IOUtils;


/**
 * Using Compression enum values in StreamProviders you can compress your dumps transparently.
 * 
 * With <code>Compression.LZ4</code> and <code>Compression.Snappy</code> there are two options for very fast
 * compression, where one might expect, that performance improves overall, simply because you do less IO.
 * Unfortunately, in a single-threaded use-case with no other IO load this is not the case, even when using 
 * the fastest option, LZ4. Externalization creates high load on CPU, compression increases that load. 
 * 
 * Compression could be faster than no compression, when the stored instances are big and compressable, and
 * IO load on the server is high. 
 * 
 * Idea: put the compression calculations into a second thread, to increase performance.  
 */
public enum Compression {
   GZipLevel0, GZipLevel1, GZipLevel2, GZipLevel3, GZipLevel4, GZipLevel5, GZipLevel6, GZipLevel7, GZipLevel8, GZipLevel9, Snappy, LZ4;

   private static LZ4Compressor       _lz4Compressor   = null;
   private static LZ4FastDecompressor _lz4Decompressor = null;


   public byte[] compress( byte[] bytes, int bytesLength, @Nullable byte[] target ) throws IOException {
      switch ( this ) {
      case GZipLevel0:
         return gzip(0, bytes, bytesLength);
      case GZipLevel1:
         return gzip(1, bytes, bytesLength);
      case GZipLevel2:
         return gzip(2, bytes, bytesLength);
      case GZipLevel3:
         return gzip(3, bytes, bytesLength);
      case GZipLevel4:
         return gzip(4, bytes, bytesLength);
      case GZipLevel5:
         return gzip(5, bytes, bytesLength);
      case GZipLevel6:
         return gzip(6, bytes, bytesLength);
      case GZipLevel7:
         return gzip(7, bytes, bytesLength);
      case GZipLevel8:
         return gzip(8, bytes, bytesLength);
      case GZipLevel9:
         return gzip(9, bytes, bytesLength);
      case Snappy:
         return snappy(bytes, bytesLength, target);
      case LZ4:
         return lz4(bytes, bytesLength, target);
      }
      return bytes;
   }

   public byte[] uncompress( byte[] bytes, @Nullable byte[] target ) throws IOException {
      switch ( this ) {
      case GZipLevel0:
      case GZipLevel1:
      case GZipLevel2:
      case GZipLevel3:
      case GZipLevel4:
      case GZipLevel5:
      case GZipLevel6:
      case GZipLevel7:
      case GZipLevel8:
      case GZipLevel9:
         return gunzip(bytes);
      case Snappy:
         return unsnappy(bytes, target);
      case LZ4:
         return unLZ4(bytes, target);

      }
      return bytes;
   }

   private byte[] gunzip( byte[] bytes ) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
      InputStream in = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)), 8192);
      byte[] buffer = new byte[8192];
      int n = 0;
      while ( -1 != (n = in.read(buffer)) ) {
         out.write(buffer, 0, n);
      }
      in.close();
      out.close();
      return out.toByteArray();
   }

   private byte[] gzip( int compressionLevel, byte[] bytes, int bytesLength ) throws IOException {
      ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream();
      ConfigurableGZIPOutputStream outputStream = new ConfigurableGZIPOutputStream(compressedBytes, compressionLevel);
      outputStream.write(bytes, 0, bytesLength);
      IOUtils.close(outputStream);
      return compressedBytes.toByteArray();
   }

   private byte[] lz4( byte[] bytes, int bytesLength, byte[] target ) {
      LZ4Compressor compressor = getLZ4Compressor();
      int uncompressedSize = bytesLength;
      int maxCompressedSize = compressor.maxCompressedLength(bytesLength);
      if ( maxCompressedSize + 8 > target.length ) {
         target = new byte[maxCompressedSize + 8];
      }
      int compressedSize = compressor.compress(bytes, 0, bytesLength, target, 8, maxCompressedSize) + 4; // +4 because of uncompressedSize header

      target[0] = (byte)((compressedSize >>> 24) & 0xFF);
      target[1] = (byte)((compressedSize >>> 16) & 0xFF);
      target[2] = (byte)((compressedSize >>> 8) & 0xFF);
      target[3] = (byte)((compressedSize >>> 0) & 0xFF);

      target[4] = (byte)((uncompressedSize >>> 24) & 0xFF);
      target[5] = (byte)((uncompressedSize >>> 16) & 0xFF);
      target[6] = (byte)((uncompressedSize >>> 8) & 0xFF);
      target[7] = (byte)((uncompressedSize >>> 0) & 0xFF);

      return target;
   }

   private byte[] unLZ4( byte[] bytes, byte[] target ) {
      LZ4FastDecompressor lz4Decompressor = getLZ4Decompressor();
      int length = (((bytes[0] & 0xff) << 24) + ((bytes[1] & 0xff) << 16) + ((bytes[2] & 0xff) << 8) + ((bytes[3] & 0xff) << 0));
      if ( target == null || target.length < length ) {
         target = new byte[length];
      }
      lz4Decompressor.decompress(bytes, 4, target, 0, length);
      return target;
   }

   private LZ4Compressor getLZ4Compressor() {
      initLZ4();
      return _lz4Compressor;
   }

   private LZ4FastDecompressor getLZ4Decompressor() {
      initLZ4();
      return _lz4Decompressor;
   }

   private void initLZ4() {
      if ( _lz4Compressor == null ) {
         synchronized ( getClass() ) {
            if ( _lz4Compressor == null ) {
               LZ4Factory factory = LZ4Factory.fastestInstance();
               _lz4Compressor = factory.fastCompressor();
               _lz4Decompressor = factory.fastDecompressor();
            }
         }
      }
   }

   private byte[] snappy( byte[] data, int dataLength, byte[] target ) {
      int length = org.iq80.snappy.Snappy.maxCompressedLength(dataLength) + 4;
      if ( target == null || target.length < length ) {
         target = new byte[length];
      }
      int compressedSize = org.iq80.snappy.Snappy.compress(data, 0, dataLength, target, 4);
      target[0] = (byte)((compressedSize >>> 24) & 0xFF);
      target[1] = (byte)((compressedSize >>> 16) & 0xFF);
      target[2] = (byte)((compressedSize >>> 8) & 0xFF);
      target[3] = (byte)((compressedSize >>> 0) & 0xFF);
      return target;
   }

   private byte[] unsnappy( byte[] bytes, byte[] target ) {
      int length = org.iq80.snappy.Snappy.getUncompressedLength(bytes, 0);
      if ( target == null || target.length < length ) {
         target = new byte[length];
      }
      org.iq80.snappy.Snappy.uncompress(bytes, 0, bytes.length, target, 0);
      return target;
   }

}
