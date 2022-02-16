package util.dump.stream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.annotation.Nullable;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import util.dump.cache.LRUCache;
import util.dump.io.IOUtils;


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
public enum Compression implements ByteArrayPacker {
   GZipLevel0,
   GZipLevel1,
   GZipLevel2,
   GZipLevel3,
   GZipLevel4,
   GZipLevel5,
   GZipLevel6,
   GZipLevel7,
   GZipLevel8,
   GZipLevel9,
   Snappy,
   LZ4,
   Zstd1,
   Zstd5,
   Zstd10,
   Zstd15,
   Zstd22;

   private static volatile LZ4Compressor       _lz4Compressor   = null;
   private static          LZ4FastDecompressor _lz4Decompressor = null;

   private final Map<byte[], ZstdDictCompress>   _zstdDictCompress   = Collections.synchronizedMap(new LRUCache<>(4));
   private final Map<byte[], ZstdDictDecompress> _zstdDictDecompress = Collections.synchronizedMap(new LRUCache<>(4));

   @Override
   public <E extends Externalizable> byte[] initDictionary( Iterable<E> dictInputProvider, ObjectStreamProvider objectStreamProvider ) {
      switch ( this ) {
      case Zstd1:
      case Zstd5:
      case Zstd10:
      case Zstd15:
      case Zstd22:
         /*
          * from dictBuilder/zdict.h
          *  Tips: In general, a reasonable dictionary has a size of ~ 100 KB.
          *        It's possible to select smaller or larger size, just by specifying `dictBufferCapacity`.
          *        In general, it's recommended to provide a few thousands samples, though this can vary a lot.
          *        It's recommended that total size of all samples be about ~x100 times the target size of dictionary.
          */
         ZstdDictTrainer trainer = new ZstdDictTrainer(10_000_000, 102_400);
         try {
            for ( E e : dictInputProvider ) {
               ByteArrayOutputStream bytes = new ByteArrayOutputStream();
               ObjectOutput objectOutput = objectStreamProvider.createObjectOutput(bytes);
               objectOutput.writeObject(e);
               boolean added = trainer.addSample(bytes.toByteArray());
               if ( !added ) {
                  break;
               }
            }
         }
         catch ( IOException e ) {
            throw new RuntimeException("Failed to init compression dictionary", e);
         }
         return trainer.trainSamples();
      }
      return null;
   }

   @Override
   public boolean isPackedSizeInFirstFourBytes() {
      switch ( this ) {
      case Snappy:
      case LZ4:
      case Zstd1:
      case Zstd5:
      case Zstd10:
      case Zstd15:
      case Zstd22:
         return true;
      }
      return false;
   }

   @Override
   public byte[] pack( byte[] bytes, int bytesLength, @Nullable byte[] target, @Nullable byte[] dict ) throws IOException {
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
      case Zstd1:
         return zstd(1, bytes, bytesLength, target, dict);
      case Zstd5:
         return zstd(5, bytes, bytesLength, target, dict);
      case Zstd10:
         return zstd(10, bytes, bytesLength, target, dict);
      case Zstd15:
         return zstd(15, bytes, bytesLength, target, dict);
      case Zstd22:
         return zstd(22, bytes, bytesLength, target, dict);
      }
      return bytes;
   }

   @Override
   public byte[] unpack( byte[] source, int sourceLength, @Nullable byte[] target, @Nullable byte[] dict ) throws IOException {
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
         return gunzip(source, sourceLength);
      case Snappy:
         return unsnappy(source, sourceLength, target);
      case LZ4:
         return unLZ4(source, target);
      case Zstd1:
      case Zstd5:
      case Zstd10:
      case Zstd15:
      case Zstd22:
         return unZstd(source, sourceLength, target, dict);
      }
      return source;
   }

   private LZ4Compressor getLZ4Compressor() {
      initLZ4();
      return _lz4Compressor;
   }

   private LZ4FastDecompressor getLZ4Decompressor() {
      initLZ4();
      return _lz4Decompressor;
   }

   private ZstdDictDecompress getZDict( byte[] dict ) {
      return _zstdDictDecompress.computeIfAbsent(dict, ZstdDictDecompress::new);
   }

   private ZstdDictCompress getZDict( byte[] dict, int compressionLevel ) {
      return _zstdDictCompress.computeIfAbsent(dict, d -> new ZstdDictCompress(d, compressionLevel));
   }

   private byte[] gunzip( byte[] bytes, int sourceLength ) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
      InputStream in = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes, 0, sourceLength)), 8192);
      byte[] buffer = new byte[8192];
      int n;
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

   private void initLZ4() {
      if ( _lz4Compressor == null ) {
         synchronized ( Compression.class ) {
            if ( _lz4Compressor == null ) {
               LZ4Factory factory = LZ4Factory.fastestInstance();
               _lz4Compressor = factory.fastCompressor();
               _lz4Decompressor = factory.fastDecompressor();
            }
         }
      }
   }

   private byte[] lz4( byte[] bytes, int bytesLength, byte[] target ) {
      LZ4Compressor compressor = getLZ4Compressor();
      //noinspection UnnecessaryLocalVariable
      int uncompressedSize = bytesLength;
      int maxCompressedSize = compressor.maxCompressedLength(bytesLength);
      if ( target == null || maxCompressedSize + 8 > target.length ) {
         target = new byte[maxCompressedSize + 8];
      }
      int compressedSize = compressor.compress(bytes, 0, bytesLength, target, 8, maxCompressedSize) + 4; // +4 because of uncompressedSize header

      target[0] = (byte)((compressedSize >>> 24) & 0xFF);
      target[1] = (byte)((compressedSize >>> 16) & 0xFF);
      target[2] = (byte)((compressedSize >>> 8) & 0xFF);
      target[3] = (byte)((compressedSize) & 0xFF);

      target[4] = (byte)((uncompressedSize >>> 24) & 0xFF);
      target[5] = (byte)((uncompressedSize >>> 16) & 0xFF);
      target[6] = (byte)((uncompressedSize >>> 8) & 0xFF);
      target[7] = (byte)((uncompressedSize) & 0xFF);

      return target;
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
      target[3] = (byte)((compressedSize) & 0xFF);
      return target;
   }

   private byte[] unLZ4( byte[] bytes, byte[] target ) {
      LZ4FastDecompressor lz4Decompressor = getLZ4Decompressor();
      int length = (((bytes[0] & 0xff) << 24) + ((bytes[1] & 0xff) << 16) + ((bytes[2] & 0xff) << 8) + (bytes[3] & 0xff));
      if ( length > 100_000_000 ) {
         throw new RuntimeException("insane size for decompressed length:" + length + " - failing now to prevent OutOfMemoryErrors");
      }
      if ( target == null || target.length < length ) {
         target = new byte[length];
      }
      lz4Decompressor.decompress(bytes, 4, target, 0, length);
      return target;
   }

   private byte[] unZstd( byte[] source, int sourceLength, byte[] target, byte[] dict ) {
      long length = Zstd.decompressedSize(source);
      if ( length > 100_000_000 ) {
         throw new RuntimeException("insane size for decompressed length:" + length + " - failing now to prevent OutOfMemoryErrors");
      }
      if ( target == null || length > target.length ) {
         target = new byte[(int)length];
      }
      if ( dict != null ) {
         Zstd.decompressFastDict(target, 0, source, 0, sourceLength, getZDict(dict));
      } else {
         Zstd.decompress(target, source);
      }
      return target;
   }

   private byte[] unsnappy( byte[] bytes, int sourceLength, byte[] target ) {
      int length = org.iq80.snappy.Snappy.getUncompressedLength(bytes, 0);
      if ( length > 100_000_000 ) {
         throw new RuntimeException("insane size for decompressed length:" + length + " - failing now to prevent OutOfMemoryErrors");
      }
      if ( target == null || target.length < length ) {
         target = new byte[length];
      }
      org.iq80.snappy.Snappy.uncompress(bytes, 0, sourceLength, target, 0);
      return target;
   }

   private byte[] zstd( int compressionLevel, byte[] source, int sourceLength, byte[] target, byte[] dict ) {
      int length = sourceLength * 2;
      if ( target == null || target.length < length ) {
         target = new byte[length + 4];
      }
      long compressedSize;
      if ( dict != null ) {
         compressedSize = Zstd.compressFastDict(target, 4, source, 0, sourceLength, getZDict(dict, compressionLevel));
      } else {
         compressedSize = Zstd.compressByteArray(target, 4, target.length - 4, source, 0, sourceLength, compressionLevel);
      }

      target[0] = (byte)((compressedSize >>> 24) & 0xFF);
      target[1] = (byte)((compressedSize >>> 16) & 0xFF);
      target[2] = (byte)((compressedSize >>> 8) & 0xFF);
      target[3] = (byte)((compressedSize) & 0xFF);

      return target;
   }

}
