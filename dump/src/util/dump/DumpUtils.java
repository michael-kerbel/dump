package util.dump;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;


public class DumpUtils {

   private static final Logger LOG = LoggerFactory.getLogger(DumpUtils.class);

   private static ThreadLocal<byte[]> _writeUTFReusableByteArray = ThreadLocal.withInitial(() -> new byte[2048]);
   private static ThreadLocal<byte[]> _readUTFReusableByteArray  = ThreadLocal.withInitial(() -> new byte[2048]);
   private static ThreadLocal<char[]> _readUTFReusableCharArray  = ThreadLocal.withInitial(() -> new char[1024]);

   /**
    * Calls <code>dump.add(e)</code> and catches any IOException.
    * @return true if the add operation did not throw any IOException, false otherwise
    */
   public static <E> boolean addSilently( Dump<E> dump, E e ) {
      if ( dump != null ) {
         try {
            dump.add(e);
            return true;
         }
         catch ( IOException ex ) {
            // do nothing
         }
      }
      return false;
   }

   /**
    * Calls <code>dump.add(e)</code> and wraps any IOException in a RuntimeException.
    * */
   public static <E> void addUnchecked( Dump<E> dump, E e ) {
      if ( dump != null ) {
         try {
            dump.add(e);
         }
         catch ( IOException ex ) {
            throw new RuntimeException(ex);
         }
      }
   }

   /**
    * Copies non deleted elements from source to target. Tries to ignore and skip all corrupt elements.<p/>
    * <b>Beware:</b> Repairing corrupt elements only works if the element size in bytes is stable and not too many elements were deleted from the dump.
    * Also some retained elements might contain broken data afterwards, if the binary representation of the element is still externalizable. I.e. there
    * is no checksum mechanism in the dump (yet).
    * @throws RuntimeException if the cleanup fails
    */
   public static <E> void cleanup( final Dump<E> source, final Dump<E> destination ) {
      try {
         source.flush();
         Iterator<E> iterator = source.new DeletionAwareDumpReader(source._dumpFile, source._streamProvider) {

            TIntIntMap _elementSizes = new TIntIntHashMap();

            @Override
            public boolean hasNext() {
               while ( true ) {
                  try {
                     long oldLastPos = _lastPos;
                     boolean hasNext = super.hasNext();
                     int size = (int)(_lastPos - oldLastPos);
                     _elementSizes.put(size, _elementSizes.get(size) + 1);
                     return hasNext;
                  }
                  catch ( Throwable e ) {
                     if ( e instanceof OutOfMemoryError || e.getMessage().contains("Failed to read externalized instance") ) {
                        // let's guess the next pos which is hopefully not also corrupt
                        long bytesRead = _positionAwareInputStream._rafPos - _lastPos;
                        long mostFrequentElementSize = getMostFrequentElementSize();
                        int bytesToSkip = (int)(mostFrequentElementSize - (bytesRead % mostFrequentElementSize));
                        try {
                           for ( int i = 0; i < bytesToSkip; i++ ) {
                              if ( _positionAwareInputStream.read() < 0 ) {
                                 return false;
                              }
                           }
                        }
                        catch ( IOException ee ) {
                           throw new RuntimeException("Failed to cleanup dump " + source.getDumpFile(), ee);
                        }
                        _lastPos += bytesRead + bytesToSkip;
                     } else {
                        throw new RuntimeException("Failed to cleanup dump " + source.getDumpFile(), e);
                     }
                  }
               }
            }

            @Override
            void closeStreams( boolean isEOF ) {
               // don't
            }

            private long getMostFrequentElementSize() {
               final int[] maxNumber = { 0 };
               final int[] mostFrequentSize = { 1 };
               _elementSizes.forEachEntry(( size, number ) -> {
                  if ( number > maxNumber[0] ) {
                     mostFrequentSize[0] = size;
                     maxNumber[0] = number;
                  }
                  return true;
               });
               return Math.max(1, mostFrequentSize[0]);
            }

         }.iterator();

         for ( ; iterator.hasNext(); ) {
            E e = iterator.next();
            destination.add(e);
         }
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to cleanup dump " + source.getDumpFile(), e);
      }
   }

   /**
    * Calls <code>dump.close()</code> and catches any IOException.
    */
   public static void closeSilently( Dump<?> dump ) {
      if ( dump != null ) {
         try {
            dump.close();
         }
         catch ( IOException e ) {
            LOG.warn("Failed to close dump " + dump.getDumpFile(), e);
         }
      }
   }

   /**
    * Calls <code>index.close()</code> and catches any IOException.
    */
   public static void closeSilently( DumpIndex<?> index ) {
      if ( index != null ) {
         try {
            index.close();
         }
         catch ( IOException e ) {
            LOG.warn("Failed to close index " + index.getLookupFile(), e);
         }
      }
   }

   /**
    * <p>remove all files that belong to the dump (i.e. all indices, meta file, deletions file)</p>
    *
    * <p>Note: The dump must be closed before!</p>
    *
    * @throws IllegalArgumentException if the dump wasn't closed before
    * @throws IOException if the deletion failed
    */
   public static void deleteDumpFiles( Dump<?> dump ) throws IOException {
      if ( !dump._isClosed ) {
         throw new IllegalArgumentException("dump wasn't closed");
      }

      if ( dump._dumpFile != null && dump._dumpFile.exists() ) {
         if ( !dump._dumpFile.delete() ) {
            throw new IOException("dump file couldn't be deleted");
         }
      }

      if ( dump._metaFile != null && dump._metaFile.exists() ) {
         if ( !dump._metaFile.delete() ) {
            throw new IOException("meta file couldn't be deleted");
         }
      }

      if ( dump._deletionsFile != null && dump._deletionsFile.exists() ) {
         if ( !dump._deletionsFile.delete() ) {
            throw new IOException("deletions file couldn't be deleted");
         }
      }
   }

   /**
    * delete all files that belong to the dump on exit ({@link java.io.File#deleteOnExit()})
    */
   public static void deleteDumpFilesOnExit( Dump<?> dump ) {
      if ( dump._dumpFile != null ) {
         dump._dumpFile.deleteOnExit();
      }

      if ( dump._metaFile != null ) {
         dump._metaFile.deleteOnExit();
      }

      if ( dump._deletionsFile != null ) {
         dump._deletionsFile.deleteOnExit();
      }
   }

   /**
    * <p>removes and deletes all indices that are open for the given dump</p>
    *
    * <p>Note: The dump must be opened!</p>
    *
    * @throws IllegalArgumentException if the dump is closed
    * @throws IOException if an index couldn't be removed
    */
   public static void deleteDumpIndexFiles( Dump<?> dump ) throws IOException {
      if ( dump._isClosed ) {
         throw new IllegalArgumentException("dump is closed");
      }

      for ( DumpIndex<?> index : new ArrayList<DumpIndex<?>>(dump._indexes) ) {
         index.close();
         index.deleteAllIndexFiles();
      }
      if ( dump._indexes.size() > 0 ) {
         throw new IllegalStateException("not all indices could be closed and deleted");
      }
   }

   /** This is an extension of {@link java.io.DataInputStream.readUTF()} which allows more than 65535 chars.
    * Use with writeUtf() from this class. */
   public static String readUTF( DataInput in ) throws IOException {
      int utflen = readUTFLength(in);
      return readUTFString(in, utflen);
   }

   public static int readUTFLength( DataInput in ) throws IOException {
      int utflen = in.readUnsignedShort();
      if ( utflen == 0xffff ) {
         utflen = in.readInt();
      }
      return utflen;
   }

   public static String readUTFString( DataInput in, int utflen ) throws IOException {
      byte[] bytearr = _readUTFReusableByteArray.get();
      if ( bytearr.length < utflen ) {
         bytearr = new byte[utflen * 2];
         _readUTFReusableByteArray.set(bytearr);
      }
      char[] chararr = _readUTFReusableCharArray.get();
      if ( chararr.length < utflen ) {
         chararr = new char[utflen * 2];
         _readUTFReusableCharArray.set(chararr);
      }

      int c, char2, char3;
      int count = 0;
      int chararr_count = 0;

      in.readFully(bytearr, 0, utflen);

      while ( count < utflen ) {
         c = bytearr[count] & 0xff;
         if ( c > 127 ) {
            break;
         }
         count++;
         chararr[chararr_count++] = (char)c;
      }

      while ( count < utflen ) {
         c = bytearr[count] & 0xff;
         switch ( c >> 4 ) {
         case 0:
         case 1:
         case 2:
         case 3:
         case 4:
         case 5:
         case 6:
         case 7:
            /* 0xxxxxxx*/
            count++;
            chararr[chararr_count++] = (char)c;
            break;
         case 12:
         case 13:
            /* 110x xxxx   10xx xxxx*/
            count += 2;
            if ( count > utflen ) {
               throw new UTFDataFormatException("malformed input: partial character at end");
            }
            char2 = bytearr[count - 1];
            if ( (char2 & 0xC0) != 0x80 ) {
               throw new UTFDataFormatException("malformed input around byte " + count);
            }
            chararr[chararr_count++] = (char)(((c & 0x1F) << 6) | (char2 & 0x3F));
            break;
         case 14:
            /* 1110 xxxx  10xx xxxx  10xx xxxx */
            count += 3;
            if ( count > utflen ) {
               throw new UTFDataFormatException("malformed input: partial character at end");
            }
            char2 = bytearr[count - 2];
            char3 = bytearr[count - 1];
            if ( ((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80) ) {
               throw new UTFDataFormatException("malformed input around byte " + (count - 1));
            }
            chararr[chararr_count++] = (char)(((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
            break;
         default:
            /* 10xx xxxx,  1111 xxxx */
            throw new UTFDataFormatException("malformed input around byte " + count);
         }
      }
      // The number of chars produced may be less than utflen
      return new String(chararr, 0, chararr_count);
   }

   /**
    * Calls <code>dump.updateLast(e)</code> and catches any IOException.
    * @return null if update was not successful
    */
   public static <E> E updateLastSilently( Dump<E> dump, E e ) {
      if ( dump != null ) {
         try {
            return dump.updateLast(e);
         }
         catch ( IOException ex ) {
            // do nothing
         }
      }
      return null;
   }

   /**
    * Calls <code>dump.updateLast(e)</code> and wraps any IOException in a RuntimeException.
    */
   public static <E> E updateLastUnchecked( Dump<E> dump, E e ) {
      if ( dump != null ) {
         try {
            return dump.updateLast(e);
         }
         catch ( IOException ex ) {
            throw new RuntimeException(ex);
         }
      }
      return null;
   }

   /** this is an extension of {@link java.io.DataOutputStream.writeUTF()} which allows more than 65535 chars. 
    * Use with readUtf() from this class. */
   public static int writeUTF( String str, DataOutput out ) throws IOException {
      int strlen = str.length();
      int utflen = 0;
      int c, count = 0;

      /* use charAt instead of copying String to char array */
      for ( int i = 0; i < strlen; i++ ) {
         c = str.charAt(i);
         if ( (c >= 0x0001) && (c <= 0x007F) ) {
            utflen++;
         } else if ( c > 0x07FF ) {
            utflen += 3;
         } else {
            utflen += 2;
         }
      }

      int headerLength = utflen >= 65535 ? 6 : 2;

      byte[] bytearr = _writeUTFReusableByteArray.get();
      if ( bytearr.length < (utflen + headerLength) ) {
         bytearr = new byte[(utflen * 2) + headerLength];
      }
      _writeUTFReusableByteArray.set(bytearr);

      if ( utflen >= 65535 ) {
         bytearr[count++] = (byte)0xFF;
         bytearr[count++] = (byte)0xFF;
         bytearr[count++] = (byte)((utflen >>> 24) & 0xFF);
         bytearr[count++] = (byte)((utflen >>> 16) & 0xFF);
      }

      bytearr[count++] = (byte)((utflen >>> 8) & 0xFF);
      bytearr[count++] = (byte)((utflen >>> 0) & 0xFF);

      int i = 0;
      for ( i = 0; i < strlen; i++ ) {
         c = str.charAt(i);
         if ( !((c >= 0x0001) && (c <= 0x007F)) ) {
            break;
         }
         bytearr[count++] = (byte)c;
      }

      for ( ; i < strlen; i++ ) {
         c = str.charAt(i);
         if ( (c >= 0x0001) && (c <= 0x007F) ) {
            bytearr[count++] = (byte)c;

         } else if ( c > 0x07FF ) {
            bytearr[count++] = (byte)(0xE0 | ((c >> 12) & 0x0F));
            bytearr[count++] = (byte)(0x80 | ((c >> 6) & 0x3F));
            bytearr[count++] = (byte)(0x80 | ((c >> 0) & 0x3F));
         } else {
            bytearr[count++] = (byte)(0xC0 | ((c >> 6) & 0x1F));
            bytearr[count++] = (byte)(0x80 | ((c >> 0) & 0x3F));
         }
      }
      out.write(bytearr, 0, utflen + headerLength);
      return utflen + headerLength;
   }
}
