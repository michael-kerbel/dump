package util.dump.stream;

import java.io.Externalizable;
import java.io.IOException;

import javax.annotation.Nullable;


public interface ByteArrayPacker {

   <E extends Externalizable> byte[] initDictionary( Iterable<E> dictInputProvider, ObjectStreamProvider objectStreamProvider );

   default boolean isAlwaysPack() {
      return false;
   }

   boolean isPackedSizeInFirstFourBytes();

   byte[] pack( byte[] bytes, int bytesLength, @Nullable byte[] target, @Nullable byte[] dict ) throws IOException;

   byte[] unpack( byte[] source, int sourceLength, @Nullable byte[] target, @Nullable byte[] dict ) throws IOException;

}
