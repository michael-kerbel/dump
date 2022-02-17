package util.dump.stream;

import java.io.ByteArrayOutputStream;


class FastByteArrayOutputStream extends ByteArrayOutputStream {

   public byte[] getBuf() {
      return buf;
   }
}