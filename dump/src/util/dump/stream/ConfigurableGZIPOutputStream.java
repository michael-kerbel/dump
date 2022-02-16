package util.dump.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;


public class ConfigurableGZIPOutputStream extends GZIPOutputStream {

   public ConfigurableGZIPOutputStream( OutputStream out, int compressionLevel ) throws IOException {
      super(out);
      def.setLevel(compressionLevel);
   }
}