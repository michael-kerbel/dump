package util.dump.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;


public interface ObjectStreamProvider {

   public ObjectInput createObjectInput( InputStream in ) throws IOException;

   public ObjectOutput createObjectOutput( OutputStream out ) throws IOException;

}
