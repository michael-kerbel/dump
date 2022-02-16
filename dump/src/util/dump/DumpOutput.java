package util.dump;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;


public interface DumpOutput<E> extends Closeable, Flushable {

   /**
    * Writes the given object. 
    * 
    * @param objectToWrite object to write into the dump
    * @throws IOException
    */
   public void write( E objectToWrite ) throws Exception;

   /**
    * Writes all objects from the incoming <code>DumpInput</code>.
    * 
    * @param input dump input containing the elements to write
    * @throws Exception
    */
   public void writeAll( DumpInput<E> input ) throws Exception;

}
