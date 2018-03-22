package util.dump;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.reflect.Method;

import util.dump.stream.ExternalizableObjectStreamProvider;
import util.dump.stream.ObjectStreamProvider;


/**
 * 
 * This class implements a type safe object output stream without object cache in 
 * order to avoid memory shortages when too many items are being written to 
 * the stream.
 * 
 * Please notice that this stream wraps the provided stream (or file) object with
 * a BufferedOutputStream of 64Kb. If you want to change the
 * size of the BufferedOutputStream, use the provided 
 * constructor. Just in case you DO NOT with a buffered output stream then specify
 * a "0" (zero length) value for the buffered output stream parameter. 
 * 
 */
public class DumpWriter<E> implements DumpOutput<E> {

   // default configuration for the buffered output stream  
   public static final int      DEFAULT_BUFFER_SIZE = 65536; // 64Kb

   // private output streams
   private OutputStream         primitiveOutputStream;
   private BufferedOutputStream bufferedOutputStream;
   private ObjectOutput         objectOutputStream;
   private Method               outputResetMethod;

   /**
    * Convenience constructor allowing you to specify a File instance as target for 
    * the serialization. The class will internally create the needed OutputStreams.
    * 
    * This constructor wraps the internal OutputStream with a BufferedOutputStream. 
    * The size of the BufferedOutputStream can be read from the static 
    * field DEFAULT_BUFFER_SIZE. There is a parallel constructor to this one 
    * allowing you to specify another size for the BufferedOutputStream, including 
    * the option of avoiding it completely.
    * 
    * @param outputfile target file to use in order to store the serialized objects
    * @throws FileNotFoundException
    * @throws IOException
    */
   public DumpWriter( File outputfile ) throws FileNotFoundException, IOException {
      this(new FileOutputStream(outputfile), DEFAULT_BUFFER_SIZE, null);
   }

   /**
    * Convenience constructor allowing you to specify a File instance as target for 
    * the serialization. The class will internally create the needed OutputStreams.
    * 
    * This constructor wraps the internal OutputStream with a BufferedOutputStream. 
    * The size of the BufferedOutputReader is given by the parameter 
    * "buffersize". If "buffersize" equals 0 then no BufferedOutputStream 
    * is created.
    * 
    * @param outputfile target file to use in order to store the serialized objects
    * @param buffersize the size of the BufferedOutputStream. If 0, then no BufferedOutputStream is created 
    * @throws FileNotFoundException
    * @throws IOException
    */
   public DumpWriter( File outputfile, int buffersize ) throws FileNotFoundException, IOException {
      this(new FileOutputStream(outputfile), buffersize, null);
   }

   public DumpWriter( File outputfile, ObjectStreamProvider objectStreamProvider ) throws FileNotFoundException, IOException {
      this(new FileOutputStream(outputfile), DEFAULT_BUFFER_SIZE, objectStreamProvider);
   }

   public DumpWriter( OutputStream outputstream, int buffersize, ObjectStreamProvider objectStreamProvider ) throws IOException {
      init(outputstream, buffersize, objectStreamProvider);
   }

   public void close() throws IOException {

      objectOutputStream.close();

      if ( bufferedOutputStream != null ) {
         bufferedOutputStream.close();
      }

      primitiveOutputStream.close();
   }

   public void flush() throws IOException {
      objectOutputStream.flush();
   }

   /**
    * Use at your own risk. Doing so may corrupt the stream!
    */
   public ObjectOutput getObjectOutput() {
      return objectOutputStream;
   }

   /**
    * 
    * Writes the given object into the internal ObjectOutputSteam. 
    * 
    * @param objectToSerialize obejct to write into the output stream
    * @throws IOException
    */
   public void write( E objectToSerialize ) throws IOException {
      // writes the object into the output stream and resets
      // the memory cache in order to avoid an out of memory exception
      objectOutputStream.writeObject(objectToSerialize);

      if ( outputResetMethod != null ) {
         try {
            outputResetMethod.invoke(objectOutputStream);
         }
         catch ( Exception e ) {
            throw new RuntimeException("Failed to invoke ObjectOutput.reset().", e);
         }
      }
   }

   /**
    * 
    * Writes the given object collection into the internal ObjectOutputSteam. 
    * 
    * @param inputelements stream of objects to write into the output stream
    * @throws IOException
    * @throws TypeSafeInputException
    */
   public void writeAll( DumpInput<E> inputelements ) throws IOException {

      for ( E e : inputelements ) {
         write(e);
      }
   }

   private void init( OutputStream primitiveOutputStream, int bufferSize, ObjectStreamProvider objectStreamProvider ) throws IOException {

      objectStreamProvider = objectStreamProvider == null ? new ExternalizableObjectStreamProvider() : objectStreamProvider;

      this.primitiveOutputStream = primitiveOutputStream;

      if ( bufferSize == 0 ) {
         this.bufferedOutputStream = null;
         this.objectOutputStream = objectStreamProvider.createObjectOutput(primitiveOutputStream);
      } else {
         this.bufferedOutputStream = new BufferedOutputStream(primitiveOutputStream, bufferSize);
         this.objectOutputStream = objectStreamProvider.createObjectOutput(bufferedOutputStream);
      }

      try {
         outputResetMethod = objectOutputStream.getClass().getMethod("reset");
      }
      catch ( Exception e ) {
         outputResetMethod = null;
      }
   }

}
