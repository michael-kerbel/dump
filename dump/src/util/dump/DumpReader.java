package util.dump;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.OptionalDataException;
import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import util.dump.stream.ExternalizableObjectStreamProvider;
import util.dump.stream.ObjectStreamProvider;


/**
 *
 * <p>This class implements a type safe object input stream.</p>
 *
 */
public class DumpReader<E> implements DumpInput<E>, Iterator<E> {

   // default configuration for the buffered output stream
   public static final int DEFAULT_BUFFER_SIZE = 65536;

   BufferedInputStream _inputbuffer;
   InputStream         _primitiveInputStream;
   ObjectInput         _objectInputStream;
   E                   _nextObject      = (E)null;
   boolean             _nextPrepared    = false;
   File                _sourceFile      = null;
   boolean             _deleteFileOnEOF = false;


   /**
    *
    * Convenience constructor allowing you to specify a File Object as source for
    * the deserialization. The class will internally create the needed InputStream
    * obejcts.
    *
    * The class will retain a reference to the file object until it is closed. You can
    * also use the extended constructor in order to indicate this class to delete the
    * file automatically when its EOF is reached
    *
    * @param sourceFile source file to use in order to store the serialized objects
    */
   public DumpReader( File sourceFile ) throws IOException {
      this(sourceFile, false, DEFAULT_BUFFER_SIZE, null);
   }

   public DumpReader( File sourceFile, boolean deleteFileOnEOF, int buffersize, @Nullable ObjectStreamProvider objectStreamProvider ) throws IOException {
      initFile(sourceFile, deleteFileOnEOF, buffersize, objectStreamProvider);
   }

   public DumpReader( File sourceFile, boolean deleteFileOnEOF, @Nullable ObjectStreamProvider objectStreamProvider ) throws IOException {
      this(sourceFile, deleteFileOnEOF, DEFAULT_BUFFER_SIZE, objectStreamProvider);
   }

   /**
    *
    * Convenience constructor allowing you to specify a File instance as source for
    * the deserialization. The class will internally create the needed InputStreams.
    *
    * The class will retain a reference to the File until it is closed. You can
    * also use the extended constructor in order to indicate this class to delete the
    * file automatically when its EOF is reached
    *
    * @param sourceFile source file to use in order to store the serialized objects
    * @param buffersize size in bytes for the input stream, use "0" for no input buffering
    */
   public DumpReader( File sourceFile, int buffersize ) throws IOException {
      this(sourceFile, false, buffersize, null);
   }

   /**
    * Generic constructor accepting a standard InputStream to use in order to deserialize the
    * type safe objects.
    *
    * The DumpReader instance will use a 64Kb input buffer.
    *
    * @param primitiveInputStream generic input stream to use in order to serialize the objects
    */
   public DumpReader( InputStream primitiveInputStream ) throws IOException {
      this(primitiveInputStream, DEFAULT_BUFFER_SIZE);
   }

   /**
    * Generic constructor accepting a standard InputStream to use in order to deserialize the
    * type safe objects.
    *
    * @param primitiveInputStream generic input stream to use in order to serialize the objects
    * @param buffersize size in bytes for the input stream, use "0" for no input buffering
    */
   public DumpReader( InputStream primitiveInputStream, int buffersize ) throws IOException {
      this(primitiveInputStream, buffersize, null);
   }

   public DumpReader( InputStream primitiveInputStream, int buffersize, ObjectStreamProvider objectStreamProvider ) throws IOException {
      reset(primitiveInputStream, buffersize, objectStreamProvider);
   }

   /**
    * Closes the internal streams and, if present, releases the reference to the provided file.
    *
    * By invoking this method the provided file will not be deleted, no matter which
    * value was assigned to the boolean parameter <code>_deleteFileOnEOF</code> in the constructor -
    * this happends automatically only when the stream reaches its EOF.
    *
    * You only need using this method when a premature interruption of the reading process
    * is needed.
    *
    * This method is invoqued automatically when <code>hasNext()</code> returns <code>false</code>.
    *
    */
   @Override
   public void close() {
      closeStreams(false);
   }

   public File getSourceFile() {
      return _sourceFile;
   }

   @Override
   public boolean hasNext() {
      if ( _nextPrepared ) {
         return _nextObject != null;
      }

      try {
         // reads the object and demands a object validation
         //noinspection unchecked
         _nextObject = (E)_objectInputStream.readObject();
         _nextPrepared = true;
         return true;
      }
      catch ( OptionalDataException e ) {
         _nextObject = null;
         if ( e.eof ) {
            closeStreams(true);
            _nextPrepared = true;
            return false;
         } else {
            throw new RuntimeException(e);
         }
      }
      catch ( EOFException e ) {
         _nextObject = null;
         _nextPrepared = true;
         closeStreams(true);
         return false;
      }
      catch ( Exception e ) {
         boolean kryoEofException = e.getMessage() != null && e.getMessage().contains("Buffer underflow");

         _nextObject = null;
         closeStreams(true);
         if ( kryoEofException ) {
            _nextPrepared = true;
            return false;
         } else {
            throw new RuntimeException(e);
         }
      }
   }

   @Override
   public @Nonnull Iterator<E> iterator() {
      return this;
   }

   /** BEWARE: only the first call of next() after a successful hasNext() returns the element! Consecutive
    * calls yield null. The reference to the read object is not kept, in order to keep memory usage low.
    * (huge objects would stay on the heap otherwise) */
   @Override
   public E next() {
      _nextPrepared = false;
      E nextObject = _nextObject;
      _nextObject = null; // don't keep reference to potentially huge object
      return nextObject;
   }

   @Override
   public void remove() {
      throw new UnsupportedOperationException("remove() not supported by DumpReader!");
   }

   public void reset( InputStream primitiveInputStream, int bufferSize, ObjectStreamProvider objectStreamProvider ) throws IOException {

      if ( _objectInputStream != null ) {
         _objectInputStream.close();
      }

      objectStreamProvider = objectStreamProvider == null ? new ExternalizableObjectStreamProvider() : objectStreamProvider;

      this._primitiveInputStream = primitiveInputStream;

      if ( bufferSize == 0 ) {
         this._inputbuffer = null;
         this._objectInputStream = objectStreamProvider.createObjectInput(primitiveInputStream);
      } else {
         this._inputbuffer = new BufferedInputStream(this._primitiveInputStream, bufferSize);
         this._objectInputStream = objectStreamProvider.createObjectInput(_inputbuffer);
      }
   }

   @Override
   protected void finalize() throws Throwable {
      close();
      super.finalize();
   }

   // close streams and delete file if necessary
   void closeStreams( boolean isEOF ) {

      try {
         _objectInputStream.close();

         if ( _inputbuffer != null ) {
            _inputbuffer.close();
         }

         _primitiveInputStream.close();

         // and on demand deletes the file
         if ( isEOF && _deleteFileOnEOF && this._sourceFile != null && !this._sourceFile.delete() ) { //
            throw new IOException("could not delete file " + _sourceFile.getAbsolutePath());
         }

         this._sourceFile = null;
      }
      catch ( IOException argh ) {
         throw new RuntimeException(argh);
      }
   }

   private void initFile( File fileForSource, boolean deleteFileOnEOF, int bufferSize, ObjectStreamProvider objectStreamProvider ) throws IOException {

      this._sourceFile = fileForSource;
      this._deleteFileOnEOF = deleteFileOnEOF;

      // optimizes memory by reducing the input buffer to the file size, only when applicable
      long fileLen = fileForSource.length();
      if ( fileLen > 0L && fileLen < bufferSize ) {
         bufferSize = (int)fileLen;
      }

      reset(new FileInputStream(this._sourceFile), bufferSize, objectStreamProvider);

   }
}
