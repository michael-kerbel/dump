package util.dump;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutput;
import java.security.AccessControlException;

import util.dump.UniqueIndex.DuplicateKeyException;


/**
 * This is an experimental implementation of a Dump, which provides an add() method, that is not
 * completely synchronized. The most cpu expensive operation, the externalization of the instance,
 * is not synchronized, which allows the user to add to the Dump in multiple threads. The order 
 * of the instances will be random, when using this!   
 */
public class MultithreadedDump<E> extends Dump<E> {

   ThreadLocal<ByteArrayOutputStream> _byteOutputs   = ThreadLocal.withInitial(() -> new ByteArrayOutputStream(4096));
   ThreadLocal<ObjectOutput>          _objectOutputs = ThreadLocal.withInitial(() -> {
                                                        try {
                                                           return _streamProvider.createObjectOutput(_byteOutputs.get());
                                                        }
                                                        catch ( IOException e ) {
                                                           throw new RuntimeException("Failed to create ObjectOutputStream", e);
                                                        }
                                                     });


   public MultithreadedDump( Class beanClass, File dumpFile ) {
      super(beanClass, dumpFile);
   }

   public void addSilently( E o ) {
      try {
         add(o);
      }
      catch ( IOException e ) {
         throw new RuntimeException("Failed to add to dump", e);
      }
   }

   @Override
   public void add( E o ) throws IOException {
      if ( !_mode.contains(DumpAccessFlag.add) ) {
         throw new AccessControlException("Add operation not allowed with current modes.");
      }
      assertOpen();

      ByteArrayOutputStream out = _byteOutputs.get();
      _objectOutputs.get().writeObject(o);
      out.flush();
      byte[] bytes = out.toByteArray();
      out.reset();

      synchronized ( this ) {
         for ( DumpIndex<E> index : _indexes ) {
            if ( index instanceof UniqueIndex && index.contains(((UniqueIndex)index).getKey(o)) ) {
               // check this before actually adding anything
               throw new DuplicateKeyException("Dump already contains an instance with the key " + ((UniqueIndex)index).getKey(o));
            }
         }

         long pos = _outputStream._n;
         _outputStream.write(bytes);
         _sequence++;
         for ( DumpIndex<E> index : _indexes ) {
            index.add(o, pos);
         }
      }
   }
}
