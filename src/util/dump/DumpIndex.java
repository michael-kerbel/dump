package util.dump;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.list.TLongList;
import util.dump.Dump.DumpAccessFlag;
import util.dump.stream.ExternalizableObjectOutputStream;
import util.dump.stream.SingleTypeObjectOutputStream;
import util.io.IOUtils;
import util.reflection.FieldAccessor;
import util.reflection.FieldFieldAccessor;
import util.reflection.Reflection;


/**
 * Implements a persistent index lookup for usage with {@link Dump} instances.
 * The index key can be of any type and must be accessable with a {@link FieldAccessor}, i.e. either a Field or a Method
 * of your bean <code>E</code> must return the key.<p/>
 * The indexed field should be either int, long, String or Externalizable for good initialization and writing performance.<p/>
 * <b>Beware</b>: Your key instances are used in a HashMap, so you probably want to implement <nobr><code>hashCode()</code></nobr> and
 * <nobr><code>equals()</code></nobr> accordingly, if you use a custom key instance (i.e. not <code>int</code>, <code>long</code>,
 * {@link String}, any {@link Number}, ...)<p/>
 */
public abstract class DumpIndex<E> implements Closeable {

   private static final Logger LOG = LoggerFactory.getLogger(DumpIndex.class);


   public static List<IndexMeta> discoverIndexes( final Dump dump ) {
      File[] indexFiles = dump.getDumpFile().getParentFile().listFiles(new FilenameFilter() {

         @Override
         public boolean accept( File dir, String name ) {
            return name.startsWith(dump.getDumpFile().getName()) && name.endsWith(".lookup");
         }
      });

      List<IndexMeta> metas = new ArrayList<IndexMeta>();
      for ( File lookupFile : indexFiles ) {
         File metaFile = new File(dump.getDumpFile().getParentFile(), lookupFile.getName().replaceAll("\\.[^.]*$", ".meta"));
         IndexMeta indexMeta = new IndexMeta();
         checkMeta(dump, metaFile, null, indexMeta);
         if ( indexMeta.isValid() ) {
            indexMeta._fieldAccessorName = metaFile.getName().substring(dump.getDumpFile().getName().length() + 1, metaFile.getName().length() - 5);
            metas.add(indexMeta);
         }
      }

      return metas;
   }

   public static String getIndexFileName( File dumpFile, FieldAccessor fieldAccessor ) {
      return dumpFile.getName() + "." + fieldAccessor.getName() + ".lookup";
   }

   static boolean checkMeta( Dump dump, File metaFile, String indexType, IndexMeta indexMeta ) {
      try {
         DataInputStream in = null;
         try {
            in = new DataInputStream(new FileInputStream(metaFile));
            long dumpSequence = in.readLong();
            String beanClassName = in.readUTF();
            String metaIndexType = in.readUTF();
            boolean valid = dumpSequence == dump._sequence && dump._beanClass.getName().equals(beanClassName)
               && (indexType == null || indexType.equals(metaIndexType));
            if ( indexMeta != null ) {
               indexMeta._beanClassName = beanClassName;
               indexMeta._dumpSequence = dumpSequence;
               indexMeta._indexType = metaIndexType;
               indexMeta._valid = valid;
            }
            return valid;
         }
         finally {
            if ( in != null ) {
               in.close();
            }
         }
      }
      catch ( IOException argh ) {
         if ( indexMeta != null ) {
            indexMeta._valid = false;
         }
         return false;
      }
   }


   protected final Dump<E> _dump;

   private final File            _lookupFile;
   private final File            _metaFile;
   private RandomAccessFile      _metaRaf;
   protected DataOutputStream    _lookupOutputStream;
   protected final FieldAccessor _fieldAccessor;
   protected final boolean       _fieldIsInt;
   protected final boolean       _fieldIsIntObject;
   protected final boolean       _fieldIsLong;
   protected final boolean       _fieldIsLongObject;
   protected final boolean       _fieldIsString;
   protected final boolean       _fieldIsExternalizable;

   private final File       _updatesFile;
   private DataOutputStream _updatesOutput;


   /**
      * Creates an index and adds it to the {@link Dump}.
      * @param dump the parent dump to add this index to
      * @param fieldAccessor the accessor to the field containing the index key
      */
   public DumpIndex( Dump<E> dump, FieldAccessor fieldAccessor ) {
      this(dump, fieldAccessor, null);
   }

   public DumpIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      this(dump, new FieldFieldAccessor(Reflection.getField(dump._beanClass, fieldName)));
   }

   DumpIndex( Dump<E> dump, FieldAccessor fieldAccessor, File lookupFile ) {
      if ( !dump.getMode().contains(DumpAccessFlag.indices) ) {
         throw new AccessControlException("Using indices is not allowed with current modes.");
      }

      _dump = dump;

      // the lookup file can be overridden by a subclass. in that case, the argument is not null
      if ( lookupFile == null ) {
         String indexFileName = getIndexFileName(dump.getDumpFile(), fieldAccessor);
         _lookupFile = IOUtils.getCanonicalFileQuietly(new File(dump.getDumpFile().getParentFile(), indexFileName));
      } else {
         _lookupFile = lookupFile;
      }

      File metaFile = new File(_dump.getDumpFile().getParentFile(), _lookupFile.getName().replaceAll("\\.[^.]*$", ".meta"));
      _metaFile = IOUtils.getCanonicalFileQuietly(metaFile);

      File updatesFile = new File(_dump.getDumpFile().getParentFile(), _lookupFile.getName().replaceAll("\\.[^.]*$", ".updatedPositions"));
      _updatesFile = IOUtils.getCanonicalFileQuietly(updatesFile);

      _fieldAccessor = fieldAccessor;
      Class fieldType = fieldAccessor.getType();
      _fieldIsInt = fieldType == int.class || fieldType == Integer.class;
      _fieldIsIntObject = fieldType == Integer.class;
      _fieldIsLong = fieldType == long.class || fieldType == Long.class;
      _fieldIsLongObject = fieldType == Long.class;
      _fieldIsString = fieldType == String.class;
      _fieldIsExternalizable = Externalizable.class.isAssignableFrom(_fieldAccessor.getType());
   }

   /**
    * Failing to close the index may result in data loss!
    */
   @Override
   public void close() throws IOException {
      writeMeta();
      if ( _metaRaf != null ) {
         _metaRaf.close();
         _metaRaf = null;
      }
      if ( _lookupOutputStream != null ) {
         _lookupOutputStream.close();
      }
      closeUpdatesOutput();
      _dump.removeIndex(this);
   }

   public abstract boolean contains( int key );

   public abstract boolean contains( long key );

   public abstract boolean contains( Object key );

   @Override
   public boolean equals( Object obj ) {
      if ( this == obj ) {
         return true;
      }
      if ( obj == null ) {
         return false;
      }
      if ( !(obj instanceof DumpIndex) ) {
         return false;
      }
      DumpIndex other = (DumpIndex)obj;
      if ( _lookupFile == null ) {
         if ( other._lookupFile != null ) {
            return false;
         }
      } else if ( !_lookupFile.getPath().equals(other._lookupFile.getPath()) ) {
         return false;
      }
      return true;
   }

   public void flush() throws IOException {
      if ( _lookupOutputStream != null ) {
         _lookupOutputStream.flush();
      }
   }

   public void flushMeta() throws IOException {
      writeMeta();
   }

   public abstract TLongList getAllPositions();

   public FieldAccessor getFieldAccessor() {
      return _fieldAccessor;
   }

   /**
    * Returns the number of keys in this index
    *
    * @return the number of keys in this index.
    */
   public abstract int getNumKeys();

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_lookupFile == null) ? 0 : _lookupFile.getPath().hashCode());
      return result;
   }

   protected boolean checkMeta() {
      return checkMeta(_dump, _metaFile, getIndexType(), null);
   }

   protected void closeAndDeleteUpdatesOutput() throws IOException {
      closeUpdatesOutput();
      _updatesFile.delete();
      boolean exists = _updatesFile.exists();
      if ( exists ) {
         throw new IOException("unable to delete " + _updatesFile);
      }
   }

   protected void closeUpdatesOutput() throws IOException {
      if ( _updatesOutput != null ) {
         _updatesOutput.close();
         _updatesOutput = null;
      }
   }

   /**
    * create or open index
    */
   protected void createOrLoad() {

      boolean indexInvalid = !_lookupFile.exists() || _lookupFile.length() == 0 || !checkMeta();
      if ( indexInvalid ) {
         deleteAllIndexFiles();
      }

      initLookupOutputStream();

      try {
         _dump.flush();
      }
      catch ( IOException e ) {
         throw new RuntimeException("failed to flush dump", e);
      }

      if ( _dump.getDumpFile().length() > 0 && indexInvalid ) {
         // rebuild index if it is not current
         initFromDump();
      } else {
         load();
      }
   }

   protected void deleteAllIndexFiles() {
      final String indexPrefix = _lookupFile.getName().replaceAll("\\.[^.]*$", "");
      File dir = _dump.getDumpFile().getParentFile();
      File[] indexFiles = dir.listFiles(new FilenameFilter() {

         @Override
         public boolean accept( File dir, String name ) {
            return name.startsWith(indexPrefix);
         }
      });
      for ( File f : indexFiles ) {
         if(f.isDirectory())
            IOUtils.deleteDir(f);
         else if ( !f.delete() ) {
            LOG.error("Failed to delete invalid index file " + f);
         }
      }
   }

   protected abstract String getIndexType();

   protected int getIntKey( E o ) {
      int key;
      try {
         key = _fieldIsIntObject ? ((Integer)_fieldAccessor.get(o)) : _fieldAccessor.getInt(o);
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }
      return key;
   }

   protected long getLongKey( E o ) {
      long key;
      try {
         key = _fieldIsLongObject ? ((Long)_fieldAccessor.get(o)) : _fieldAccessor.getLong(o);
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }
      return key;
   }

   protected File getLookupFile() {
      return _lookupFile;
   }

   protected File getMetaFile() {
      return _metaFile;
   }

   protected Object getObjectKey( E o ) {
      Object key;
      try {
         key = _fieldAccessor.get(o);
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }
      return key;
   }

   protected File getUpdatesFile() {
      return _updatesFile;
   }

   protected DataOutputStream getUpdatesOutput() {
      synchronized ( _dump ) {
         if ( _updatesOutput == null ) {
            try {
               _updatesOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_updatesFile, true), DumpWriter.DEFAULT_BUFFER_SIZE));
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to init updates outputstream " + _updatesFile, argh);
            }
         }
         return _updatesOutput;
      }
   }

   protected void init() {
      initLookupMap();

      if ( !_dump.getDumpFile().exists() || _dump.getDumpFile().length() == 0 ) {
         _lookupFile.delete();
      }

      // make sure there are no other threads/processes that open/create the index
      synchronized ( _dump ) {

         boolean locked = _dump.acquireFileLock();
         try {
            createOrLoad();
            _dump.addIndex(this);
         }
         catch ( Exception argh ) {
            try {
               close();
            }
            catch ( IOException arghargh ) {
               LOG.error("Failed to close dump index after exception during init", arghargh);
            }
            throw new RuntimeException(argh);
         }
         finally {
            if ( locked ) {
               _dump.releaseFileLock();
            }
         }
      }
   }

   protected void initFromDump() {
      try (DumpIterator<E> iterator = _dump.iterator()) {
         while ( iterator.hasNext() ) {
            add(iterator.next(), iterator.getPosition());
         }

         try {
            writeMeta();
         }
         catch ( IOException argh ) {
            throw new RuntimeException("failed to write meta data", argh);
         }
      }
      catch ( IOException argh ) {
         LOG.warn("failed to add close dump iterator", argh);
      }
   }

   protected abstract void initLookupMap();

   protected void initLookupOutputStream() {
      try {
         if ( !_fieldIsInt && !_fieldIsLong && !_fieldIsString ) {
            if ( _fieldIsExternalizable ) {
               _lookupOutputStream = new SingleTypeObjectOutputStream(new BufferedOutputStream(new FileOutputStream(_lookupFile, true)),
                  _fieldAccessor.getType());
            } else {
               _lookupOutputStream = new ExternalizableObjectOutputStream(
                  new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_lookupFile, true))));
            }
         } else {
            _lookupOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_lookupFile, true)));
         }
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to initialize dump index with lookup file " + _lookupFile, argh);
      }
   }

   protected abstract void load();

   protected void writeMeta() throws IOException {
      RandomAccessFile metaRAF = getMetaRAF();
      metaRAF.seek(0);
      metaRAF.writeLong(_dump._sequence);
      metaRAF.writeUTF(_dump._beanClass.getName());
      metaRAF.writeUTF(getIndexType());
      metaRAF.setLength(metaRAF.getFilePointer());
   }

   abstract void add( E o, long pos );

   abstract void delete( E o, long pos );

   boolean isUpdatable( E oldItem, E newItem ) {
      Object oldKey = getObjectKey(oldItem);
      if ( oldKey == null ) {
         return false;
      }
      Object newKey = getObjectKey(newItem);
      if ( newKey == null ) {
         return false;
      }
      return oldKey.equals(newKey);
   }

   abstract void update( long pos, E oldItem, E newItem );

   private RandomAccessFile getMetaRAF() throws FileNotFoundException {
      synchronized ( this ) {
         if ( _metaRaf == null ) {
            _metaRaf = new RandomAccessFile(_metaFile, "rw");
         }
         return _metaRaf;
      }
   }


   public static class IndexMeta {

      long    _dumpSequence;
      String  _beanClassName;
      String  _indexType;
      String  _fieldAccessorName;
      boolean _valid;


      public String getFieldAccessorName() {
         return _fieldAccessorName;
      }

      public String getIndexType() {
         return _indexType;
      }

      public boolean isValid() {
         return _valid;
      }
   }
}
