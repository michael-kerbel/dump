package util.dump;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.procedure.TIntObjectProcedure;
import gnu.trove.procedure.TLongObjectProcedure;
import util.collections.SoftLRUCache;
import util.dump.GroupIndex.Positions;
import util.dump.sort.InfiniteSorter;
import util.dump.stream.SingleTypeObjectStreamProvider;
import util.reflection.FieldAccessor;


/**
 * <b>Beware</b>: Your key instances <i>must</i> implement <nobr><code>hashCode()</code></nobr> and
 * <nobr><code>equals()</code></nobr>, if you use a custom key instance (i.e. not <code>int</code>, <code>long</code>,
 * {@link String}, any {@link Number}, ...)<p/>
 * 
 * <b>Beware 2</b>: this implementation is currently not thread-safe! It will fail hard in a multi-threaded environment. 
 */
public class InfiniteGroupIndex<E> extends DumpIndex<E> implements NonUniqueIndex<E> {

   private static final Logger   LOG                               = LoggerFactory.getLogger(InfiniteGroupIndex.class);

   private static final int      MAX_POSITIONS_LENGTH_IN_CACHE     = 1000;
   /* TODO [MKR 08.06.2009] add following two methods for efficient lookup of many keys at once:
      public IntKeyMap fullIndexScan(IntSet keys);
      public LongKeyMap fullIndexScan(LongSet keys);
   
      Check if it is possible to add
      public Map<Object, E> fullIndexScan(Set keys);
    */

   /* TODO synchronization */

   private static final int      DEFAULT_MAX_LOOKUP_SIZE_IN_MEMORY = 25000;
   protected static final byte[] EMPTY_BYTES                       = new byte[0];


   private static File getOverflowIndexFile( Dump<?> dump, FieldAccessor fieldAccessor ) {
      File dumpFile = dump.getDumpFile();
      return new File(dumpFile.getParentFile(), DumpIndex.getIndexFileName(dumpFile, fieldAccessor).replaceFirst("lookup$", "overflow.lookup"));
   }


   /**
    * this index is used in order to delay index updates on disk
    */
   private MyGroupIndex                    _overflowIndex;

   private final int                       _maxLookupSizeInMemory;
   private File                            _objectKeyDumpFile;

   private int                             _currentLookupSize;

   /**
    * case 1:
    *  - key is a real int: this dump is used to point to the first position of to store all positions per key (see {@link #findIntKey(int, int)})
    *
    * case 2:
    *  - key is a complex object: the dump contains the hash codes of the objects and points to a dump position with
    *                             that specific hashcode (see {@link #getObjectKeyPositions(Object)}) in _externalizableKeyDump or _stringKeyDump (resp.)
    */
   private Dump<IntKeyPosition>            _intKeyDump;

   /**
    * see case 1 of _intKeyDump
    */
   private Dump<LongKeyPosition>           _longKeyDump;

   private Dump<StringKeyPosition>         _stringKeyDump;

   private Dump<ExternalizableKeyPosition> _externalizableKeyDump;

   private long                            _lookupFileLength;

   private Map<Object, long[]>             _cache = null;                                             // default is to have no cache


   public InfiniteGroupIndex( Dump<E> dump, FieldAccessor fieldAccessor, int maxLookupSizeInMemory ) {
      super(dump, fieldAccessor);
      _maxLookupSizeInMemory = maxLookupSizeInMemory;
      init();
      isFieldCompatible();
   }

   public InfiniteGroupIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      this(dump, fieldName, DEFAULT_MAX_LOOKUP_SIZE_IN_MEMORY);
   }

   public InfiniteGroupIndex( Dump<E> dump, String fieldName, int maxLookupSizeInMemory ) throws NoSuchFieldException {
      super(dump, fieldName);
      _maxLookupSizeInMemory = maxLookupSizeInMemory;

      init();
      isFieldCompatible();
   }

   @Override
   public void add( E o, long pos ) {
      _overflowIndex.superAdd(o, pos);
      _currentLookupSize++;

      if ( _fieldIsInt ) {
         int key = getIntKey(o);
         removePositionsFromCache(key);
      } else if ( _fieldIsLong ) {
         long key = getLongKey(o);
         removePositionsFromCache(key);
      } else {
         Object key = getObjectKey(o);
         removePositionsFromCache(key);
      }

      if ( _currentLookupSize > _maxLookupSizeInMemory ) {
         mergeOverflowIntoIndex();
         _currentLookupSize = 0;
      }
   }

   @Override
   public void close() throws IOException {
      super.close();
      if ( _overflowIndex != null ) {
         _overflowIndex.close();
      }
      if ( _intKeyDump != null ) {
         _intKeyDump.close();
      }
      if ( _longKeyDump != null ) {
         _longKeyDump.close();
      }
      if ( _stringKeyDump != null ) {
         _stringKeyDump.close();
      }
      if ( _externalizableKeyDump != null ) {
         _externalizableKeyDump.close();
      }
   }

   @Override
   public boolean contains( int key ) {
      return getPositions(key).length > 0;
   }

   @Override
   public boolean contains( long key ) {
      return getPositions(key).length > 0;
   }

   @Override
   public boolean contains( Object key ) {
      return getPositions(key).length > 0;
   }

   @Override
   public TLongList getAllPositions() {
      TLongList pos = new TLongArrayList((int)(_lookupFileLength / (8 + (_fieldIsLong ? 8 : 4))));

      if ( _fieldIsInt || _fieldIsLong ) {
         DataInputStream in = null;
         try {
            in = new DataInputStream(new BufferedInputStream(new FileInputStream(getLookupFile())));
            while ( true ) {
               if ( _fieldIsLong ) {
                  in.readLong();
               } else {
                  in.readInt();
               }
               long p = in.readLong();
               if ( !_dump._deletedPositions.contains(p) ) {
                  pos.add(p);
               }
            }
         }
         catch ( EOFException e ) {
            // ignore, since all is good
         }
         catch ( Exception e ) {
            throw new RuntimeException("Failed to read dump positions", e);
         }
         finally {
            if ( in != null ) {
               try {
                  in.close();
               }
               catch ( IOException e ) {
                  LOG.error("Failed to close inputstream.");
               }
            }
         }
      } else if ( _fieldIsExternalizable ) {
         for ( ExternalizableKeyPosition kp : _externalizableKeyDump ) {
            if ( !_dump._deletedPositions.contains(kp._pos) ) {
               pos.add(kp._pos);
            }
         }
      } else if ( _fieldIsString ) {
         for ( StringKeyPosition kp : _stringKeyDump ) {
            if ( !_dump._deletedPositions.contains(kp._pos) ) {
               pos.add(kp._pos);
            }
         }
      }
      pos.addAll(_overflowIndex.getAllPositions());
      // TODO sort since we added the positions from groupIndex?
      return pos;
   }

   @Override
   public int getNumKeys() {

      boolean first = true;
      int numKeys = 0;
      if ( _fieldIsInt ) {
         int before = 0;
         for ( IntKeyPosition keyPos : _intKeyDump ) {
            if ( _dump._deletedPositions.contains(keyPos._pos) ) {
               continue;
            }

            int key = keyPos._key;
            if ( first || key != before ) {
               first = false;
               if ( !_overflowIndex.contains(key) ) {
                  numKeys++;
               }
            }
            before = key;
         }
      } else if ( _fieldIsLong ) {
         long before = 0;
         for ( LongKeyPosition keyPos : _longKeyDump ) {
            if ( _dump._deletedPositions.contains(keyPos._pos) ) {
               continue;
            }

            long key = keyPos._key;
            if ( first || key != before ) {
               first = false;
               if ( !_overflowIndex.contains(key) ) {
                  numKeys++;
               }
            }
            before = key;
         }
      } else {
         if ( !_fieldIsExternalizable && !_fieldIsString ) {
            throw new IllegalStateException("must not happen");
         }

         int before = 0;
         Set<Object> set = new HashSet<Object>();
         for ( IntKeyPosition keyPos : _intKeyDump ) {

            int hashCode = keyPos._key;
            if ( first || hashCode != before ) {
               numKeys += countDistinctObjects(set);
               first = false;
            }

            Object objectKey = null;
            long objectPos = -1;
            if ( _fieldIsString ) {
               StringKeyPosition object = _stringKeyDump.get(keyPos._pos);
               objectPos = object._pos;
               objectKey = object._key;
            } else {
               ExternalizableKeyPosition object = _externalizableKeyDump.get(keyPos._pos);
               objectPos = object._pos;
               objectKey = _externalizableKeyDump.get(keyPos._pos)._key;
            }
            if ( _dump._deletedPositions.contains(objectPos) ) {
               continue;
            }
            set.add(objectKey);
            before = hashCode;
         }

         numKeys += countDistinctObjects(set);
      }

      return numKeys + _overflowIndex.getNumKeys();
   }

   @Override
   public Iterable<E> lookup( int key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return new GroupIterable(pos);
      }
   }

   @Override
   public Iterable<E> lookup( long key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return new GroupIterable(pos);
      }
   }

   @Override
   public Iterable<E> lookup( Object key ) {
      synchronized ( _dump ) {
         long[] pos = getPositions(key);
         return new GroupIterable(pos);
      }
   }

   public void setLRUCacheSize( int lruCacheSize ) {
      if ( lruCacheSize > 0 ) {
         _cache = Collections.synchronizedMap(new SoftLRUCache(lruCacheSize));
      } else {
         _cache = null;
      }
   }

   @Override
   protected void createOrLoad() {
      super.createOrLoad();

      _overflowIndex = new MyGroupIndex(_dump, _fieldAccessor);

      _currentLookupSize = _fieldIsInt ? _overflowIndex._lookupInt.size()
            : (_fieldIsLong ? _overflowIndex._lookupLong.size() : _overflowIndex._lookupObject.size());
   }

   @Override
   protected String getIndexType() {
      return InfiniteGroupIndex.class.getSimpleName();
   }

   protected long[] getPositions( int key ) {
      if ( !(_fieldIsInt || _fieldIsExternalizable || _fieldIsString) ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
      }

      long[] cachedPositions = getPositionsFromCache(key);
      if ( cachedPositions != null ) {
         return cachedPositions;
      }

      TLongList pos = new TLongArrayList(_overflowIndex.getPositions(key));

      int keyLength = 4 + 8; // in bytes

      long firstIndex = findIntKey(key, keyLength);
      if ( firstIndex >= 0 ) {
         for ( long p = firstIndex * keyLength; p < _lookupFileLength; p += keyLength ) {
            IntKeyPosition ip = _intKeyDump.get(p);
            if ( ip._key != key ) {
               break;
            }
            if ( ip._pos >= 0 && !_dump._deletedPositions.contains(ip._pos) ) {
               pos.add(ip._pos);
            }
         }
      }

      long[] positions = pos.toArray();
      putPositionsIntoCache(key, positions);
      return positions;
   }

   protected long[] getPositions( long key ) {
      if ( !_fieldIsLong ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
      }

      long[] cachedPositions = getPositionsFromCache(key);
      if ( cachedPositions != null ) {
         return cachedPositions;
      }

      TLongList pos = new TLongArrayList(_overflowIndex.getPositions(key));

      int keyLength = 8 + 8; // in bytes

      long firstIndex = findLongKey(key, keyLength);
      if ( firstIndex >= 0 ) {
         for ( long p = firstIndex * keyLength; p < _lookupFileLength; p += keyLength ) {
            LongKeyPosition ip = _longKeyDump.get(p);
            if ( ip._key != key ) {
               break;
            }
            if ( ip._pos >= 0 && !_dump._deletedPositions.contains(ip._pos) ) {
               pos.add(ip._pos);
            }
         }
      }

      long[] positions = pos.toArray();
      putPositionsIntoCache(key, positions);
      return positions;
   }

   protected long[] getPositions( Object key ) {
      if ( (_fieldIsLong || _fieldIsLongObject) && key instanceof Long ) {
         return getPositions(((Long)key).longValue());
      }
      if ( (_fieldIsInt || _fieldIsIntObject) && key instanceof Integer ) {
         return getPositions(((Integer)key).intValue());
      }
      if ( _fieldIsLong || _fieldIsInt ) {
         throw new IllegalArgumentException(
            "The type of the used key class of this index is " + _fieldAccessor.getType() + ". Please use the appropriate lookup(.) method.");
      }
      if ( (_fieldIsExternalizable && !(key instanceof Externalizable)) || (_fieldIsString && !(key instanceof String)) ) {
         throw new IllegalArgumentException("Incompatible key type. The type of the used key class of this index is " + _fieldAccessor.getType()
            + ". You tried to using the index with a key of type " + key.getClass() + ".");
      }

      long[] cachedPositions = getPositionsFromCache(key);
      if ( cachedPositions != null ) {
         return cachedPositions;
      }

      TLongList keyPositions = getObjectKeyPositions(key);

      TLongList positions = new TLongArrayList(_overflowIndex.getPositions(key));
      for ( TLongIterator iterator = keyPositions.iterator(); iterator.hasNext(); ) {
         long pos = iterator.next();
         if ( _fieldIsExternalizable ) {
            ExternalizableKeyPosition keyPosition = _externalizableKeyDump.get(pos);
            long kp = keyPosition._pos;
            if ( kp >= 0 && keyPosition._key.equals(key) && !_dump._deletedPositions.contains(kp) ) {
               positions.add(kp);
            }
         } else if ( _fieldIsString ) {
            StringKeyPosition keyPosition = _stringKeyDump.get(pos);
            long kp = keyPosition._pos;
            if ( kp >= 0 && keyPosition._key.equals(key) && !_dump._deletedPositions.contains(kp) ) {
               positions.add(kp);
            }
         }
      }

      long[] posArray = positions.toArray();
      putPositionsIntoCache(key, posArray);
      return posArray;
   }

   protected long[] getPositionsFromCache( Object key ) {
      if ( _cache != null ) {
         return _cache.get(key);
      }
      return null;
   }

   @Override
   protected void initFromDump() {

      try {
         if ( _fieldIsInt ) {
            final InfiniteSorter<IntKeyPosition> sorter = new InfiniteSorter<IntKeyPosition>(_maxLookupSizeInMemory, _dump.getDumpFile().getParentFile(),
               new SingleTypeObjectStreamProvider<Externalizable>(IntKeyPosition.class));
            try (DumpIterator<E> iterator = _dump.iterator()) {
               while ( iterator.hasNext() ) {
                  sorter.add(new IntKeyPosition(getIntKey(iterator.next()), iterator.getPosition()));
               }
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to sort InfiniteGroupIndex on disk", argh);
            }
            _intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, getLookupFile());
            _intKeyDump.addAll(sorter);
            _intKeyDump.flush();
            _lookupFileLength = getLookupFile().length();
         } else if ( _fieldIsLong ) {
            final InfiniteSorter<LongKeyPosition> sorter = new InfiniteSorter<LongKeyPosition>(_maxLookupSizeInMemory, _dump.getDumpFile().getParentFile(),
               new SingleTypeObjectStreamProvider<Externalizable>(LongKeyPosition.class));
            try (DumpIterator<E> iterator = _dump.iterator()) {
               while ( iterator.hasNext() ) {
                  sorter.add(new LongKeyPosition(getLongKey(iterator.next()), iterator.getPosition()));
               }
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to sort InfiniteGroupIndex on disk", argh);
            }
            _longKeyDump = new Dump<LongKeyPosition>(LongKeyPosition.class, getLookupFile());
            _longKeyDump.addAll(sorter);
            _longKeyDump.flush();
            _lookupFileLength = getLookupFile().length();
         } else if ( _fieldIsString || _fieldIsExternalizable ) {
            if ( _fieldIsExternalizable ) {
               _externalizableKeyDump = new Dump<ExternalizableKeyPosition>(ExternalizableKeyPosition.class, _objectKeyDumpFile);
            } else if ( _fieldIsString ) {
               _stringKeyDump = new Dump<StringKeyPosition>(StringKeyPosition.class, _objectKeyDumpFile);
            }
            final InfiniteSorter<IntKeyPosition> sorter = new InfiniteSorter<IntKeyPosition>(_maxLookupSizeInMemory, _dump.getDumpFile().getParentFile(),
               new SingleTypeObjectStreamProvider<Externalizable>(IntKeyPosition.class));
            try (DumpIterator<E> iterator = _dump.iterator()) {
               while ( iterator.hasNext() ) {
                  long keyPos = -1;
                  Object objectKey = getObjectKey(iterator.next());
                  if ( _fieldIsExternalizable ) {
                     keyPos = _externalizableKeyDump._outputStream._n;
                     _externalizableKeyDump.add(new ExternalizableKeyPosition((Externalizable)objectKey, iterator.getPosition()));
                  } else if ( _fieldIsString ) {
                     keyPos = _stringKeyDump._outputStream._n;
                     _stringKeyDump.add(new StringKeyPosition((String)objectKey, iterator.getPosition()));
                  }
                  sorter.add(new IntKeyPosition(objectKey.hashCode(), keyPos));
               }
            }
            catch ( IOException argh ) {
               throw new RuntimeException("Failed to sort InfiniteGroupIndex on disk", argh);
            }
            _intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, getLookupFile());
            _intKeyDump.addAll(sorter);
            _intKeyDump.flush();
            _lookupFileLength = getLookupFile().length();
         } else {
            throw new UnsupportedOperationException("unsupported key type: " + _fieldAccessor.getType());
         }
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to init InfiniteGroupIndex from dump", argh);
      }
   }

   @Override
   protected void initLookupMap() {
      if ( _fieldIsExternalizable || _fieldIsString ) {
         _objectKeyDumpFile = new File(_dump.getDumpFile().getParentFile(), getLookupFile().getName().replaceAll("lookup$", "keys"));
      }
   }

   @Override
   protected void initLookupOutputStream() {
      // we don't need _lookupOutputStream
   }

   @Override
   protected void load() {
      // we load nothing, since this is a disk-based index.
      if ( _fieldIsInt ) {
         _intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, getLookupFile());
      } else if ( _fieldIsLong ) {
         _longKeyDump = new Dump<LongKeyPosition>(LongKeyPosition.class, getLookupFile());
      } else if ( _fieldIsExternalizable || _fieldIsString ) {
         _intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, getLookupFile());
      } else {
         throw new UnsupportedOperationException("unsupported key type: " + _fieldAccessor.getType());
      }

      if ( _fieldIsExternalizable ) {
         _externalizableKeyDump = new Dump<ExternalizableKeyPosition>(ExternalizableKeyPosition.class, _objectKeyDumpFile);
      } else if ( _fieldIsString ) {
         _stringKeyDump = new Dump<StringKeyPosition>(StringKeyPosition.class, _objectKeyDumpFile);
      }

      _lookupFileLength = getLookupFile().length();
   }

   protected void mergeOverflowIntoIndex() {
      try {
         String tmpLookupFileName = getLookupFile().getName() + ".tmp";
         if ( _fieldIsInt ) {
            final InfiniteSorter<IntKeyPosition> sorter = new InfiniteSorter<IntKeyPosition>(_maxLookupSizeInMemory, _dump.getDumpFile().getParentFile(),
               new SingleTypeObjectStreamProvider<Externalizable>(IntKeyPosition.class));
            // the existing dump is already sorted, so this is an optimization to avoid re-externalization and re-sorting of the existing objects
            sorter.addSortedSegment(_intKeyDump);

            // close early, because InfiniteSorter will want to delete this segment as soon as it is finished with merging it
            _intKeyDump.close(); // TODO synchronize 

            _overflowIndex._lookupInt.forEachEntry(new TIntObjectProcedure<Positions>() {

               @Override
               public boolean execute( int key, Positions positions ) {
                  try {
                     for ( long pos : positions.toArray() ) {
                        sorter.add(new IntKeyPosition(key, pos));
                     }
                     return true;
                  }
                  catch ( IOException argh ) {
                     throw new RuntimeException(argh);
                  }
               }
            });
            File tmpLookupFile = new File(getLookupFile().getParent(), tmpLookupFileName);
            Dump<IntKeyPosition> intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, tmpLookupFile);
            intKeyDump.addAll(sorter);
            intKeyDump.close();
            renameTmpLookupFile(tmpLookupFileName, tmpLookupFile);
            _intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, getLookupFile());
         } else if ( _fieldIsLong ) {
            final InfiniteSorter<LongKeyPosition> sorter = new InfiniteSorter<LongKeyPosition>(_maxLookupSizeInMemory, _dump.getDumpFile().getParentFile(),
               new SingleTypeObjectStreamProvider<Externalizable>(LongKeyPosition.class));
            // the existing dump is already sorted, so this is an optimisation to avoid re-externalization and re-sorting of the existing objects
            sorter.addSortedSegment(_longKeyDump);

            // close early, because InfiniteSorter will want to delete this segment as soon as it is finished with merging it
            _longKeyDump.close(); // TODO synchronize

            _overflowIndex._lookupLong.forEachEntry(new TLongObjectProcedure<Positions>() {

               @Override
               public boolean execute( long key, Positions positions ) {
                  try {
                     for ( long pos : positions.toArray() ) {
                        sorter.add(new LongKeyPosition(key, pos));
                     }
                     return true;
                  }
                  catch ( IOException argh ) {
                     throw new RuntimeException(argh);
                  }
               }
            });

            File tmpLookupFile = new File(getLookupFile().getParent(), tmpLookupFileName);
            Dump<LongKeyPosition> longKeyDump = new Dump<LongKeyPosition>(LongKeyPosition.class, tmpLookupFile);
            longKeyDump.addAll(sorter);
            longKeyDump.close();
            renameTmpLookupFile(tmpLookupFileName, tmpLookupFile);
            _longKeyDump = new Dump<LongKeyPosition>(LongKeyPosition.class, getLookupFile());
         } else {
            InfiniteSorter<IntKeyPosition> sorter = new InfiniteSorter<IntKeyPosition>(_maxLookupSizeInMemory, _dump.getDumpFile().getParentFile(),
               new SingleTypeObjectStreamProvider<Externalizable>(IntKeyPosition.class));
            // the existing dump is already sorted, so this is an optimisation to avoid re-externalization and re-sorting of the existing objects
            sorter.addSortedSegment(_intKeyDump);

            // close early, because InfiniteSorter will want to delete this segment as soon as it is finished with merging it
            _intKeyDump.close(); // TODO synchronize 

            for ( Entry<Object, Positions> e : _overflowIndex._lookupObject.entrySet() ) {
               Object objectKey = e.getKey();
               Positions positions = e.getValue();
               for ( long pos : positions.toArray() ) {
                  long keyPos = -1;
                  if ( _fieldIsExternalizable ) {
                     keyPos = _externalizableKeyDump._outputStream._n;
                     _externalizableKeyDump.add(new ExternalizableKeyPosition((Externalizable)objectKey, pos));
                  } else if ( _fieldIsString ) {
                     keyPos = _stringKeyDump._outputStream._n;
                     _stringKeyDump.add(new StringKeyPosition((String)objectKey, pos));
                  }
                  sorter.add(new IntKeyPosition(objectKey.hashCode(), keyPos));
               }
            }
            File tmpLookupFile = new File(getLookupFile().getParent(), tmpLookupFileName);
            Dump<IntKeyPosition> intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, tmpLookupFile);
            intKeyDump.addAll(sorter);
            intKeyDump.close();
            renameTmpLookupFile(tmpLookupFileName, tmpLookupFile);
            _intKeyDump = new Dump<IntKeyPosition>(IntKeyPosition.class, getLookupFile());
         }

         _lookupFileLength = getLookupFile().length();

         _overflowIndex.close();
         _overflowIndex.getLookupFile().delete();
         _overflowIndex.closeAndDeleteUpdatesOutput();
         _overflowIndex = new MyGroupIndex(_dump, _fieldAccessor);

         if ( _cache != null ) {
            _cache.clear();
         }
      }
      catch ( Exception argh ) {
         throw new RuntimeException("Failed to re-organize InfiniteGroupIndex", argh);
      }
   }

   protected void putPositionsIntoCache( Object key, long[] positions ) {
      if ( _cache != null && positions.length < MAX_POSITIONS_LENGTH_IN_CACHE ) {
         _cache.put(key, positions);
      }
   }

   protected void removePositionsFromCache( Object key ) {
      if ( _cache != null ) {
         _cache.remove(key);
      }
   }

   @Override
   void delete( E o, long pos ) {
      _overflowIndex.delete(o, pos);
      // we have no cache in memory, so there's nothing to do
   }

   @Override
   boolean isUpdatable( E oldItem, E newItem ) {
      return true;
   }

   @Override
   void update( long pos, E oldItem, E newItem ) {
      if ( super.isUpdatable(oldItem, newItem) ) {
         return;
      }

      try {
         long[] overflowPositions = _fieldIsInt ? _overflowIndex.getPositions(getIntKey(oldItem))
               : (_fieldIsLong ? _overflowIndex.getPositions(getLongKey(oldItem)) : _overflowIndex.getPositions(getObjectKey(oldItem)));
         for ( long p : overflowPositions ) {
            if ( p == pos ) {
               // only update overflow index if the pos exists there
               _overflowIndex.superUpdate(pos, oldItem, newItem);
            }
         }

         if ( _fieldIsInt ) {
            int key = getIntKey(oldItem);
            int keyLength = 4 + 8; // in bytes

            long firstIndex = findIntKey(key, keyLength);
            if ( firstIndex >= 0 ) {
               for ( long p = firstIndex * keyLength; p < _lookupFileLength; p += keyLength ) {
                  IntKeyPosition ip = _intKeyDump.get(p);
                  if ( ip._key != key ) {
                     break;
                  }
                  if ( ip._pos == pos ) {
                     ip._pos = -1; // mark element as deleted
                     _intKeyDump.update(p, ip);
                  }
               }
            }
            removePositionsFromCache(key);
         } else if ( _fieldIsLong ) {
            long key = getLongKey(oldItem);
            int keyLength = 8 + 8; // in bytes

            long firstIndex = findLongKey(key, keyLength);
            if ( firstIndex >= 0 ) {
               for ( long p = firstIndex * keyLength; p < _lookupFileLength; p += keyLength ) {
                  LongKeyPosition ip = _longKeyDump.get(p);
                  if ( ip._key != key ) {
                     break;
                  }
                  if ( ip._pos == pos ) {
                     ip._pos = -1; // mark element as deleted
                     _longKeyDump.update(p, ip);
                  }
               }
            }
            removePositionsFromCache(key);
         } else {
            Object key = getObjectKey(oldItem);
            TLongList keyPositions = getObjectKeyPositions(key);

            for ( TLongIterator iterator = keyPositions.iterator(); iterator.hasNext(); ) {
               long p = iterator.next();
               if ( _fieldIsExternalizable ) {
                  ExternalizableKeyPosition keyPosition = _externalizableKeyDump.get(p);
                  if ( keyPosition._key.equals(key) && keyPosition._pos == pos ) {
                     keyPosition._pos = -1; // mark element as deleted
                     _externalizableKeyDump.update(p, keyPosition);
                  }
               } else if ( _fieldIsString ) {
                  StringKeyPosition keyPosition = _stringKeyDump.get(p);
                  if ( keyPosition._key.equals(key) && keyPosition._pos == pos ) {
                     keyPosition._pos = -1; // mark element as deleted
                     _stringKeyDump.update(p, keyPosition);
                  }
               }
            }
            removePositionsFromCache(key);
         }

         add(newItem, pos);
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to update InfiniteGroupIndex.", argh);
      }
   }

   private int countDistinctObjects( Set<Object> set ) {
      int num = 0;
      for ( Object object : set ) {
         if ( !_overflowIndex.contains(object) ) {
            num++;
         }
      }
      set.clear();
      return num;
   };

   private long findFirst( int key, int keyLength, long low, long high ) {
      long mid = -1;
      while ( low <= high ) {
         mid = (low + high) >>> 1;
         IntKeyPosition midVal = _intKeyDump.get(mid * keyLength);

         if ( midVal._key < key ) {
            low = mid + 1;
         } else if ( midVal._key == key ) {
            high = mid - 1;
         }
      }

      return low;
   }

   private long findFirst( long key, int keyLength, long low, long high ) {
      long mid = -1;
      while ( low <= high ) {
         mid = (low + high) >>> 1;
         LongKeyPosition midVal = _longKeyDump.get(mid * keyLength);

         if ( midVal._key < key ) {
            low = mid + 1;
         } else if ( midVal._key == key ) {
            high = mid - 1;
         }
      }

      return low;
   }

   /**
    * binary search in the _intKeyDump
    */
   private long findIntKey( int key, int keyLength ) {
      long low = 0;
      long high = (int)_lookupFileLength / keyLength - 1;

      while ( low <= high ) {
         long mid = (low + high) >>> 1;
         IntKeyPosition midVal = _intKeyDump.get(mid * keyLength);

         if ( midVal._key < key ) {
            low = mid + 1;
         } else if ( midVal._key > key ) {
            high = mid - 1;
         } else {
            return findFirst(key, keyLength, low, mid);
         }
      }
      return -1; // not found
   }

   /**
    * binary search in the _longKeyDump
    */
   private long findLongKey( long key, int keyLength ) {
      long low = 0;
      long high = _lookupFileLength / keyLength - 1;

      while ( low <= high ) {
         long mid = (low + high) >>> 1;
         LongKeyPosition midVal = _longKeyDump.get(mid * keyLength);

         if ( midVal._key < key ) {
            low = mid + 1;
         } else if ( midVal._key > key ) {
            high = mid - 1;
         } else {
            return findFirst(key, keyLength, low, mid);
         }
      }
      return -1; // not found
   }

   private TLongList getObjectKeyPositions( Object key ) {
      TLongList keyPositions = new TLongArrayList();
      int keyLength = 4 + 8; // in bytes

      int keyHashCode = key.hashCode();
      long firstIndex = findIntKey(keyHashCode, keyLength);
      if ( firstIndex >= 0 ) {
         for ( long p = firstIndex * keyLength; p < _lookupFileLength; p += keyLength ) {
            IntKeyPosition ip = _intKeyDump.get(p);
            if ( ip._key != keyHashCode ) {
               break;
            }
            keyPositions.add(ip._pos);
         }
      }
      return keyPositions;
   }

   private void isFieldCompatible() {
      if ( !_fieldIsInt && !_fieldIsLong && !_fieldIsExternalizable && !_fieldIsString ) {
         throw new IllegalArgumentException("For usage in an InfiniteGroupIndex the key field must be either int, long, String or Externalizable .");
      }
      if ( _fieldIsExternalizable ) {
         try {
            Object o1 = _fieldAccessor.getType().newInstance();
            Object o2 = _fieldAccessor.getType().newInstance();
            if ( o1.hashCode() == System.identityHashCode(o1) && o2.hashCode() == System.identityHashCode(o2) ) {
               throw new IllegalArgumentException(
                  "For usage in an InfiniteGroupIndex a key type must provide a hashCode() implementation! " + _fieldAccessor.getType() + " doesn't.");
            }
            if ( !o1.equals(o2) ) {
               throw new IllegalArgumentException(
                  "For usage in an InfiniteGroupIndex a key type must provide an equals() implementation! " + _fieldAccessor.getType() + " doesn't.");
            }
         }
         catch ( Exception argh ) {
            if ( argh instanceof IllegalArgumentException ) {
               throw (IllegalArgumentException)argh;
               // else ignore
            }
         }
      }
   }

   private void renameTmpLookupFile( String tmpLookupFileName, File tmpLookupFile ) {
      if ( getLookupFile().exists() && !getLookupFile().delete() ) {
         throw new RuntimeException("Failed to delete old InfiniteGroupIndex lookup " + getLookupFile());
      }
      if ( !tmpLookupFile.renameTo(getLookupFile()) ) {
         throw new RuntimeException("Failed to rename temporary lookup " + tmpLookupFile);
      }
      File meta = new File(getLookupFile().getParent(), tmpLookupFileName + ".meta");
      if ( meta.exists() && !meta.delete() ) {
         throw new RuntimeException("Failed to delete temporary lookup meta file " + meta);
      }
   }


   public static class ExternalizableKeyPosition implements ExternalizableBean, Comparable<ExternalizableKeyPosition> {

      @externalize(1)
      Externalizable _key;

      @externalize(2)
      long           _pos;


      public ExternalizableKeyPosition() {}

      public ExternalizableKeyPosition( Externalizable key, long pos ) {
         _key = key;
         _pos = pos;
      }

      @Override
      public int compareTo( ExternalizableKeyPosition o ) {
         return _key.hashCode() < o._key.hashCode() ? -1 : (_key.hashCode() == o._key.hashCode() ? 0 : 1);
         // don't compare positions (it might become -1 which would lead into a broken sorting)
      }
   }

   public static class IntKeyPosition implements Externalizable, Comparable<IntKeyPosition> {

      int  _key;
      long _pos;


      public IntKeyPosition() {}

      public IntKeyPosition( int key, long pos ) {
         _key = key;
         _pos = pos;
      }

      @Override
      public int compareTo( IntKeyPosition o ) {
         return _key < o._key ? -1 : (_key == o._key ? 0 : 1);
         // don't compare positions (it might become -1 which would lead into a broken sorting)
      }

      @Override
      public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
         _key = in.readInt();
         _pos = in.readLong();
      }

      @Override
      public void writeExternal( ObjectOutput out ) throws IOException {
         out.writeInt(_key);
         out.writeLong(_pos);
      }

   }

   public static class LongKeyPosition implements Externalizable, Comparable<LongKeyPosition> {

      long _key;

      long _pos;


      public LongKeyPosition() {}

      public LongKeyPosition( long key, long pos ) {
         _key = key;
         _pos = pos;
      }

      @Override
      public int compareTo( LongKeyPosition o ) {
         return _key < o._key ? -1 : (_key == o._key ? 0 : 1);
         // don't compare positions (it might become -1 which would lead into a broken sorting)
      }

      @Override
      public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
         _key = in.readLong();
         _pos = in.readLong();
      }

      @Override
      public void writeExternal( ObjectOutput out ) throws IOException {
         out.writeLong(_key);
         out.writeLong(_pos);
      }

   }

   public static class StringKeyPosition implements ExternalizableBean, Comparable<StringKeyPosition> {

      @externalize(1)
      String _key;

      @externalize(2)
      long   _pos;


      public StringKeyPosition() {}

      public StringKeyPosition( String key, long pos ) {
         _key = key;
         _pos = pos;
      }

      @Override
      public int compareTo( StringKeyPosition o ) {
         int r = _key.hashCode() < o._key.hashCode() ? -1 : (_key.hashCode() == o._key.hashCode() ? 0 : 1);
         if ( r != 0 ) {
            return r;
         }
         return _pos < o._pos ? -1 : (_pos == o._pos ? 0 : 1);
      }
   }

   /**
    * The 'overflow' index of this InfiniteGroupIndex, where all keys not yet added to the InfiniteGroupIndex are kept.
    */
   protected class MyGroupIndex extends GroupIndex<E> {

      public MyGroupIndex( Dump<E> dump, FieldAccessor fieldAccessor ) {
         super(dump, fieldAccessor, getOverflowIndexFile(dump, fieldAccessor));
         dump.removeIndex(this);
      }

      @Override
      public void add( E o, long pos ) {
         // do nothing, since the work is being done in InfiniteGroupIndex.this.add(.)
      }

      public void superAdd( E o, long pos ) {
         super.add(o, pos);
      }

      @Override
      protected void initFromDump() {
         // don't initFromDump, we want only the overflow in this index
         super.load();
      }

      void superUpdate( long pos, E oldItem, E newItem ) {
         super.update(pos, oldItem, newItem);
      }

      @Override
      void update( long pos, E oldItem, E newItem ) {
         // do nothing, since the work is being done in InfiniteGroupIndex.this.update(.)
      }
   }

   private final class GroupIterable implements Iterable<E> {

      private final long[] _pos;


      public GroupIterable( long[] pos ) {
         _pos = pos;
      }

      @Override
      public Iterator<E> iterator() {
         return new GroupIterator(_pos);
      }

   }

   private final class GroupIterator implements Iterator<E> {

      private final long[] _pos;
      int                  _i = 0;


      private GroupIterator( long[] pos ) {
         _pos = pos;
         while ( _i < _pos.length && _dump._deletedPositions.contains(_pos[_i]) ) {
            _i++;
         }
      }

      @Override
      public boolean hasNext() {
         return _i < _pos.length;
      }

      @Override
      public E next() {
         if ( _i >= _pos.length ) {
            throw new NoSuchElementException();
         }
         E e = _dump.get(_pos[_i]);
         do {
            _i++;
         }
         while ( _i < _pos.length && _dump._deletedPositions.contains(_pos[_i]) );
         return e;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }
   }
}
