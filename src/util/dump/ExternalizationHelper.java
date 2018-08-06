package util.dump;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.slf4j.LoggerFactory;

import util.dump.ExternalizableBean.externalizationPadding;
import util.dump.ExternalizableBean.externalize;
import util.dump.stream.ExternalizableObjectOutputStream;
import util.reflection.FieldAccessor;
import util.reflection.FieldFieldAccessor;
import util.reflection.MethodFieldAccessor;
import util.reflection.UnsafeFieldFieldAccessor;


@SuppressWarnings({ "unchecked", "ForLoopReplaceableByForEach", "WeakerAccess" })
class ExternalizationHelper {

   private static final long serialVersionUID = -1816997029156670474L;

   static boolean USE_UNSAFE_FIELD_ACCESSORS = true;

   private static Map<Class, ClassConfig> CLASS_CONFIGS = new ConcurrentHashMap<>();

   static Map<Class, Boolean>      CLASS_CHANGED_INCOMPATIBLY = new HashMap<>();
   static ThreadLocal<StreamCache> STREAM_CACHE               = ThreadLocal.withInitial(StreamCache::new);


   static {
      try {
         boolean config = Boolean.parseBoolean(System.getProperty("ExternalizableBean.USE_UNSAFE_FIELD_ACCESSORS", "true"));
         USE_UNSAFE_FIELD_ACCESSORS &= config;
         Class.forName("sun.misc.Unsafe");
      }
      catch ( Exception argh ) {
         USE_UNSAFE_FIELD_ACCESSORS = false;
      }
   }

   static Class<? extends Externalizable> forName( String className, ClassConfig config ) throws ClassNotFoundException {
      return (Class<? extends Externalizable>)Class.forName(className, true, config._classLoader);
   }

   static ClassConfig getConfig( Class<? extends ExternalizableBean> c ) {
      ClassConfig config = CLASS_CONFIGS.get(c);
      if ( config == null ) {
         config = new ClassConfig(c);
         CLASS_CONFIGS.put(c, config);
      }
      return config;
   }

   static byte[] readByteArray( DataInput in ) throws IOException {
      byte[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new byte[in.readInt()];
         for ( int k = 0, length = d.length; k < length; k++ ) {
            d[k] = in.readByte();
         }
      }
      return d;
   }

   static Collection readCollectionContainer( ObjectInput in, Class defaultType, boolean isDefaultType, int size, ClassConfig config,
         ThrowingSupplier instanceReader ) throws Exception {

      boolean unmodifiableList = false, unmodifiableSet = false;
      Collection d;
      if ( isDefaultType ) {
         if ( defaultType.equals(ArrayList.class) ) {
            d = new ArrayList(size);
         } else if ( defaultType.equals(HashSet.class) ) {
            d = new HashSet(size);
         } else {
            d = (Collection)defaultType.newInstance();
         }
      } else {
         String className = in.readUTF();
         if ( "java.util.Collections$SingletonList".equals(className) ) {
            d = Collections.singletonList(instanceReader.get());
            return d;
         } else if ( "java.util.Collections$SingletonSet".equals(className) ) {
            d = Collections.singleton(instanceReader.get());
            return d;
         } else if ( "java.util.Collections$EmptyList".equals(className) ) {
            d = Collections.emptyList();
         } else if ( "java.util.Collections$EmptySet".equals(className) ) {
            d = Collections.emptySet();
         } else if ( "java.util.Collections$UnmodifiableRandomAccessList".equals(className) || "java.util.Collections$UnmodifiableList".equals(className) ) {
            d = new ArrayList<>();
            unmodifiableList = true;
         } else if ( "java.util.Collections$UnmodifiableSet".equals(className) ) {
            d = new HashSet<>();
            unmodifiableSet = true;
         } else if ( "java.util.Arrays$ArrayList".equals(className) ) {
            d = new ArrayList<>();
         } else {
            Class c = forName(className, config);
            d = (Collection)c.newInstance();
         }
      }

      for ( int k = 0; k < size; k++ ) {
         d.add(instanceReader.get());
      }

      if ( unmodifiableList )
         d = Collections.unmodifiableList((List)d);
      else if ( unmodifiableSet )
         d = Collections.unmodifiableSet((Set)d);

      return d;
   }

   static void readCollectionOfExternalizables( ObjectInput in, FieldAccessor f, Class defaultType, Class defaultGenericType, ExternalizableBean thisInstance,
         ClassConfig config ) throws Exception {
      Collection d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         boolean isDefaultType = in.readBoolean();
         int size = in.readInt();
         Class[] lastNonDefaultClass = new Class[1];
         d = readCollectionContainer(in, defaultType, isDefaultType, size, config,
            () -> readExternalizable(in, defaultGenericType, lastNonDefaultClass, config));

      }
      if ( f != null ) {
         f.set(thisInstance, d);
      }
   }

   static void readCollectionOfStrings( ObjectInput in, FieldAccessor f, Class defaultType, ExternalizableBean thisInstance, ClassConfig config )
         throws Exception {
      Collection d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         boolean isDefaultType = in.readBoolean();
         int size = in.readInt();
         d = readCollectionContainer(in, defaultType, isDefaultType, size, config, () -> {
            String s = null;
            if ( in.readBoolean() ) {
               s = DumpUtils.readUTF(in);
            }
            return s;
         });
      }
      if ( f != null ) {
         f.set(thisInstance, d);
      }
   }

   static Date readDate( ObjectInput in ) throws IOException {
      Date d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new Date(in.readLong());
      }
      return d;
   }

   static Date[] readDateArray( ObjectInput in ) throws IOException {
      Date[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new Date[in.readInt()];
         for ( int k = 0, length = d.length; k < length; k++ ) {
            d[k] = readDate(in);
         }
      }
      return d;
   }

   static double[] readDoubleArray( DataInput in ) throws IOException {
      double[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new double[in.readInt()];
         for ( int k = 0, length = d.length; k < length; k++ ) {
            d[k] = in.readDouble();
         }
      }
      return d;
   }

   static Externalizable readExternalizable( ObjectInput in, Class<? extends Externalizable> defaultType, Class[] lastNonDefaultClass, ClassConfig config )
         throws Exception {
      Externalizable instance = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         boolean isDefaultType = in.readBoolean();
         if ( isDefaultType ) {
            instance = defaultType.newInstance();
         } else {
            boolean isSameAsLastNonDefault = in.readBoolean();
            Class c;
            if ( isSameAsLastNonDefault ) {
               c = lastNonDefaultClass[0];
            } else {
               c = forName(in.readUTF(), config);
               lastNonDefaultClass[0] = c;
            }
            instance = (Externalizable)c.newInstance();
         }
         instance.readExternal(in);
      }
      return instance;
   }

   static Externalizable[] readExternalizableArray( ObjectInput in, Class componentType, ClassConfig config ) throws Exception {
      Externalizable[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         int size = in.readInt();
         Class<? extends Externalizable> externalizableClass = componentType;
         d = (Externalizable[])Array.newInstance(externalizableClass, size);
         Class[] lastNonDefaultClass = new Class[1];
         for ( int k = 0; k < size; k++ ) {
            d[k] = readExternalizable(in, externalizableClass, lastNonDefaultClass, config);
         }
      }
      return d;
   }

   static float[] readFloatArray( DataInput in ) throws IOException {
      float[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new float[in.readInt()];
         for ( int k = 0, length = d.length; k < length; k++ ) {
            d[k] = in.readFloat();
         }
      }
      return d;
   }

   static int[] readIntArray( DataInput in ) throws IOException {
      int[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new int[in.readInt()];
         for ( int k = 0, length = d.length; k < length; k++ ) {
            d[k] = in.readInt();
         }
      }
      return d;
   }

   static long[] readLongArray( DataInput in ) throws IOException {
      long[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new long[in.readInt()];
         for ( int k = 0, length = d.length; k < length; k++ ) {
            d[k] = in.readLong();
         }
      }
      return d;
   }

   static String readString( ObjectInput in ) throws IOException {
      String s = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         s = DumpUtils.readUTF(in);
      }
      return s;
   }

   static String[] readStringArray( ObjectInput in ) throws IOException {
      String[] d = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         d = new String[in.readInt()];
         for ( int k = 0, length = d.length; k < length; k++ ) {
            d[k] = readString(in);
         }
      }
      return d;
   }

   static UUID readUUID( ObjectInput in ) throws IOException {
      UUID uuid = null;
      boolean isNotNull = in.readBoolean();
      if ( isNotNull ) {
         long mostSignificantBits = in.readLong();
         long leastSignificantBits = in.readLong();
         uuid = new UUID(mostSignificantBits, leastSignificantBits);
      }
      return uuid;
   }

   static void writeByteArray( byte[] d, ObjectOutput out ) throws IOException {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            out.writeByte(d[j]);
         }
      }
   }

   static void writeCollectionContainer( ObjectOutput out, Class defaultType, Collection d ) throws IOException {
      Class listClass = d.getClass();
      boolean isDefaultType = listClass.equals(defaultType);
      out.writeBoolean(isDefaultType);
      out.writeInt(d.size());
      if ( !isDefaultType ) {
         out.writeUTF(listClass.getName());
      }
   }

   static void writeDate( ObjectOutput out, Date s ) throws IOException {
      out.writeBoolean(s != null);
      if ( s != null ) {
         out.writeLong(s.getTime());
      }
   }

   static void writeDateArray( Date[] d, ObjectOutput out ) throws IOException {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            writeDate(out, d[j]);
         }
      }
   }

   static void writeDoubleArray( double[] d, ObjectOutput out ) throws IOException {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            out.writeDouble(d[j]);
         }
      }
   }

   static void writeExternalizable( ObjectOutput out, Externalizable instance, Class defaultType, Class[] lastNonDefaultClass ) throws Exception {
      out.writeBoolean(instance != null);
      if ( instance != null ) {
         Class c = instance.getClass();
         boolean isDefaultGenericType = c.equals(defaultType);
         out.writeBoolean(isDefaultGenericType);
         if ( !isDefaultGenericType ) {
            boolean isSameAsLastNonDefault = c.equals(lastNonDefaultClass[0]);
            out.writeBoolean(isSameAsLastNonDefault);
            if ( !isSameAsLastNonDefault ) {
               out.writeUTF(c.getName());
               lastNonDefaultClass[0] = c;
            }
         }
         instance.writeExternal(out);
      }
   }

   static void writeExternalizableArray( ObjectOutput out, Externalizable[] d, Class defaultType ) throws Exception {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);

         Class[] lastNonDefaultClass = new Class[0];
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            Externalizable instance = d[j];
            writeExternalizable(out, instance, defaultType, lastNonDefaultClass);
         }
      }

   }

   static void writeFloatArray( float[] d, ObjectOutput out ) throws IOException {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            out.writeFloat(d[j]);
         }
      }
   }

   static void writeIntArray( int[] d, ObjectOutput out ) throws IOException {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            out.writeInt(d[j]);
         }
      }
   }

   static void writeListOfExternalizables( ObjectOutput out, FieldAccessor f, Class defaultType, Class defaultGenericType, ExternalizableBean thisInstance )
         throws Exception {
      List d = (List)f.get(thisInstance);
      out.writeBoolean(d != null);
      if ( d != null ) {
         writeCollectionContainer(out, defaultType, d);

         Class[] lastNonDefaultClass = new Class[1];
         for ( int j = 0, llength = d.size(); j < llength; j++ ) {
            Externalizable instance = (Externalizable)d.get(j);
            writeExternalizable(out, instance, defaultGenericType, lastNonDefaultClass);
         }
      }
   }

   static void writeListOfStrings( ObjectOutput out, FieldAccessor f, Class defaultType, ExternalizableBean thisInstance ) throws Exception {
      List d = (List)f.get(thisInstance);
      out.writeBoolean(d != null);
      if ( d != null ) {
         writeCollectionContainer(out, defaultType, d);
         for ( int j = 0, llength = d.size(); j < llength; j++ ) {
            String s = (String)d.get(j);
            out.writeBoolean(s != null);
            if ( s != null ) {
               DumpUtils.writeUTF(s, out);
            }
         }
      }
   }

   static void writeLongArray( long[] d, ObjectOutput out ) throws IOException {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            out.writeLong(d[j]);
         }
      }
   }

   static void writeSetOfExternalizables( ObjectOutput out, FieldAccessor f, Class defaultType, Class defaultGenericType, ExternalizableBean thisInstance )
         throws Exception {
      Set<Externalizable> d = (Set)f.get(thisInstance);
      out.writeBoolean(d != null);
      if ( d != null ) {
         writeCollectionContainer(out, defaultType, d);

         Class[] lastNonDefaultClass = new Class[1];
         for ( Externalizable instance : d ) {
            writeExternalizable(out, instance, defaultGenericType, lastNonDefaultClass);
         }
      }
   }

   static void writeSetOfStrings( ObjectOutput out, FieldAccessor f, Class defaultType, ExternalizableBean thisInstance ) throws Exception {
      Set<String> d = (Set)f.get(thisInstance);
      out.writeBoolean(d != null);
      if ( d != null ) {
         writeCollectionContainer(out, defaultType, d);

         for ( String s : d ) {
            out.writeBoolean(s != null);
            if ( s != null ) {
               DumpUtils.writeUTF(s, out);
            }
         }
      }
   }

   static void writeString( ObjectOutput out, String s ) throws IOException {
      out.writeBoolean(s != null);
      if ( s != null ) {
         DumpUtils.writeUTF(s, out);
      }
   }

   static void writeStringArray( String[] d, ObjectOutput out ) throws IOException {
      out.writeBoolean(d != null);
      if ( d != null ) {
         out.writeInt(d.length);
         for ( int j = 0, llength = d.length; j < llength; j++ ) {
            writeString(out, d[j]);
         }
      }
   }

   static void writeUUID( ObjectOutput out, UUID uuid ) throws IOException {
      out.writeBoolean(uuid != null);
      if ( uuid != null ) {
         out.writeLong(uuid.getMostSignificantBits());
         out.writeLong(uuid.getLeastSignificantBits());
      }
   }


   @FunctionalInterface
   public interface ThrowingSupplier<T> {

      T get() throws Exception;
   }

   public enum FieldType {
      pInt(int.class, 0), //
      pBoolean(boolean.class, 1), //
      pByte(byte.class, 2), //
      pChar(char.class, 3), //
      pDouble(double.class, 4), //
      pFloat(float.class, 5), //
      pLong(long.class, 6), //
      pShort(short.class, 7), //
      String(String.class, 8), //
      Date(Date.class, 9), //
      Integer(Integer.class, 10), //
      Boolean(Boolean.class, 11), //
      Byte(Byte.class, 12), //
      Character(Character.class, 13), //
      Double(Double.class, 14), //
      Float(Float.class, 15), //
      Long(Long.class, 16), //
      Short(Short.class, 17), //
      Externalizable(Externalizable.class, 18, true), //
      StringArray(String[].class, 19), //
      DateArray(Date[].class, 20), //
      pIntArray(int[].class, 21), //
      pByteArray(byte[].class, 22), //
      pDoubleArray(double[].class, 23), //
      pFloatArray(float[].class, 24), //
      pLongArray(long[].class, 25), //
      ListOfExternalizables(List.class, 26, true), //
      ExternalizableArray(Externalizable[].class, 27, true), //
      ExternalizableArrayArray(Externalizable[][].class, 28, true), //
      Object(Object.class, 29), //
      UUID(UUID.class, 30), //
      StringArrayArray(String[][].class, 31), //
      DateArrayArray(Date[][].class, 32), //
      pIntArrayArray(int[][].class, 33), //
      pByteArrayArray(byte[][].class, 34), //
      pDoubleArrayArray(double[][].class, 35), //
      pFloatArrayArray(float[][].class, 36), //
      pLongArrayArray(long[][].class, 37), //
      EnumOld(Void.class, 38), // Void is just a placeholder - this FieldType is deprecated
      EnumSetOld(Override.class, 39), // Ovcrride is just a placeholder - this FieldType is deprecated
      ListOfStrings(System.class, 40, true), // System is just a placeholder - this FieldType is handled specially
      SetOfExternalizables(Set.class, 41, true), //
      SetOfStrings(Runtime.class, 42, true), // Runtime is just a placeholder - this FieldType is handled specially
      Enum(Enum.class, 43, true), //
      EnumSet(EnumSet.class, 44, true), //
      Padding(Thread.class, 45), //
      BigDecimal(BigDecimal.class, 46), //
      LocalDateTime(LocalDateTime.class, 47), //
      // TODO add Map (beware of Collections.*Map or Treemaps using custom Comparators!)
      ;

      private static Map<Class, FieldType> _classLookup = new HashMap<>(FieldType.values().length);
      private static FieldType[] _idLookup = new FieldType[127];


      static {
         for ( FieldType ft : FieldType.values() ) {
            if ( _classLookup.get(ft._class) != null ) {
               throw new Error("Implementation mistake: FieldType._class must be unique! " + ft._class);
            }
            _classLookup.put(ft._class, ft);
            if ( _idLookup[ft._id] != null ) {
               throw new Error("Implementation mistake: FieldType._id must be unique! " + ft._id);
            }
            _idLookup[ft._id] = ft;
         }
      }

      public static FieldType forClass( Class c ) {
         return _classLookup.get(c);
      }

      public static FieldType forId( byte id ) {
         return _idLookup[id];
      }


      final Class _class;
      final byte _id;
      boolean _lengthDynamic = false;


      FieldType( Class c, int id ) {
         _class = c;
         _id = (byte)id;
      }

      FieldType( Class c, int id, boolean lengthDynamic ) {
         this(c, id);
         _lengthDynamic = lengthDynamic;
      }

      public boolean isLengthDynamic() {
         return _lengthDynamic;
      }
   }

   static class BytesCache extends OutputStream {

      // this is basically an unsynchronized ByteArrayOutputStream with a writeTo(ObjectOutput) method

      protected byte[] _buffer;
      protected int    _count;


      public BytesCache() {
         this(1024);
      }

      public BytesCache( int size ) {
         if ( size < 0 ) {
            throw new IllegalArgumentException("Negative initial size: " + size);
         }
         this._buffer = new byte[size];
      }

      public void reset() {
         _count = 0;
         if ( _buffer.length > 1048576 ) {
            // let it shrink
            _buffer = new byte[1024];
         }
      }

      public int size() {
         return _count;
      }

      @Override
      public void write( @Nonnull byte[] bytes, int start, int length ) {
         if ( (start < 0) || (start > bytes.length) || (length < 0) || (start + length > bytes.length) || (start + length < 0) ) {
            throw new IndexOutOfBoundsException();
         }
         if ( length == 0 ) {
            return;
         }
         int i = _count + length;
         if ( i > _buffer.length ) {
            _buffer = Arrays.copyOf(_buffer, Math.max(_buffer.length << 1, i));
         }
         System.arraycopy(bytes, start, _buffer, _count, length);
         this._count = i;
      }

      @Override
      public void write( int data ) {
         int i = this._count + 1;
         if ( i > this._buffer.length ) {
            this._buffer = Arrays.copyOf(_buffer, Math.max(_buffer.length << 1, i));
         }
         _buffer[_count] = (byte)data;
         _count = i;
      }

      public void writeTo( ObjectOutput out ) throws IOException {
         out.write(_buffer, 0, _count);
      }
   }

   static class ClassConfig {

      Class           _class;
      ClassLoader     _classLoader;
      FieldAccessor[] _fieldAccessors;
      byte[]          _fieldIndexes;
      FieldType[]     _fieldTypes;
      Class[]         _defaultTypes;
      Class[]         _defaultGenericTypes0;
      Class[]         _defaultGenericTypes1;
      int             _sizeModulo = -1;


      public ClassConfig( Class clientClass ) {
         try {
            clientClass.getConstructor();
         }
         catch ( NoSuchMethodException argh ) {
            throw new RuntimeException(clientClass + " extends ExternalizableBean, but does not have a public nullary constructor.");
         }

         _class = clientClass;
         _classLoader = clientClass.getClassLoader();
         List<FieldInfo> fieldInfos = new ArrayList<>();

         initFromFields(fieldInfos);

         initFromMethods(fieldInfos);

         initSizeModulo(fieldInfos);

         Collections.sort(fieldInfos);

         _fieldAccessors = new FieldAccessor[fieldInfos.size()];
         _fieldIndexes = new byte[fieldInfos.size()];
         _fieldTypes = new FieldType[fieldInfos.size()];
         _defaultTypes = new Class[fieldInfos.size()];
         _defaultGenericTypes0 = new Class[fieldInfos.size()];
         _defaultGenericTypes1 = new Class[fieldInfos.size()];
         for ( int i = 0, length = fieldInfos.size(); i < length; i++ ) {
            FieldInfo fi = fieldInfos.get(i);
            _fieldAccessors[i] = fi._fieldAccessor;
            _fieldIndexes[i] = fi._fieldIndex;
            _fieldTypes[i] = fi._fieldType;
            _defaultTypes[i] = fi._defaultType;
            _defaultGenericTypes0[i] = fi._defaultGenericType0;
            _defaultGenericTypes1[i] = fi._defaultGenericType1;
         }
         if ( _fieldAccessors.length == 0 ) {
            throw new RuntimeException(_class + " extends ExternalizableBean, but it has no externalizable fields or methods. "
               + "This is most probably a bug. Externalizable fields and methods must be public.");
         }
      }

      private void addFieldInfo( List<FieldInfo> fieldInfos, externalize annotation, FieldAccessor fieldAccessor, Class type, String fieldName ) {
         FieldInfo fi = new FieldInfo();
         fi._fieldAccessor = fieldAccessor;
         fi._fieldType = getFieldType(fi, type);
         fi.setDefaultType(type, fi._fieldAccessor, fi._fieldType, annotation);

         byte index = annotation.value();
         for ( FieldInfo ffi : fieldInfos ) {
            if ( ffi._fieldIndex == index ) {
               throw new RuntimeException(_class + " extends ExternalizableBean, but " + fieldName + " has a non-unique index " + index);
            }
         }
         fi._fieldIndex = index;
         fieldInfos.add(fi);
      }

      private FieldType getFieldType( FieldInfo fi, Class type ) {
         FieldType ft = FieldType.forClass(type);
         if ( ft == null ) {
            if ( Externalizable.class.isAssignableFrom(type) ) {
               ft = FieldType.Externalizable;
            }
         }
         if ( ft == null ) {
            Class arrayType = type.getComponentType();
            if ( arrayType != null && Externalizable.class.isAssignableFrom(arrayType) ) {
               ft = FieldType.ExternalizableArray;
            }
         }
         if ( ft == null ) {
            Class arrayType = type.getComponentType();
            if ( arrayType != null ) {
               arrayType = arrayType.getComponentType();
               if ( arrayType != null && Externalizable.class.isAssignableFrom(arrayType) ) {
                  ft = FieldType.ExternalizableArrayArray;
               }
            }
         }
         if ( ft == null ) {
            if ( Enum.class.isAssignableFrom(type) ) {
               ft = FieldType.Enum;
            }
         }
         if ( ft == null ) {
            ft = FieldType.Object;
            LoggerFactory.getLogger(_class)
                  .warn("The field type of index " + fi._fieldIndex + //
                     " is not of a supported type, thus falling back to Object serialization." + //
                     " This might be very slow of even fail, dependant on your ObjectStreamProvider." + //
                     " Please check, whether this is really what you wanted!");
         }
         if ( (ft == FieldType.ListOfExternalizables || ft == FieldType.SetOfExternalizables) //
            && (fi._fieldAccessor.getGenericTypes().length != 1 || !Externalizable.class.isAssignableFrom(fi._fieldAccessor.getGenericTypes()[0])) ) {
            if ( fi._fieldAccessor.getGenericTypes().length == 1 && String.class == fi._fieldAccessor.getGenericTypes()[0] ) {
               ft = (ft == FieldType.ListOfExternalizables) ? FieldType.ListOfStrings : FieldType.SetOfStrings;
            } else {
               ft = FieldType.Object;
               LoggerFactory.getLogger(_class)
                     .warn("The field type of index " + fi._fieldIndex + //
                        " has a Collection with an unsupported type as generic parameter, thus falling back to Object serialization." + //
                        " This might be very slow of even fail, dependant on your ObjectStreamProvider." + //
                        " Please check, whether this is really what you wanted!");
            }
         }

         return ft;
      }

      private void initFromFields( List<FieldInfo> fieldInfos ) {
         Class c = _class;
         while ( c != Object.class ) {
            for ( Field f : c.getDeclaredFields() ) {
               int mod = f.getModifiers();
               if ( Modifier.isFinal(mod) || Modifier.isStatic(mod) ) {
                  continue;
               }
               externalize annotation = f.getAnnotation(externalize.class);
               if ( annotation == null ) {
                  continue;
               }

               if ( !Modifier.isPublic(mod) ) {
                  f.setAccessible(true); // enable access to the field - ...hackity hack
               }

               FieldFieldAccessor fieldAccessor = USE_UNSAFE_FIELD_ACCESSORS ? new UnsafeFieldFieldAccessor(f) : new FieldFieldAccessor(f);
               Class type = f.getType();
               addFieldInfo(fieldInfos, annotation, fieldAccessor, type, f.getName());
            }
            c = c.getSuperclass();
         }
      }

      private void initFromMethods( List<FieldInfo> fieldInfos ) {
         methodLoop: for ( Method m : _class.getMethods() ) {
            int mod = m.getModifiers();
            if ( Modifier.isStatic(mod) ) {
               continue;
            }

            Method getter = null, setter = null;
            if ( m.getName().startsWith("get")
               || (m.getName().startsWith("is") && (m.getReturnType() == boolean.class || m.getReturnType() == Boolean.class)) ) {
               getter = m;
            } else if ( m.getName().startsWith("set") ) {
               setter = m;
            } else {
               continue;
            }

            if ( getter != null ) {
               for ( FieldInfo ffi : fieldInfos ) {
                  if ( ffi._fieldAccessor instanceof MethodFieldAccessor ) {
                     MethodFieldAccessor mfa = (MethodFieldAccessor)ffi._fieldAccessor;
                     if ( mfa.getGetter().getName().equals(getter.getName()) ) {
                        continue methodLoop;
                     }
                  }
               }

               Class type = getter.getReturnType();
               if ( getter.getParameterTypes().length > 0 ) {
                  externalize getterAnnotation = getter.getAnnotation(externalize.class);
                  if ( getterAnnotation != null ) {
                     throw new RuntimeException(
                        _class + " extends ExternalizableBean, but the annotated getter method " + getter.getName() + " has a parameter.");
                  } else {
                     continue;
                  }
               }

               try {
                  String name = getter.getName();
                  name = getter.getName().startsWith("is") ? name.substring(2) : name.substring(3);
                  setter = _class.getMethod("set" + name, type);
               }
               catch ( NoSuchMethodException e ) {
                  externalize getterAnnotation = getter.getAnnotation(externalize.class);
                  if ( getterAnnotation != null ) {
                     throw new RuntimeException(_class + " extends ExternalizableBean, but the annotated getter method " + getter.getName()
                        + " has no appropriate setter with the correct parameter.");
                  } else {
                     continue;
                  }
               }
            } else if ( setter != null ) {
               for ( FieldInfo ffi : fieldInfos ) {
                  if ( ffi._fieldAccessor instanceof MethodFieldAccessor ) {
                     MethodFieldAccessor mfa = (MethodFieldAccessor)ffi._fieldAccessor;
                     if ( mfa.getSetter().getName().equals(setter.getName()) ) {
                        continue methodLoop;
                     }
                  }
               }

               if ( setter.getParameterTypes().length != 1 ) {
                  externalize setterAnnotation = setter.getAnnotation(externalize.class);
                  if ( setterAnnotation != null ) {
                     throw new RuntimeException(
                        _class + " extends ExternalizableBean, but the annotated setter method " + setter.getName() + " does not have a single parameter.");
                  } else {
                     continue;
                  }
               }
               Class type = setter.getParameterTypes()[0];

               try {
                  String prefix = (type == boolean.class || type == Boolean.class) ? "is" : "get";
                  getter = _class.getMethod(prefix + setter.getName().substring(3));
               }
               catch ( NoSuchMethodException e ) {
                  externalize setterAnnotation = setter.getAnnotation(externalize.class);
                  if ( setterAnnotation != null ) {
                     throw new RuntimeException(
                        _class + " extends ExternalizableBean, but the annotated setter method " + setter.getName() + " has no appropriate getter.");
                  } else {
                     continue;
                  }
               }

               if ( getter.getReturnType() != type ) {
                  externalize setterAnnotation = setter.getAnnotation(externalize.class);
                  externalize getterAnnotation = getter.getAnnotation(externalize.class);
                  if ( getterAnnotation != null || setterAnnotation != null ) {
                     throw new RuntimeException(_class + " extends ExternalizableBean, but the annotated setter method " + setter.getName()
                        + " has no getter with the correct return type.");
                  } else {
                     continue;
                  }
               }
            }

            externalize getterAnnotation = getter.getAnnotation(externalize.class);
            externalize setterAnnotation = setter.getAnnotation(externalize.class);
            if ( getterAnnotation == null && setterAnnotation == null ) {
               continue;
            }
            if ( getterAnnotation != null && setterAnnotation != null && getterAnnotation.value() != setterAnnotation.value() ) {
               throw new RuntimeException(_class + " extends ExternalizableBean, but the getter/setter pair " + getter.getName()
                  + " has different indexes in the externalize annotations.");
            }
            externalize annotation = getterAnnotation == null ? setterAnnotation : getterAnnotation;

            FieldAccessor fieldAccessor = new MethodFieldAccessor(getter, setter);
            Class type = getter.getReturnType();
            addFieldInfo(fieldInfos, annotation, fieldAccessor, type, getter.getName());
         }
      }

      private void initSizeModulo( List<FieldInfo> fieldInfos ) {
         Class c = _class;
         while ( c != Object.class ) {
            externalizationPadding annotation = (externalizationPadding)c.getAnnotation(externalizationPadding.class);
            if ( annotation != null ) {
               _sizeModulo = annotation.sizeModulo();

               FieldInfo fi = new FieldInfo();
               fi._fieldIndex = (byte)255;
               fi._fieldType = FieldType.Padding;
               fieldInfos.add(fi);
               break;
            }

            c = c.getSuperclass();
         }
      }
   }

   static class FieldInfo implements Comparable<FieldInfo> {

      FieldAccessor _fieldAccessor;
      FieldType     _fieldType;
      byte          _fieldIndex;
      Class         _defaultType;
      Class         _defaultGenericType0;
      Class         _defaultGenericType1;


      @Override
      public int compareTo( @Nonnull FieldInfo o ) {
         int fieldIndex = _fieldIndex & 0xFF;
         int otherFieldIndex = o._fieldIndex & 0xFF;
         return Integer.compare(fieldIndex, otherFieldIndex);
      }

      private void setDefaultType( Class type, FieldAccessor fieldAccessor, FieldType ft, externalize annotation ) {
         _defaultType = type;
         if ( ft == FieldType.ExternalizableArray ) {
            _defaultType = type.getComponentType();
         } else if ( ft == FieldType.ExternalizableArrayArray ) {
            _defaultType = type.getComponentType().getComponentType();
         } else if ( ft == FieldType.ListOfExternalizables || ft == FieldType.ListOfStrings ) {
            _defaultType = ArrayList.class;
         } else if ( ft == FieldType.SetOfExternalizables || ft == FieldType.SetOfStrings ) {
            _defaultType = HashSet.class;
         }

         if ( annotation.defaultType() != System.class ) {
            _defaultType = annotation.defaultType();

            try {
               _defaultType.newInstance();
            }
            catch ( Exception argh ) {
               throw new RuntimeException("Field " + fieldAccessor.getName() + " with index " + _fieldIndex + " has defaultType " + _defaultType
                  + " which has no public nullary constructor.");
            }

            if ( ft == FieldType.ListOfExternalizables || ft == FieldType.ListOfStrings ) {
               if ( !List.class.isAssignableFrom(_defaultType) ) {
                  throw new RuntimeException("defaultType for a List field must be a List! Field " + fieldAccessor.getName() + " with index " + _fieldIndex
                     + " has defaultType " + _defaultType);
               }
            }
            if ( ft == FieldType.SetOfExternalizables || ft == FieldType.SetOfStrings ) {
               if ( !Set.class.isAssignableFrom(_defaultType) ) {
                  throw new RuntimeException("defaultType for a Set field must be a Set! Field " + fieldAccessor.getName() + " with index " + _fieldIndex
                     + " has defaultType " + _defaultType);
               }
            }
         }

         if ( ft == FieldType.ListOfExternalizables || ft == FieldType.SetOfExternalizables ) {
            _defaultGenericType0 = fieldAccessor.getGenericTypes()[0];
            if ( annotation.defaultGenericType0() != System.class ) {
               _defaultGenericType0 = annotation.defaultType();

               try {
                  _defaultGenericType0.newInstance();
               }
               catch ( Exception argh ) {
                  throw new RuntimeException(" Field " + fieldAccessor.getName() + " with index " + _fieldIndex + " has defaultGenericType0 "
                     + _defaultGenericType0 + " which has no public nullary constructor.");
               }

               if ( !Externalizable.class.isAssignableFrom(_defaultType) ) {
                  throw new RuntimeException("defaultGenericType0 for a field with a collection of Externalizables must be an Externalizable! Field "
                     + fieldAccessor.getName() + " with index " + _fieldIndex + " has defaultGenericType0 " + _defaultGenericType0);
               }
            }
         }
      }
   }

   static class StreamCache {

      BytesCache   _bytesCache = new BytesCache();
      ObjectOutput _objectOutput;
      boolean      _inUse      = false;


      public StreamCache() {
         try {
            _objectOutput = new ExternalizableObjectOutputStream(_bytesCache);
         }
         catch ( IOException argh ) {
            // insane, cannot happen
         }
      }
   }
}
