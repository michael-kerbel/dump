package util.dump.reflection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;


public class UnsafeFieldFieldAccessor extends FieldFieldAccessor {

   private static final Logger _log = LoggerFactory.getLogger(UnsafeFieldFieldAccessor.class);

   static sun.misc.Unsafe UNSAFE;

   static {
      try {
         Class unsafeClass = sun.misc.Unsafe.class;
         Field unsafeField = Reflection.getField(unsafeClass, "theUnsafe");
         UNSAFE = (sun.misc.Unsafe)unsafeField.get(null);
      }
      catch ( Exception argh ) {
         _log.warn("Failed to get instance of sun.misc.Unsafe - fallback to regular reflection", argh);
      }
   }

   private final long _fieldOffset;


   public UnsafeFieldFieldAccessor( Field field ) {
      super(field);
      _fieldOffset = UNSAFE.objectFieldOffset(field);
   }

   @Override
   public Object get( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getObject(obj, _fieldOffset);
      return super.get(obj);
   }

   @Override
   public boolean getBoolean( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getBoolean(obj, _fieldOffset);
      return super.getBoolean(obj);
   }

   @Override
   public byte getByte( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getByte(obj, _fieldOffset);
      return super.getByte(obj);
   }

   @Override
   public char getChar( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getChar(obj, _fieldOffset);
      return super.getChar(obj);
   }

   @Override
   public double getDouble( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getDouble(obj, _fieldOffset);
      return super.getDouble(obj);
   }

   @Override
   public float getFloat( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getFloat(obj, _fieldOffset);
      return super.getFloat(obj);
   }

   @Override
   public int getInt( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getInt(obj, _fieldOffset);
      return super.getInt(obj);
   }

   @Override
   public long getLong( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getLong(obj, _fieldOffset);
      return super.getLong(obj);
   }

   @Override
   public short getShort( Object obj ) throws Exception {
      if ( UNSAFE != null ) return UNSAFE.getShort(obj, _fieldOffset);
      return super.getShort(obj);
   }

   @Override
   public void set( Object o, Object d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putObject(o, _fieldOffset, d);
      else
         super.set(o, d);
   }

   @Override
   public void setBoolean( Object o, boolean d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putBoolean(o, _fieldOffset, d);
      else
         super.setBoolean(o, d);
   }

   @Override
   public void setByte( Object o, byte d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putByte(o, _fieldOffset, d);
      else
         super.setByte(o, d);
   }

   @Override
   public void setChar( Object o, char d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putChar(o, _fieldOffset, d);
      else
         super.setChar(o, d);
   }

   @Override
   public void setDouble( Object o, double d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putDouble(o, _fieldOffset, d);
      else
         super.setDouble(o, d);
   }

   @Override
   public void setFloat( Object o, float d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putFloat(o, _fieldOffset, d);
      else
         super.setFloat(o, d);
   }

   @Override
   public void setInt( Object o, int d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putInt(o, _fieldOffset, d);
      else
         super.setInt(o, d);
   }

   @Override
   public void setLong( Object o, long d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putLong(o, _fieldOffset, d);
      else
         super.setLong(o, d);
   }

   @Override
   public void setShort( Object o, short d ) throws Exception {
      if ( UNSAFE != null )
         UNSAFE.putShort(o, _fieldOffset, d);
      else
         super.setShort(o, d);
   }

}
