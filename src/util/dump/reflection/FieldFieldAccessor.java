package util.dump.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;


public class FieldFieldAccessor
    implements FieldAccessor {

  Field   _field;
  Class   _type;
  Class[] _genericTypes;

  public FieldFieldAccessor( Field field ) {
    _field = field;
    _type = _field.getType();
    Type type = _field.getGenericType();
    if (type instanceof ParameterizedType) {
      Type[] actualTypeArguments = ((ParameterizedType)type).getActualTypeArguments();
      _genericTypes = new Class[actualTypeArguments.length];
      for (int i = 0, length = actualTypeArguments.length; i < length; i++) {
        _genericTypes[i] = (Class)actualTypeArguments[i];
      }
    }
  }

  public String getName() {
    return _field.getName();
  }

  public Object get( Object obj ) throws Exception {
    return _field.get( obj );
  }

  public boolean getBoolean( Object obj ) throws Exception {
    return _field.getBoolean( obj );
  }

  public byte getByte( Object obj ) throws Exception {
    return _field.getByte( obj );
  }

  public char getChar( Object obj ) throws Exception {
    return _field.getChar( obj );
  }

  public double getDouble( Object obj ) throws Exception {
    return _field.getDouble( obj );
  }

  public float getFloat( Object obj ) throws Exception {
    return _field.getFloat( obj );
  }

  public Class[] getGenericTypes() {
    return _genericTypes;
  }

  public int getInt( Object obj ) throws Exception {
    return _field.getInt( obj );
  }

  public long getLong( Object obj ) throws Exception {
    return _field.getLong( obj );
  }

  public short getShort( Object obj ) throws Exception {
    return _field.getShort( obj );
  }

  public Class getType() {
    return _type;
  }

  public void set( Object o, Object d ) throws Exception {
    _field.set( o, d );
  }

  public void setBoolean( Object o, boolean d ) throws Exception {
    _field.setBoolean( o, d );
  }

  public void setByte( Object o, byte d ) throws Exception {
    _field.setByte( o, d );
  }

  public void setChar( Object o, char d ) throws Exception {
    _field.setChar( o, d );
  }

  public void setDouble( Object o, double d ) throws Exception {
    _field.setDouble( o, d );
  }

  public void setFloat( Object o, float d ) throws Exception {
    _field.setFloat( o, d );
  }

  public void setInt( Object o, int d ) throws Exception {
    _field.setInt( o, d );
  }

  public void setLong( Object o, long d ) throws Exception {
    _field.setLong( o, d );
  }

  public void setShort( Object o, short d ) throws Exception {
    _field.setShort( o, d );
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_field == null) ? 0 : _field.hashCode());
    result = prime * result + ((_type == null) ? 0 : _type.hashCode());
    return result;
  }

  public boolean equals( Object obj ) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    FieldFieldAccessor other = (FieldFieldAccessor)obj;
    if (_field == null) {
      if (other._field != null) return false;
    } else if (!_field.equals( other._field )) return false;
    if (_type == null) {
      if (other._type != null) return false;
    } else if (!_type.equals( other._type )) return false;
    return true;
  }

}
