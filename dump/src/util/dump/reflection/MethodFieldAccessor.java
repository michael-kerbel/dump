package util.dump.reflection;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;


public class MethodFieldAccessor
    implements FieldAccessor {

  private final Method _getter;
  private final Method _setter;
  private final Class  _type;
  private Class[]      _genericTypes;

  public MethodFieldAccessor( Method getter, Method setter ) {
    _getter = getter;
    _setter = setter;

    _type = getter.getReturnType();

    Type type = _getter.getGenericReturnType();
    if (type instanceof ParameterizedType) {
      Type setterTypes = _setter.getGenericParameterTypes()[0];
      if (!(setterTypes instanceof ParameterizedType)) { throw new RuntimeException( "getter/setter pair " + getter.getName() +
                                                                                     " must have the same generic types." ); }
      Type[] actualTypeArguments = ((ParameterizedType)type).getActualTypeArguments();
      Type[] setterActualTypeArguments = ((ParameterizedType)setterTypes).getActualTypeArguments();
      _genericTypes = new Class[actualTypeArguments.length];
      for (int i = 0, length = actualTypeArguments.length; i < length; i++) {
        _genericTypes[i] = (Class)actualTypeArguments[i];
        if (!_genericTypes[i].equals( setterActualTypeArguments[i] )) { throw new RuntimeException(
                                                                                                    "getter/setter pair " +
                                                                                                        getter.getName() +
                                                                                                        " must have the same generic types. " +
                                                                                                        actualTypeArguments[i] +
                                                                                                        "!=" +
                                                                                                        setterActualTypeArguments[i] ); }
      }
    }
  }

  public String getName() {
    return Character.toLowerCase( _getter.getName().charAt( 3 ) ) + _getter.getName().substring( 4 );
  }

  public Method getSetter() {
    return _setter;
  }

  public Method getGetter() {
    return _getter;
  }

  public Object get( Object obj ) throws Exception {
    return _getter.invoke( obj );
  }

  public boolean getBoolean( Object obj ) throws Exception {
    return (Boolean)_getter.invoke( obj );
  }

  public byte getByte( Object obj ) throws Exception {
    return (Byte)_getter.invoke( obj );
  }

  public char getChar( Object obj ) throws Exception {
    return (Character)_getter.invoke( obj );
  }

  public double getDouble( Object obj ) throws Exception {
    return (Double)_getter.invoke( obj );
  }

  public float getFloat( Object obj ) throws Exception {
    return (Float)_getter.invoke( obj );
  }

  public Class[] getGenericTypes() {
    return _genericTypes;
  }

  public int getInt( Object obj ) throws Exception {
    return (Integer)_getter.invoke( obj );
  }

  public long getLong( Object obj ) throws Exception {
    return (Long)_getter.invoke( obj );
  }

  public short getShort( Object obj ) throws Exception {
    return (Short)_getter.invoke( obj );
  }

  public Class getType() {
    return _type;
  }

  public void set( Object o, Object d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setBoolean( Object o, boolean d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setByte( Object o, byte d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setChar( Object o, char d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setDouble( Object o, double d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setFloat( Object o, float d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setInt( Object o, int d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setLong( Object o, long d ) throws Exception {
    _setter.invoke( o, d );
  }

  public void setShort( Object o, short d ) throws Exception {
    _setter.invoke( o, d );
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((_getter == null) ? 0 : _getter.hashCode());
    result = prime * result + ((_type == null) ? 0 : _type.hashCode());
    return result;
  }

  public boolean equals( Object obj ) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    MethodFieldAccessor other = (MethodFieldAccessor)obj;
    if (_getter == null) {
      if (other._getter != null) return false;
    } else if (!_getter.equals( other._getter )) return false;
    if (_type == null) {
      if (other._type != null) return false;
    } else if (!_type.equals( other._type )) return false;
    return true;
  }
}
