package util.dump.reflection;

public interface FieldAccessor {

  public Object get( Object obj ) throws Exception;

  public boolean getBoolean( Object obj ) throws Exception;

  public byte getByte( Object obj ) throws Exception;

  public char getChar( Object obj ) throws Exception;

  public double getDouble( Object obj ) throws Exception;

  public float getFloat( Object obj ) throws Exception;

  public Class[] getGenericTypes();

  public int getInt( Object obj ) throws Exception;

  public long getLong( Object obj ) throws Exception;

  public short getShort( Object obj ) throws Exception;

  public Class getType();

  public void set( Object o, Object d ) throws Exception;

  public void setBoolean( Object o, boolean d ) throws Exception;

  public void setByte( Object o, byte d ) throws Exception;

  public void setChar( Object o, char d ) throws Exception;

  public void setDouble( Object o, double d ) throws Exception;

  public void setFloat( Object o, float d ) throws Exception;

  public void setInt( Object o, int d ) throws Exception;

  public void setLong( Object o, long d ) throws Exception;

  public void setShort( Object o, short d ) throws Exception;

  public String getName();
}