package util.dump.reflection;

import java.lang.reflect.Field;
import java.lang.reflect.Method;


public class Reflection {

   /**
    * Get a <code>Field</code> instance for any the combination of class and field name.
    * This utility method allows accessing protected, package protected and private fields - at least if the
    * <code>SecurityManager</code> allows this hack, which it does in practically all runtimes...
    */
   public static Field getField( Class c, String fieldName ) throws NoSuchFieldException {
      try {
         return c.getField(fieldName);
      }
      catch ( NoSuchFieldException e ) {
         try {
            // search protected, package protected and private methods
            while ( c != null && c != Object.class ) {
               for ( Field f : c.getDeclaredFields() ) {
                  if ( f.getName().equals(fieldName) ) {
                     f.setAccessible(true); // enable access to the method - ...hackity hack
                     return f;
                  }
               }
               c = c.getSuperclass();
            }
         }
         catch ( SecurityException ee ) {
            // ignore and throw the original NoSuchFieldException
         }
         // search not successful -> throw original NoSuchFieldException
         throw e;
      }
   }

   /**
    * Get a <code>Field</code> instance for any the combination of class and field name.
    * This utility method allows accessing protected, package protected and private fields - at least if the
    * <code>SecurityManager</code> allows this hack, which it does in practically all runtimes...
    */
   public static Field getFieldQuietly( Class c, String fieldName ) {
      try {
         return getField(c, fieldName);
      }
      catch ( NoSuchFieldException argh ) {
         return null;
      }
   }

   /**
    * Get a <code>Method</code> instance for any the combination of class, method name and parameter signature.
    * This utility method allows accessing protected, package protected and private methods - at least if the
    * <code>SecurityManager</code> allows this hack, which it does in practically all runtimes...
    */
   public static Method getMethod( Class c, String methodName, Class... argumentClasses ) throws NoSuchMethodException {
      try {
         return c.getMethod(methodName, argumentClasses);
      }
      catch ( NoSuchMethodException e ) {
         try {
            // search protected, package protected and private methods
            while ( c != null && c != Object.class ) {
               out:
               for ( Method m : c.getDeclaredMethods() ) {
                  Class[] parameterTypes = m.getParameterTypes();
                  if ( m.getName().equals(methodName) && argumentClasses.length == parameterTypes.length ) {
                     for ( int j = 0, length = argumentClasses.length; j < length; j++ ) {
                        if ( !argumentClasses[j].equals(parameterTypes[j]) ) {
                           continue out;
                        }
                     }
                     m.setAccessible(true); // enable access to the method - ...hackity hack
                     return m;
                  }
               }
               c = c.getSuperclass();
            }
         }
         catch ( SecurityException ee ) {
            // ignore and throw the original NoSuchMethodException
         }
         // search not successful -> throw original NoSuchMethodException
         throw e;
      }
   }
}
