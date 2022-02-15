/* -------------------------------------------------
 * Projekt:     cooceo
 * Datum:       2005/11/25 12:24:31
 * Autor:       michaelkrkoska
 * Version:     1.1
 * Datei:       /cvs/cooceo/src/mentasys/cooceo/util/io/IOUtils.java,v
 * angelegt:    15.02.2005 14:20:27
 * --------------------------------------------------
 * Copyright (c) Mentasys GmbH, 2005
 * --------------------------------------------------
 */
package util.dump.io;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.regex.Pattern;


public class IOUtils {

   /**
    * Closes the given <code>{@link Closeable}</code>s, ignoring any thrown exceptions.
    * @param closeables the <code>Closeable</code>s to close.
    */
   public static void close( Closeable... closeables ) {
      for ( Closeable c : closeables )
         closeCloseable(c);
   }

   /**
    * Deletes a directory and its content recursively.
    * If <code>file</code> is not a directory, it is deleted. If <code>file</code> does not exist, nothing happens.
    */
   public static boolean deleteDir( File file ) {
      if ( !file.exists() ) {
         return false;
      }
      if ( file.isDirectory() ) {
         File[] files = file.listFiles();
         for ( int i = 0; files != null && i < files.length; i++ ) {
            deleteDir(files[i]);
         }
      }
      return file.delete();
   }

   /**
    * Quietly get the canonical file (i.e. throw a {@link RuntimeException} instead of the {@link IOException})
    */
   public static File getCanonicalFileQuietly( File file ) {
      try {
         return file.getCanonicalFile();
      }
      catch ( IOException e ) {
         throw new RuntimeException("failed to resolve canonical file for " + file, e);
      }
   }

   public static String readAsString( File file ) throws IOException {
      return readAsString(new FileInputStream(file));
   }

   public static String readAsString( InputStream in ) throws IOException {
      return readAsString(in, Charset.defaultCharset().name());
   }

   public static String readAsString( InputStream in, String encoding ) throws IOException {
      BufferedReader reader = null;
      StringBuilder sb;
      try {
         reader = new BufferedReader(new InputStreamReader(in, encoding));
         sb = new StringBuilder();
         String line = null;
         while ( (line = reader.readLine()) != null ) {
            sb.append(line).append('\n');
         }
      }
      finally {
         if ( reader != null ) {
            try {
               reader.close();
            }
            catch ( IOException e ) {}
         }
      }

      return sb.toString();
   }

   private static void closeCloseable( Closeable c ) {
      if ( c == null ) return;
      try {
         c.close();
      }
      catch ( Exception e ) {}
   }
}

//--------------------------------------------------
// IOUtils.java,v 1.1 2005/11/25 12:24:31 michaelkrkoska Exp
//--------------------------------------------------
// Historie:
//
// IOUtils.java,v
// Revision 1.1  2005/11/25 12:24:31  michaelkrkoska
// *** empty log message ***
//
// Revision 1.2  2005/02/16 13:57:37  michaelkrkoska
// *** empty log message ***
//
// Revision 1.1  2005/02/15 18:03:20  michaelkrkoska
// *** empty log message ***
//
//--------------------------------------------------
