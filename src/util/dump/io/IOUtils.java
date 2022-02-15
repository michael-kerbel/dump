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
    * Copies a single file to another location. Overwrites existing targets.
    * Implementation is native and memory efficient.
    */
   public static void copy( File source, File dest ) throws IOException {
      if ( Boolean.getBoolean("mentasys.util.io.noNIO") ) {
         copyOldIO(source, dest);
         return;
      }

      FileChannel in = null, out = null;
      try {
         in = new FileInputStream(source).getChannel();
         out = new FileOutputStream(dest).getChannel();

         long size = in.size();
         long pos = 0;
         while ( pos < size ) {
            pos += in.transferTo(pos, size, out);
         }
      }
      finally {
         if ( in != null ) {
            in.close();
         }
         if ( out != null ) {
            out.close();
         }
      }
   }

   /**
    * Copies a directory to another location. Overwrites existing targets. Doesn't delete the contents of destDir, if any.
    * Implementation is native and memory efficient.
    */
   public static void copyDir( File sourceDir, File destDir ) throws IOException {
      if ( !sourceDir.isDirectory() ) {
         return;
      }
      if ( destDir.exists() && !destDir.isDirectory() ) {
         return;
      }
      if ( !destDir.exists() ) {
         destDir.mkdirs();
      }
      File[] files = sourceDir.listFiles();
      for ( int i = 0; files != null && i < files.length; i++ ) {
         if ( files[i].isDirectory() ) {
            copyDir(files[i], new File(destDir, files[i].getName()));
         } else {
            copy(files[i], new File(destDir, files[i].getName()));
         }
      }
   }

   public static void copyDirOldIO( File sourceDir, File destDir ) throws IOException {
      copyDirOldIO(sourceDir, destDir, null);
   }

   /**
    * Copies a directory to another location. Overwrites existing targets. Doesn't delete the contents of destDir, if any.
    * Implementation is native and memory efficient.
    */
   public static void copyDirOldIO( File sourceDir, File destDir, Pattern exclude ) throws IOException {
      if ( !sourceDir.isDirectory() ) {
         return;
      }
      if ( destDir.exists() && !destDir.isDirectory() ) {
         return;
      }
      if ( !destDir.exists() ) {
         destDir.mkdirs();
      }
      File[] files = sourceDir.listFiles();
      for ( int i = 0; files != null && i < files.length; i++ ) {
         if ( exclude != null && exclude.matcher(files[i].getName()).find() ) {
            continue;
         }
         if ( files[i].isDirectory() ) {
            copyDirOldIO(files[i], new File(destDir, files[i].getName()), exclude);
         } else {
            copyOldIO(files[i], new File(destDir, files[i].getName()));
         }
      }
   }

   public static void copyOldIO( File in, File out ) throws IOException {
      FileInputStream fis = new FileInputStream(in);
      FileOutputStream fos = new FileOutputStream(out);
      byte[] buf = new byte[4096];
      int i = 0;
      while ( (i = fis.read(buf)) != -1 ) {
         fos.write(buf, 0, i);
      }
      fis.close();
      fos.close();
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
    * Deletes the content of a directory recursively, but lets the directory itself untouched.
    * If <code>dir</code> does not exist, nothing happens.
    */
   public static void deleteDirContent( File dir ) {
      if ( !dir.exists() ) {
         return;
      }
      if ( dir.isDirectory() ) {
         File[] files = dir.listFiles();
         for ( int i = 0; files != null && i < files.length; i++ ) {
            deleteDir(files[i]);
         }
      }
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

   /**
    * Quietly get the canonical pathname of the file (i.e. throw a {@link RuntimeException} instead of the {@link IOException})
    */
   public static String getCanonicalPathQuietly( File file ) {
      try {
         return file.getCanonicalPath();
      }
      catch ( IOException e ) {
         throw new RuntimeException("failed to resolve canonical path of " + file, e);
      }
   }

   /**
    * <p>ensures the directory to be existing</p>
    * try's to create the directory (and parents) if they don't exist yet
    * @throws IOException if the creation failed
    */
   public static void makeDirsIfNotExist( File directory ) throws IOException {
      if ( directory == null ) throw new IllegalArgumentException("directory must not be null");

      if ( directory.exists() ) {
         if ( directory.isDirectory() ) {
            return;
         } else {
            throw new IOException(directory + " already exists but is no directory");
         }
      }
      directory.mkdirs(); // ignore it
      if ( !directory.exists() || !directory.isDirectory() ) throw new IOException("unable to create directory (or parents): " + directory);
   }

   public static String readAsString( File file ) throws IOException {
      return readAsString(new FileInputStream(file));
   }

   public static String readAsString( InputStream in ) throws IOException {
      return readAsString(in, Charset.defaultCharset().name());
   }

   public static String readAsString( InputStream in, String encoding ) throws IOException {
      BufferedReader reader = null;
      StringBuffer sb;
      try {
         reader = new BufferedReader(new InputStreamReader(in, encoding));
         sb = new StringBuffer();
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

   public static String readAsString( String filename ) throws IOException {
      return readAsString(new FileInputStream(filename));
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
