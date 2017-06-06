package util.dump.sort;

import java.io.File;
import java.io.IOException;


/**
 * <p>This class encapsulates the temporal File facilities provided by the standart JAVA VM. It allows
 * you specifying prefixes, suffixes and standard temporal directory in a reusable object. Useful
 * when working intensivelly with temporal files.</p>
 *
 * <p>This facility class provides the possibility of specifing a two level prefix for the generated
 * temporal files. This may prove useful when the "main prefix" just represents a special task and the sub prefix
 * just helps you to identify the file type. There is a special constructor allowing you to generate a new
 * <code>TempFileProvider</code> using a running instance as model, this constructor allows you to overwrite the
 * "sub prefix"</p>
 *
 * <p>This class provides also all thinkable possible contructor combinations, so you will always find
 * the fitting constructor for every situation. Of course you can reconfigure the instance after creating
 * it by using the provided setters.</p>
 *
 * @author Martin
 *
 */
public class TempFileProvider {

   /**
    * Default prefix for temporal files
    */
   public final static String           DEFAULT_FILE_PREFIX             = "~temp.";

   /**
    * Default sub prefix for temporal files
    */
   public final static String           DEFAULT_FILE_SUB_PREFIX         = "";

   /**
    * Default sufix for temporal files
    */
   public final static String           DEFAULT_FILE_SUFIX              = ".dmp";

   /**
    * Default folder for temporal files (null = OS Default)
    */
   public final static File             DEFAULT_USER_TEMPORAL_FOLDER    = null;

   /**
    * Default value for indicating the JAVA VM to delete files on exit
    */
   public final static boolean          DEFAULT_USE_DELETE_FILE_ON_EXIT = true;

   public static final TempFileProvider DEFAULT_PROVIDER                = new TempFileProvider();

   private String                       filePrefix                      = DEFAULT_FILE_PREFIX;
   private String                       fileSubPrefix                   = DEFAULT_FILE_SUB_PREFIX;
   private String                       fileSufix                       = DEFAULT_FILE_SUFIX;
   private File                         userTemporalFolder              = DEFAULT_USER_TEMPORAL_FOLDER;
   private boolean                      useDeleteFileOnExit             = DEFAULT_USE_DELETE_FILE_ON_EXIT;

   public TempFileProvider() {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFIX, DEFAULT_USER_TEMPORAL_FOLDER, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( boolean useDeleteFileOnExit ) {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFIX, DEFAULT_USER_TEMPORAL_FOLDER, useDeleteFileOnExit);
   }

   public TempFileProvider( File userTemporalFolder ) {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFIX, userTemporalFolder, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( File userTemporalFolder, boolean useDeleteFileOnExit ) {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFIX, userTemporalFolder, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String fileSufix ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, fileSufix, DEFAULT_USER_TEMPORAL_FOLDER, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String fileSufix, boolean useDeleteFileOnExit ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, fileSufix, DEFAULT_USER_TEMPORAL_FOLDER, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String fileSufix, File userTemporalFolder ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, fileSufix, userTemporalFolder, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String fileSufix, File userTemporalFolder, boolean useDeleteFileOnExit ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, fileSufix, userTemporalFolder, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String fileSufix ) {
      init(filePrefix, fileSubPrefix, fileSufix, DEFAULT_USER_TEMPORAL_FOLDER, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String fileSufix, boolean useDeleteFileOnExit ) {
      init(filePrefix, fileSubPrefix, fileSufix, DEFAULT_USER_TEMPORAL_FOLDER, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String fileSufix, File userTemporalFolder ) {
      init(filePrefix, fileSubPrefix, fileSufix, userTemporalFolder, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String fileSufix, File userTemporalFolder, boolean useDeleteFileOnExit ) {
      init(filePrefix, fileSubPrefix, fileSufix, userTemporalFolder, useDeleteFileOnExit);
   }

   public TempFileProvider( TempFileProvider model ) {
      init(model.getFilePrefix(), model.getFileSubPrefix(), model.getFileSufix(), model.getUserTemporalFolder(), model.isUseDeleteFileOnExit());
   }

   public TempFileProvider( TempFileProvider model, String fileSubPrefix ) {
      init(model.getFilePrefix(), fileSubPrefix, model.getFileSufix(), model.getUserTemporalFolder(), model.isUseDeleteFileOnExit());
   }

   /**
    * @return Returns the filePrefix.
    */
   public String getFilePrefix() {
      return filePrefix;
   }

   /**
    * @return Returns the fileSubPrefix.
    */
   public String getFileSubPrefix() {
      return fileSubPrefix;
   }

   /**
    * @return Returns the fileSufix.
    */
   public String getFileSufix() {
      return fileSufix;
   }

   /**
    * Returns the next temporal file. For more info see <code>File.createTempFile(String, String, File)</code>
    *
    * @return the new temporal file reference
    * @throws IOException
    */
   public File getNextTemporalFile() throws IOException {
      return createTempFile(this.fileSubPrefix);
   }

   /**
    * Returns the next temporal file using the specified fileSubPrefix. Please notice that
    * the provided sub prefix overrides the configured sub prefix only for this method call,
    * if you want to change the sub prefix permantly then use the provided setter.
    *
    * For more info see <code>File.createTempFile(String, String, File)</code>
    *
    * @param subPrefixToUse temporal sub prefix to use
    * @throws IOException
    */
   public File getNextTemporalFile( String subPrefixToUse ) throws IOException {
      return createTempFile(subPrefixToUse);
   }

   /**
    * @return Returns the userTemporalFolder.
    */
   public File getUserTemporalFolder() {
      return userTemporalFolder;
   }

   /**
    * @return Returns the useDeleteFileOnExit.
    */
   public boolean isUseDeleteFileOnExit() {
      return useDeleteFileOnExit;
   }

   /**
    * @param filePrefix The filePrefix to set.
    */
   public void setFilePrefix( String filePrefix ) {
      this.filePrefix = filePrefix;
   }

   /**
    * @param fileSubPrefix The fileSubPrefix to set.
    */
   public void setFileSubPrefix( String fileSubPrefix ) {
      this.fileSubPrefix = fileSubPrefix;
   }

   /**
    * @param fileSufix The fileSufix to set.
    */
   public void setFileSufix( String fileSufix ) {
      this.fileSufix = fileSufix;
   }

   /**
    * @param useDeleteFileOnExit The useDeleteFileOnExit to set.
    */
   public void setUseDeleteFileOnExit( boolean useDeleteFileOnExit ) {
      this.useDeleteFileOnExit = useDeleteFileOnExit;
   }

   /**
    * @param userTemporalFolder The userTemporalFolder to set.
    */
   public void setUserTemporalFolder( File userTemporalFolder ) {
      this.userTemporalFolder = userTemporalFolder;
   }

   // creates a new temp file
   private File createTempFile( String subPrefixToUse ) throws IOException {

      // the file reference to be returned
      File nextTempFile = File.createTempFile(filePrefix + subPrefixToUse, fileSufix, userTemporalFolder);

      // delete on exit?
      if ( useDeleteFileOnExit ) {
         nextTempFile.deleteOnExit();
      }

      return nextTempFile;

   }

   // inits the class
   private void init( String filePrefix, String fileSubPrefix, String fileSufix, File userTemporalFolder, boolean useDeleteFileOnExit ) {
      this.filePrefix = filePrefix;
      this.fileSubPrefix = fileSubPrefix;
      this.fileSufix = fileSufix;
      this.userTemporalFolder = userTemporalFolder;
      this.useDeleteFileOnExit = useDeleteFileOnExit;
   }
}
