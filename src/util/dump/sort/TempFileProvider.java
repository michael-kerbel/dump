package util.dump.sort;

import java.io.File;
import java.io.IOException;


/**
 * <p>This class encapsulates the temporary File facilities provided by the standard JAVA VM. It allows
 * you specifying prefixes, suffixes and standard temporary directory in a reusable object. Useful
 * when working intensively with temporary files.</p>
 *
 * <p>This facility class provides the possibility of specifying a two level prefix for the generated
 * temporary files. This may prove useful when the "main prefix" just represents a special task and the sub prefix
 * just helps you to identify the file type. There is a special constructor allowing you to generate a new
 * <code>TempFileProvider</code> using a running instance as model, this constructor allows you to overwrite the
 * "sub prefix"</p>
 *
 * <p>This class provides also all thinkable possible constructor combinations, so you will always find
 * the fitting constructor for every situation. Of course you can reconfigure the instance after creating
 * it by using the provided setters.</p>
 *
 */
@SuppressWarnings("unused")
public class TempFileProvider {

   /**
    * Default prefix for temporary files
    */
   public final static String DEFAULT_FILE_PREFIX = "~temp.";

   /**
    * Default sub prefix for temporary files
    */
   public final static String DEFAULT_FILE_SUB_PREFIX = "";

   /**
    * Default sufix for temporary files
    */
   public final static String DEFAULT_FILE_SUFFIX = ".dmp";

   /**
    * Default folder for temporary files (null = OS Default)
    */
   public final static File DEFAULT_USER_TEMP_FOLDER = null;

   /**
    * Default value for indicating the JAVA VM to delete files on exit
    */
   public final static boolean DEFAULT_USE_DELETE_FILE_ON_EXIT = true;

   public static final TempFileProvider DEFAULT_PROVIDER = new TempFileProvider();

   private String  _filePrefix          = DEFAULT_FILE_PREFIX;
   private String  _fileSubPrefix       = DEFAULT_FILE_SUB_PREFIX;
   private String  _fileSuffix          = DEFAULT_FILE_SUFFIX;
   private File    _userTempFolder      = DEFAULT_USER_TEMP_FOLDER;
   private boolean _useDeleteFileOnExit = DEFAULT_USE_DELETE_FILE_ON_EXIT;


   public TempFileProvider() {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFFIX, DEFAULT_USER_TEMP_FOLDER, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( boolean useDeleteFileOnExit ) {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFFIX, DEFAULT_USER_TEMP_FOLDER, useDeleteFileOnExit);
   }

   public TempFileProvider( File userTempFolder ) {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFFIX, userTempFolder, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( File userTempFolder, boolean useDeleteFileOnExit ) {
      init(DEFAULT_FILE_PREFIX, DEFAULT_FILE_SUB_PREFIX, DEFAULT_FILE_SUFFIX, userTempFolder, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String _fileSuffix ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, _fileSuffix, DEFAULT_USER_TEMP_FOLDER, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String _fileSuffix, boolean useDeleteFileOnExit ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, _fileSuffix, DEFAULT_USER_TEMP_FOLDER, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String _fileSuffix, File userTempFolder ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, _fileSuffix, userTempFolder, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String _fileSuffix, File userTempFolder, boolean useDeleteFileOnExit ) {
      init(filePrefix, DEFAULT_FILE_SUB_PREFIX, _fileSuffix, userTempFolder, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String _fileSuffix ) {
      init(filePrefix, fileSubPrefix, _fileSuffix, DEFAULT_USER_TEMP_FOLDER, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String _fileSuffix, boolean useDeleteFileOnExit ) {
      init(filePrefix, fileSubPrefix, _fileSuffix, DEFAULT_USER_TEMP_FOLDER, useDeleteFileOnExit);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String _fileSuffix, File userTempFolder ) {
      init(filePrefix, fileSubPrefix, _fileSuffix, userTempFolder, DEFAULT_USE_DELETE_FILE_ON_EXIT);
   }

   public TempFileProvider( String filePrefix, String fileSubPrefix, String _fileSuffix, File userTempFolder, boolean useDeleteFileOnExit ) {
      init(filePrefix, fileSubPrefix, _fileSuffix, userTempFolder, useDeleteFileOnExit);
   }

   public TempFileProvider( TempFileProvider model ) {
      init(model.getFilePrefix(), model.getFileSubPrefix(), model.getFileSuffix(), model.getUserTempFolder(), model.isUseDeleteFileOnExit());
   }

   public TempFileProvider( TempFileProvider model, String fileSubPrefix ) {
      init(model.getFilePrefix(), fileSubPrefix, model.getFileSuffix(), model.getUserTempFolder(), model.isUseDeleteFileOnExit());
   }

   /**
    * @return Returns the filePrefix.
    */
   public String getFilePrefix() {
      return _filePrefix;
   }

   /**
    * @return Returns the fileSubPrefix.
    */
   public String getFileSubPrefix() {
      return _fileSubPrefix;
   }

   /**
    * @return Returns the fileSufix.
    */
   public String getFileSuffix() {
      return _fileSuffix;
   }

   /**
    * Returns the next temporary file. For more info see <code>File.createTempFile(String, String, File)</code>
    *
    * @return the new temporary file reference
    */
   public File getNextTemporaryFile() throws IOException {
      return createTempFile(this._fileSubPrefix);
   }

   /**
    * Returns the next temporary file using the specified fileSubPrefix. Please notice that
    * the provided sub prefix overrides the configured sub prefix only for this method call,
    * if you want to change the sub prefix permantly then use the provided setter.
    *
    * For more info see <code>File.createTempFile(String, String, File)</code>
    *
    * @param subPrefixToUse temporary sub prefix to use
    */
   public File getNextTemporaryFile( String subPrefixToUse ) throws IOException {
      return createTempFile(subPrefixToUse);
   }

   /**
    * @return Returns the userTempFolder.
    */
   public File getUserTempFolder() {
      return _userTempFolder;
   }

   /**
    * @return Returns the useDeleteFileOnExit.
    */
   public boolean isUseDeleteFileOnExit() {
      return _useDeleteFileOnExit;
   }

   /**
    * @param filePrefix The filePrefix to set.
    */
   public void setFilePrefix( String filePrefix ) {
      this._filePrefix = filePrefix;
   }

   /**
    * @param fileSubPrefix The fileSubPrefix to set.
    */
   public void setFileSubPrefix( String fileSubPrefix ) {
      this._fileSubPrefix = fileSubPrefix;
   }

   /**
    * @param fileSuffix The fileSufix to set.
    */
   public void setFileSuffix( String fileSuffix ) {
      this._fileSuffix = fileSuffix;
   }

   /**
    * @param useDeleteFileOnExit The useDeleteFileOnExit to set.
    */
   public void setUseDeleteFileOnExit( boolean useDeleteFileOnExit ) {
      this._useDeleteFileOnExit = useDeleteFileOnExit;
   }

   /**
    * @param userTempFolder The userTempFolder to set.
    */
   public void setUserTempFolder( File userTempFolder ) {
      this._userTempFolder = userTempFolder;
   }

   // creates a new temp file
   private File createTempFile( String subPrefixToUse ) throws IOException {

      // the file reference to be returned
      File nextTempFile = File.createTempFile(_filePrefix + subPrefixToUse, _fileSuffix, _userTempFolder);

      // delete on exit?
      if ( _useDeleteFileOnExit ) {
         nextTempFile.deleteOnExit();
      }

      return nextTempFile;

   }

   // inits the class
   private void init( String filePrefix, String fileSubPrefix, String fileSufix, File userTemporaryFolder, boolean useDeleteFileOnExit ) {
      this._filePrefix = filePrefix;
      this._fileSubPrefix = fileSubPrefix;
      this._fileSuffix = fileSufix;
      this._userTempFolder = userTemporaryFolder;
      this._useDeleteFileOnExit = useDeleteFileOnExit;
   }
}
