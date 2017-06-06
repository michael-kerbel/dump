package util.dump.stream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import util.dump.Dump;
import util.dump.DumpUtils;
import util.dump.ExternalizableBean;
import util.time.StopWatch;


public class CompressionBenchmark {

   private static final int BEAN_NUMBER       = 1000000;
   private static final int IO_THREADS_NUMBER = 0;


   public static void main( String[] args ) throws Exception {
      new CompressionBenchmark(Compression.LZ4).doIt();
   }

   private static void writeAndRead( byte[] b, File file ) {
      try {
         OutputStream out = new BufferedOutputStream(new FileOutputStream(file), 1024 * 1024);
         out.write(b);
         out.close();

         InputStream in = new BufferedInputStream(new FileInputStream(file));
         byte[] contents = new byte[1024 * 1024];

         while ( in.read(contents) != -1 ) {
            // nop               
         }
         in.close();
      }
      catch ( Exception argh ) {
         argh.printStackTrace();
      }
   }


   private Compression _compressor;


   public CompressionBenchmark( Compression compressor ) {
      _compressor = compressor;
   }

   private void doIt() throws IOException {
      for ( int i = 0; i < IO_THREADS_NUMBER; i++ ) {
         new IOGenerator().start();
      }

      for ( int i = 0; i < 2; i++ ) {
         File dumpFile = new File("compression-benchmark.dmp");
         Dump uncompressed = new Dump<TestBean>(TestBean.class, dumpFile);
         measure(uncompressed, "no compression");
         uncompressed.close();
         System.err.println("file size: " + dumpFile.length());
         dumpFile.delete();
         Dump compressed = new Dump<TestBean>(TestBean.class, dumpFile, _compressor);
         measure(compressed, "lz4");
         compressed.close();
         System.err.println("file size: " + dumpFile.length());
         DumpUtils.deleteDumpFiles(compressed);
      }
   }

   private void measure( Dump<TestBean> d, String comp ) throws IOException {
      TestBean b = new TestBean();
      StopWatch t = new StopWatch();
      for ( int i = 0; i < BEAN_NUMBER; i++ ) {
         d.add(b);
      }
      System.err.println(comp + " write: " + t);

      // reset OS file cache
      byte[] emptyBytes = new byte[10000000];
      for ( int i = 0; i < 50; i++ ) {
         writeAndRead(emptyBytes, new File("compression-benchmark-emptyBytes"));
      }

      t = new StopWatch();
      for ( TestBean bb : d ) {
         // nop
      }
      System.err.println(comp + " read: " + t);
   }


   private static class TestBean implements ExternalizableBean {

      @externalize(1)
      String _data = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.";


      public TestBean() {}
   }

   private static class IOGenerator extends Thread {

      public IOGenerator() {
         setDaemon(true);
         setPriority(MIN_PRIORITY);
      }

      @Override
      public void run() {
         byte[] b = new byte[1000000];
         File file = new File("compression-benchmark-" + getName());
         file.deleteOnExit();
         while ( true ) {
            writeAndRead(b, file);
         }
      }
   }
}
