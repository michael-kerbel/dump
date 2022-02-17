package util.dump.stream;

import java.io.File;
import java.io.IOException;

import util.dump.Dump;
import util.dump.DumpUtils;
import util.dump.ExternalizableBean;
import util.dump.time.StopWatch;


public class AesBenchmark {

   private static final int BEAN_NUMBER = 1000000;


   public static void main( String[] args ) throws IOException {
      new AesBenchmark().doIt();
   }

   private void doIt() throws IOException {
      File dumpFile = new File("aes-benchmark.dmp");
      dumpFile.delete();
      Dump unencrypted = new Dump<>(TestBean.class, dumpFile);
      measure(unencrypted, "no encryption");
      unencrypted.close();
      System.err.println("file size: " + dumpFile.length());
      dumpFile.delete();

      Dump encrypted = new Dump<>(TestBean.class, dumpFile, new AesCrypter(AesCrypter.createRandomKey()));
      measure(encrypted, "AES256");
      encrypted.close();
      System.err.println("file size: " + dumpFile.length());
      DumpUtils.deleteDumpFiles(encrypted);
   }

   private void measure( Dump<TestBean> d, String comp ) throws IOException {
      TestBean b = new TestBean();
      StopWatch t = new StopWatch().setResetOnToString(true);
      for ( int i = 0; i < BEAN_NUMBER; i++ ) {
         d.add(b);
      }
      System.err.println(comp + " write: " + t);

      for ( TestBean bb : d ) {
         if ( !bb._data.equals(TestBean.LOREM_IPSUM) ) {
            System.err.println("decrypted data doesn't match original data!" + bb._data);
         }
      }
      System.err.println(comp + " read: " + t);
   }


   private static class TestBean implements ExternalizableBean {

      public static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.";

      @externalize(1)
      String _data = LOREM_IPSUM;


      public TestBean() {}
   }
}
