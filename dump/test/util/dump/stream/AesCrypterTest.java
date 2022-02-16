package util.dump.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import util.dump.Dump;
import util.dump.ExternalizableBean;
import util.dump.io.IOUtils;


public class AesCrypterTest {

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = new File(".").listFiles(f -> f.getName().startsWith("AesCrypterTest."));
      for ( File df : dumpFile ) {
         if ( !df.delete() ) {
            System.out.println("Failed to delete old dump file " + df);
         }
      }
   }

   @Test
   public void test() throws GeneralSecurityException, IOException {

      byte[] key = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 };

      AesCrypter cut = new AesCrypter(key);

      byte[] bytes = "Hello World".getBytes();
      byte[] encrypted = cut.pack(bytes, bytes.length, null, null);
      byte[] decrypted = cut.unpack(encrypted, encrypted.length, null, null);
      assertThat(decrypted).isEqualTo(bytes);
   }

   @Test
   public void testDump() throws IOException {
      String text = "encrypted text";
      File dumpFile = new File("AesCrypterTest.dmp");
      byte[] key = AesCrypter.createRandomKey();
      System.err.println("Using following key for encryption " + java.util.Base64.getEncoder().encodeToString(key));
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile, new AesCrypter(key))) {
         dump.add(new Bean(text));

         int n = 0;
         for ( Bean b : dump ) {
            assertThat(b._text).isEqualTo(text);
            n++;
         }
         assertThat(n).isEqualTo(1);
      }

      String s = IOUtils.readAsString(dumpFile);
      assertThat(s.contains(text)).as("data is not encrypted").isFalse();
   }

   public static class Bean implements ExternalizableBean {

      @externalize(1)
      String _text;

      public Bean() {}

      public Bean( String text ) {
         _text = text;
      }
   }

}
