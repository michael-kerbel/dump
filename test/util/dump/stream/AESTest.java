package util.dump.stream;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.AeadFactory;
import com.google.crypto.tink.subtle.Base64;

import util.dump.Dump;
import util.dump.ExternalizableBean;
import util.io.IOUtils;


public class AESTest {

   private static final String KEY = "{\n" //
      + "    \"primaryKeyId\": 42,\n" //
      + "    \"key\": [{\n" //
      + "        \"keyData\": {\n" //
      + "            \"typeUrl\": \"type.googleapis.com/google.crypto.tink.AesGcmKey\",\n" //
      + "            \"keyMaterialType\": \"SYMMETRIC\",\n" //
      + "            \"value\": \"GiABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fIA==\"\n" //
      + "        },\n" //
      + "        \"keyId\": 42,\n" //
      + "        \"outputPrefixType\": \"TINK\",\n" //
      + "        \"status\": \"ENABLED\"\n" //
      + "    }]\n" //
      + "}";


   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = new File(".").listFiles(f -> f.getName().startsWith("AESTest."));
      for ( File df : dumpFile ) {
         if ( !df.delete() ) {
            System.out.println("Failed to delete old dump file " + df);
         }
      }
   }

   @Test
   public void test() throws GeneralSecurityException, IOException {
      System.err.println(Base64.encodeToString(
         new byte[] { 26, 32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 }, 2));

      AeadConfig.register();
      //      KeysetHandle keysetHandle = KeysetHandle.generateNew(AeadKeyTemplates.AES256_GCM);
      //      ByteArrayOutputStream out = new ByteArrayOutputStream();
      //      CleartextKeysetHandle.write(keysetHandle, JsonKeysetWriter.withOutputStream(out));
      //      String json = new String(out.toByteArray());
      //      System.err.println(json);
      KeysetHandle keysetHandle = CleartextKeysetHandle.read(JsonKeysetReader.withString(KEY));

      Aead aead = AeadFactory.getPrimitive(keysetHandle);

      byte[] bytes = "Hello World".getBytes();
      byte[] ciphertext = aead.encrypt(bytes, null);
      byte[] decrypted = aead.decrypt(ciphertext, null);
      assertThat(decrypted).isEqualTo(bytes);
   }

   @Test
   public void testDump() throws IOException {
      String text = "encrypted text";
      File dumpFile = new File("AESTest.dmp");
      byte[] key = AESCrypter.createRandomKey();
      System.err.println("Using following key for encryption " + java.util.Base64.getEncoder().encodeToString(key));
      try (Dump<Bean> dump = new Dump<>(Bean.class, dumpFile, AESCrypter.AES256, key)) {
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
