package util.dump.stream;

import java.io.Externalizable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;

import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;


public class AesCrypter implements ByteArrayPacker {

   private static final ThreadLocal<Cipher>       CIPHER = ThreadLocal.withInitial(() -> {
      try {
         return Cipher.getInstance("AES/GCM/NoPadding");
      }
      catch ( NoSuchAlgorithmException | NoSuchPaddingException e ) {
         throw new RuntimeException("Failed to instantiate Cipher", e);
      }
   });
   private static final ThreadLocal<SecureRandom> RANDOM = new ThreadLocal<SecureRandom>() {

      @Override
      protected SecureRandom initialValue() {
         return newDefaultSecureRandom();
      }
   };


   public static byte[] createRandomKey() {
      try {
         byte[] bytes = new byte[32];
         SecureRandom.getInstanceStrong().nextBytes(bytes);
         return bytes;
      }
      catch ( NoSuchAlgorithmException e ) {
         throw new RuntimeException("Failed to obtain SecureRandom instance", e);
      }
   }

   private static AlgorithmParameterSpec getParams( final byte[] iv ) {
      return getParams(iv, 0, iv.length);
   }

   private static AlgorithmParameterSpec getParams( final byte[] buf, int offset, int len ) {
      return new GCMParameterSpec(128, buf, offset, len);
   }

   private static Cipher instance() {
      return CIPHER.get();
   }

   private static SecureRandom newDefaultSecureRandom() {
      SecureRandom retval = new SecureRandom();
      retval.nextLong();
      return retval;
   }

   private static byte[] randBytes( int size ) {
      byte[] rand = new byte[size];
      ((SecureRandom)RANDOM.get()).nextBytes(rand);
      return rand;
   }

   private static void validateAesKeySize( int sizeInBytes ) {
      if ( sizeInBytes != 16 && sizeInBytes != 32 ) {
         throw new IllegalArgumentException("invalid key size; only 128-bit and 256-bit AES keys are supported");
      }
   }


   private final SecretKey keySpec;


   public AesCrypter( final byte[] key ) {
      validateAesKeySize(key.length);
      keySpec = new SecretKeySpec(key, "AES");
   }

   public byte[] decrypt( final byte[] ciphertext ) throws GeneralSecurityException {
      if ( ciphertext.length < 28 ) {
         throw new RuntimeException("ciphertext too short");
      } else {
         AlgorithmParameterSpec params = getParams(ciphertext, 0, 12);
         Cipher cipher = instance();
         cipher.init(Cipher.DECRYPT_MODE, keySpec, params);

         return cipher.doFinal(ciphertext, 12, ciphertext.length - 12);
      }
   }

   @Override
   public <E extends Externalizable> byte[] initDictionary( Iterable<E> dictInputProvider, ObjectStreamProvider objectStreamProvider ) {
      return null;
   }

   @Override
   public boolean isAlwaysPack() {
      return true;
   }

   @Override
   public boolean isPackedSizeInFirstFourBytes() {
      return false;
   }

   @Override
   public byte[] pack( byte[] source, int sourceLength, @Nullable byte[] target, @Nullable byte[] dict ) throws IOException {
      try {
         if ( sourceLength > 2147483619 ) {
            throw new RuntimeException("source too long");
         } else {
            byte[] encrypted = new byte[12 + sourceLength + 16];
            byte[] iv = randBytes(12);
            System.arraycopy(iv, 0, encrypted, 0, 12);
            Cipher cipher = instance();
            AlgorithmParameterSpec params = getParams(iv);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, params);

            int written = cipher.doFinal(source, 0, sourceLength, encrypted, 12);
            if ( written != sourceLength + 16 ) {
               int actualTagSize = written - sourceLength;
               throw new RuntimeException(String.format("encryption failed; GCM tag must be %s bytes, but got only %s bytes", 16, actualTagSize));
            } else {
               return encrypted;
            }
         }
      }
      catch ( InvalidKeyException e ) {
         throw new RuntimeException(
            "Failed to create encryption key, most probably you are using a JVM that doesn't allow AES256, please upgrade to at least Java 8 u162 or install JCE",
            e);
      }
      catch ( GeneralSecurityException e ) {
         throw new RuntimeException("Failed to create encrypt data", e);
      }
   }

   @Override
   public byte[] unpack( byte[] source, int sourceLength, @Nullable byte[] target, @Nullable byte[] dict ) throws IOException {
      try {
         if ( source.length < 28 ) {
            throw new RuntimeException("source too short");
         } else {
            AlgorithmParameterSpec params = getParams(source, 0, 12);
            Cipher cipher = instance();
            cipher.init(Cipher.DECRYPT_MODE, keySpec, params);

            return cipher.doFinal(source, 12, sourceLength - 12);
         }
      }
      catch ( GeneralSecurityException e ) {
         throw new RuntimeException("Failed to create decrypt data", e);
      }
   }

}
