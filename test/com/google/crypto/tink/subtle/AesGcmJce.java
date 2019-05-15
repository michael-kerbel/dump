//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.google.crypto.tink.subtle;

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.crypto.tink.Aead;


public final class AesGcmJce implements Aead {

   private static final int IV_SIZE_IN_BYTES  = 12;
   private static final int TAG_SIZE_IN_BYTES = 16;

   private static final ThreadLocal<Cipher> CIPHER = ThreadLocal.withInitial(() -> {
      try {
         return Cipher.getInstance("AES/GCM/NoPadding");
      }
      catch ( NoSuchAlgorithmException | NoSuchPaddingException e ) {
         throw new RuntimeException("Failed to instantiate Cipher", e);
      }
   });


   private static AlgorithmParameterSpec getParams( final byte[] iv ) throws GeneralSecurityException {
      return getParams(iv, 0, iv.length);
   }

   private static AlgorithmParameterSpec getParams( final byte[] buf, int offset, int len ) throws GeneralSecurityException {
      return new GCMParameterSpec(128, buf, offset, len);
   }

   private static Cipher instance() throws GeneralSecurityException {
      return CIPHER.get();
   }


   private final SecretKey keySpec;


   public AesGcmJce( final byte[] key ) throws GeneralSecurityException {
      Validators.validateAesKeySize(key.length);
      keySpec = new SecretKeySpec(key, "AES");
   }

   @Override
   public byte[] decrypt( final byte[] ciphertext, final byte[] associatedData ) throws GeneralSecurityException {
      if ( ciphertext.length < 28 ) {
         throw new GeneralSecurityException("ciphertext too short");
      } else {
         AlgorithmParameterSpec params = getParams(ciphertext, 0, 12);
         Cipher cipher = instance();
         cipher.init(2, keySpec, params);
         if ( associatedData != null && associatedData.length != 0 ) {
            cipher.updateAAD(associatedData);
         }

         return cipher.doFinal(ciphertext, 12, ciphertext.length - 12);
      }
   }

   @Override
   public byte[] encrypt( final byte[] plaintext, final byte[] associatedData ) throws GeneralSecurityException {
      if ( plaintext.length > 2147483619 ) {
         throw new GeneralSecurityException("plaintext too long");
      } else {
         byte[] ciphertext = new byte[12 + plaintext.length + 16];
         byte[] iv = Random.randBytes(12);
         System.arraycopy(iv, 0, ciphertext, 0, 12);
         Cipher cipher = instance();
         AlgorithmParameterSpec params = getParams(iv);
         cipher.init(1, keySpec, params);
         if ( associatedData != null && associatedData.length != 0 ) {
            cipher.updateAAD(associatedData);
         }

         int written = cipher.doFinal(plaintext, 0, plaintext.length, ciphertext, 12);
         if ( written != plaintext.length + 16 ) {
            int actualTagSize = written - plaintext.length;
            throw new GeneralSecurityException(String.format("encryption failed; GCM tag must be %s bytes, but got only %s bytes", 16, actualTagSize));
         } else {
            return ciphertext;
         }
      }
   }
}
