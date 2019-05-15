//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.google.crypto.tink.subtle;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.Signature;

import javax.crypto.Cipher;
import javax.crypto.KeyAgreement;
import javax.crypto.Mac;


public interface EngineWrapper2<T> {

   T getInstance( String algorithm, Provider provider ) throws GeneralSecurityException;


   public static class TCipher implements EngineWrapper<Cipher> {

      ThreadLocal<Cipher>   _cache          = new ThreadLocal<>();
      ThreadLocal<String>   _cacheAlgorithm = new ThreadLocal<>();
      ThreadLocal<Provider> _cacheProvider  = new ThreadLocal<>();


      public TCipher() {}

      @Override
      public Cipher getInstance( String algorithm, Provider provider ) throws GeneralSecurityException {
         if ( algorithm != null && algorithm.equals(_cacheAlgorithm.get()) && (provider == _cacheProvider.get()) ) {
            return _cache.get();
         }
         _cache.set(provider == null ? Cipher.getInstance(algorithm) : Cipher.getInstance(algorithm, provider));
         _cacheAlgorithm.set(algorithm);
         _cacheProvider.set(provider);
         return _cache.get();
      }
   }

   public static class TKeyAgreement implements EngineWrapper<KeyAgreement> {

      public TKeyAgreement() {}

      @Override
      public KeyAgreement getInstance( String algorithm, Provider provider ) throws GeneralSecurityException {
         return provider == null ? KeyAgreement.getInstance(algorithm) : KeyAgreement.getInstance(algorithm, provider);
      }
   }

   public static class TKeyFactory implements EngineWrapper<KeyFactory> {

      public TKeyFactory() {}

      @Override
      public KeyFactory getInstance( String algorithm, Provider provider ) throws GeneralSecurityException {
         return provider == null ? KeyFactory.getInstance(algorithm) : KeyFactory.getInstance(algorithm, provider);
      }
   }

   public static class TKeyPairGenerator implements EngineWrapper<KeyPairGenerator> {

      public TKeyPairGenerator() {}

      @Override
      public KeyPairGenerator getInstance( String algorithm, Provider provider ) throws GeneralSecurityException {
         return provider == null ? KeyPairGenerator.getInstance(algorithm) : KeyPairGenerator.getInstance(algorithm, provider);
      }
   }

   public static class TMac implements EngineWrapper<Mac> {

      public TMac() {}

      @Override
      public Mac getInstance( String algorithm, Provider provider ) throws GeneralSecurityException {
         return provider == null ? Mac.getInstance(algorithm) : Mac.getInstance(algorithm, provider);
      }
   }

   public static class TMessageDigest implements EngineWrapper<MessageDigest> {

      public TMessageDigest() {}

      @Override
      public MessageDigest getInstance( String algorithm, Provider provider ) throws GeneralSecurityException {
         return provider == null ? MessageDigest.getInstance(algorithm) : MessageDigest.getInstance(algorithm, provider);
      }
   }

   public static class TSignature implements EngineWrapper<Signature> {

      public TSignature() {}

      @Override
      public Signature getInstance( String algorithm, Provider provider ) throws GeneralSecurityException {
         return provider == null ? Signature.getInstance(algorithm) : Signature.getInstance(algorithm, provider);
      }
   }
}
