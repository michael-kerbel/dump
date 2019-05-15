package util.dump.stream;

import java.io.Externalizable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.AeadFactory;
import com.google.crypto.tink.subtle.Base64;

import util.string.StringTool;


public enum AESCrypter implements ByteArrayPacker {

   AES256;

   private static final String KEY_TEMPLATE = "{\n" //
      + "    \"primaryKeyId\": 42,\n" //
      + "    \"key\": [{\n" //
      + "        \"keyData\": {\n" //
      + "            \"typeUrl\": \"type.googleapis.com/google.crypto.tink.AesGcmKey\",\n" //
      + "            \"keyMaterialType\": \"SYMMETRIC\",\n" //
      + "            \"value\": \"%\"\n" //
      + "        },\n" //
      + "        \"keyId\": 42,\n" //
      + "        \"outputPrefixType\": \"TINK\",\n" //
      + "        \"status\": \"ENABLED\"\n" //
      + "    }]\n" //
      + "}";

   private static ConcurrentHashMap<byte[], Aead> _keyCache = new ConcurrentHashMap<>();


   static {
      try {
         AeadConfig.register();
      }
      catch ( GeneralSecurityException e ) {
         e.printStackTrace();
      }
   }

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
   public byte[] pack( byte[] bytes, int bytesLength, @Nullable byte[] target, @Nullable byte[] dict ) throws IOException {
      try {
         Aead aead = getAead(dict);
         return aead.encrypt(bytes, null);
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
         Aead aead = getAead(dict);
         byte[] data = new byte[sourceLength];
         System.arraycopy(source, 0, data, 0, sourceLength);
         return aead.decrypt(data, null);
      }
      catch ( GeneralSecurityException e ) {
         throw new RuntimeException("Failed to create encrypt data", e);
      }
   }

   private Aead getAead( @Nullable byte[] dict ) {
      Aead aead = _keyCache.get(dict);
      if ( aead == null ) {
         try {
            byte[] protobuffedDict = new byte[dict.length + 2];
            protobuffedDict[0] = 26;
            protobuffedDict[1] = (byte)dict.length;
            System.arraycopy(dict, 0, protobuffedDict, 2, dict.length);
            String k = Base64.encodeToString(protobuffedDict, Base64.NO_WRAP);
            KeysetHandle keysetHandle = CleartextKeysetHandle.read(JsonKeysetReader.withString(StringTool.replace(KEY_TEMPLATE, "%", k)));
            aead = AeadFactory.getPrimitive(keysetHandle);
            _keyCache.put(dict, aead);
         }
         catch ( Exception e ) {
            throw new RuntimeException("Failed to create encryption instance", e);
         }
      }
      return aead;
   }
}
