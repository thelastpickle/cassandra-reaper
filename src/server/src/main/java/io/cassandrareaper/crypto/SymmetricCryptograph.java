/*
 * Copyright 2020-2020 The Last Pickle Ltd
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.crypto;

import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

public final class SymmetricCryptograph implements Cryptograph {

  private static final String PREFIX = "{cipher}";
  private static final String REGEX_PREFIX = "\\{cipher\\}";
  private static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
  private static final String DEFAULT_SALT = "deadbeef";
  private static final String DEFAULT_CIPHER = "AES/CBC/PKCS5Padding";
  private static final String DEFAULT_CIPHER_TYPE = "AES";
  private static final String DEFAULT_ALGORITHM = "PBKDF2WithHmacSHA512";
  private static final String DEFAULT_PROPERTY_KEY_SECRET = "CASSANDRA_REAPER_PROPERTY_KEY_SECRET";
  private static final int DEFAULT_ITERATION_COUNT = 1024;
  private static final int DEFAULT_KEY_STRENGTH = 256;

  private final String algorithm;
  private final String cipher;
  private final String cipherType;
  private final Integer iterationCount;
  private final Integer keyStrength;
  private final String salt;
  private final String systemPropertySecret;

  private SymmetricCryptograph(SymmetricCryptograph.Builder builder) {
    this.salt = builder.salt == null ? DEFAULT_SALT : builder.salt;
    this.cipher = builder.cipher == null ? DEFAULT_CIPHER : builder.cipher;
    this.cipherType = builder.cipherType == null ? DEFAULT_CIPHER_TYPE : builder.cipherType;
    this.algorithm = builder.algorithm == null ? DEFAULT_ALGORITHM : builder.algorithm;
    this.iterationCount = builder.iterationCount == null ? DEFAULT_ITERATION_COUNT : builder.iterationCount;
    this.keyStrength = builder.keyStrength == null ? DEFAULT_KEY_STRENGTH : builder.keyStrength;
    this.systemPropertySecret = builder.systemPropertySecret == null
            ? DEFAULT_PROPERTY_KEY_SECRET : builder.systemPropertySecret;
  }

  @Override
  public String encrypt(String plainText) {
    String trimmedText = StringUtils.trimToNull(plainText);
    Preconditions.checkNotNull(trimmedText);

    try {
      String passphrase = fetchPassphrase();
      return PREFIX + encryptText(passphrase, trimmedText);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeySpecException
            | InvalidAlgorithmParameterException | InvalidKeyException | BadPaddingException
            | IllegalBlockSizeException e) {
      throw new IllegalStateException("Unable to encrypt text", e);
    }
  }

  @Override
  public String decrypt(String encryptedText) {
    String trimmedText = StringUtils.trimToNull(encryptedText);
    Preconditions.checkNotNull(trimmedText);

    if (!trimmedText.startsWith(PREFIX)) {
      return encryptedText;
    }

    try {
      String passphrase = fetchPassphrase();
      return decryptText(passphrase, trimmedText.replaceFirst(REGEX_PREFIX, ""));
    } catch (InvalidKeySpecException | NoSuchAlgorithmException | NoSuchPaddingException
            | InvalidKeyException | BadPaddingException | IllegalBlockSizeException
            | InvalidAlgorithmParameterException e) {
      throw new IllegalStateException("Unable to decrypt text", e);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private String fetchPassphrase() {
    String passphrase = System.getenv(systemPropertySecret);
    if (passphrase == null) {
      passphrase = System.getProperty(systemPropertySecret);
    }

    if (passphrase == null) {
      throw new IllegalStateException("No passphrase detected in environment for: " + systemPropertySecret);
    }

    return passphrase;
  }

  private String encryptText(String key, String plainText) throws NoSuchAlgorithmException, NoSuchPaddingException,
          InvalidKeySpecException, InvalidAlgorithmParameterException, InvalidKeyException,
          BadPaddingException, IllegalBlockSizeException {
    Cipher encipher = Cipher.getInstance(cipher);

    byte[] initVector = createRandomInitialVector(encipher);
    IvParameterSpec ivspec = new IvParameterSpec(initVector);
    SecretKey secretKey = createSecretKey(key);

    encipher.init(Cipher.ENCRYPT_MODE, secretKey, ivspec);
    byte[] encryptedBytes = encipher.doFinal(plainText.getBytes());

    return DatatypeConverter.printHexBinary(initVector) + DatatypeConverter.printHexBinary(encryptedBytes);
  }

  private String decryptText(String key, String encryptedText) throws InvalidKeySpecException, NoSuchAlgorithmException,
          NoSuchPaddingException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException,
          InvalidAlgorithmParameterException {
    byte[] encryptedData = decode(encryptedText);
    if (encryptedData.length <= 16) {
      throw new IllegalArgumentException("Invalid format for supplied encrypted value");
    }

    byte[] initVector = subArray(encryptedData, 0, 16);
    byte[] encryptedBytes = subArray(encryptedData, initVector.length, encryptedData.length);

    IvParameterSpec ivspec = new IvParameterSpec(initVector);
    SecretKey secretKey = createSecretKey(key);

    Cipher decipher = Cipher.getInstance(cipher);
    decipher.init(Cipher.DECRYPT_MODE, secretKey, ivspec);
    byte[] decryptedBytes = decipher.doFinal(encryptedBytes);

    return new String(decryptedBytes, StandardCharsets.UTF_8);
  }

  private byte[] createRandomInitialVector(Cipher cipher) throws NoSuchAlgorithmException {
    SecureRandom sha1Random = SecureRandom.getInstance(SECURE_RANDOM_ALGORITHM);
    byte[] initVector = new byte[cipher.getBlockSize()];
    sha1Random.nextBytes(initVector);
    return initVector;
  }

  private SecretKey createSecretKey(String key) throws NoSuchAlgorithmException, InvalidKeySpecException {
    PBEKeySpec keySpec = new PBEKeySpec(key.toCharArray(), salt.getBytes(), iterationCount, keyStrength);
    SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm);
    SecretKey secretKey = factory.generateSecret(keySpec);
    return new SecretKeySpec(secretKey.getEncoded(), cipherType);
  }

  private byte[] decode(CharSequence chars) {
    int numChars = chars.length();
    if (numChars % 2 != 0) {
      throw new IllegalArgumentException("Hex-encoded string must have an even number of characters");
    } else {
      byte[] result = new byte[numChars / 2];

      for (int i = 0; i < numChars; i += 2) {
        int msb = Character.digit(chars.charAt(i), 16);
        int lsb = Character.digit(chars.charAt(i + 1), 16);
        if (msb < 0 || lsb < 0) {
          throw new IllegalArgumentException("Detected a Non-hex character at " + (i + 1) + " or "
                  + (i + 2) + " position");
        }

        result[i / 2] = (byte) (msb << 4 | lsb);
      }

      return result;
    }
  }

  private byte[] subArray(byte[] array, int beginIndex, int endIndex) {
    int length = endIndex - beginIndex;
    byte[] subarray = new byte[length];
    System.arraycopy(array, beginIndex, subarray, 0, length);
    return subarray;
  }

  public static final class Builder {

    private String algorithm;
    private String cipher;
    private String cipherType;
    private Integer iterationCount;
    private Integer keyStrength;
    private String salt;
    private String systemPropertySecret;

    private Builder() {
    }

    public SymmetricCryptograph.Builder withAlgorithm(String algorithm) {
      this.algorithm = StringUtils.trimToNull(algorithm);
      return this;
    }

    public SymmetricCryptograph.Builder withCipher(String cipher) {
      this.cipher = StringUtils.trimToNull(cipher);
      return this;
    }

    public SymmetricCryptograph.Builder withCipherType(String cipherType) {
      this.cipherType = StringUtils.trimToNull(cipherType);
      return this;
    }

    public SymmetricCryptograph.Builder withIterationCount(Integer iterationCount) {
      this.iterationCount = iterationCount;
      return this;
    }

    public SymmetricCryptograph.Builder withKeyStrength(Integer keyStrength) {
      this.keyStrength = keyStrength;
      return this;
    }

    public SymmetricCryptograph.Builder withSalt(String salt) {
      this.salt = StringUtils.trimToNull(salt);
      return this;
    }

    public SymmetricCryptograph.Builder withSystemPropertySecret(String systemPropertySecret) {
      String trimmedPropertyKey = StringUtils.trimToNull(systemPropertySecret);
      Preconditions.checkNotNull(trimmedPropertyKey);
      this.systemPropertySecret = trimmedPropertyKey;
      return this;
    }

    public SymmetricCryptograph build() {
      return new SymmetricCryptograph(this);
    }

  }

}
