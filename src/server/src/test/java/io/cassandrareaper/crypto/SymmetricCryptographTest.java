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

import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.util.Throwables;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SymmetricCryptographTest {

  @Test
  public void decryptionAllowsForClearTextIfNotPrefixedProperly() {
    String secretText = "someText";
    SymmetricCryptograph symmetricCryptograph = SymmetricCryptograph.builder()
            .withSystemPropertySecret(RandomStringUtils.randomAlphabetic(20))
            .build();

    String result = symmetricCryptograph.decrypt(secretText);

    assertEquals(secretText, result);
  }

  @Test
  public void encryptAndDecryptText() {
    String secretText = "someText";
    String systemPropertyKey = RandomStringUtils.randomAlphabetic(20);
    System.setProperty(systemPropertyKey, "any_secret_value");
    SymmetricCryptograph symmetricCryptograph = SymmetricCryptograph.builder()
            .withSystemPropertySecret(systemPropertyKey)
            .build();

    String encryptedText = symmetricCryptograph.encrypt(secretText);
    String decryptedText = symmetricCryptograph.decrypt(encryptedText);

    assertNotEquals(secretText, encryptedText);
    assertTrue(encryptedText + " must start with {cipher} but does not", encryptedText.startsWith("{cipher}"));
    assertEquals(secretText, decryptedText);
  }

  @Test
  public void encryptingSameValueMultipleTimesCreatesDifferentResults() {
    String secretText = "someText";
    String systemPropertyKey = RandomStringUtils.randomAlphabetic(20);
    System.setProperty(systemPropertyKey, "any_secret_value");
    SymmetricCryptograph symmetricCryptograph = SymmetricCryptograph.builder()
            .withSystemPropertySecret(systemPropertyKey)
            .build();

    Set<String> encryptedValues = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      String encryptedText = symmetricCryptograph.encrypt(secretText);
      assertFalse(encryptedText + " has been created already", encryptedValues.contains(encryptedText));
      encryptedValues.add(encryptedText);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void failEncryptionWhenSystemPropertyKeyNotSet() {
    SymmetricCryptograph symmetricCryptograph = SymmetricCryptograph.builder()
            .withSystemPropertySecret(RandomStringUtils.randomAlphabetic(20))
            .build();

    symmetricCryptograph.encrypt("sometext");
  }

  @Test(expected = IllegalStateException.class)
  public void failDecryptionWhenSystemPropertyKeyNotSet() {
    SymmetricCryptograph symmetricCryptograph = SymmetricCryptograph.builder()
            .withSystemPropertySecret(RandomStringUtils.randomAlphabetic(20))
            .build();

    symmetricCryptograph.decrypt("{cipher}sometext");
  }

  @Test
  public void failEncryptionWhenInvalidCipherPropertiesAreSet() {
    String secretText = "someText";
    String systemPropertyKey = RandomStringUtils.randomAlphabetic(20);
    System.setProperty(systemPropertyKey, "any_secret_value");
    SymmetricCryptograph symmetricCryptograph = SymmetricCryptograph.builder()
            .withAlgorithm("FOO")
            .withCipher("BAR")
            .withCipherType("ZAR")
            .withKeyStrength(0)
            .withIterationCount(0)
            .withSystemPropertySecret(systemPropertyKey)
            .build();

    try {
      symmetricCryptograph.encrypt(secretText);
      fail("Should not be able to encrypt text using a bogus cipher: BAR");
    } catch (IllegalStateException e) {
      if (!(Throwables.getRootCause(e) instanceof NoSuchAlgorithmException)) {
        fail("Should fail with NoSuchAlgorithmException but found "
                + Throwables.getRootCause(e).getClass().getName() + " instead");
      }
    }
  }

}
