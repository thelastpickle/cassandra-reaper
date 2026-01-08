/*
 * Copyright 2025-2025 DataStax, Inc.
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

package io.cassandrareaper.resources;

import io.cassandrareaper.crypto.Cryptograph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CryptoResourceTest {

  @Mock private Cryptograph cryptograph;

  private CryptoResource resource;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    resource = new CryptoResource(cryptograph);
  }

  @Test
  public void testEncrypt_simpleText() {
    // Given
    String plainText = "password123";
    String encryptedText = "encrypted_password123";
    when(cryptograph.encrypt(plainText)).thenReturn(encryptedText);

    // When
    Response response = resource.encrypt(plainText);

    // Then
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isEqualTo(encryptedText);
  }

  @Test
  public void testEncrypt_emptyString() {
    // Given
    String plainText = "";
    String encryptedText = "encrypted_empty";
    when(cryptograph.encrypt(plainText)).thenReturn(encryptedText);

    // When
    Response response = resource.encrypt(plainText);

    // Then
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isEqualTo(encryptedText);
  }

  @Test
  public void testEncrypt_specialCharacters() {
    // Given
    String plainText = "p@ssw0rd!#$%^&*()";
    String encryptedText = "encrypted_special_chars";
    when(cryptograph.encrypt(plainText)).thenReturn(encryptedText);

    // When
    Response response = resource.encrypt(plainText);

    // Then
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isEqualTo(encryptedText);
  }

  @Test
  public void testEncrypt_longText() {
    // Given
    String plainText = "a".repeat(1000);
    String encryptedText = "encrypted_long_text";
    when(cryptograph.encrypt(plainText)).thenReturn(encryptedText);

    // When
    Response response = resource.encrypt(plainText);

    // Then
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isEqualTo(encryptedText);
  }

  @Test
  public void testEncrypt_unicodeCharacters() {
    // Given
    String plainText = "测试密码🔐";
    String encryptedText = "encrypted_unicode";
    when(cryptograph.encrypt(plainText)).thenReturn(encryptedText);

    // When
    Response response = resource.encrypt(plainText);

    // Then
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.getEntity()).isEqualTo(encryptedText);
  }
}
