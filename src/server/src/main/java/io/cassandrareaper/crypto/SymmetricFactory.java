/*
 * Copyright 2018-2019 The Last Pickle Ltd
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

import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("symmetric")
public class SymmetricFactory implements CryptographFactory {

  @NotNull
  @DefaultValue("deadbeef")
  @JsonProperty
  private String salt;

  @NotNull
  @DefaultValue("AES/CBC/PKCS5Padding")
  @JsonProperty
  private String cipher;

  @NotNull
  @DefaultValue("AES")
  @JsonProperty
  private String cipherType;

  @NotNull
  @DefaultValue("PBKDF2WithHmacSHA512")
  @JsonProperty
  private String algorithm;

  @NotNull
  @DefaultValue("1024")
  @JsonProperty
  private Integer iterationCount;

  @NotNull
  @DefaultValue("256")
  @JsonProperty
  private Integer keyStrength;

  @JsonProperty
  @NotNull
  private String systemPropertySecret;

  @Override
  public Cryptograph create() {
    return SymmetricCryptograph.builder()
            .withAlgorithm(this.algorithm)
            .withCipher(this.cipher)
            .withCipherType(this.cipherType)
            .withIterationCount(this.iterationCount)
            .withKeyStrength(this.keyStrength)
            .withSalt(this.salt)
            .withSystemPropertySecret(this.systemPropertySecret)
            .build();
  }

  public String getSalt() {
    return salt;
  }

  public void setSalt(String salt) {
    this.salt = salt;
  }

  public String getCipher() {
    return cipher;
  }

  public void setCipher(String cipher) {
    this.cipher = cipher;
  }

  public String getCipherType() {
    return cipherType;
  }

  public void setCipherType(String cipherType) {
    this.cipherType = cipherType;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(String algorithm) {
    this.algorithm = algorithm;
  }

  public Integer getIterationCount() {
    return iterationCount;
  }

  public void setIterationCount(Integer iterationCount) {
    this.iterationCount = iterationCount;
  }

  public Integer getKeyStrength() {
    return keyStrength;
  }

  public void setKeyStrength(Integer keyStrength) {
    this.keyStrength = keyStrength;
  }

  public String getSystemPropertySecret() {
    return systemPropertySecret;
  }

  public void setSystemPropertySecret(String systemPropertySecret) {
    this.systemPropertySecret = systemPropertySecret;
  }

}
