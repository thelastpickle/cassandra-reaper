package io.cassandrareaper.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for SymmetricFactory class. Tests factory configuration and SymmetricCryptograph
 * creation.
 */
public class SymmetricFactoryTest {

  private SymmetricFactory factory;

  @BeforeEach
  public void setUp() {
    factory = new SymmetricFactory();
  }

  @Test
  public void testDefaultValues() {
    // Given: A new SymmetricFactory instance
    // When: Getting default values
    // Then: Default values should be null (not set by defaults in test)
    assertThat(factory.getSalt()).isNull();
    assertThat(factory.getCipher()).isNull();
    assertThat(factory.getCipherType()).isNull();
    assertThat(factory.getAlgorithm()).isNull();
    assertThat(factory.getIterationCount()).isNull();
    assertThat(factory.getKeyStrength()).isNull();
    assertThat(factory.getSystemPropertySecret()).isNull();
  }

  @Test
  public void testSaltGetterAndSetter() {
    // Given: A salt value
    String salt = "customsalt";

    // When: Setting and getting salt
    factory.setSalt(salt);

    // Then: Salt should be correctly set and retrieved
    assertThat(factory.getSalt()).isEqualTo(salt);
  }

  @Test
  public void testCipherGetterAndSetter() {
    // Given: A cipher value
    String cipher = "AES/GCM/NoPadding";

    // When: Setting and getting cipher
    factory.setCipher(cipher);

    // Then: Cipher should be correctly set and retrieved
    assertThat(factory.getCipher()).isEqualTo(cipher);
  }

  @Test
  public void testCipherTypeGetterAndSetter() {
    // Given: A cipher type value
    String cipherType = "AES";

    // When: Setting and getting cipher type
    factory.setCipherType(cipherType);

    // Then: Cipher type should be correctly set and retrieved
    assertThat(factory.getCipherType()).isEqualTo(cipherType);
  }

  @Test
  public void testAlgorithmGetterAndSetter() {
    // Given: An algorithm value
    String algorithm = "PBKDF2WithHmacSHA256";

    // When: Setting and getting algorithm
    factory.setAlgorithm(algorithm);

    // Then: Algorithm should be correctly set and retrieved
    assertThat(factory.getAlgorithm()).isEqualTo(algorithm);
  }

  @Test
  public void testIterationCountGetterAndSetter() {
    // Given: An iteration count value
    Integer iterationCount = 2048;

    // When: Setting and getting iteration count
    factory.setIterationCount(iterationCount);

    // Then: Iteration count should be correctly set and retrieved
    assertThat(factory.getIterationCount()).isEqualTo(iterationCount);
  }

  @Test
  public void testKeyStrengthGetterAndSetter() {
    // Given: A key strength value
    Integer keyStrength = 512;

    // When: Setting and getting key strength
    factory.setKeyStrength(keyStrength);

    // Then: Key strength should be correctly set and retrieved
    assertThat(factory.getKeyStrength()).isEqualTo(keyStrength);
  }

  @Test
  public void testSystemPropertySecretGetterAndSetter() {
    // Given: A system property secret value
    String systemPropertySecret = "CUSTOM_SECRET_KEY";

    // When: Setting and getting system property secret
    factory.setSystemPropertySecret(systemPropertySecret);

    // Then: System property secret should be correctly set and retrieved
    assertThat(factory.getSystemPropertySecret()).isEqualTo(systemPropertySecret);
  }

  @Test
  public void testCreateWithAllProperties() {
    // Given: A factory with all properties set
    factory.setSalt("testsalt");
    factory.setCipher("AES/CBC/PKCS5Padding");
    factory.setCipherType("AES");
    factory.setAlgorithm("PBKDF2WithHmacSHA512");
    factory.setIterationCount(1024);
    factory.setKeyStrength(256);
    factory.setSystemPropertySecret("TEST_SECRET");

    // When: Creating a cryptograph
    Cryptograph cryptograph = factory.create();

    // Then: A SymmetricCryptograph should be created
    assertThat(cryptograph).isNotNull();
    assertThat(cryptograph).isInstanceOf(SymmetricCryptograph.class);
  }

  @Test
  public void testCreateWithNullValues() {
    // Given: A factory with null values (should use defaults in SymmetricCryptograph)
    factory.setSalt(null);
    factory.setCipher(null);
    factory.setCipherType(null);
    factory.setAlgorithm(null);
    factory.setIterationCount(null);
    factory.setKeyStrength(null);
    factory.setSystemPropertySecret("TEST_SECRET"); // This is required

    // When: Creating a cryptograph
    Cryptograph cryptograph = factory.create();

    // Then: A SymmetricCryptograph should be created with defaults
    assertThat(cryptograph).isNotNull();
    assertThat(cryptograph).isInstanceOf(SymmetricCryptograph.class);
  }

  @Test
  public void testSettersWithNullValues() {
    // Given: A factory
    // When: Setting null values
    factory.setSalt(null);
    factory.setCipher(null);
    factory.setCipherType(null);
    factory.setAlgorithm(null);
    factory.setIterationCount(null);
    factory.setKeyStrength(null);
    factory.setSystemPropertySecret(null);

    // Then: All values should be null
    assertThat(factory.getSalt()).isNull();
    assertThat(factory.getCipher()).isNull();
    assertThat(factory.getCipherType()).isNull();
    assertThat(factory.getAlgorithm()).isNull();
    assertThat(factory.getIterationCount()).isNull();
    assertThat(factory.getKeyStrength()).isNull();
    assertThat(factory.getSystemPropertySecret()).isNull();
  }

  @Test
  public void testSettersWithEmptyStrings() {
    // Given: A factory
    // When: Setting empty string values
    factory.setSalt("");
    factory.setCipher("");
    factory.setCipherType("");
    factory.setAlgorithm("");
    factory.setSystemPropertySecret("");

    // Then: Empty strings should be preserved
    assertThat(factory.getSalt()).isEqualTo("");
    assertThat(factory.getCipher()).isEqualTo("");
    assertThat(factory.getCipherType()).isEqualTo("");
    assertThat(factory.getAlgorithm()).isEqualTo("");
    assertThat(factory.getSystemPropertySecret()).isEqualTo("");
  }

  @Test
  public void testSettersWithSpecialCharacters() {
    // Given: Values with special characters
    String saltWithSpecialChars = "salt-with-special_chars@123";
    String secretWithSpecialChars = "SECRET_KEY_WITH_123!@#";

    // When: Setting values with special characters
    factory.setSalt(saltWithSpecialChars);
    factory.setSystemPropertySecret(secretWithSpecialChars);

    // Then: Special characters should be preserved
    assertThat(factory.getSalt()).isEqualTo(saltWithSpecialChars);
    assertThat(factory.getSystemPropertySecret()).isEqualTo(secretWithSpecialChars);
  }

  @Test
  public void testSettersWithExtremeIntegerValues() {
    // Given: Extreme integer values
    Integer minValue = Integer.MIN_VALUE;
    Integer maxValue = Integer.MAX_VALUE;
    Integer zero = 0;

    // When: Setting extreme values
    factory.setIterationCount(minValue);
    factory.setKeyStrength(maxValue);

    // Then: Values should be preserved
    assertThat(factory.getIterationCount()).isEqualTo(minValue);
    assertThat(factory.getKeyStrength()).isEqualTo(maxValue);

    // When: Setting zero
    factory.setIterationCount(zero);
    factory.setKeyStrength(zero);

    // Then: Zero values should be preserved
    assertThat(factory.getIterationCount()).isEqualTo(zero);
    assertThat(factory.getKeyStrength()).isEqualTo(zero);
  }
}
