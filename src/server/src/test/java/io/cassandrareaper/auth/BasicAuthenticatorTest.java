package io.cassandrareaper.auth;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.basic.BasicCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for BasicAuthenticator class. Tests basic authentication functionality with various
 * credential scenarios.
 */
public class BasicAuthenticatorTest {

  @Mock private UserStore mockUserStore;
  private BasicAuthenticator basicAuthenticator;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    basicAuthenticator = new BasicAuthenticator(mockUserStore);
  }

  @Test
  public void testConstructor() {
    // Given: A UserStore instance
    UserStore userStore = mock(UserStore.class);

    // When: Creating a BasicAuthenticator
    BasicAuthenticator authenticator = new BasicAuthenticator(userStore);

    // Then: The authenticator should be created successfully
    assertThat(authenticator).isNotNull();
  }

  @Test
  public void testAuthenticateWithValidCredentials() throws AuthenticationException {
    // Given: Valid credentials and user exists in store
    String username = "testuser";
    String password = "testpass";
    Set<String> roles = Set.of("user");
    User expectedUser = new User(username, roles);
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password)).thenReturn(true);
    when(mockUserStore.findUser(username)).thenReturn(expectedUser);

    // When: Authenticating with valid credentials
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should succeed and return the user
    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(expectedUser);
    assertThat(result.get().getName()).isEqualTo(username);
    assertThat(result.get().getRoles()).isEqualTo(roles);
  }

  @Test
  public void testAuthenticateWithInvalidCredentials() throws AuthenticationException {
    // Given: Invalid credentials (authentication fails)
    String username = "testuser";
    String password = "wrongpassword";
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password)).thenReturn(false);

    // When: Authenticating with invalid credentials
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should fail
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateWithNullCredentials() throws AuthenticationException {
    // Given: Null credentials
    BasicCredentials credentials = null;

    // When: Authenticating with null credentials
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should fail
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateWithValidCredentialsButUserNotFound() throws AuthenticationException {
    // Given: Valid credentials but user not found in store
    String username = "testuser";
    String password = "testpass";
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password)).thenReturn(true);
    when(mockUserStore.findUser(username)).thenReturn(null);

    // When: Authenticating with valid credentials but user not found
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should fail
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateWithEmptyUsername() throws AuthenticationException {
    // Given: Credentials with empty username
    String username = "";
    String password = "testpass";
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password)).thenReturn(false);

    // When: Authenticating with empty username
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should fail
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateWithEmptyPassword() throws AuthenticationException {
    // Given: Credentials with empty password
    String username = "testuser";
    String password = "";
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password)).thenReturn(false);

    // When: Authenticating with empty password
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should fail
    assertThat(result).isEmpty();
  }

  @Test
  public void testAuthenticateWithUserHavingMultipleRoles() throws AuthenticationException {
    // Given: Valid credentials and user with multiple roles
    String username = "adminuser";
    String password = "adminpass";
    Set<String> roles = Set.of("user", "operator");
    User expectedUser = new User(username, roles);
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password)).thenReturn(true);
    when(mockUserStore.findUser(username)).thenReturn(expectedUser);

    // When: Authenticating with valid credentials
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should succeed and return the user with all roles
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(username);
    assertThat(result.get().getRoles()).containsExactlyInAnyOrder("user", "operator");
    assertThat(result.get().hasRole("user")).isTrue();
    assertThat(result.get().hasRole("operator")).isTrue();
  }

  @Test
  public void testAuthenticateWithSpecialCharactersInCredentials() throws AuthenticationException {
    // Given: Credentials with special characters
    String username = "test@user.com";
    String password = "p@ssw0rd!";
    Set<String> roles = Set.of("user");
    User expectedUser = new User(username, roles);
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password)).thenReturn(true);
    when(mockUserStore.findUser(username)).thenReturn(expectedUser);

    // When: Authenticating with special characters in credentials
    Optional<User> result = basicAuthenticator.authenticate(credentials);

    // Then: Authentication should succeed
    assertThat(result).isPresent();
    assertThat(result.get().getName()).isEqualTo(username);
  }

  @Test
  public void testAuthenticateUserStoreThrowsException() throws AuthenticationException {
    // Given: UserStore authentication throws an exception
    String username = "testuser";
    String password = "testpass";
    BasicCredentials credentials = new BasicCredentials(username, password);

    when(mockUserStore.authenticate(username, password))
        .thenThrow(new RuntimeException("Store error"));

    // When/Then: Exception should be propagated
    try {
      basicAuthenticator.authenticate(credentials);
    } catch (RuntimeException e) {
      assertThat(e.getMessage()).isEqualTo("Store error");
    }
  }
}
