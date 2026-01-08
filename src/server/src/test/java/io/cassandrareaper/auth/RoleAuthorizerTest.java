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

package io.cassandrareaper.auth;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.UriInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class RoleAuthorizerTest {

  @Mock private ContainerRequestContext requestContext;
  @Mock private UriInfo uriInfo;

  private RoleAuthorizer authorizer;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    authorizer = new RoleAuthorizer();
  }

  @Test
  public void testAuthorize_operatorRoleHasAllPermissions() {
    // Given
    User operatorUser = new User("operator", Set.of("operator"));
    when(requestContext.getMethod()).thenReturn("DELETE");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/repair_run/123");

    // When
    boolean authorized = authorizer.authorize(operatorUser, "admin", requestContext);

    // Then
    assertThat(authorized).isTrue();
  }

  @Test
  public void testAuthorize_userRoleCanOnlyRead() {
    // Given
    User userRole = new User("user", Set.of("user"));
    when(requestContext.getMethod()).thenReturn("GET");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/cluster");

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", requestContext);

    // Then
    assertThat(authorized).isTrue();
  }

  @Test
  public void testAuthorize_userRoleCannotWrite() {
    // Given
    User userRole = new User("user", Set.of("user"));
    when(requestContext.getMethod()).thenReturn("POST");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/cluster");

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", requestContext);

    // Then
    assertThat(authorized).isFalse();
  }

  @Test
  public void testAuthorize_userRoleCanUseOptions() {
    // Given
    User userRole = new User("user", Set.of("user"));
    when(requestContext.getMethod()).thenReturn("OPTIONS");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/cluster");

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", requestContext);

    // Then
    assertThat(authorized).isTrue();
  }

  @Test
  public void testAuthorize_userRoleDeniedWithoutRequestContext() {
    // Given
    User userRole = new User("user", Set.of("user"));

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", null);

    // Then
    assertThat(authorized).isFalse();
  }

  @Test
  public void testAuthorize_exactRoleMatch() {
    // Given
    User adminUser = new User("admin", Set.of("admin"));
    when(requestContext.getMethod()).thenReturn("DELETE");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/repair_run/123");

    // When
    boolean authorized = authorizer.authorize(adminUser, "admin", requestContext);

    // Then
    assertThat(authorized).isTrue();
  }

  @Test
  public void testAuthorize_noRoleMatch() {
    // Given
    User basicUser = new User("basic", Set.of("basic"));
    when(requestContext.getMethod()).thenReturn("GET");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/cluster");

    // When
    boolean authorized = authorizer.authorize(basicUser, "admin", requestContext);

    // Then
    assertThat(authorized).isFalse();
  }

  @Test
  public void testAuthorize_multipleRoles() {
    // Given
    User multiRoleUser = new User("multi", Set.of("user", "admin", "operator"));
    when(requestContext.getMethod()).thenReturn("DELETE");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/repair_run/123");

    // When
    boolean authorized = authorizer.authorize(multiRoleUser, "admin", requestContext);

    // Then
    assertThat(authorized).isTrue(); // Should be true because user has operator role
  }

  @Test
  public void testAuthorize_handlesNullUriInfo() {
    // Given
    User userRole = new User("user", Set.of("user"));
    when(requestContext.getMethod()).thenReturn("GET");
    when(requestContext.getUriInfo()).thenReturn(null);

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", requestContext);

    // Then
    assertThat(authorized).isTrue(); // GET should still be allowed for user role
  }

  @Test
  public void testAuthorize_handlesNullMethod() {
    // Given
    User userRole = new User("user", Set.of("user"));
    when(requestContext.getMethod()).thenReturn(null);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/cluster");

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", requestContext);

    // Then
    assertThat(authorized).isFalse(); // Should deny when method is null
  }

  @Test
  public void testAuthorize_userRoleCannotDelete() {
    // Given
    User userRole = new User("user", Set.of("user"));
    when(requestContext.getMethod()).thenReturn("DELETE");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/repair_run/123");

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", requestContext);

    // Then
    assertThat(authorized).isFalse();
  }

  @Test
  public void testAuthorize_userRoleCannotPut() {
    // Given
    User userRole = new User("user", Set.of("user"));
    when(requestContext.getMethod()).thenReturn("PUT");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/cluster/test");

    // When
    boolean authorized = authorizer.authorize(userRole, "admin", requestContext);

    // Then
    assertThat(authorized).isFalse();
  }

  @Test
  public void testAuthorize_emptyRoles() {
    // Given
    User noRoleUser = new User("norole", Set.of());
    when(requestContext.getMethod()).thenReturn("GET");
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("/cluster");

    // When
    boolean authorized = authorizer.authorize(noRoleUser, "admin", requestContext);

    // Then
    assertThat(authorized).isFalse();
  }
}
