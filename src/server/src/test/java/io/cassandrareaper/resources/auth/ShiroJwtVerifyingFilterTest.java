/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
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

package io.cassandrareaper.resources.auth;

import io.cassandrareaper.AppContext;

import java.security.Principal;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

public final class ShiroJwtVerifyingFilterTest {

  @Test
  public void testIsRemembered() throws Exception {
    try {
      Subject subject = Mockito.mock(Subject.class);
      Mockito.when(subject.getPrincipal()).thenReturn(Mockito.mock(Object.class));
      Mockito.when(subject.isRemembered()).thenReturn(true);
      ThreadContext.bind(subject);
      ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

      Assertions.assertThat(
          filter.isAccessAllowed(
              Mockito.mock(HttpServletRequest.class),
              Mockito.mock(ServletResponse.class),
              Mockito.mock(Object.class)))
          .isTrue();
    } finally {
      ThreadContext.unbindSubject();
    }
  }

  @Test
  public void testIsAuthenticated() throws Exception {
    try {
      Subject subject = Mockito.mock(Subject.class);
      Mockito.when(subject.getPrincipal()).thenReturn(Mockito.mock(Object.class));
      Mockito.when(subject.isAuthenticated()).thenReturn(true);
      ThreadContext.bind(subject);
      ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

      Assertions.assertThat(
          filter.isAccessAllowed(
              Mockito.mock(HttpServletRequest.class),
              Mockito.mock(ServletResponse.class),
              Mockito.mock(Object.class)))
          .isTrue();
    } finally {
      ThreadContext.unbindSubject();
    }
  }

  @Test
  public void testNewRequest() throws Exception {
    ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

    Assertions.assertThat(
        filter.isAccessAllowed(
            Mockito.mock(HttpServletRequest.class),
            Mockito.mock(ServletResponse.class),
            Mockito.mock(Object.class)))
        .isFalse();
  }

  @Test
  public void testAuthorization0() throws Exception {
    try {
      SecurityUtils.setSecurityManager(new DefaultSecurityManager());
      new ShiroJwtProvider(Mockito.mock(AppContext.class));
      HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
      Mockito.when(req.getHeader("Authorization")).thenReturn("junk");
      ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

      Assertions.assertThat(
          filter.isAccessAllowed(
              req,
              Mockito.mock(ServletResponse.class),
              Mockito.mock(Object.class)))
          .isFalse();
    } finally {
      ThreadContext.unbindSubject();
      ThreadContext.unbindSecurityManager();
    }
  }

  @Test
  public void testAuthorization1() throws Exception {
    try {
      SecurityUtils.setSecurityManager(new DefaultSecurityManager());
      new ShiroJwtProvider(Mockito.mock(AppContext.class));
      HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
      Mockito.when(req.getHeader("Authorization")).thenReturn("Bearer ");
      ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

      Assertions.assertThat(
          filter.isAccessAllowed(
              req,
              Mockito.mock(ServletResponse.class),
              Mockito.mock(Object.class)))
          .isFalse();
    } finally {
      ThreadContext.unbindSubject();
      ThreadContext.unbindSecurityManager();
    }
  }

  @Test
  public void testAuthorization2() throws Exception {
    try {
      SecurityUtils.setSecurityManager(new DefaultSecurityManager());
      new ShiroJwtProvider(Mockito.mock(AppContext.class));
      HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
      Mockito.when(req.getHeader("Authorization")).thenReturn("Bearer eyJhbGciOiJIUzI1NiJ9");
      ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

      Assertions.assertThat(
          filter.isAccessAllowed(
              req,
              Mockito.mock(ServletResponse.class),
              Mockito.mock(Object.class)))
          .isFalse();
    } finally {
      ThreadContext.unbindSubject();
      ThreadContext.unbindSecurityManager();
    }
  }

  @Test
  public void testAuthorization3() throws Exception {
    try {
      SecurityUtils.setSecurityManager(new DefaultSecurityManager());
      new ShiroJwtProvider(Mockito.mock(AppContext.class));
      HttpServletRequest req = Mockito.mock(HttpServletRequest.class);

      Mockito
          .when(req.getHeader("Authorization"))
          .thenReturn(
              "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.neIA5mbTFZsZokqG5CFwK7gIxMiBoGOU0anDZmD7kkU");

      ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

      Assertions.assertThat(
          filter.isAccessAllowed(
              req,
              Mockito.mock(ServletResponse.class),
              Mockito.mock(Object.class)))
          .isFalse();
    } finally {
      ThreadContext.unbindSubject();
      ThreadContext.unbindSecurityManager();
    }
  }

  @Test
  public void testAuthorizationValid() throws Exception {
    try {
      SecurityUtils.setSecurityManager(new DefaultSecurityManager());
      HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
      Principal principal = Mockito.mock(Principal.class);
      Mockito.when(principal.getName()).thenReturn("test-user");
      Mockito.when(req.getUserPrincipal()).thenReturn(principal);
      String jwt = new ShiroJwtProvider(Mockito.mock(AppContext.class)).getJwt(req);
      Mockito.when(req.getHeader("Authorization")).thenReturn("Bearer " + jwt);
      ShiroJwtVerifyingFilter filter = new ShiroJwtVerifyingFilter();

      Assertions.assertThat(
          filter.isAccessAllowed(
              req,
              Mockito.mock(ServletResponse.class),
              Mockito.mock(Object.class)))
          .isTrue();
    } finally {
      ThreadContext.unbindSubject();
      ThreadContext.unbindSecurityManager();
    }
  }

  @Test
  public void testOptionsRequestWithoutAuthorizationIsAllowed() throws Exception {
    ShiroJwtVerifyingFilter filter = Mockito.spy(ShiroJwtVerifyingFilter.class);
    HttpServletRequest mockHttpServletRequest = Mockito.spy(HttpServletRequest.class);
    Mockito.when(mockHttpServletRequest.getMethod()).thenReturn(HttpMethod.OPTIONS);
    Mockito.when(filter.isAllowAllOptionsRequests()).thenReturn(true);

    boolean allowed = filter.isAccessAllowed(
        mockHttpServletRequest,
        Mockito.mock(ServletResponse.class),
        Mockito.mock(Object.class)
    );
    Assertions.assertThat(allowed).isTrue();
  }

}
