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

import io.cassandrareaper.resources.RequestUtils;

import java.util.Optional;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.lang.Strings;

import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.web.filter.AccessControlFilter;
import org.apache.shiro.web.subject.WebSubject;
import org.apache.shiro.web.util.WebUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShiroJwtVerifyingFilter extends AccessControlFilter {

  private static final Logger LOG = LoggerFactory.getLogger(ShiroJwtVerifyingFilter.class);

  private final boolean allowAllOptionsRequests;

  public ShiroJwtVerifyingFilter() {
    allowAllOptionsRequests = RequestUtils.isAllowAllOptionsRequests();
  }

  @VisibleForTesting
  boolean isAllowAllOptionsRequests() {
    return allowAllOptionsRequests;
  }

  @Override
  protected boolean isAccessAllowed(ServletRequest req, ServletResponse res, Object mappedValue) throws Exception {
    if (isAllowAllOptionsRequests() && RequestUtils.isOptionsRequest(req)) {
      return true;
    }

    Subject nonJwt = getSubject(req, res);

    return null != nonJwt.getPrincipal() && (nonJwt.isRemembered() || nonJwt.isAuthenticated())
      ? true
      : getJwtUser(req).isPresent();
  }

  static Subject getJwtSubject(Subject nonJwt, ServletRequest req, ServletResponse res) {
    return null != nonJwt.getPrincipal() && (nonJwt.isRemembered() || nonJwt.isAuthenticated())
      ? nonJwt
      : new WebSubject.Builder(req, res)
          .principals(new SimplePrincipalCollection(getJwtUser(req).get(), "jwtRealm"))
          .buildSubject();
  }

  @Override
  protected boolean onAccessDenied(ServletRequest req, ServletResponse res) throws Exception {
    WebUtils.toHttp(res).setStatus(HttpServletResponse.SC_FORBIDDEN);
    WebUtils.toHttp(res).setHeader("Content-Type", "text/plain");
    WebUtils.toHttp(res).getOutputStream().print("Forbidden access. Please login to access this page.");
    WebUtils.toHttp(res).flushBuffer();
    return false;
  }

  private static Optional<String> getJwtUser(ServletRequest req) {
    String jwt = WebUtils.toHttp(req).getHeader("Authorization");
    if (null != jwt && jwt.startsWith("Bearer ")) {
      try {
        jwt = jwt.substring(jwt.indexOf(' ') + 1);
        Jws<Claims> claims = Jwts.parser().setSigningKey(ShiroJwtProvider.SIGNING_KEY).parseClaimsJws(jwt);
        String user = claims.getBody().getSubject();
        return Strings.hasText(user) ? Optional.of(user) : Optional.empty();
      } catch (JwtException | IllegalArgumentException e) {
        LOG.error("Failed validating JWT {} from {}", jwt, WebUtils.toHttp(req).getRemoteAddr());
        LOG.debug("exception", e);
      }
    }
    return Optional.empty();
  }
}
