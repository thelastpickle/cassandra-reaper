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

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.lang.Strings;
import org.apache.shiro.web.filter.AccessControlFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShiroJwtVerifyingFilter extends AccessControlFilter {

  private static final Logger LOG = LoggerFactory.getLogger(ShiroJwtVerifyingFilter.class);

  public ShiroJwtVerifyingFilter() {}

  @Override
  protected boolean isAccessAllowed(ServletRequest req, ServletResponse res, Object mappedValue) throws Exception {
    if (null != getSubject(req, res).getPrincipal()
        && (getSubject(req, res).isRemembered() || getSubject(req, res).isAuthenticated())) {
      return true;
    }
    HttpServletRequest httpRequest = (HttpServletRequest) req;
    String jwt = httpRequest.getHeader("Authorization");
    if (null != jwt && jwt.startsWith("Bearer ")) {
      try {
        jwt = jwt.substring(jwt.indexOf(' ') + 1);
        Jws<Claims> claims = Jwts.parser().setSigningKey(ShiroJwtProvider.SIGNING_KEY).parseClaimsJws(jwt);
        String user = claims.getBody().getSubject();
        return Strings.hasText(user);
      } catch (JwtException | IllegalArgumentException e) {
        LOG.error("Failed validating JWT: " + jwt, e);
      }
    }
    return false;
  }

  @Override
  protected boolean onAccessDenied(ServletRequest req, ServletResponse res) throws Exception {
    HttpServletResponse response = (HttpServletResponse) res;
    response.setStatus(HttpServletResponse.SC_FORBIDDEN);
    return false;
  }
}
