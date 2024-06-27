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

import java.io.IOException;
import java.util.Date;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.web.filter.authz.HttpMethodPermissionFilter;
import org.apache.shiro.web.util.WebUtils;

public final class RestPermissionsFilter extends HttpMethodPermissionFilter {

  @VisibleForTesting
  boolean isCorsEnabled() {
    return RequestUtils.isCorsEnabled();
  }

  @Override
  public boolean isAccessAllowed(ServletRequest request, ServletResponse response, Object mappedValue)
      throws IOException {
    if (isCorsEnabled() && RequestUtils.isOptionsRequest(request)) {
      return true;
    }

    Subject subject = getSubject(request, response);

    if (!subject.getPrincipals().getRealmNames().contains("jwtRealm")
        && !RequestUtils.getSessionTimeout().isNegative()
        && subject.getSession().getStartTimestamp().before(
            new Date(System.currentTimeMillis() - RequestUtils.getSessionTimeout().toMillis()))) {
      // Session has lived longer than its timeout already. Force logout.
      subject.logout();
      return false;
    }
    return super.isAccessAllowed(request, response, mappedValue);
  }

  @Override
  protected Subject getSubject(ServletRequest request, ServletResponse response) {
    return ShiroJwtVerifyingFilter.getJwtSubject(super.getSubject(request, response), request, response);
  }

  @Override
  protected boolean onAccessDenied(ServletRequest req, ServletResponse res) throws IOException {
    WebUtils.toHttp(res).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    WebUtils.toHttp(res).setHeader("Content-Type", "text/plain");
    Object user = getSubject(req, res).getPrincipal();
    String err = String.format("Unauthorized `%s` operation for user: %s.", getHttpMethodAction(req), user);
    WebUtils.toHttp(res).getOutputStream().print(err);
    WebUtils.toHttp(res).flushBuffer();
    return false;
  }
}
