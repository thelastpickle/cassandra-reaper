/*
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

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.Subject;
import org.secnod.shiro.jaxrs.Auth;

@Path("/")
public class LoginResource {
  private final AppContext context;

  public LoginResource(AppContext context) {
    this.context = context;
  }

  @Path("/login")
  @POST
  public void login(
      @FormParam("username") String username,
      @FormParam("password") String password,
      @Auth Subject subject)
      throws IOException {
    ensurePresent(username, "Invalid credentials: missing username.");
    ensurePresent(password, "Invalid credentials: missing password.");

    try {
      subject.login(new UsernamePasswordToken(username, password));
    } catch (AuthenticationException e) {
      throw new IncorrectCredentialsException(
          "Invalid credentials combination for user: " + username);
    }
  }

  @Path("/logout")
  @POST
  public void logout(@Auth Subject subject) throws IOException {
    subject.logout();
  }

  private void ensurePresent(String value, String message) {
    if (StringUtils.isBlank(value)) {
      throw new IncorrectCredentialsException(message);
    }
  }

  @Produces(MediaType.APPLICATION_JSON)
  @Path("/loginRequired")
  @GET
  public Response loginRequired() {
    Map<String, Boolean> authRequired = Maps.newHashMap();
    authRequired.put("auth", context.config.isAccessControlEnabled());

    return Response.ok().entity(authRequired).build();
  }
}
