/*
 * Copyright 2018-2018 The Last Pickle Ltd
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

package io.cassandrareaper.resources.auth;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.shiro.ShiroException;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authz.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class ShiroExceptionMapper implements ExceptionMapper<ShiroException> {

  private static final Logger LOG = LoggerFactory.getLogger(ShiroExceptionMapper.class);

  @Override
  public Response toResponse(ShiroException exception) {
    if (AuthorizationException.class.isAssignableFrom(exception.getClass())
        || AuthenticationException.class.isAssignableFrom(exception.getClass())) {
      LOG.info("Authentication failed", exception);
      return Response.status(Response.Status.FORBIDDEN).entity(exception.getMessage()).build();
    }

    LOG.error("Unexpected ShiroException", exception);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
  }
}
