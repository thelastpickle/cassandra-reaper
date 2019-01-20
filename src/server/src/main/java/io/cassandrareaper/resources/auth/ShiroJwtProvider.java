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
import io.cassandrareaper.storage.IDistributedStorage;

import java.io.IOException;
import java.security.Key;

import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

@Path("/jwt")
public final class ShiroJwtProvider {

  static volatile Key SIGNING_KEY;
  private static final SignatureAlgorithm SIG_ALG = SignatureAlgorithm.HS256;

  public ShiroJwtProvider(AppContext cxt) {
    SIGNING_KEY = getSigningKey(cxt);
  }

  @GET
  public Response get(@Context HttpServletRequest request) throws IOException {
    return Response
        .ok()
        .entity(
            Jwts.builder().setSubject(request.getUserPrincipal().getName()).signWith(SIG_ALG, SIGNING_KEY).compact())
        .build();
  }

  private static Key getSigningKey(AppContext cxt) {
    String txt = System.getenv("JWT_SECRET");
    if (null == txt) {
      txt = cxt.storage instanceof IDistributedStorage
          ? cxt.config.getCassandraFactory().getClusterName()
          : AppContext.REAPER_INSTANCE_ADDRESS;
    }
    return new SecretKeySpec(DatatypeConverter.parseBase64Binary(txt), SIG_ALG.getJcaName());
  }
}
