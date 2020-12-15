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
import io.cassandrareaper.storage.CassandraStorage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Base64;

import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

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
    return Response.ok().entity(getJwt(request)).build();
  }

  String getJwt(HttpServletRequest request) throws IOException {
    return Jwts.builder().setSubject(request.getUserPrincipal().getName()).signWith(SIG_ALG, SIGNING_KEY).compact();
  }

  private static Key getSigningKey(AppContext cxt) {
    byte[] key;
    String txt = System.getenv("JWT_SECRET");
    if (null == txt) {
      if (cxt.storage instanceof CassandraStorage) {
        txt = cxt.config.getCassandraFactory().getClusterName();
      } else {
        txt = AppContext.REAPER_INSTANCE_ADDRESS;
      }
      key = txt.getBytes(StandardCharsets.UTF_8);
    } else {
      key = Base64.getDecoder().decode(txt);
    }

    return new SecretKeySpec(Base64.getDecoder().decode(txt), SIG_ALG.getJcaName());
  }
}
