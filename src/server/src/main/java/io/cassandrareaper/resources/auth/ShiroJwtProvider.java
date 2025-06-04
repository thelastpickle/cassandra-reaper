/*
 *
 * Copyright 2019-2019 The Last Pickle Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cassandrareaper.resources.auth;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.resources.RequestUtils;
import io.cassandrareaper.storage.cassandra.CassandraStorageFacade;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.spec.SecretKeySpec;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import io.jsonwebtoken.Jwts;

@Path("/jwt")
public final class ShiroJwtProvider {

  static volatile Key SIGNING_KEY;

  public ShiroJwtProvider(AppContext cxt) {
    SIGNING_KEY = getSigningKey(cxt);
  }

  @GET
  public Response get(@Context HttpServletRequest request) throws IOException {
    return Response.ok().entity(getJwt(request)).build();
  }

  String getJwt(HttpServletRequest request) throws IOException {
    if (RequestUtils.getSessionTimeout().isNegative()) {
      // No session timeout set, so return a JWT with no expiration time
      return Jwts.builder()
          .subject(request.getUserPrincipal().getName())
          .signWith((javax.crypto.SecretKey) SIGNING_KEY, Jwts.SIG.HS256)
          .compact();
    } else {
      // Return a JWT with an expiration time based on the session timeout
      return Jwts.builder()
          .subject(request.getUserPrincipal().getName())
          .expiration(
              new java.util.Date(
                  System.currentTimeMillis() + RequestUtils.getSessionTimeout().toMillis()))
          .signWith((javax.crypto.SecretKey) SIGNING_KEY, Jwts.SIG.HS256)
          .compact();
    }
  }

  private static Key getSigningKey(AppContext cxt) {
    byte[] key;
    String txt = System.getenv("JWT_SECRET");
    if (null == txt) {
      if (cxt.storage instanceof CassandraStorageFacade) {
        txt = cxt.config.getCassandraFactory().getSessionName();
      } else {
        txt = AppContext.REAPER_INSTANCE_ADDRESS;
      }
      key = txt.getBytes(StandardCharsets.UTF_8);
    } else {
      key = Base64.getDecoder().decode(txt);
    }

    // Ensure the key is at least 256 bits (32 bytes) for HS256
    if (key.length < 32) {
      try {
        // Use SHA-256 to derive a proper 256-bit key
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        key = digest.digest(key);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("SHA-256 algorithm not available", e);
      }
    }

    return new SecretKeySpec(key, "HmacSHA256");
  }
}
