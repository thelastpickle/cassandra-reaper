/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.resources;

import io.cassandrareaper.crypto.Cryptograph;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/crypto")
@Produces(MediaType.TEXT_PLAIN)
public final class CryptoResource {

  private final Cryptograph cryptograph;

  public CryptoResource(Cryptograph cryptograph) {
    this.cryptograph = cryptograph;
  }

  @GET
  @Path("/encrypt/{text}")
  @RolesAllowed({"user", "operator"})
  public Response encrypt(@PathParam("text") String text) {
    return Response.ok().entity(cryptograph.encrypt(text)).build();
  }
}
