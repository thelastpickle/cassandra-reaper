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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/crypto")
@Produces(MediaType.TEXT_PLAIN)
public final class CryptoResource {

  private static final Logger LOG = LoggerFactory.getLogger(CryptoResource.class);

  private final Cryptograph cryptograph;

  public CryptoResource(Cryptograph cryptograph) {
    this.cryptograph = cryptograph;
  }

  @GET
  @Path("/encrypt/{text}")
  public Response encrypt(@PathParam("text") String text) {
    return Response.ok().entity(cryptograph.encrypt(text)).build();
  }

}
