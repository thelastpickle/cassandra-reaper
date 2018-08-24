/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2018 The Last Pickle Ltd
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

import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.health.HealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/ping")
@Produces(MediaType.TEXT_PLAIN)
public final class PingResource {

  private static final Logger LOG = LoggerFactory.getLogger(PingResource.class);

  private final HealthCheck healthCheck;

  public PingResource(HealthCheck healthCheck) {
    this.healthCheck = healthCheck;
  }

  @HEAD
  public Response headPing() {
    LOG.debug("ping called");

    return healthCheck.execute().isHealthy()
        ? Response.noContent().build()
        : Response.serverError().build();
  }

  @GET
  public Response getPing() {
    LOG.debug("ping called");

    return healthCheck.execute().isHealthy()
        ? Response.noContent().build()
        : Response.serverError().build();
  }
}
