/*
 * Copyright 2018-2018 The Last Pickle Ltd
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

import io.cassandrareaper.AppContext;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides an endpoint to retrieve reaper configuration */
@Path("/reaper")
@Produces(MediaType.APPLICATION_JSON)
public final class ReaperResource {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotResource.class);

  private final AppContext context;

  public ReaperResource(AppContext context) {
    this.context = context;
  }

  /**
   * Endpoint used to retrieve datacenterAvailability config parameter
   *
   * @return value of datacenterAvailability configuration parameter
   */
  @GET
  @Path("/datacenterAvailability")
  public Response getDatacenterAvailability() {
    return Response.ok()
        .entity(
            ImmutableMap.of("datacenterAvailability", context.config.getDatacenterAvailability()))
        .build();
  }
}
