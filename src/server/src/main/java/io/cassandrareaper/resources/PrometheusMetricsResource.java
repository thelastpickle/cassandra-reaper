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

package io.cassandrareaper.resources;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Enumeration;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

/**
 * JAX-RS resource that exposes Prometheus metrics instead of using a servlet. This avoids the
 * javax/jakarta servlet compatibility issues.
 */
@Path("/prometheusMetrics")
@Produces(TextFormat.CONTENT_TYPE_004)
public class PrometheusMetricsResource {

  @GET
  public Response getMetrics() {
    try {
      StringWriter writer = new StringWriter();
      Enumeration<io.prometheus.client.Collector.MetricFamilySamples> mfs =
          CollectorRegistry.defaultRegistry.metricFamilySamples();
      TextFormat.write004(writer, mfs);

      return Response.ok(writer.toString())
          .header("Content-Type", TextFormat.CONTENT_TYPE_004)
          .build();
    } catch (IOException e) {
      return Response.serverError().entity("Error generating metrics: " + e.getMessage()).build();
    }
  }
}
