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

package io.cassandrareaper.core;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DiagEventSubscription {

  @JsonProperty
  private UUID id;

  @JsonProperty
  private String cluster;

  @JsonProperty
  private String description;

  @JsonProperty("include_nodes")
  private List<String> includeNodes;

  @JsonProperty
  private List<String> events;

  @JsonProperty("export_sse")
  private boolean exportSse;

  @JsonProperty("export_file_logger")
  private String exportFileLogger;

  @JsonProperty("export_http_endpoint")
  private String exportHttpEndpoint;

  /**
   * Default public constructor Required for Jackson JSON parsing.
   */
  public DiagEventSubscription() {
  }

  public DiagEventSubscription(UUID id, String cluster, String description,
                               List<String> includeNodes, List<String> events,
                               boolean exportSse, String exportFileLogger, String exportHttpEndpoint) {
    this.id = id;
    this.cluster = cluster;
    this.description = description;
    this.includeNodes = includeNodes;
    this.events = events;
    this.exportSse = exportSse;
    this.exportFileLogger = exportFileLogger;
    this.exportHttpEndpoint = exportHttpEndpoint;
  }

  public UUID getId() {
    return id;
  }

  public String getCluster() {
    return cluster;
  }

  public String getDescription() {
    return description;
  }

  public List<String> getIncludeNodes() {
    return includeNodes;
  }

  public List<String> getEvents() {
    return events;
  }

  public String getExportFileLogger() {
    return exportFileLogger;
  }

  public String getExportHttpEndpoint() {
    return exportHttpEndpoint;
  }

  public boolean getExportSse() {
    return exportSse;
  }

  public DiagEventSubscription withId(UUID id) {
    return new DiagEventSubscription(id, cluster, description, includeNodes, events,
            exportSse, exportFileLogger, exportHttpEndpoint);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    DiagEventSubscription that = (DiagEventSubscription) obj;
    return exportSse == that.exportSse
            && Objects.equals(id, that.id)
            && Objects.equals(cluster, that.cluster)
            && Objects.equals(description, that.description)
            && Objects.equals(includeNodes, that.includeNodes)
            && Objects.equals(events, that.events)
            && Objects.equals(exportFileLogger, that.exportFileLogger)
            && Objects.equals(exportHttpEndpoint, that.exportHttpEndpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, cluster, description, includeNodes, events,
            exportSse, exportFileLogger, exportHttpEndpoint);
  }

  @Override
  public String toString() {
    return "DiagEventSubscription{"
            + "id=" + id
            + ", cluster='" + cluster + '\''
            + ", description='" + description + '\''
            + ", includeNodes=" + includeNodes
            + ", events=" + events
            + ", exportSse=" + exportSse
            + ", exportFileLogger='" + exportFileLogger + '\''
            + ", exportHttpEndpoint='" + exportHttpEndpoint + '\''
            + '}';
  }

}
