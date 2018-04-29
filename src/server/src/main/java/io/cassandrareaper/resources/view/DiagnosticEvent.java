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

package io.cassandrareaper.resources.view;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DiagnosticEvent {

  @JsonProperty
  private String cluster;

  @JsonProperty
  private String node;

  @JsonProperty
  private String eventClass;

  @JsonProperty
  private String eventType;

  @JsonProperty
  private Long timestamp;

  @JsonProperty
  private Map<String, String> event;

  public DiagnosticEvent(String cluster, String node, String eventClass, String eventType, Long timestamp,
                         Map<String, String> event) {
    this.cluster = cluster;
    this.node = node;
    this.eventClass = eventClass;
    this.eventType = eventType;
    this.timestamp = timestamp;
    this.event = event;
  }

  public String getEventClass() {
    return eventClass;
  }

  public String getEventType() {
    return eventType;
  }

  public String getCluster() {
    return cluster;
  }

  public String getNode() {
    return node;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Map<String, String> getEvent() {
    return event;
  }
}
