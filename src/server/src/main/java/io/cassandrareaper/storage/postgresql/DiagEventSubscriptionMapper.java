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

package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.core.DiagEventSubscription;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;


public class DiagEventSubscriptionMapper implements ResultSetMapper<DiagEventSubscription> {

  @Override
  public DiagEventSubscription map(int index, ResultSet rs, StatementContext ctx) throws SQLException {

    UUID id = UuidUtil.fromSequenceId(rs.getLong("id"));
    String cluster = rs.getString("cluster");
    String description = rs.getString("description");
    List<String> includeNodes = parseStringArray(rs.getArray("include_nodes").getArray());
    List<String> events = parseStringArray(rs.getArray("events").getArray());
    Boolean exportWebUi = rs.getBoolean("export_sse");
    String exportFileLogger = rs.getString("export_file_logger");
    String exportHttpEndpoint = rs.getString("export_http_endpoint");

    return new DiagEventSubscription(id, cluster, description, includeNodes, events,
            exportWebUi, exportFileLogger, exportHttpEndpoint);
  }

  private List<String> parseStringArray(Object obj) {
    List<String> values = Collections.emptyList();
    if (obj instanceof String[]) {
      values = Arrays.asList((String[]) obj);
    } else if (obj instanceof Object[]) {
      Object[] ocf = (Object[]) obj;
      values = Arrays.asList(Arrays.copyOf(ocf, ocf.length, String[].class));
    }

    return values;
  }

  public static DiagEventSubscription fromParamMap(Map<String, String> map) {

    UUID id = null;
    String sid = map.get("id");
    if (sid != null) {
      id = UuidUtil.fromSequenceId(Long.valueOf(sid));
    }

    List<String> includeNodes = Collections.emptyList();
    if (map.containsKey("includeNodes")) {
      includeNodes = Arrays.asList((map.get("includeNodes")).split(","));
    }

    List<String> events = Collections.emptyList();
    if (map.containsKey("events")) {
      events = Arrays.asList((map.get("events")).split(","));
    }

    boolean exportSse = false;
    if (map.containsKey("exportSse")) {
      exportSse = Boolean.valueOf(map.get("exportSse"));
    }

    String clusterName = map.get("clusterName");
    String description = map.get("description");
    String exportFileLogger = map.get("exportFileLogger");
    String exportHttpEndpoint = map.get("exportHttpEndpoint");

    return new DiagEventSubscription(id, clusterName, description, includeNodes, events,
            exportSse, exportFileLogger, exportHttpEndpoint);
  }
}
