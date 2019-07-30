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

package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.ClusterProperties;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;


public final class ClusterMapper implements ResultSetMapper<Cluster> {

  @Override
  public Cluster map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    String[] seedHosts = null;
    ObjectMapper objectMapper = new ObjectMapper();
    Object obj = rs.getArray("seed_hosts").getArray();
    if (obj instanceof String[]) {
      seedHosts = (String[]) obj;
    } else if (obj instanceof Object[]) {
      Object[] ol = (Object[]) obj;
      seedHosts = Arrays.copyOf(ol, ol.length, String[].class);
    }

    ClusterProperties clusterProperties;
    try {
      clusterProperties
          = rs.getString("properties") != null
              ? objectMapper.readValue(rs.getString("properties"), ClusterProperties.class)
              : ClusterProperties.builder().withJmxPort(Cluster.DEFAULT_JMX_PORT).build();
    } catch (IOException e) {
      throw new SQLException(e); // Ugly but the interface won't let us throw anything else...
    }

    Cluster.Builder builder = Cluster.builder()
        .withName(rs.getString("name"))
        .withSeedHosts(ImmutableSet.copyOf(seedHosts))
        .withJmxPort(clusterProperties.getJmxPort());

    if (null != rs.getString("partitioner")) {
      builder = builder.withPartitioner(rs.getString("partitioner"));
    }
    return builder.build();
  }
}
