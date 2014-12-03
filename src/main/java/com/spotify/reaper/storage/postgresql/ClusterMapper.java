package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.core.Cluster;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;

public class ClusterMapper implements ResultSetMapper<Cluster> {

  public Cluster map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    String[] seedHosts = (String[]) r.getArray("seed_hosts").getArray();
    return new Cluster.Builder()
        .name(r.getString("name"))
        .partitioner(r.getString("partitioner"))
        .seedHosts(new HashSet<String>(Arrays.asList(seedHosts)))
        .build();
  }

}
