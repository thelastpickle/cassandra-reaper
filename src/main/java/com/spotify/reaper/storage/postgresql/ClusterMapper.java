package com.spotify.reaper.storage.postgresql;

import com.google.common.collect.Sets;

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
    return new Cluster.Builder(r.getString("name"), r.getString("partitioner"),
                               Sets.newHashSet(Arrays.asList(seedHosts))).build();
  }

}
