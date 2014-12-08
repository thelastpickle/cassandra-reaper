package com.spotify.reaper.storage.postgresql;

import com.spotify.reaper.core.ColumnFamily;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnFamilyMapper implements ResultSetMapper<ColumnFamily> {

  public ColumnFamily map(int index, ResultSet r, StatementContext ctx) throws SQLException {
    return new ColumnFamily.Builder(r.getString("cluster_name"),
                                    r.getString("keyspace_name"),
                                    r.getString("name"),
                                    r.getInt("segment_count"),
                                    r.getBoolean("snapshot_repair"))
        .build(r.getLong("id"));
  }

}
