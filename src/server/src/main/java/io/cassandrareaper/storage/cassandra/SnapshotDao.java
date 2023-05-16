/*
 * Copyright 2016-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
 * Copyright 2020-2020 DataStax, Inc.
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

package io.cassandrareaper.storage.cassandra;

import io.cassandrareaper.core.Snapshot;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.joda.time.DateTime;

public class SnapshotDao {

  PreparedStatement getSnapshotPrepStmt;
  PreparedStatement deleteSnapshotPrepStmt;
  PreparedStatement saveSnapshotPrepStmt;
  private final Session session;

  public SnapshotDao(Session session) {
    this.session = session;
    prepareStatements();
  }

  private void prepareStatements() {
    getSnapshotPrepStmt = session.prepare("SELECT * FROM snapshot WHERE cluster = ? and snapshot_name = ?");
    deleteSnapshotPrepStmt = session.prepare("DELETE FROM snapshot WHERE cluster = ? and snapshot_name = ?");
    saveSnapshotPrepStmt = session.prepare(
        "INSERT INTO snapshot (cluster, snapshot_name, owner, cause, creation_time)"
            + " VALUES(?,?,?,?,?)");

  }


  public boolean saveSnapshot(Snapshot snapshot) {
    session.execute(
        saveSnapshotPrepStmt.bind(
            snapshot.getClusterName(),
            snapshot.getName(),
            snapshot.getOwner().orElse("reaper"),
            snapshot.getCause().orElse("taken with reaper"),
            snapshot.getCreationDate().get()));

    return true;
  }


  public boolean deleteSnapshot(Snapshot snapshot) {
    session.execute(deleteSnapshotPrepStmt.bind(snapshot.getClusterName(), snapshot.getName()));
    return false;
  }


  public Snapshot getSnapshot(String clusterName, String snapshotName) {
    Snapshot.Builder snapshotBuilder = Snapshot.builder().withClusterName(clusterName).withName(snapshotName);

    ResultSet result = session.execute(getSnapshotPrepStmt.bind(clusterName, snapshotName));
    for (Row row : result) {
      snapshotBuilder
          .withCause(row.getString("cause"))
          .withOwner(row.getString("owner"))
          .withCreationDate(new DateTime(row.getTimestamp("creation_time")));
    }

    return snapshotBuilder.build();
  }
}