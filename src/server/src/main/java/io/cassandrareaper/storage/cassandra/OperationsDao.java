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

import io.cassandrareaper.storage.OpType;

import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class OperationsDao {

  private static final DateTimeFormatter TIME_BUCKET_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmm");
  private PreparedStatement insertOperationsPrepStmt;
  private PreparedStatement listOperationsForNodePrepStmt;
  private final Session session;

  public OperationsDao(Session session) {
    this.session = session;
    prepareOperationsStatements();
  }
  private void prepareOperationsStatements() {
    insertOperationsPrepStmt = session.prepare(
        "INSERT INTO node_operations(cluster, type, time_bucket, host, ts, data) "
            + "values(?,?,?,?,?,?)");

    listOperationsForNodePrepStmt = session.prepare(
        "SELECT cluster, type, time_bucket, host, ts, data FROM node_operations "
            + "WHERE cluster = ? AND type = ? and time_bucket = ? and host = ? LIMIT 1");
  }

  public void storeOperations(String clusterName, OpType operationType, String host, String operationsJson) {
    session.executeAsync(
        insertOperationsPrepStmt.bind(
            clusterName,
            operationType.getName(),
            DateTime.now().toString(TIME_BUCKET_FORMATTER),
            host,
            DateTime.now().toDate(),
            operationsJson));
  }
  public String listOperations(String clusterName, OpType operationType, String host) {
    List<ResultSetFuture> futures = Lists.newArrayList();
    futures.add(session.executeAsync(
        listOperationsForNodePrepStmt.bind(
            clusterName, operationType.getName(), DateTime.now().toString(TIME_BUCKET_FORMATTER), host)));
    futures.add(session.executeAsync(
        listOperationsForNodePrepStmt.bind(
            clusterName,
            operationType.getName(),
            DateTime.now().minusMinutes(1).toString(TIME_BUCKET_FORMATTER),
            host)));
    for (ResultSetFuture future : futures) {
      ResultSet operations = future.getUninterruptibly();
      for (Row row : operations) {
        return row.getString("data");
      }
    }

    return "";
  }


  public void purgeNodeOperations() {
  }
}