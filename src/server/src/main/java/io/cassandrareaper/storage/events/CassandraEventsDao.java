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

package io.cassandrareaper.storage.events;

import io.cassandrareaper.core.DiagEventSubscription;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;

public class CassandraEventsDao implements IEventsDao {
  PreparedStatement getDiagnosticEventsPrepStmt;
  PreparedStatement getDiagnosticEventPrepStmt;
  PreparedStatement deleteDiagnosticEventPrepStmt;
  PreparedStatement saveDiagnosticEventPrepStmt;
  private final Session session;

  public CassandraEventsDao(Session session) {
    this.session = session;
    prepareStatements();
  }

  private void prepareStatements() {
    getDiagnosticEventsPrepStmt = session.prepare("SELECT * FROM diagnostic_event_subscription");
    getDiagnosticEventPrepStmt = session.prepare("SELECT * FROM diagnostic_event_subscription WHERE id = ?");
    deleteDiagnosticEventPrepStmt = session.prepare("DELETE FROM diagnostic_event_subscription WHERE id = ?");

    saveDiagnosticEventPrepStmt = session.prepare("INSERT INTO diagnostic_event_subscription "
        + "(id,cluster,description,nodes,events,export_sse,export_file_logger,export_http_endpoint)"
        + " VALUES(?,?,?,?,?,?,?,?)");
  }

  static DiagEventSubscription createDiagEventSubscription(Row row) {
    return new DiagEventSubscription(
        Optional.of(row.getUUID("id")),
        row.getString("cluster"),
        Optional.of(row.getString("description")),
        row.getSet("nodes", String.class),
        row.getSet("events", String.class),
        row.getBool("export_sse"),
        row.getString("export_file_logger"),
        row.getString("export_http_endpoint"));
  }


  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions() {
    return session.execute(getDiagnosticEventsPrepStmt.bind()).all().stream()
        .map((row) -> createDiagEventSubscription(row))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions(String clusterName) {
    Preconditions.checkNotNull(clusterName);

    return session.execute(getDiagnosticEventsPrepStmt.bind()).all().stream()
        .map((row) -> createDiagEventSubscription(row))
        .filter((subscription) -> clusterName.equals(subscription.getCluster()))
        .collect(Collectors.toList());
  }

  @Override
  public DiagEventSubscription getEventSubscription(UUID id) {
    Row row = session.execute(getDiagnosticEventPrepStmt.bind(id)).one();
    if (null != row) {
      return createDiagEventSubscription(row);
    }
    throw new IllegalArgumentException("No event subscription with id " + id);
  }

  @Override
  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    Preconditions.checkArgument(subscription.getId().isPresent());

    session.execute(saveDiagnosticEventPrepStmt.bind(
        subscription.getId().get(),
        subscription.getCluster(),
        subscription.getDescription(),
        subscription.getNodes(),
        subscription.getEvents(),
        subscription.getExportSse(),
        subscription.getExportFileLogger(),
        subscription.getExportHttpEndpoint()));

    return subscription;
  }

  @Override
  public boolean deleteEventSubscription(UUID id) {
    session.execute(deleteDiagnosticEventPrepStmt.bind(id));
    return true;
  }
}