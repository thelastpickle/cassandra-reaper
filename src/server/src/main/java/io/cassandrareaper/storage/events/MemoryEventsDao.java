/*
 * Copyright 2014-2017 Spotify AB
 * Copyright 2016-2019 The Last Pickle Ltd
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
import io.cassandrareaper.storage.MemoryStorageFacade;
import io.cassandrareaper.storage.sqlite.SqliteHelper;
import io.cassandrareaper.storage.sqlite.UuidUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryEventsDao implements IEventsDao {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryEventsDao.class);

  private final MemoryStorageFacade storage;
  private final Connection connection;

  private final PreparedStatement insertSubscriptionStmt;
  private final PreparedStatement getSubscriptionByIdStmt;
  private final PreparedStatement getAllSubscriptionsStmt;
  private final PreparedStatement getSubscriptionsByClusterStmt;
  private final PreparedStatement deleteSubscriptionStmt;

  public MemoryEventsDao(MemoryStorageFacade storage) {
    this.storage = storage;
    this.connection = storage.getSqliteConnection();

    try {
      this.insertSubscriptionStmt =
          connection.prepareStatement(
              "INSERT OR REPLACE INTO diag_event_subscription (id, cluster, description, nodes, "
                  + "events, export_sse, export_file_logger, export_http_endpoint) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
      this.getSubscriptionByIdStmt =
          connection.prepareStatement("SELECT * FROM diag_event_subscription WHERE id = ?");
      this.getAllSubscriptionsStmt =
          connection.prepareStatement("SELECT * FROM diag_event_subscription");
      this.getSubscriptionsByClusterStmt =
          connection.prepareStatement("SELECT * FROM diag_event_subscription WHERE cluster = ?");
      this.deleteSubscriptionStmt =
          connection.prepareStatement("DELETE FROM diag_event_subscription WHERE id = ?");
    } catch (SQLException e) {
      LOG.error("Failed to prepare statements for MemoryEventsDao", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions() {
    try (ResultSet rs = getAllSubscriptionsStmt.executeQuery()) {
      Collection<DiagEventSubscription> subscriptions = new ArrayList<>();
      while (rs.next()) {
        subscriptions.add(mapRowToSubscription(rs));
      }
      return subscriptions;
    } catch (SQLException e) {
      LOG.error("Failed to get all event subscriptions", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Collection<DiagEventSubscription> getEventSubscriptions(String clusterName) {
    Preconditions.checkNotNull(clusterName);
    try {
      getSubscriptionsByClusterStmt.setString(1, clusterName);
      try (ResultSet rs = getSubscriptionsByClusterStmt.executeQuery()) {
        Collection<DiagEventSubscription> subscriptions = new ArrayList<>();
        while (rs.next()) {
          subscriptions.add(mapRowToSubscription(rs));
        }
        return subscriptions;
      }
    } catch (SQLException e) {
      LOG.error("Failed to get event subscriptions for cluster {}", clusterName, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public DiagEventSubscription getEventSubscription(UUID id) {
    try {
      getSubscriptionByIdStmt.setBytes(1, UuidUtil.toBytes(id));
      try (ResultSet rs = getSubscriptionByIdStmt.executeQuery()) {
        if (rs.next()) {
          return mapRowToSubscription(rs);
        }
      }
    } catch (SQLException e) {
      LOG.error("Failed to get event subscription {}", id, e);
      throw new RuntimeException(e);
    }
    throw new IllegalArgumentException("No event subscription with id " + id);
  }

  @Override
  public DiagEventSubscription addEventSubscription(DiagEventSubscription subscription) {
    Preconditions.checkArgument(subscription.getId().isPresent());
    try {
      insertSubscription(subscription);
      return subscription;
    } catch (SQLException e) {
      LOG.error("Failed to add event subscription {}", subscription.getId().get(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean deleteEventSubscription(UUID id) {
    try {
      deleteSubscriptionStmt.setBytes(1, UuidUtil.toBytes(id));
      int deleted = deleteSubscriptionStmt.executeUpdate();
      return deleted > 0;
    } catch (SQLException e) {
      LOG.error("Failed to delete event subscription {}", id, e);
      throw new RuntimeException(e);
    }
  }

  private void insertSubscription(DiagEventSubscription subscription) throws SQLException {
    insertSubscriptionStmt.setBytes(1, UuidUtil.toBytes(subscription.getId().get()));
    insertSubscriptionStmt.setString(2, subscription.getCluster());
    insertSubscriptionStmt.setString(3, subscription.getDescription());
    insertSubscriptionStmt.setString(4, SqliteHelper.toJson(subscription.getNodes()));
    insertSubscriptionStmt.setString(5, SqliteHelper.toJson(subscription.getEvents()));
    insertSubscriptionStmt.setInt(6, subscription.getExportSse() ? 1 : 0);
    insertSubscriptionStmt.setString(7, subscription.getExportFileLogger());
    insertSubscriptionStmt.setString(8, subscription.getExportHttpEndpoint());
    insertSubscriptionStmt.executeUpdate();
  }

  private DiagEventSubscription mapRowToSubscription(ResultSet rs) throws SQLException {
    UUID id = UuidUtil.fromBytes(rs.getBytes("id"));
    String cluster = rs.getString("cluster");
    String description = rs.getString("description");
    Set<String> nodes = SqliteHelper.fromJsonStringCollection(rs.getString("nodes"), HashSet.class);
    Set<String> events =
        SqliteHelper.fromJsonStringCollection(rs.getString("events"), HashSet.class);
    boolean exportSse = rs.getInt("export_sse") == 1;
    String exportFileLogger = rs.getString("export_file_logger");
    String exportHttpEndpoint = rs.getString("export_http_endpoint");

    return new DiagEventSubscription(
        Optional.of(id),
        cluster,
        Optional.ofNullable(description),
        nodes,
        events,
        exportSse,
        exportFileLogger,
        exportHttpEndpoint);
  }
}
