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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.Snapshot.Builder;
import io.cassandrareaper.jmx.JmxProxy;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SnapshotManager {
  public static final String SNAPSHOT_PREFIX = "reaper";
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);
  private final AppContext context;
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
  private final ExecutorService executor = Executors.newFixedThreadPool(5);
  private final Cache<String, Snapshot> cache =
      CacheBuilder.newBuilder().weakValues().maximumSize(1000).build();

  private SnapshotManager(AppContext context) {
    this.context = context;
  }

  public static SnapshotManager create(AppContext context) {
    return new SnapshotManager(context);
  }

  public Pair<Node, String> takeSnapshot(String snapshotName, Node host, String... keyspace)
      throws ReaperException {
    try {
      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connect(
              host, context.config.getJmxConnectionTimeoutInSeconds());

      LOG.info("Taking snapshot for node {} and keyspace {}", host, keyspace);
      return Pair.of(host, jmxProxy.takeSnapshot(snapshotName, keyspace));
    } catch (RuntimeException | InterruptedException e) {
      LOG.error("Failed taking snapshot for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  Callable<Pair<Node, String>> takeSnapshotTask(
      String snapshotName, Node host, String... keyspace) {
    return () -> {
      return takeSnapshot(snapshotName, host, keyspace);
    };
  }

  public List<Pair<Node, String>> takeSnapshotClusterWide(
      String snapshotName, String clusterName, String owner, String cause, String... keyspace)
      throws ReaperException {
    try {
      List<Pair<Node, String>> snapshotResults = Lists.newArrayList();
      Optional<Cluster> cluster = context.storage.getCluster(clusterName);
      Snapshot snapshot =
          Snapshot.builder()
              .withClusterName(clusterName)
              .withName(snapshotName)
              .withOwner(owner)
              .withCause(cause)
              .withCreationDate(DateTime.now())
              .build();

      context.storage.saveSnapshot(snapshot);

      LOG.info("Cluster : {}", clusterName);
      LOG.info("Cluster obj : {}", cluster.get());

      Preconditions.checkArgument(cluster.isPresent());

      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connectAny(
              cluster.get(), context.config.getJmxConnectionTimeoutInSeconds());

      List<String> liveNodes = jmxProxy.getLiveNodes();
      List<Callable<Pair<Node, String>>> snapshotTasks =
          liveNodes
              .stream()
              .map(host -> Node.builder().withClusterName(clusterName).withHostname(host).build())
              .map(node -> takeSnapshotTask(snapshotName, node, keyspace))
              .collect(Collectors.toList());

      List<Future<Pair<Node, String>>> futures = executor.invokeAll(snapshotTasks);
      for (Future<Pair<Node, String>> future : futures) {
        snapshotResults.add(future.get());
      }

      return snapshotResults;
    } catch (RuntimeException | InterruptedException | ExecutionException e) {
      LOG.error("Failed taking snapshot for cluster {}", clusterName, e);
      throw new ReaperException(e);
    }
  }

  public Pair<Node, String> takeSnapshotForKeyspaces(
      String snapshotName, Node host, String... keyspaces) throws ReaperException {
    try {
      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connect(
              host, context.config.getJmxConnectionTimeoutInSeconds());

      return Pair.of(host, jmxProxy.takeSnapshot(snapshotName, keyspaces));

    } catch (RuntimeException | InterruptedException e) {
      LOG.error("Failed taking snapshot for host {} and keyspaces {}", host, keyspaces, e);
      throw new ReaperException(e);
    }
  }

  public Map<String, List<Snapshot>> listSnapshotsGroupedByName(Node host) throws ReaperException {
    try {
      List<Snapshot> snapshots = listSnapshots(host);

      return snapshots
          .stream()
          .collect(Collectors.groupingBy(Snapshot::getName, Collectors.toList()));

    } catch (RuntimeException e) {
      LOG.error("Failed taking snapshot for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  public List<Snapshot> listSnapshots(Node host) throws ReaperException {
    try {
      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connect(
              host, context.config.getJmxConnectionTimeoutInSeconds());

      return jmxProxy
          .listSnapshots()
          .stream()
          .map(snapshot -> enrichSnapshotWithMetadata(snapshot))
          .collect(Collectors.toList());
    } catch (UnsupportedOperationException e1) {
      LOG.debug("Listing snapshot is unsupported with Cassandra 2.0 and prior");
      throw e1;
    } catch (RuntimeException | InterruptedException e) {
      LOG.error("Failed listing snapshots for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  public Map<String, Map<String, List<Snapshot>>> listSnapshotsClusterWide(String clusterName)
      throws ReaperException {
    try {
      // Map with the snapshot name as key and a map of <host,
      Optional<Cluster> cluster = context.storage.getCluster(clusterName);

      Preconditions.checkArgument(cluster.isPresent());

      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connectAny(
              cluster.get(), context.config.getJmxConnectionTimeoutInSeconds());

      List<String> liveNodes = jmxProxy.getLiveNodes();
      List<Callable<List<Snapshot>>> listSnapshotTasks =
          liveNodes
              .stream()
              .map(host -> Node.builder().withClusterName(clusterName).withHostname(host).build())
              .map(node -> listSnapshotTask(node))
              .collect(Collectors.toList());

      List<Future<List<Snapshot>>> futures = executor.invokeAll(listSnapshotTasks);

      List<Snapshot> snapshots = Lists.newArrayList();
      for (Future<List<Snapshot>> future : futures) {
        snapshots.addAll(future.get());
      }

      Map<String, List<Snapshot>> snapshotsByName =
          snapshots.stream().collect(Collectors.groupingBy(Snapshot::getName, Collectors.toList()));

      Map<String, Map<String, List<Snapshot>>> snapshotsByNameAndHost = Maps.newHashMap();

      for (String snapshotName : snapshotsByName.keySet()) {
        Map<String, List<Snapshot>> snapshotsByHost =
            snapshotsByName
                .get(snapshotName)
                .stream()
                .collect(Collectors.groupingBy(Snapshot::getHost, Collectors.toList()));
        snapshotsByNameAndHost.put(snapshotName, snapshotsByHost);
      }

      return snapshotsByNameAndHost;
    } catch (UnsupportedOperationException e1) {
      throw e1;
    } catch (RuntimeException | InterruptedException | ExecutionException e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        throw new UnsupportedOperationException(e.getCause());
      }
      LOG.error("Failed Listing snapshot for cluster {}", clusterName, e);
      throw new ReaperException(e);
    }
  }

  Callable<List<Snapshot>> listSnapshotTask(Node host) {
    return () -> {
      return listSnapshots(host);
    };
  }

  public void clearSnapshot(String snapshotName, Node host) throws ReaperException {
    try {
      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connect(
              host, context.config.getJmxConnectionTimeoutInSeconds());

      jmxProxy.clearSnapshot(snapshotName);
    } catch (RuntimeException | InterruptedException e) {
      LOG.error("Failed taking snapshot for host {}", host, e);
      throw new ReaperException(e);
    }
  }

  Callable<Node> clearSnapshotTask(String snapshotName, Node host) {
    return () -> {
      clearSnapshot(snapshotName, host);
      return host;
    };
  }

  public void clearSnapshotClusterWide(String snapshotName, String clusterName)
      throws ReaperException {
    try {
      Optional<Cluster> cluster = context.storage.getCluster(clusterName);

      Preconditions.checkArgument(cluster.isPresent());

      JmxProxy jmxProxy =
          context.jmxConnectionFactory.connectAny(
              cluster.get(), context.config.getJmxConnectionTimeoutInSeconds());

      List<String> liveNodes = jmxProxy.getLiveNodes();
      List<Callable<Node>> clearSnapshotTasks =
          liveNodes
              .stream()
              .map(
                  host ->
                      Node.builder()
                          .withClusterName(cluster.get().getName())
                          .withHostname(host)
                          .build())
              .map(node -> clearSnapshotTask(snapshotName, node))
              .collect(Collectors.toList());

      List<Future<Node>> futures = executor.invokeAll(clearSnapshotTasks);
      for (Future<Node> future : futures) {
        future.get();
      }

      context.storage.deleteSnapshot(
          Snapshot.builder().withClusterName(clusterName).withName(snapshotName).build());
    } catch (RuntimeException | InterruptedException | ExecutionException e) {
      LOG.error("Failed taking snapshot for cluster {}", clusterName, e);
      throw new ReaperException(e);
    }
  }

  public String formatSnapshotName(String snapshotName) {
    return snapshotName + "-" + LocalDateTime.now().format(formatter);
  }

  private Snapshot enrichSnapshotWithMetadata(Snapshot snapshot) {
    Optional<Snapshot> snapshotMetadata =
        Optional.fromNullable(
            cache.getIfPresent(snapshot.getClusterName() + "-" + snapshot.getName()));
    if (!snapshotMetadata.isPresent()) {
      snapshotMetadata =
          Optional.fromNullable(
              context.storage.getSnapshot(snapshot.getClusterName(), snapshot.getName()));
      if (snapshotMetadata.isPresent()) {
        cache.put(snapshot.getClusterName() + "-" + snapshot.getName(), snapshotMetadata.get());
      }
    }

    Builder snapshotBuilder =
        Snapshot.builder()
            .withClusterName(snapshot.getClusterName())
            .withName(snapshot.getName())
            .withHost(snapshot.getHost())
            .withKeyspace(snapshot.getKeyspace())
            .withSizeOnDisk(snapshot.getSizeOnDisk())
            .withTrueSize(snapshot.getTrueSize())
            .withTable(snapshot.getTable());

    if (snapshotMetadata.isPresent()) {
      snapshotBuilder =
          snapshotBuilder
              .withCause(snapshotMetadata.get().getCause().or(""))
              .withOwner(snapshotMetadata.get().getOwner().or(""))
              .withCreationDate(snapshotMetadata.get().getCreationDate().orNull());
    }

    return snapshotBuilder.build();
  }
}
