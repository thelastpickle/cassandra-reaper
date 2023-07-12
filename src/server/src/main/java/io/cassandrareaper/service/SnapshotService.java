/*
 * Copyright 2018-2019 The Last Pickle Ltd
 *
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

package io.cassandrareaper.service;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Cluster;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.core.Snapshot;
import io.cassandrareaper.core.Snapshot.Builder;
import io.cassandrareaper.jmx.ClusterFacade;
import io.cassandrareaper.storage.snapshot.ISnapshot;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.codahale.metrics.InstrumentedExecutorService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SnapshotService {
  public static final String SNAPSHOT_PREFIX = "reaper";

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotService.class);

  private final AppContext context;
  private final ClusterFacade clusterFacade;
  private final ExecutorService executor;
  private final Cache<String, Snapshot> cache = CacheBuilder.newBuilder().weakValues().maximumSize(1000).build();

  private final ISnapshot snapshotDao;

  private SnapshotService(AppContext context, ExecutorService executor,
                          Supplier<ClusterFacade> clusterFacadeSupplier,
                          ISnapshot snapshotDao) {
    this.context = context;
    this.clusterFacade = clusterFacadeSupplier.get();
    this.executor = new InstrumentedExecutorService(executor, context.metricRegistry);
    this.snapshotDao = snapshotDao;
  }

  @VisibleForTesting
  static SnapshotService create(
      AppContext context, ExecutorService executor,
      Supplier<ClusterFacade> clusterFacadeSupplier, ISnapshot snapshotDao) {
    return new SnapshotService(context, executor, clusterFacadeSupplier, snapshotDao);
  }

  public static SnapshotService create(AppContext context, ExecutorService executor, ISnapshot snapshotDao) {
    return new SnapshotService(context, executor, () -> ClusterFacade.create(context), snapshotDao);
  }

  public Pair<Node, String> takeSnapshot(String snapshotName, Node host, String... keyspaces) throws ReaperException {
    return clusterFacade.takeSnapshot(snapshotName, host, keyspaces);
  }

  Callable<Pair<Node, String>> takeSnapshotTask(String snapshotName, Node host, String... keyspace) {
    return () -> {
      return takeSnapshot(snapshotName, host, keyspace);
    };
  }

  public List<Pair<Node, String>> takeSnapshotClusterWide(
      String snapshotName,
      String clusterName,
      String owner,
      String cause,
      String... keyspace) throws ReaperException {

    try {
      List<Pair<Node, String>> snapshotResults = Lists.newArrayList();
      Cluster cluster = context.storage.getCluster(clusterName);

      Snapshot snapshot = Snapshot.builder()
              .withClusterName(clusterName)
              .withName(snapshotName)
              .withOwner(owner)
              .withCause(cause)
              .withCreationDate(DateTime.now())
              .build();

      snapshotDao.saveSnapshot(snapshot);
      LOG.info("Cluster : {} ; Cluster obj : {}", clusterName, cluster);
      List<String> liveNodes = clusterFacade.getLiveNodes(cluster);

      List<Callable<Pair<Node, String>>> snapshotTasks = liveNodes
              .stream()
              .map(host -> Node.builder().withCluster(cluster).withHostname(host).build())
              .map(node -> takeSnapshotTask(snapshotName, node, keyspace))
              .collect(Collectors.toList());

      List<Future<Pair<Node, String>>> futures = executor.invokeAll(snapshotTasks);
      for (Future<Pair<Node, String>> future : futures) {
        snapshotResults.add(future.get());
      }

      return snapshotResults;
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed taking snapshot for cluster {}", clusterName, e);
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
    return clusterFacade
        .listSnapshots(host)
        .stream()
        .map(snapshot -> enrichSnapshotWithMetadata(snapshot))
        .collect(Collectors.toList());
  }

  public Map<String, Map<String, List<Snapshot>>> listSnapshotsClusterWide(String clusterName) throws ReaperException {
    try {
      // Map with the snapshot name as key and a map of <host,
      Cluster cluster = context.storage.getCluster(clusterName);
      List<String> liveNodes = clusterFacade.getLiveNodes(cluster);

      List<Callable<List<Snapshot>>> listSnapshotTasks = liveNodes
              .stream()
              .map(host -> Node.builder().withCluster(cluster).withHostname(host).build())
              .map(node -> listSnapshotTask(node))
              .collect(Collectors.toList());

      List<Future<List<Snapshot>>> futures = executor.invokeAll(listSnapshotTasks);

      List<Snapshot> snapshots = Lists.newArrayList();
      for (Future<List<Snapshot>> future : futures) {
        snapshots.addAll(future.get());
      }

      Map<String, List<Snapshot>> snapshotsByName
          = snapshots.stream().collect(Collectors.groupingBy(Snapshot::getName, Collectors.toList()));

      Map<String, Map<String, List<Snapshot>>> snapshotsByNameAndHost = Maps.newHashMap();

      for (String snapshotName : snapshotsByName.keySet()) {
        Map<String, List<Snapshot>> snapshotsByHost = snapshotsByName
                .get(snapshotName)
                .stream()
                .collect(Collectors.groupingBy(Snapshot::getHost, Collectors.toList()));
        snapshotsByNameAndHost.put(snapshotName, snapshotsByHost);
      }

      return snapshotsByNameAndHost;
    } catch (UnsupportedOperationException unsupported) {
      throw unsupported;
    } catch (InterruptedException | ExecutionException e) {
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
    clusterFacade.clearSnapshot(snapshotName, host);
  }

  Callable<Node> clearSnapshotTask(String snapshotName, Node host) {
    return () -> {
      clearSnapshot(snapshotName, host);
      return host;
    };
  }

  public void clearSnapshotClusterWide(String snapshotName, String clusterName) throws ReaperException {
    try {
      Cluster cluster = context.storage.getCluster(clusterName);
      List<String> liveNodes = clusterFacade.getLiveNodes(cluster);

      List<Callable<Node>> clearSnapshotTasks = liveNodes
              .stream()
              .map(host -> Node.builder().withCluster(cluster).withHostname(host).build())
              .map(node -> clearSnapshotTask(snapshotName, node))
              .collect(Collectors.toList());

      List<Future<Node>> futures = executor.invokeAll(clearSnapshotTasks);
      for (Future<Node> future : futures) {
        future.get();
      }

      snapshotDao.deleteSnapshot(Snapshot.builder().withClusterName(clusterName).withName(snapshotName).build());
    } catch (ExecutionException e) {
      LOG.error("Failed clearing {} snapshot for cluster {}", snapshotName, clusterName, e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted clearing {} snapshot for cluster {}", snapshotName, clusterName, e);
      throw new ReaperException(e);
    }
  }

  public String formatSnapshotName(String snapshotName) {
    return snapshotName + "-" + LocalDateTime.now().format(FORMATTER);
  }

  private Snapshot enrichSnapshotWithMetadata(Snapshot snapshot) {
    Optional<Snapshot> snapshotMetadata = Optional.ofNullable(
        cache.getIfPresent(snapshot.getClusterName() + "-" + snapshot.getName()));

    if (!snapshotMetadata.isPresent()) {
      snapshotMetadata = Optional.ofNullable(
          snapshotDao.getSnapshot(snapshot.getClusterName(), snapshot.getName()));

      if (snapshotMetadata.isPresent()) {
        cache.put(snapshot.getClusterName() + "-" + snapshot.getName(), snapshotMetadata.get());
      }
    }

    Builder snapshotBuilder = Snapshot.builder()
            .withClusterName(snapshot.getClusterName())
            .withName(snapshot.getName())
            .withHost(snapshot.getHost())
            .withKeyspace(snapshot.getKeyspace())
            .withSizeOnDisk(snapshot.getSizeOnDisk())
            .withTrueSize(snapshot.getTrueSize())
            .withTable(snapshot.getTable());

    if (snapshotMetadata.isPresent()) {
      snapshotBuilder = snapshotBuilder
              .withCause(snapshotMetadata.get().getCause().orElse(""))
              .withOwner(snapshotMetadata.get().getOwner().orElse(""));
      if (snapshotMetadata.get().getCreationDate().isPresent()) {
        snapshotBuilder = snapshotBuilder
              .withCreationDate(snapshotMetadata.get().getCreationDate().get());
      }
    }

    return snapshotBuilder.build();
  }
}