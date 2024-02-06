/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.AppContext;
import io.cassandrareaper.ReaperApplicationConfiguration;
import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.HostConnectionCounters;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.IManagementConnectionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.Response;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.datastax.mgmtapi.client.api.DefaultApi;
import com.datastax.mgmtapi.client.invoker.ApiClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import okhttp3.OkHttpClient;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpManagementConnectionFactory implements IManagementConnectionFactory {
  private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();
  private static final Logger LOG = LoggerFactory.getLogger(HttpManagementConnectionFactory.class);
  private static final ConcurrentMap<String, HttpCassandraManagementProxy> HTTP_CONNECTIONS = Maps.newConcurrentMap();
  private final MetricRegistry metricRegistry;
  private final HostConnectionCounters hostConnectionCounters;

  private final ScheduledExecutorService jobStatusPollerExecutor;
  private ReaperApplicationConfiguration config;

  private final Set<String> accessibleDatacenters = Sets.newHashSet();

  // Constructor for HttpManagementConnectionFactory
  public HttpManagementConnectionFactory(AppContext context, ScheduledExecutorService jobStatusPollerExecutor) {
    this.metricRegistry
        = context.metricRegistry == null ? new MetricRegistry() : context.metricRegistry;
    hostConnectionCounters = new HostConnectionCounters(metricRegistry);
    this.config = context.config;
    registerConnectionsGauge();
    this.jobStatusPollerExecutor = jobStatusPollerExecutor;
    if (context.config.getHttpManagement().getKeystore() != null && !context.config.getHttpManagement().getKeystore()
        .isEmpty()) {
      try {
        createSslWatcher();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public ICassandraManagementProxy connectAny(Collection<Node> nodes) throws ReaperException {
    Preconditions.checkArgument(
        null != nodes && !nodes.isEmpty(), "no hosts provided to connectAny");
    List<Node> nodeList = new ArrayList<>(nodes);
    Collections.shuffle(nodeList);
    for (int i = 0; i < 2; i++) {
      for (Node node : nodeList) {
        // First loop, we try the most accessible nodes, then second loop we try all nodes
        if (getHostConnectionCounters().getSuccessfulConnections(node.getHostname()) >= 0 || 1 == i) {
          try {
            LOG.debug("Trying to connect to node {} with {} successful connections with i = {}",
                node.getHostname(), getHostConnectionCounters().getSuccessfulConnections(node.getHostname()), i);
            ICassandraManagementProxy cassandraManagementProxy = connectImpl(node);
            getHostConnectionCounters().incrementSuccessfulConnections(node.getHostname());
            if (getHostConnectionCounters().getSuccessfulConnections(node.getHostname()) > 0) {
              accessibleDatacenters.add(
                  cassandraManagementProxy.getDatacenter(cassandraManagementProxy.getUntranslatedHost()));
            }
            return cassandraManagementProxy;
          } catch (ReaperException | RuntimeException | UnknownHostException e) {
            getHostConnectionCounters().decrementSuccessfulConnections(node.getHostname());
            LOG.info("Unreachable host: {}", node.getHostname(), e);
          } catch (InterruptedException expected) {
            LOG.trace("Expected exception", expected);
          }
        }
      }
    }
    throw new ReaperException("no host could be reached through HTTP");
  }

  @Override
  public HostConnectionCounters getHostConnectionCounters() {
    return hostConnectionCounters;
  }

  private void registerConnectionsGauge() {
    try {
      if (!this.metricRegistry
          .getGauges()
          .containsKey(MetricRegistry.name(HttpManagementConnectionFactory.class, "openHttpManagementConnections"))) {
        this.metricRegistry.register(
            MetricRegistry.name(HttpManagementConnectionFactory.class, "openHttpManagementConnections"),
            (Gauge<Integer>) HTTP_CONNECTIONS::size);
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot create openHttoManagementConnections metric gauge", e);
    }
  }

  private HttpCassandraManagementProxy connectImpl(Node node)
      throws ReaperException, InterruptedException {
    Response pidResponse = getPid(node);
    if (pidResponse.getStatus() != 200) {
      throw new ReaperException("Could not get PID for node " + node.getHostname());
    }
    Integer managementPort = config.getHttpManagement().getManagementApiPort();
    String rootPath = ""; // TODO - get this from the config.

    LOG.trace("Wanting to create new connection to " + node.getHostname());
    return HTTP_CONNECTIONS.computeIfAbsent(node.getHostname(), new Function<String, HttpCassandraManagementProxy>() {
      @Override
      public HttpCassandraManagementProxy apply(@Nullable String hostName) {
        ReaperApplicationConfiguration.HttpManagement httpConfig = config.getHttpManagement();
        boolean useMtls = httpConfig.getKeystore() != null && !httpConfig.getKeystore().isEmpty();

        OkHttpClient.Builder clientBuilder = new OkHttpClient().newBuilder();

        String protocol = "http";
        if (useMtls) {
          LOG.debug("Using TLS connection to " + node.getHostname());
          // We have to split TrustManagers to its own function to please OkHttpClient
          TrustManager[] trustManagers;
          SSLContext sslContext;
          try {
            trustManagers = getTrustManagers();
            sslContext = createSslContext(trustManagers);
          } catch (ReaperException e) {
            LOG.error("Failed to create SSLContext: " + e.getLocalizedMessage(), e);
            throw new RuntimeException(e);
          }
          clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustManagers[0]);
          clientBuilder.hostnameVerifier((hostname, session) -> true); // We don't want subjectAltNames verification
          protocol = "https";
        }

        OkHttpClient okHttpClient = clientBuilder
            .build();

        ApiClient apiClient = new ApiClient().setBasePath(
                protocol + "://" + node.getHostname() + ":" + managementPort + rootPath)
            .setHttpClient(okHttpClient);

        DefaultApi mgmtApiClient = new DefaultApi(apiClient);

        InstrumentedScheduledExecutorService statusTracker = new InstrumentedScheduledExecutorService(
            jobStatusPollerExecutor, metricRegistry);

        return new HttpCassandraManagementProxy(
            metricRegistry,
            rootPath,
            new InetSocketAddress(node.getHostname(), managementPort),
            statusTracker,
            mgmtApiClient,
            config.getHttpManagement().getMgmtApiMetricsPort(),
            node
        );
      }
    });
  }

  @VisibleForTesting
  SSLContext createSslContext(TrustManager[] tms) throws ReaperException {
    Path keyStorePath = Paths.get(config.getHttpManagement().getKeystore());

    try (InputStream ksIs = Files.newInputStream(keyStorePath, StandardOpenOption.READ)) {

      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(ksIs, KEYSTORE_PASSWORD);

      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, KEYSTORE_PASSWORD);

      SSLContext sslCtx = SSLContext.getInstance("TLS");
      sslCtx.init(kmf.getKeyManagers(), tms, null);
      return sslCtx;
    } catch (CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException
             | UnrecoverableKeyException | IOException e) {
      throw new ReaperException(e);
    }
  }

  private TrustManager[] getTrustManagers() throws ReaperException {
    Path trustStorePath = Paths.get(config.getHttpManagement().getTruststore());
    try (InputStream tsIs = Files.newInputStream(trustStorePath, StandardOpenOption.READ)) {
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(tsIs, KEYSTORE_PASSWORD);

      TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);

      return tmf.getTrustManagers();
    } catch (IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException e) {
      throw new ReaperException(e);
    }
  }

  @VisibleForTesting
  void createSslWatcher() throws IOException {
    WatchService watchService = FileSystems.getDefault().newWatchService();
    Path trustStorePath = Paths.get(config.getHttpManagement().getTruststore());
    Path keyStorePath = Paths.get(config.getHttpManagement().getKeystore());
    Path keystoreParent = trustStorePath.getParent();
    Path trustStoreParent = keyStorePath.getParent();

    keystoreParent.register(
        watchService,
        StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_DELETE,
        StandardWatchEventKinds.ENTRY_MODIFY);

    if (!keystoreParent.equals(trustStoreParent)) {
      trustStoreParent.register(
          watchService,
          StandardWatchEventKinds.ENTRY_CREATE,
          StandardWatchEventKinds.ENTRY_DELETE,
          StandardWatchEventKinds.ENTRY_MODIFY);
    }

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(
        () -> {
          while (true) {
            try {
              WatchKey key = watchService.take();
              List<WatchEvent<?>> events = key.pollEvents();
              boolean reloadNeeded = false;
              for (WatchEvent<?> event : events) {
                WatchEvent.Kind<?> kind = event.kind();

                WatchEvent<java.nio.file.Path> ev = (WatchEvent<Path>) event;
                Path eventFilename = ev.context();

                if (keystoreParent.resolve(eventFilename).equals(keyStorePath)
                    || trustStoreParent.resolve(eventFilename).equals(trustStorePath)) {
                  // Something in the TLS has been modified.. recreate HTTP connections
                  reloadNeeded = true;
                }
              }
              if (!key.reset()) {
                // The watched directories have disappeared..
                break;
              }
              if (reloadNeeded) {
                LOG.info("Detected change in the SSL/TLS certificates, reloading.");
                clearHttpConnections();
              }
            } catch (InterruptedException e) {
              LOG.error("Filesystem watcher received InterruptedException", e);
            }
          }
        });
  }

  private void clearHttpConnections() {
    // Clearing this causes the connectImpl() to recreate new SSLContext
    HTTP_CONNECTIONS.clear();
  }

  private Response getPid(Node node) {
    //TODO - implement me.
    return Response.ok().build();
  }

  @Override
  public final Set<String> getAccessibleDatacenters() {
    return accessibleDatacenters;
  }
}
