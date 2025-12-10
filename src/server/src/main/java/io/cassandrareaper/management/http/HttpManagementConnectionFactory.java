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
import jakarta.ws.rs.core.Response;
import okhttp3.OkHttpClient;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpManagementConnectionFactory implements IManagementConnectionFactory {
  private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

  private static final String KEYSTORE_COMPONENT_NAME = "keystore.jks";

  private static final String TRUSTSTORE_COMPONENT_NAME = "truststore.jks";

  private static final Logger LOG = LoggerFactory.getLogger(HttpManagementConnectionFactory.class);
  private static final ConcurrentMap<String, HttpCassandraManagementProxy> HTTP_CONNECTIONS =
      Maps.newConcurrentMap();
  private final MetricRegistry metricRegistry;
  private final HostConnectionCounters hostConnectionCounters;

  private final ScheduledExecutorService jobStatusPollerExecutor;
  private ReaperApplicationConfiguration config;

  private final Set<String> accessibleDatacenters = Sets.newHashSet();

  // Constructor for HttpManagementConnectionFactory
  public HttpManagementConnectionFactory(
      AppContext context, ScheduledExecutorService jobStatusPollerExecutor) {
    this.metricRegistry =
        context.metricRegistry == null ? new MetricRegistry() : context.metricRegistry;
    hostConnectionCounters = new HostConnectionCounters(metricRegistry);
    this.config = context.config;
    registerConnectionsGauge();
    this.jobStatusPollerExecutor = jobStatusPollerExecutor;

    String ts = context.config.getHttpManagement().getTruststore();
    boolean watchTruststore = ts != null && !ts.isEmpty();
    String ks = context.config.getHttpManagement().getKeystore();
    boolean watchKeystore = ks != null && !ks.isEmpty();
    String tsd = context.config.getHttpManagement().getTruststoresDir();
    boolean watchTruststoreDir = tsd != null && !tsd.isEmpty() && Files.isDirectory(Paths.get(tsd));

    try {
      if (watchKeystore || watchTruststore || watchTruststoreDir) {
        createSslWatcher(watchTruststore, watchKeystore, watchTruststoreDir);
      } else {
        LOG.debug("Not setting up any SSL watchers");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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
        if (getHostConnectionCounters().getSuccessfulConnections(node.getHostname()) >= 0
            || 1 == i) {
          try {
            LOG.debug(
                "Trying to connect to node {} with {} successful connections with i = {}",
                node.getHostname(),
                getHostConnectionCounters().getSuccessfulConnections(node.getHostname()),
                i);
            ICassandraManagementProxy cassandraManagementProxy = connectImpl(node);
            getHostConnectionCounters().incrementSuccessfulConnections(node.getHostname());
            if (getHostConnectionCounters().getSuccessfulConnections(node.getHostname()) > 0) {
              accessibleDatacenters.add(
                  cassandraManagementProxy.getDatacenter(
                      cassandraManagementProxy.getUntranslatedHost()));
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
          .containsKey(
              MetricRegistry.name(
                  HttpManagementConnectionFactory.class, "openHttpManagementConnections"))) {
        this.metricRegistry.register(
            MetricRegistry.name(
                HttpManagementConnectionFactory.class, "openHttpManagementConnections"),
            (Gauge<Integer>) HTTP_CONNECTIONS::size);
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Cannot create openHttpManagementConnections metric gauge", e);
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
    return HTTP_CONNECTIONS.computeIfAbsent(
        node.getHostname(),
        new Function<String, HttpCassandraManagementProxy>() {
          @Override
          public HttpCassandraManagementProxy apply(@Nullable String hostName) {
            ReaperApplicationConfiguration.HttpManagement httpConfig = config.getHttpManagement();

            boolean useMtls =
                (httpConfig.getKeystore() != null && !httpConfig.getKeystore().isEmpty())
                    || (httpConfig.getTruststoresDir() != null
                        && !httpConfig.getTruststoresDir().isEmpty());

            OkHttpClient.Builder clientBuilder = new OkHttpClient().newBuilder();

            String protocol = "http";

            if (useMtls) {

              Path truststoreName = getTruststoreComponentPath(node, TRUSTSTORE_COMPONENT_NAME);
              Path keystoreName = getTruststoreComponentPath(node, KEYSTORE_COMPONENT_NAME);

              LOG.debug("Using TLS connection to " + node.getHostname());
              // We have to split TrustManagers to its own function to please OkHttpClient
              TrustManager[] trustManagers;
              SSLContext sslContext;
              try {
                trustManagers = getTrustManagers(truststoreName);
                sslContext = createSslContext(trustManagers, keystoreName);
              } catch (ReaperException e) {
                LOG.error("Failed to create SSLContext: " + e.getLocalizedMessage(), e);
                throw new RuntimeException(e);
              }
              clientBuilder.sslSocketFactory(
                  sslContext.getSocketFactory(), (X509TrustManager) trustManagers[0]);
              clientBuilder.hostnameVerifier(
                  (hostname, session) -> true); // We don't want subjectAltNames verification
              protocol = "https";
            }

            OkHttpClient okHttpClient = clientBuilder.build();

            ApiClient apiClient =
                new ApiClient()
                    .setBasePath(
                        protocol + "://" + node.getHostname() + ":" + managementPort + rootPath)
                    .setHttpClient(okHttpClient);

            DefaultApi mgmtApiClient = new DefaultApi(apiClient);

            InstrumentedScheduledExecutorService statusTracker =
                new InstrumentedScheduledExecutorService(jobStatusPollerExecutor, metricRegistry);

            OkHttpClient.Builder metricsClientBuilder = new OkHttpClient().newBuilder();
            if (httpConfig.getMetricsTLSEnabled()) {
              // Pass the clientBuilder to the HttpCassandraManagementProxy
              LOG.debug("Using metrics TLS connection to " + node.getHostname());
              metricsClientBuilder = clientBuilder;
            }

            return new HttpCassandraManagementProxy(
                metricRegistry,
                rootPath,
                new InetSocketAddress(node.getHostname(), managementPort),
                statusTracker,
                mgmtApiClient,
                config.getHttpManagement().getMgmtApiMetricsPort(),
                node,
                metricsClientBuilder);
          }
        });
  }

  @VisibleForTesting
  SSLContext createSslContext(TrustManager[] tms, Path keyStorePath) throws ReaperException {

    try (InputStream ksIs = Files.newInputStream(keyStorePath, StandardOpenOption.READ)) {

      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(ksIs, KEYSTORE_PASSWORD);

      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, KEYSTORE_PASSWORD);

      SSLContext sslCtx = SSLContext.getInstance("TLS");
      sslCtx.init(kmf.getKeyManagers(), tms, null);
      return sslCtx;
    } catch (CertificateException
        | NoSuchAlgorithmException
        | KeyStoreException
        | KeyManagementException
        | UnrecoverableKeyException
        | IOException e) {
      throw new ReaperException(e);
    }
  }

  @VisibleForTesting
  TrustManager[] getTrustManagers(Path trustStorePath) throws ReaperException {

    LOG.trace(String.format("Calling getSingleTrustManager with %s", trustStorePath));

    try (InputStream tsIs = Files.newInputStream(trustStorePath, StandardOpenOption.READ)) {
      KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(tsIs, KEYSTORE_PASSWORD);

      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);

      return tmf.getTrustManagers();
    } catch (IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException e) {
      throw new ReaperException("Error loading trust managers");
    }
  }

  @VisibleForTesting
  Path getTruststoreComponentPath(Node node, String truststoreComponentName) {
    Path trustStorePath;

    String clusterName = node.getClusterName();
    // the cluster name is not available, or we don't have the per-cluster truststores
    // we fall back to the global trust stores
    if (clusterName.equals("") || config.getHttpManagement().getTruststoresDir() == null) {

      trustStorePath =
          truststoreComponentName.equals(TRUSTSTORE_COMPONENT_NAME)
              ? Paths.get(config.getHttpManagement().getTruststore()).toAbsolutePath()
              : Paths.get(config.getHttpManagement().getKeystore()).toAbsolutePath();
    } else {
      // load a cluster-specific trust store otherwise
      Path storesRootPath = Paths.get(config.getHttpManagement().getTruststoresDir());
      trustStorePath =
          storesRootPath
              .resolve(String.format("%s-%s", clusterName, truststoreComponentName))
              .toAbsolutePath();
    }

    return trustStorePath;
  }

  @VisibleForTesting
  void createSslWatcher(boolean watchTruststore, boolean watchKeystore, boolean watchTruststoreDir)
      throws IOException {

    WatchService watchService = FileSystems.getDefault().newWatchService();

    Path trustStorePath =
        watchTruststore ? Paths.get(config.getHttpManagement().getTruststore()) : null;
    Path keyStorePath = watchKeystore ? Paths.get(config.getHttpManagement().getKeystore()) : null;
    Path truststoreDirPath =
        watchTruststoreDir ? Paths.get(config.getHttpManagement().getTruststoresDir()) : null;

    if (watchKeystore) {
      keyStorePath
          .getParent()
          .register(
              watchService,
              StandardWatchEventKinds.ENTRY_CREATE,
              StandardWatchEventKinds.ENTRY_DELETE,
              StandardWatchEventKinds.ENTRY_MODIFY);
    }
    if (watchTruststore && watchKeystore) {
      if (!trustStorePath.getParent().equals(keyStorePath.getParent())) {
        trustStorePath
            .getParent()
            .register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY);
      }
    }
    if (watchTruststoreDir) {
      truststoreDirPath.register(
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
              clearHttpConnections();
              WatchKey key = watchService.take();
              List<WatchEvent<?>> events = key.pollEvents();
              boolean reloadNeeded = false;
              for (WatchEvent<?> event : events) {
                WatchEvent.Kind<?> kind = event.kind();

                WatchEvent<java.nio.file.Path> ev = (WatchEvent<Path>) event;
                Path eventFilename = ev.context();

                if (watchKeystore) {
                  if (keyStorePath.getParent().resolve(eventFilename).equals(keyStorePath)) {
                    reloadNeeded = true;
                  }
                }
                if (watchTruststore) {
                  if (trustStorePath.getParent().resolve(eventFilename).equals(trustStorePath)) {
                    // Something in the TLS has been modified.. recreate HTTP connections
                    reloadNeeded = true;
                  }
                }
                if (watchTruststoreDir) {
                  if (eventFilename.toString().endsWith(".jks")) {
                    reloadNeeded = true;
                  }
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

  @VisibleForTesting
  void clearHttpConnections() {
    // Clearing this causes the connectImpl() to recreate new SSLContext
    HTTP_CONNECTIONS.clear();
  }

  private Response getPid(Node node) {
    // TODO - implement me.
    return Response.ok().build();
  }

  @Override
  public final Set<String> getAccessibleDatacenters() {
    return accessibleDatacenters;
  }
}
