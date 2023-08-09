package io.cassandrareaper.management.http;

import io.cassandrareaper.core.GenericMetric;
import io.cassandrareaper.core.Node;
import io.cassandrareaper.management.MetricsProxy;

import javax.management.JMException;
import java.io.IOException;
import java.util.List;

public class HttpMetricsProxy implements MetricsProxy {

    private HttpCassandraManagementProxy proxy;

    private HttpMetricsProxy(HttpCassandraManagementProxy proxy) {
        this.proxy = proxy;
    }
    public static HttpMetricsProxy create(HttpCassandraManagementProxy proxy) {
        return new HttpMetricsProxy(proxy);
    }

    @Override
    public List<GenericMetric> collectTpStats(Node node) throws JMException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<GenericMetric> collectDroppedMessages(Node node) throws JMException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<GenericMetric> collectLatencyMetrics(Node node) throws JMException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<GenericMetric> collectGenericMetrics(Node node) throws JMException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<GenericMetric> collectPercentRepairedMetrics(Node node, String keyspaceName) throws JMException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }
}
