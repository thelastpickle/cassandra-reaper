+++
title = "Graphite Reporter"
menuTitle = "Graphite Reporter"
weight = 1
identifier = "graphite"
parent = "metrics"
+++


Reaper can be configured to periodically report metrics to a Graphite host. This can be done using the following properties in the Reaper configuration YAML file.

```yaml
Metrics:
  frequency: 1 minute
  reporters:
    - type: graphite
      host: <host_address>
      port: <port_number>
      prefix: <prefix>
```

Where:

* `host_address` is hostname of the Graphite server to report to.
* `port_number` is port of the Graphite server to report to.
* `prefix` is prefix for Metric key names that are reported to the Graphite server. Typically this will be the hostname sending the metrics to the Graphite server.
