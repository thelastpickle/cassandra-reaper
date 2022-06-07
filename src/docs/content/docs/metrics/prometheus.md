+++
[menu.docs]
name = "Prometheus"
weight = 1
identifier = "prometheus"
parent = "metrics"
+++

## Prometheus

Reaper exposes all its metrics in a Prometheus-ready format under the `/prometheusMetrics` endpoint on the admin port.

It's fairly straightforward to configure Prometheus to grab them. The config can look something like:

```yaml
scrape_configs:
  - job_name: 'reaper'
    metrics_path: '/prometheusMetrics'
    scrape_interval: 5s
    static_configs:
      - targets: ['host.docker.internal:8081']
```

* The `host.docker.internal` tells a Prometheus instance running inside a docker container to connect to the host's `8081` port where Raper runs from a JAR.
