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

### Metric Relabelling

Reaper doesn't do anything with the metrics. For practical purposes, it might be useful to relabel them. Here's an example of relabeling the metric tracking a repair progress.

There are actually two kinds of this metric:

```
#1 io_cassandrareaper_service_RepairRunner_repairProgress_[cluster]_7c326d904ba811eaa7a0634758da0ae9 0.0
#2 io_cassandrareaper_service_RepairRunner_repairProgress_[cluster]_[keyspace]_7c326d904ba811eaa7a0634758da0ae9 0.0
```

The difference is that #1 does not include the keyspace name. One way to handle this is to first drop #1 and then do relabeling only on #2.

To drop #1, we can use the following item in the `metric_relabel_configs` list:

```yaml
metric_relabel_configs:
  - source_labels: [__name__]
    regex: "io_cassandrareaper_service_RepairRunner_repairProgress_([^_]+)_([^_]+)$"
    action: drop 
```
* We pick `__name__` as the `source_label`, meaning we try to match the `regex` against the whole name of the metric. 
* We match for two groups after `repairProgress_` that are made of 1 or more characters that are not an `_`.

Then, we can add the following:

```yaml
  - source_labels: [__name__]
    regex: "io_cassandrareaper_service_RepairRunner_repairProgress_(.*)_(.*)_(.*)"
    target_label: cluster
    replacement: '${1}'
  - source_labels: [__name__]
    regex: "io_cassandrareaper_service_RepairRunner_repairProgress_(.*)_(.*)_(.*)"
    target_label: keyspace
    replacement: '${2}'
  - source_labels: [__name__]
    regex: "io_cassandrareaper_service_RepairRunner_repairProgress_(.*)_(.*)_(.*)"
    target_label: runid
    replacement: '${3}'
```
* Once again, we match against the whole metric name. 
* Each of the 3 items matches the same pattern that ends with three `_`-separated strings.
* However, each of the items adds a different label to the metric as specified by `target_label`.
* Finally, the `replacement` defines which matched group from `regex` to use as a value for the new label.

With this setup, a metric that looks like:

```
io_cassandrareaper_service_RepairRunner_repairProgress_testcluster_tlpstress_7c326d904ba811eaa7a0634758da0ae9
```

Will get new labels:
```
io_cassandrareaper_service_RepairRunner_repairProgress_testcluster_tlpstress_7c326d904ba811eaa7a0634758da0ae9{cluster="testcluster", keyspace="tlpstress", run_id="7c326d904ba811eaa7a0634758da0ae9"}
```

One last thing we might want to do is to modify the metric name itself. Not only we'll not have redundant information in our metrics, but we'll make querying for these metrics more convenient.

To rename the metric, we use the same relabeling config, but target the `__name__` itself:

```yaml
  - source_labels: [__name__]
    regex: "io_cassandrareaper_service_RepairRunner_repairProgress_.*"
    target_label: __name__
    replacement: "io_cassandrareaper_service_RepairRunner_repairProgress"
```

With this in place, our final metric will look like this:

```
io_cassandrareaper_service_RepairRunner_repairProgress{cluster="testcluster", keyspace="tlpstress", run_id="7c326d904ba811eaa7a0634758da0ae9"}
```

Now, when building dashboards to monitor Reaper, we can simply query for `io_cassandrareaper_service_RepairRunner_repairProgress` and use the labels fro grouping etc.