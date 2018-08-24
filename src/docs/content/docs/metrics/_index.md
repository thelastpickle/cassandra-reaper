+++
[menu.docs]
name = "Metrics"
weight = 20
identifier = "metrics"
+++

# Metrics Reporting

Reaper metrics are provided via the [Dropwizard Metrics](http://www.dropwizard.io/1.1.4/docs/manual/configuration.html#metrics) interface. The interface gives Reaper the ability to configure various metrics reporting systems. Metrics reporting can be configured in Reaper with the **metrics** property in the Reaper configuration YAML file. The **metrics** property has two fields; **frequency** and **reporters**. Specific metric reporters are defined in the **reporters** field as follows.

```yaml
metrics:
  frequency: 1 minute
  reporters:
    - type: <type>
```
