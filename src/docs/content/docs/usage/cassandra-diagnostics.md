+++
[menu.docs]
name = "Cassandra Diagnostic Events"
weight = 100
identifier = "diagnostics"
parent = "usage"
+++

# Monitoring Cassandra Diagnostic Events

Reaper has the ability to listen and display live Cassandra's emitted Diagnostic Events.

In Cassandra 4.0 internal system "diagnostic events" have become available, via the work done in [CASSANDRA-12944](https://issues.apache.org/jira/browse/CASSANDRA-12944). These allow to observe internal Cassandra events, for example in unit tests and with external tools. These diagnostic events provide operational monitoring and troubleshooting beyond logs and metrics.

## Enabling Diagnostic Events server-side in Apache Cassandra 4.0

Available from Apache Cassandra version 4.0, Diagnostic Events are not enabled (published) by default.

To enable the publishing of diagnostic events, on the Cassandra node enable the `diagnostic_events_enabled` flag.

```yaml
# Diagnostic Events #
# If enabled, diagnostic events can be helpful for troubleshooting operational issues. Emitted events contain details
# on internal state and temporal relationships across events, accessible by clients via JMX.
diagnostic_events_enabled: true
```

Restarting the node is required after this change.

## Reaper

In Reaper go to the "Live Diagnostics" page.

Select the cluster that is running Cassandra version 4.0 with `diagnostic_events_enabled: true`, using the "Filter cluster" field.

Expand the "Add Events Subscription" section.

Type in a description for the subscription to be created. Select the node you want to observe diagnostic events to. Select the diagnostic events you want to observe. Check "Enable Live View".

Press "Save". In the list of subscriptions there should now be a row with what was entered.

Press "View". A green bar will appear. Underneath this green bar will appear diagnostic events of the types and from the nodes subscribed to. Press the green bar to stop displaying live events.

