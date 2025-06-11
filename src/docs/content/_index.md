---
title: "Reaper for Apache Cassandra"
type: docs
---

# Reaper: Easy Repair Management for Apache Cassandra

Reaper is a centralized, stateful, and highly configurable tool for running Apache Cassandra repairs for multi-site clusters.

## Why Use Reaper?

Instead of manually managing crontabs and nodetool commands, Reaper provides:

- **Centralized Management**: One place to manage repairs across all your Cassandra clusters
- **Intelligent Coordination**: Prevents overlapping repairs that could impact cluster performance  
- **Fault Tolerance**: Built-in retry logic and state management for reliable repair execution
- **Granular Control**: Schedule repairs at keyspace, table, or even token range level
- **Multi-Site Awareness**: Coordinate repairs across datacenters intelligently

## Key Features

- **🔧 Intelligent Scheduling**: Automatically schedules repairs to avoid putting too much load on your cluster
- **📊 Web Interface**: Simple and intuitive web UI to schedule repairs as granularly as needed
- **💾 Multi-Backend Support**: Store Reaper's data in Cassandra itself for fault tolerance
- **🌍 Multi-Datacenter**: Built for multi-site clusters with cross-datacenter repair coordination
- **📈 Monitoring & Metrics**: Comprehensive metrics and monitoring integration
- **🔌 REST API**: Full REST API for programmatic control and integration

## Quick Start

### Using Docker
```bash
docker run -p 8080:8080 thelastpickle/cassandra-reaper:latest
```

### Download JAR
1. Download the latest JAR from the [releases page](docs/download/)
2. Create a configuration file
3. Run: `java -jar cassandra-reaper-*.jar server reaper.yaml`

### Build from Source
1. Clone the repository: `git clone https://github.com/thelastpickle/cassandra-reaper.git`
2. Build: `mvn clean package`
3. Run: `java -jar target/cassandra-reaper-*.jar server src/main/resources/cassandra-reaper.yaml`

## Documentation

Explore the documentation to learn how to:

- [Install and configure](docs/) Reaper in your environment  
- [Use the web interface](docs/usage/) to manage repairs
- [Configure backends](docs/backends/) for storing Reaper's data
- [Set up monitoring](docs/metrics/) and metrics collection
- [Use the REST API](docs/api/) for automation

---

*Reaper is developed and maintained by [The Last Pickle](http://thelastpickle.com/) and the open source community.* 