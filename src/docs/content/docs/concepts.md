---
title: "Core Concepts"
weight: 5
---

## Segments

Reaper splits repair runs in segments. A segment is a subrange of tokens that fits entirely in one of the cluster token ranges. The minimum number of segments for a repair run is the number of token ranges in the cluster. With a 3 nodes cluster using 256 vnodes per node, a repair run will have at least 768 segments. If necessary, each repair can define a higher number of segments than the number of token ranges.

**As of Reaper 1.2.0 and Apache Cassandra 2.2, token ranges that have the same replicas can be consolidated into a single segment.** If the total number of requested segments is lower than the number of vnodes, Reaper will try to group token ranges so that each segment has the appropriate number of tokens.
In Cassandra 2.2, one repair session will be started for each subrange of the segment, so the gain will be the reduction of overhead in Reaper. Starting with 3.0, Cassandra will generate a single repair session for all the subranges that share the same replicas, which then further reduces the overhead of vnodes in Cassandra.

## Back-pressure

Reaper will associate each segment with its replicas, and run repairs sequentially on only one of the replicas. If a repair is already running on one of the replicas or if there are more than 20 pending compactions, Reaper will postpone the segment for future processing and try to repair the next segment.

## Concurrency and Multithreading

Being a multi threaded service, Reaper will compute how many concurrent repair sessions can run on the cluster and adjust its thread pool accordingly. To that end, it will check the number of nodes in the cluster and the RF (Replication Factor) of the repaired keyspace. On a three node cluster with RF=3, only one segment can be repaired at a time. On a six node cluster with RF=3, two segments can be repaired at the same time.

In the case of a cluster spread around multiple datacenters, the number of concurrent repair will depend on the datacenter with the smallest RF by node. On a cluster composed of a main datacenter with 9 nodes and RF=3 and a backup datacenter with 4 nodes and RF=3, only one segment can be repaired at the same time because of the backup datacenter.

The maximum number of concurrent repairs is 15 by default and can be modified in the YAML configuration (_cassandra-reaper.yaml_) file.

Since Cassandra 2.2, repairs are multithreaded in order to process several token ranges concurrently and speed up the process. No more than four threads are authorized by Cassandra. The number of repair threads can be set differently for each repair run/schedule.
This setting will be ignored for clusters running an older version of Apache Cassandra.

## Timeout
By default, each segment must complete within 30 minutes. Reaper subscribes to the repair service notifications of Cassandra to monitor completion, and if a segment takes more than 30 minutes it gets cancelled and postponed. This means that if a repair job is subject to frequent segment cancellation, it is necessary to either split it up into more segments or raise the timeout over its default value.

## Pause and Resume
Pausing a repair will force the termination of all running segments. Once the job is resumed, cancelled segments will be fully processed once again from the beginning.

## Intensity
Intensity controls the eagerness by which Reaper triggers repair segments. The Reaper will use the duration of the previous repair segment to compute how much time to wait before triggering the next one. The idea behind this is that long segments mean a lot of data mismatch, and thus a lot of streaming and compaction. Intensity allows reaper to adequately back off and give the cluster time to handle the load caused by the repair.

## Scheduling Interval
Reaper polls for new segments to process at a fixed interval. By default the interval is set to 30 seconds and this value can be modified in the Reaper configuration YAML file.
