FROM registry.opensource.zalan.do/stups/openjdk:8-cd26

COPY target/cassandra-reaper-0.3.3-SNAPSHOT.jar /


RUN java -jar /cassandra-reaper-0.3.3-SNAPSHOT.jar server config.yaml