+++
title = "Upgrade Guide"
menuTitle = "Upgrade Guide"
weight = 10
identifier = "install"
parent = "install"
+++


## Upgrading from 1.2.0/1.2.1 to 1.2.2
We unfortunately had to break schema migrations for upgrades to 1.2.2 from 1.2.0 and 1.2.1.
Here is the upgrade procedure for each Reaper backend:

### Cassandra

* Stop all Reaper instances
* Run the following DDL statement on the backend cluster :

```
ALTER TABLE reaper_db.repair_unit_v1 DROP repair_thread_count;
```
* Upgrade Reaper to 1.2.2 and start it

**Note:** by doing so the number of threads for each existing schedule will revert back to 0, which will be translated to a single repair thread. If you previously had defined a specific number of threads you will need to recreate these schedules or update the `repair_unit_v1` table manually (this field is only useful with Cassandra 2.2, there is no benefit in Cassandra <=2.1 or >=3.0).

### H2

* Stop Reaper
* Pointing to your current Reaper jar file (located in `/usr/share/cassandra-reaper` for packaged installs), run the following command :
```
java -cp /usr/share/cassandra-reaper/cassandra-reaper-1.2.*.jar org.h2.tools.Shell
```
When asked, provide the JDBC URL from your Cassandra Reaper yaml file (for example : `jdbc:h2:~/reaper-db/db;MODE=PostgreSQL`), use the default `Driver` and leave username/password empty.
Then run the following statements:

```
ALTER TABLE REPAIR_UNIT DROP COLUMN repair_thread_count;
ALTER TABLE REPAIR_SEGMENT DROP COLUMN TOKEN_RANGES;
```
* Upgrade Reaper to 1.2.2 and start it

**Note:** by doing so the number of threads for each existing schedule will revert back to 0, which will be translated to a single repair thread. If you previously had defined a specific number of threads you will need to recreate these schedules or update the `repair_unit` table manually (this field is only useful with Cassandra 2.2, there is no benefit in Cassandra <=2.1 or >=3.0).

### Postgres

* Stop Reaper
* Connect to your Postgres database using the psql shell (or any other client) and run the following statements:

```
ALTER TABLE "repair_unit" DROP COLUMN "repair_thread_count";
ALTER TABLE "repair_segment" DROP COLUMN "token_ranges";
```
* Upgrade Reaper to 1.2.2 and start it

**Note:** by doing so the number of threads for each existing schedule will revert back to 0, which will be translated to a single repair thread. If you previously had defined a specific number of threads you will need to recreate these schedules or update the `repair_unit` table manually (this field is only useful with Cassandra 2.2, there is no benefit in Cassandra <=2.1 or >=3.0).


## Upgrading from 1.2.2 to 2.0.0

You need to be running at least 1.2.2 before upgrading to 2.0.0.
