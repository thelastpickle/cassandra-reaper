+++
[menu.docs]
name = "Downloads"
weight = 1
identifier = "download"
+++


# Downloads and Installation

## Packages

The current stable version can be downloaded in the following packaging formats : 

* [ ![Deb package](https://api.bintray.com/packages/thelastpickle/reaper-deb/cassandra-reaper/images/download.svg) ](https://bintray.com/thelastpickle/reaper-deb/cassandra-reaper/_latestVersion) Deb package
* [ ![RPM](https://api.bintray.com/packages/thelastpickle/reaper-rpm/cassandra-reaper/images/download.svg) ](https://bintray.com/thelastpickle/reaper-rpm/cassandra-reaper/_latestVersion) RPM
* [ ![Tarball](https://api.bintray.com/packages/thelastpickle/reaper-tarball/cassandra-reaper/images/download.svg) ](https://bintray.com/thelastpickle/reaper-tarball/cassandra-reaper/_latestVersion) Tarball


The current development version can be downloaded in the following packaging formats : 

* [ ![Deb package](https://api.bintray.com/packages/thelastpickle/reaper-deb-beta/cassandra-reaper-beta/images/download.svg) ](https://bintray.com/thelastpickle/reaper-deb-beta/cassandra-reaper-beta/_latestVersion) Deb package
* [ ![RPM](https://api.bintray.com/packages/thelastpickle/reaper-rpm-beta/cassandra-reaper-beta/images/download.svg) ](https://bintray.com/thelastpickle/reaper-rpm-beta/cassandra-reaper-beta/_latestVersion) RPM
* [ ![Tarball](https://api.bintray.com/packages/thelastpickle/reaper-tarball-beta/cassandra-reaper-beta/images/download.svg) ](https://bintray.com/thelastpickle/reaper-tarball-beta/cassandra-reaper-beta/_latestVersion) Tarball


## Quick Installation Guide

<iframe width="560" height="315" src="https://www.youtube.com/embed/0dub29BgwPI" frameborder="0" gesture="media" allowfullscreen></iframe>

 
For a docker image, please see the [Docker](docker) section.

Once the appropriate package has been downloaded, head over to the [Install and Run](install) section.

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


