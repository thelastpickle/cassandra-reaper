+++
[menu.docs]
name = "Download and Install"
weight = 1
identifier = "download_install"
+++


# Downloads and Installation

The current stable version is [0.6.2](https://github.com/thelastpickle/cassandra-reaper/releases/tag/0.6.2) : 

* [Deb Package](https://github.com/thelastpickle/cassandra-reaper/releases/download/0.6.2/reaper_0.6.2_amd64.deb)
* [RPM](https://github.com/thelastpickle/cassandra-reaper/releases/download/0.6.2/reaper-0.6.2-1.x86_64.rpm)
* [Tarball](https://github.com/thelastpickle/cassandra-reaper/releases/download/0.6.2/cassandra-reaper-0.6.2-release.tar.gz)

The latest beta version is [0.7.0-beta-25082017](https://github.com/thelastpickle/cassandra-reaper/releases/tag/0.7.0-beta-25082017) :

* [Deb Package](https://github.com/thelastpickle/cassandra-reaper/releases/download/0.7.0-beta-25082017/reaper_0.7-SNAPSHOT_amd64.deb)
* [RPM](https://github.com/thelastpickle/cassandra-reaper/releases/download/0.7.0-beta-25082017/reaper-0.7_SNAPSHOT-1.x86_64.rpm)
* [Tarball](https://github.com/thelastpickle/cassandra-reaper/releases/download/0.7.0-beta-25082017/cassandra-reaper-0.7.0-SNAPSHOT-release.tar.gz)
 

For a docker image, please see the Docker section.


### Running Reaper

After modifying the `resource/cassandra-reaper.yaml` config file, Reaper can be started using the following command line :

```java -jar target/cassandra-reaper-X.X.X.jar server resource/cassandra-reaper.yaml```

Once started, the UI can be accessed through : http://127.0.0.1:8080/webui/

Reaper can also be accessed using the REST API exposed on port 8080, or using the command line tool `bin/spreaper`



## Install RPM or DEB package and run as a service

Install the RPM (Fedora based distros like RHEL or Centos) using : `sudo rpm -ivh reaper-*.*.*.x86_64.rpm`  
Install the DEB (Debian based distros like Ubuntu) using : `sudo dpkg -i reaper_*.*.*_amd64.deb`

The yaml file used by the service is located at `/etc/cassandra-reaper/cassandra-reaper.yaml` and alternate config templates can be found under `/etc/cassandra-reaper/configs`.
It is recommended to create a new file with your specific configuration and symlink it as `/etc/cassandra-reaper/cassandra-reaper.yaml` to avoid your configuration from being overwritten during upgrades.  
Adapt the config file to suit your setup and then run `sudo service cassandra-reaper start`.  
  
Log files can be found at `/var/log/cassandra-reaper.log` and `/var/log/cassandra-reaper.err`.  

Stop the service by running : `sudo service cassandra-reaper stop`  



