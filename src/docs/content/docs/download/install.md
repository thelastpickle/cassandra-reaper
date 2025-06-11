---
title: "Install and Run"
weight: 1
identifier: "install"
parent: "download"
---

## Requirements

Since Reaper v4, Java 11 is required to compile and run it. More recent versions of the JDK should also be able to run the compiled version of Reaper.

## Running Reaper using the jar

After modifying the `resource/cassandra-reaper.yaml` config file, Reaper can be started using the following command line :

```bash
java -jar target/cassandra-reaper-X.X.X.jar server resource/cassandra-reaper.yaml
```

Once started, the UI can be accessed through : http://127.0.0.1:8080/webui/

Reaper can also be accessed using the REST API exposed on port 8080, or using the command line tool `bin/spreaper`


## Installing and Running as a Service

We provide prebuilt packages for reaper on [Cloudsmith](https://cloudsmith.io/~thelastpickle/repos/).


### RPM Install (CentOS, Fedora, RHEK)

Grab the RPM from GitHub and install using the `rpm` command:

```bash
sudo rpm -ivh reaper-*.*.*.x86_64.rpm
```

#### Using yum (stable releases)

1/ Run the following to install the repo:
```
curl -1sLf \
  'https://dl.cloudsmith.io/public/thelastpickle/reaper/setup.rpm.sh' \
  | sudo -E bash
```

2/ Install reaper : 

```
sudo yum install reaper
```

In case of problem, check the alternate procedure on [cloudsmith.io](https://cloudsmith.io/~thelastpickle/repos/reaper/setup/#formats-rpm).

#### Using yum (development builds)

1/ Run the following to install the repo:
```
curl -1sLf \
  'https://dl.cloudsmith.io/public/thelastpickle/reaper-beta/setup.rpm.sh' \
  | sudo -E bash
```

2/ Install reaper : 

```
sudo yum install reaper
```

In case of problem, check the alternate procedure on [cloudsmith.io](https://cloudsmith.io/~thelastpickle/repos/reaper-beta/setup/#formats-rpm).

### DEB (Debian based distros like Ubuntu)

After downloading the DEB package, install using the `dpkg` command: 

```bash
sudo dpkg -i reaper_*.*.*_amd64.deb
```

#### Using apt-get (stable releases)

1/ Using the command line, run the following:
```
curl -1sLf \
  'https://dl.cloudsmith.io/public/thelastpickle/reaper/setup.deb.sh' \
  | sudo -E bash
```

2/ Install reaper :

```
sudo apt-get update
sudo apt-get install reaper
```

In case of problem, check the alternate procedure on [cloudsmith.io](https://cloudsmith.io/~thelastpickle/repos/reaper/setup/#formats-deb).

#### Using apt-get (development builds)

1/ Using the command line, run the following command:
```
curl -1sLf \
  'https://dl.cloudsmith.io/public/thelastpickle/reaper-beta/setup.deb.sh' \
  | sudo -E bash
```

2/ Install reaper :

```
sudo apt-get update
sudo apt-get install reaper
```

In case of problem, check the alternate procedure on [cloudsmith.io](https://cloudsmith.io/~thelastpickle/repos/reaper-beta/setup/#formats-deb).

## Service Configuration

The yaml file used by the service is located at `/etc/cassandra-reaper/cassandra-reaper.yaml` and alternate config templates can be found under `/etc/cassandra-reaper/configs`.
It is recommended to create a new file with your specific configuration and symlink it as `/etc/cassandra-reaper/cassandra-reaper.yaml` to avoid your configuration from being overwritten during upgrades.  
Adapt the config file to suit your setup and then run `sudo service cassandra-reaper start`.  
  
Log files can be found at `/var/log/cassandra-reaper.log` and `/var/log/cassandra-reaper.err`.  

Stop the service by running : `sudo service cassandra-reaper stop`  



