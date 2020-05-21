+++
title = "Package Install Guide"
menuTitle = "Package Install Guide"
weight = 1
identifier = "install"
parent = "install"
+++

## We provide prebuilt install binaries

The prebuilt install binaries for Reaper can be found on [Bintray](https://bintray.com/thelastpickle) and on the [Releases](https://github.com/thelastpickle/cassandra-reaper/releases) page of the GitHub Repository.

You can also find the installation binaries on the [downloads](../../downloads) page.

## Reaper can be run as a foreground application 

You can run Reaper in the foreground. This is useful when developing and testing the application.

The Reaper binaries are available in a `tar.gz` package. Once decompressed, you can run the application using the following command: 

```
java -jar target/cassandra-reaper-X.X.X.jar server resource/cassandra-reaper.yaml
```

This will start the Reaper application in the foreground using the configuration that stores the repair information in an [in-memory](../../backends/memory) database. 

Once started, the web UI can be accessed using the following URL. See the [Using Reaper](../../usage) section for more information on using the Reaper UI.

```
http://127.0.0.1:8080/webui/
```

{{% notice note %}}
Reaper can also be accessed using the [REST API](../../development/api) exposed on port 8080, or using the command line tool `bin/spreaper`
{{% /notice %}}

The _resource/_ directory has a configuration for each of the respective [backend](../../backends) storage options. You can further configure the Reaper application by modifying settings in the _resource/cassandra-reaper.yaml_ [configuration](../../configuration) file.

## Reaper can be run as a Service

Reaper can operate as a service when installed using the RPM or DEB package.

### You can install Reaper using an RPM

Download the RPM and install using the `rpm` command:

```
sudo rpm -ivh reaper-*.*.*.x86_64.rpm
```

#### Stable release install

1. Run the following to get a generated .repo file:
```
wget https://bintray.com/thelastpickle/reaper-rpm/rpm -O bintray-thelastpickle-reaper-rpm.repo
```
* OR - Copy this text into a _bintray-thelastpickle-reaper-rpm.repo_ file on your Linux machine:
```
#bintraybintray-thelastpickle-reaper-rpm - packages by thelastpickle from Bintray
[bintraybintray-thelastpickle-reaper-rpm]
name=bintray-thelastpickle-reaper-rpm
baseurl=https://dl.bintray.com/thelastpickle/reaper-rpm
gpgcheck=0
repo_gpgcheck=0
enabled=1
``` 

2. Run the following command: 
```
sudo mv bintray-thelastpickle-reaper-rpm.repo /etc/yum.repos.d/
```

3. Install reaper:

```
sudo yum install reaper
```

#### Development release install

1. Run the following to get a generated .repo file:
```
wget https://bintray.com/thelastpickle/reaper-rpm-beta/rpm -O bintray-thelastpickle-reaper-rpm-beta.repo
```
* OR - Copy this text into a _bintray-thelastpickle-reaper-rpm-beta.repo_ file on your Linux machine:
```
#bintraybintray-thelastpickle-reaper-rpm-beta - packages by thelastpickle from Bintray
[bintraybintray-thelastpickle-reaper-rpm-beta]
name=bintray-thelastpickle-reaper-rpm-beta
baseurl=https://dl.bintray.com/thelastpickle/reaper-rpm-beta
gpgcheck=0
repo_gpgcheck=0
enabled=1
```  

2. Run the following command:
```
sudo mv bintray-thelastpickle-reaper-rpm-beta.repo /etc/yum.repos.d/
```

3. Install reaper:
```
sudo yum install reaper
```


### You can install Reaper using a DEB package

Download the DEB package and install using the `dpkg` command: 

```
sudo dpkg -i reaper_*.*.*_amd64.deb
```

#### Stable release install

1. Using the command line, add the following to your /etc/apt/sources.list system config file: 
```
echo "deb https://dl.bintray.com/thelastpickle/reaper-deb wheezy main" | sudo tee -a /etc/apt/sources.list
```
* OR - Add the repository URLs using the "Software Sources" admin UI:
```
deb https://dl.bintray.com/thelastpickle/reaper-deb wheezy main
```

2. Install the public key:
```
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 2895100917357435
```

3. Install reaper:
```
sudo apt-get update
sudo apt-get install reaper
```

#### Development release install

1. Using the command line, add the following to your /etc/apt/sources.list system config file:
```
echo "deb https://dl.bintray.com/thelastpickle/reaper-deb-beta wheezy main" | sudo tee -a /etc/apt/sources.list
```
* Or - Add the repository URLs using the "Software Sources" admin UI:
```
deb https://dl.bintray.com/thelastpickle/reaper-deb-beta wheezy main
```

2. Install the public key:
```
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 2895100917357435
```

3. Install reaper:
```
sudo apt-get update
sudo apt-get install reaper
```

### You can control Reaper using `systemd` commands

Start the service by running:

```
sudo service cassandra-reaper start
```

Stop the service by running:
```
sudo service cassandra-reaper stop
```

{{% notice info %}}
Log files can be found at `/var/log/cassandra-reaper.log` and `/var/log/cassandra-reaper.err`.
{{% /notice %}}

Once started, the web UI can be accessed using the following URL. See the [Using Reaper](../../usage) section for more information on using the Reaper UI.

```
http://<HOST_ADDRESS>:8080/webui/
```

{{% notice note %}}
Reaper can also be accessed using the [REST API](../../development/api) exposed on port 8080, or using the command line tool `bin/spreaper`
{{% /notice %}}


### You can configure Reaper's deployment and operation

The yaml file used by the service is located at _/etc/cassandra-reaper/cassandra-reaper.yaml_. Alternate config templates can be found under _/etc/cassandra-reaper/configs_. The directory has a configuration for each of the respective [backend](../../backends) storage options. You can further configure the Reaper application by modifying settings in the _cassandra-reaper.yaml_ [configuration](../../configuration) file.

{{% notice info %}}
It is recommended to create a new file with your specific configuration and symlink it as _/etc/cassandra-reaper/cassandra-reaper.yaml_ to avoid your configuration from being overwritten during upgrades. 
{{% /notice %}}
