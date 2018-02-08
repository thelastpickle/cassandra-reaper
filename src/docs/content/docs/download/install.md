+++
[menu.docs]
name = "Install and Run"
weight = 1
identifier = "install"
parent = "download"
+++



# Running Reaper

After modifying the `resource/cassandra-reaper.yaml` config file, Reaper can be started using the following command line :

```bash
java -jar target/cassandra-reaper-X.X.X.jar server resource/cassandra-reaper.yaml
```

Once started, the UI can be accessed through : http://127.0.0.1:8080/webui/

Reaper can also be accessed using the REST API exposed on port 8080, or using the command line tool `bin/spreaper`


## Installing and Running as a Service

We provide prebuilt packages for reaper on the [Bintray](https://bintray.com/thelastpickle).


### RPM Install (CentOS, Fedora, RHEK)

Grab the RPM from GitHub and install using the `rpm` command:

```bash
sudo rpm -ivh reaper-*.*.*.x86_64.rpm
```

#### Using yum (stable releases)

1/ Run the following to get a generated .repo file:
```
wget https://bintray.com/thelastpickle/reaper-rpm/rpm -O bintray-thelastpickle-reaper-rpm.repo
```

or - Copy this text into a 'bintray-thelastpickle-reaper-rpm.repo' file on your Linux machine:

```
#bintraybintray-thelastpickle-reaper-rpm - packages by thelastpickle from Bintray
[bintraybintray-thelastpickle-reaper-rpm]
name=bintray-thelastpickle-reaper-rpm
baseurl=https://dl.bintray.com/thelastpickle/reaper-rpm
gpgcheck=0
repo_gpgcheck=0
enabled=1
``` 

2/ Run the following command : 
```
sudo mv bintray-thelastpickle-reaper-rpm.repo /etc/yum.repos.d/
```

3/ Install reaper : 

```
sudo yum install reaper
```

#### Using yum (development builds)

1/ Run the following to get a generated .repo file:
```
wget https://bintray.com/thelastpickle/reaper-rpm-beta/rpm -O bintray-thelastpickle-reaper-rpm-beta.repo
```

or - Copy this text into a 'bintray-thelastpickle-reaper-rpm-beta.repo' file on your Linux machine:

```
#bintraybintray-thelastpickle-reaper-rpm-beta - packages by thelastpickle from Bintray
[bintraybintray-thelastpickle-reaper-rpm-beta]
name=bintray-thelastpickle-reaper-rpm-beta
baseurl=https://dl.bintray.com/thelastpickle/reaper-rpm-beta
gpgcheck=0
repo_gpgcheck=0
enabled=1
```  

2/ Run the following command : 
```
sudo mv bintray-thelastpickle-reaper-rpm-beta.repo /etc/yum.repos.d/
```

3/ Install reaper : 

```
sudo yum install reaper
```


### DEB (Debian based distros like Ubuntu)

After downloading the DEB package, install using the `dpkg` command: 

```bash
sudo dpkg -i reaper_*.*.*_amd64.deb
```

#### Using apt-get (stable releases)

1/ Using the command line, add the following to your /etc/apt/sources.list system config file: 
```
echo "deb https://dl.bintray.com/thelastpickle/reaper-deb wheezy main" | sudo tee -a /etc/apt/sources.list
```

Or, add the repository URLs using the "Software Sources" admin UI:

```
deb https://dl.bintray.com/thelastpickle/reaper-deb wheezy main
```

2/ Install reaper :

```
sudo apt-get update
sudo apt-get install reaper
```

#### Using apt-get (development builds)

1/ Using the command line, add the following to your /etc/apt/sources.list system config file:
```
echo "deb https://dl.bintray.com/thelastpickle/reaper-deb-beta wheezy main" | sudo tee -a /etc/apt/sources.list
```

Or, add the repository URLs using the "Software Sources" admin UI:

```
deb https://dl.bintray.com/thelastpickle/reaper-deb-beta wheezy main
```

2/ Install reaper :

```
sudo apt-get update
sudo apt-get install reaper
```


## Service Configuration

The yaml file used by the service is located at `/etc/cassandra-reaper/cassandra-reaper.yaml` and alternate config templates can be found under `/etc/cassandra-reaper/configs`.
It is recommended to create a new file with your specific configuration and symlink it as `/etc/cassandra-reaper/cassandra-reaper.yaml` to avoid your configuration from being overwritten during upgrades.  
Adapt the config file to suit your setup and then run `sudo service cassandra-reaper start`.  
  
Log files can be found at `/var/log/cassandra-reaper.log` and `/var/log/cassandra-reaper.err`.  

Stop the service by running : `sudo service cassandra-reaper stop`  



