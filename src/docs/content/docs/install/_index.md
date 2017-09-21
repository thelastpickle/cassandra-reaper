+++
[menu.docs]
name = "Download and Install"
weight = 1
identifier = "download_install"
+++




### Running Reaper

After modifying the `resource/cassandra-reaper.yaml` config file, Reaper can be started using the following command line :

```java -jar target/cassandra-reaper-X.X.X.jar server resource/cassandra-reaper.yaml```

Once started, the UI can be accessed through : http://127.0.0.1:8080/webui/

Reaper can also be accessed using the REST API exposed on port 8080, or using the command line tool `bin/spreaper`



## Installing and Running as a Service

We provide prebuilt packages for reaper on the [GitHub release page](https://github.com/thelastpickle/cassandra-reaper/releases).  We've linked the recommended versions above for convenience.


### RPM Install (CentOS, Fedora, RHEK)

Grab the RPM from GitHub and install using the `rpm` command:

`bash
sudo rpm -ivh reaper-*.*.*.x86_64.rpm
`  

### DEB (Debian based distros like Ubuntu)

After downloading the DEB package, install using the `dpkg` command: 

`
sudo dpkg -i reaper_*.*.*_amd64.deb
`

### Service Configuration

The yaml file used by the service is located at `/etc/cassandra-reaper/cassandra-reaper.yaml` and alternate config templates can be found under `/etc/cassandra-reaper/configs`.
It is recommended to create a new file with your specific configuration and symlink it as `/etc/cassandra-reaper/cassandra-reaper.yaml` to avoid your configuration from being overwritten during upgrades.  
Adapt the config file to suit your setup and then run `sudo service cassandra-reaper start`.  
  
Log files can be found at `/var/log/cassandra-reaper.log` and `/var/log/cassandra-reaper.err`.  

Stop the service by running : `sudo service cassandra-reaper stop`  



