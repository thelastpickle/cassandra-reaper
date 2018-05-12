# Using mvn:
VERSION := `mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -f ../../pom.xml | grep -v '\['`

# Using python:
# VERSION := `python -c "import xml.etree.ElementTree as ET; print(ET.parse(open('pom.xml')).getroot().find('{http://maven.apache.org/POM/4.0.0}version').text)"`

package:
	mvn clean package  -f ../../pom.xml -DskipTests

prepare:
	echo "$(VERSION)..."
	rm -Rf packages/*
	rm -Rf build/*
	mkdir -p build/usr/share/cassandra-reaper
	mkdir -p build/usr/local/bin
	mkdir -p build/etc/init.d
	mkdir -p build/etc/cassandra-reaper
	mkdir -p build/etc/cassandra-reaper/configs
	mkdir -p build/etc/bash_completion.d
	cp resource/cassandra-reaper.yaml build/etc/cassandra-reaper/
	cp resource/shiro.ini build/etc/cassandra-reaper/configs
	cp resource/cassandra-reaper*.yaml build/etc/cassandra-reaper/configs
	cp ../server/target/cassandra-reaper-$(VERSION).jar build/usr/share/cassandra-reaper/
	cp bin/* build/usr/local/bin/
	cp etc/bash_completion.d/spreaper build/etc/bash_completion.d/
	cp debian/reaper.init build/etc/init.d/cassandra-reaper
	chmod 755 build/etc/init.d/cassandra-reaper

deb: prepare
	fpm -s dir -t deb -n reaper -v $(VERSION) --pre-install debian/preinstall.sh -C build .

rpm: prepare
	fpm -s dir -t rpm -n reaper -v $(VERSION) --pre-install redhat/preinstall.sh --config-files /etc/cassandra-reaper/cassandra-reaper.yaml -C build .

all: package deb rpm

clean:
	rm -Rf packages/*
	rm -Rf build/*
