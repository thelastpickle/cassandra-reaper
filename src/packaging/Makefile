# Copyright 2016-2017 Spotify AB
# Copyright 2016-2018 The Last Pickle Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Using mvn:
#VERSION := `mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version -f ../../pom.xml | grep -v '\['`

# Using python:
# VERSION := `python -c "import xml.etree.ElementTree as ET; print(ET.parse(open('pom.xml')).getroot().find('{http://maven.apache.org/POM/4.0.0}version').text)"`

# Passed as argument
VERSION := $(filter-out $@,$(MAKECMDGOALS))

package:
	mvn -B clean package  -f ../../pom.xml -DskipTests

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
	mkdir -p build/lib/systemd/system/
	cp resource/cassandra-reaper.yaml build/etc/cassandra-reaper/
	cp resource/cassandra-reaper*.yaml build/etc/cassandra-reaper/configs
	cp ../server/target/cassandra-reaper-$(VERSION).jar build/usr/share/cassandra-reaper/
	cp bin/* build/usr/local/bin/
	cp etc/bash_completion.d/spreaper build/etc/bash_completion.d/
	cp debian/reaper.init build/etc/init.d/cassandra-reaper
	cp debian/cassandra-reaper.service build/lib/systemd/system/cassandra-reaper.service
	chmod 755 build/etc/init.d/cassandra-reaper

deb: prepare
	fpm -s dir -t deb -n reaper -v $(VERSION) --pre-install debian/preinstall.sh --post-install debian/postinstall.sh -C build .

rpm: prepare
	fpm -s dir -t rpm -n reaper -v $(VERSION) --pre-install redhat/preinstall.sh --post-install redhat/postinstall.sh --config-files /etc/cassandra-reaper/cassandra-reaper.yaml -C build .

all: package deb rpm

clean:
	rm -Rf packages/*
	rm -Rf build/*
