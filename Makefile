# Using mvn:
VERSION := `mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\['`

# Using python:
# VERSION := `python -c "import xml.etree.ElementTree as ET; print(ET.parse(open('pom.xml')).getroot().find('{http://maven.apache.org/POM/4.0.0}version').text)"`

package:
	mvn package

prepare:
	# needs to once at least once to pre-download dependencies
	echo $(VERSION)

	mkdir -p build/usr/share/cassandra-reaper
	mkdir -p build/usr/local/bin
	mkdir -p build/etc/init.d
	mkdir -p build/etc/spotify
	cp resource/cassandra-reaper.yaml build/etc/spotify/
	cp target/cassandra-reaper-$(VERSION).jar build/usr/share/cassandra-reaper/
	cp bin/* build/usr/local/bin/
	cp debian/reaper.init build/etc/init.d/cassandra-reaper
	chmod 755 build/etc/init.d/cassandra-reaper

deb: prepare
	rm -f reaper_*.deb
	fpm -s dir -t deb -n reaper -v $(VERSION) --pre-install debian/preinstall.sh -C build .

rpm: prepare
	rm -f reaper_*.rpm
	fpm -s dir -t rpm -n reaper -v $(VERSION) --pre-install debian/preinstall.sh -C build .

all: package deb rpm

clean:
	rm -rf reaper_*.deb reaper_*.rpm
