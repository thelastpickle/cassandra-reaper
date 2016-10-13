prepare:
	mkdir -p build/usr/share/cassandra-reaper
	mkdir -p build/usr/local/bin
	mkdir -p build/etc/{spotify,init.d}
	cp resource/cassandra-reaper.yaml build/etc/spotify/
	cp target/cassandra-reaper-0.3.0-SNAPSHOT.jar build/usr/share/cassandra-reaper/
	cp bin/* build/usr/local/bin/
	cp debian/reaper.init build/etc/init.d/cassandra-reaper
	chmod 755 build/etc/init.d/cassandra-reaper

deb: prepare
	rm -f reaper_*.deb
	fpm -s dir -t deb -n reaper -v 0.3 --pre-install debian/preinstall.sh -C build .

package:
	mvn package

all: package deb 

