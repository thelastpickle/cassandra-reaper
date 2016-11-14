package:
	mvn package

prepare:
	mkdir -p build/usr/share/cassandra-reaper
	mkdir -p build/usr/local/bin
	mkdir -p build/etc/spotify
	mkdir -p build/etc/init.d
	cp resource/cassandra-reaper.yaml build/etc/spotify/
	cp target/cassandra-reaper-0.3.1-SNAPSHOT.jar build/usr/share/cassandra-reaper/
	cp bin/* build/usr/local/bin/
	cp debian/reaper.init build/etc/init.d/cassandra-reaper
	chmod 755 build/etc/init.d/cassandra-reaper

deb: prepare
	rm -f reaper_*.deb
	fpm -s dir -t deb -n reaper -v 0.3.1 --pre-install debian/preinstall.sh -C build .

rpm: prepare
	rm -f reaper_*.rpm
	fpm -s dir -t rpm -n reaper -v 0.3.1 --pre-install debian/preinstall.sh -C build .

all: package deb 

