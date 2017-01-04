DEBIAN_PACKAGE_NAME:=cassandra-reaper

WITH_VIRTUAL_ENV=false

test: java-test

prepare-for-release: update-pom-version update-changelog

prepare-for-release: debian-prepare-for-release

include jenkins.mk
