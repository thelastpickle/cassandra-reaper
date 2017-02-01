FROM amazonlinux

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install dependencies
RUN yum update \
    && yum install -y \
        wget \
    && wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u60-b27/jdk-8u60-linux-x64.rpm" \
    && yum localinstall -y jdk-8u60-linux-x64.rpm \
    && wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo \
    && sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo \
    && yum update \
    && yum install -y \
        apache-maven \
        gcc \
        git \
        make \
        rpm-build \
        ruby-devel \
        which \
    && mvn --version \
    && gem install fpm

# grab source
RUN git clone https://github.com/thelastpickle/cassandra-reaper
WORKDIR ${WORKDIR}/cassandra-reaper

# cache maven dependencies, useful during Dockerfile testing
RUN cp pom.xml /tmp
WORKDIR /tmp
RUN mvn package
WORKDIR ${WORKDIR}/cassandra-reaper

# target directory for final build
RUN mkdir ${WORKDIR}/packages

# REQUIRED to build rpm packages
RUN git checkout v0.3.0

# build rpm
RUN mvn package \
    && make rpm \
    && cp *.rpm ${WORKDIR}/packages

# change the WORKDIR to the directory that only includes the built packages
WORKDIR ${WORKDIR}/packages
