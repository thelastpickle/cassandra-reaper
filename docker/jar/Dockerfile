FROM ubuntu

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install dependencies
RUN apt-get update \
    && apt-get install -y \
        curl \
    && curl -sL https://deb.nodesource.com/setup_6.x -o nodesource_setup.sh \
    && bash nodesource_setup.sh \
    && apt-get update \
    && apt-get install -y \
        build-essential \
        git \
        maven \
        nodejs \
        openjdk-8-jdk \
    && mvn --version \
    && npm install -g bower

# cache maven dependencies, useful during Dockerfile testing
COPY pom.xml /tmp
COPY reaper_ui/*.json reaper_ui/*.js /tmp/reaper_ui/
WORKDIR /tmp
RUN mvn clean package \
    && mvn clean package -Pbuild-ui
WORKDIR ${WORKDIR}

# grab source
COPY . ${WORKDIR}/cassandra-reaper
WORKDIR ${WORKDIR}/cassandra-reaper

# target directory for final build
RUN mkdir ${WORKDIR}/packages

# build jar
RUN mvn clean package \
    && mvn clean package -Pbuild-ui \
    && cp ${WORKDIR}/cassandra-reaper/target/*.jar ${WORKDIR}/packages

# change the WORKDIR to the directory that only includes the built packages
WORKDIR ${WORKDIR}/packages

# start reaper at container run-time
CMD java -jar cassandra-reaper-*.jar server ../cassandra-reaper/resource/cassandra-reaper-memory.yaml

# install cqlsh dependencies
RUN apt-get install -y \
        wget \
        python

# download cassandra for cqlsh
WORKDIR ${WORKDIR}
ENV CASSANDRA_FILENAME apache-cassandra-3.0.9-bin.tar.gz
RUN wget http://archive.apache.org/dist/cassandra/3.0.9/${CASSANDRA_FILENAME} \
    && tar xf ${CASSANDRA_FILENAME} \
    && rm ${CASSANDRA_FILENAME}
WORKDIR ${WORKDIR}/packages
