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

# build jar
# FAILS WITH:
#    [INFO] --- exec-maven-plugin:1.5.0:exec (exec-bower-install) @ cassandra-reaper ---
#    bower ESUDO         Cannot be run with sudo
#
#    Additional error details:
#    Since bower is a user command, there is no need to execute it with superuser permissions.
#    If you're having permission errors when using bower without sudo, please spend a few minutes learning more about how your system should work and make any necessary repairs.
#
#    http://www.joyent.com/blog/installing-node-and-npm
#    https://gist.github.com/isaacs/579814
#
#    You can however run a command with sudo using --allow-root option
#    [ERROR] Command execution failed.
RUN mvn clean package \
    && mvn clean package -Pbuild-ui

# change the WORKDIR to the directory that only includes the built packages
WORKDIR ${WORKDIR}/packages
