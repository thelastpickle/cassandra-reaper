FROM ubuntu:16.04

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
        rpm \
        ruby-dev \
    && mvn --version \
    && gem install fpm \
    && npm install -g bower

# cache maven dependencies, useful during Dockerfile testing
COPY pom.xml /tmp
COPY reaper_ui/*.json reaper_ui/*.js /tmp/reaper_ui/
WORKDIR /tmp
RUN mvn clean package \
    && mvn clean package -Pbuild-ui
WORKDIR ${WORKDIR}

# Add entrypoint script
COPY docker-build/docker-entrypoint.sh ${WORKDIR}
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
