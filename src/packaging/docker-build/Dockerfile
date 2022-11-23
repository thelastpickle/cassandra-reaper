# Copyright 2017-2017 Spotify AB
# Copyright 2017-2019 The Last Pickle Ltd
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

FROM ubuntu:20.04
# Set up non-root user, 'build', with default uid:gid
# This allows passing --build-arg to use localhost username, and uid:gid:
#   $ docker build \
#       -t cassandra-reaper-build:latest \
#       --build-arg UID_ARG=$(id -u) \
#       --build-arg GID_ARG=$(id -g) \
#       .
# OR
#   $ docker-compose build \
#       --build-arg UID_ARG=$(id -u) \
#       --build-arg GID_ARG=$(id -g)
#
ARG UID_ARG=1000
ARG GID_ARG=1000

RUN echo "Building image with arguments: UID_ARG=${UID_ARG}, GID_ARG=${GID_ARG}"

# install dependencies
RUN apt-get update \
    && apt-get install -y \
        curl \
    && curl -sL https://deb.nodesource.com/setup_10.x -o nodesource_setup.sh \
    && bash nodesource_setup.sh \
    && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
        build-essential \
        git \
        openjdk-8-jdk \
        openjdk-11-jdk \
        maven \
        nodejs \
        rpm \
        ruby-dev \
        sudo \
    && mvn --version \
    && gem install fpm \
    && npm install -g bower

# Create the build user and make it part of the password-less sudo group
ENV BUILD_USER="build"
RUN groupadd --gid ${GID_ARG} --non-unique ${BUILD_USER} && \
    useradd --create-home --shell /bin/bash --uid ${UID_ARG} --gid ${GID_ARG} --non-unique ${BUILD_USER} && \
    usermod -aG sudo ${BUILD_USER} && \
    sed -i 's/\(^%sudo\tALL=(ALL:ALL)\).*/\1 NOPASSWD:ALL/g' /etc/sudoers

ENV WORKDIR "/home/${BUILD_USER}"

# Add entrypoint script
COPY src/packaging/docker-build/docker-entrypoint.sh /usr/local/bin
USER ${BUILD_USER}
WORKDIR ${WORKDIR}
ENTRYPOINT ["docker-entrypoint.sh"]
CMD [""]
