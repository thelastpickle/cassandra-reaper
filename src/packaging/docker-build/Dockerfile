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

# use a common app path, copied from python-onbuild:latest
ENV WORKDIR /usr/src/app
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# install dependencies
RUN apt-get update \
    && apt-get install -y \
        curl \
    && curl -sL https://deb.nodesource.com/setup_10.x -o nodesource_setup.sh \
    && bash nodesource_setup.sh \
    && DEBIAN_FRONTEND="noninteractive" apt-get install -y \
        build-essential \
        git \
        openjdk-21-jdk \
        maven \
        nodejs \
        python2 \
        rpm \
        ruby-dev \
    && mvn --version \
    && gem install dotenv -v 2.8.1 \
    && gem install fpm \
    && npm install -g bower

WORKDIR ${WORKDIR}

# Add entrypoint script
COPY src/packaging/docker-build/docker-entrypoint.sh ${WORKDIR}
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD [""]
