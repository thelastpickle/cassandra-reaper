#!/bin/bash
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

BUILD_VERSION=$(grep -m1 "<version>" ../server/pom.xml | tr -d ' ' |  tr -d '<version>' | tr -d '/')
BUILD_HASH=$(git rev-parse --short HEAD)
BUILD_DATE=$(date "+%Y-%m-%d %H:%M:%S")

NAVBAR_TEMPLATE="app/navbar.template.jsx"

echo "Building navbar.jsx from template: ${NAVBAR_TEMPLATE}"
echo "Replacing ##VERSION## with ${BUILD_VERSION}"
echo "Replacing ##GIT-SHA## with ${BUILD_HASH}"
echo "Replacing ##BUILD-DATE## wih ${BUILD_DATE}"

git checkout -- ${NAVBAR_TEMPLATE} \
  && sed -i.'bak' -e "s/##VERSION##/${BUILD_VERSION}/g" ${NAVBAR_TEMPLATE} \
  && sed -i.'bak' -e "s/##GIT\-SHA##/${BUILD_HASH}/g" ${NAVBAR_TEMPLATE} \
  && sed -i.'bak' -e "s/##BUILD\-DATE##/${BUILD_DATE}/g" ${NAVBAR_TEMPLATE} \
  && cp ${NAVBAR_TEMPLATE} app/jsx/navbar.jsx \
  && rm ${NAVBAR_TEMPLATE}.bak \
  && git checkout -- ${NAVBAR_TEMPLATE}