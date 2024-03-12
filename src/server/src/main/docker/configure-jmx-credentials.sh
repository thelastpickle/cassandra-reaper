#!/bin/sh
# Copyright 2017-2017 Spotify AB
# Copyright 2017-2018 The Last Pickle Ltd
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

# Expect k8s credentials to be stored in:
# - ${REAPER_JMX_CREDENTIALS_K8S_PATH}/cluster/username
# - ${REAPER_JMX_CREDENTIALS_K8S_PATH}/cluster/password
# This is the default behavior when using k8s secrets, refs:
# - https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod
# - https://kubernetes.io/docs/concepts/configuration/secret/#consuming-secret-values-from-volumes
if [ ! -z "${REAPER_JMX_CREDENTIALS_K8S_PATH}" ]; then
    for d in $(find "${REAPER_JMX_CREDENTIALS_K8S_PATH}" -type d -depth 1) ; do
	CLUSTER=$(basename $d)
	USERNAME=$(cat "${d}/username")
	PASSWORD=$(cat "${d}/password")
	if [ ! -z "${REAPER_JMX_CREDENTIALS}" ]; then
	    REAPER_JMX_CREDENTIALS+=","
	fi
	# Add to the existing ${REAPER_JMX_CREDENTIALS} chain of credentials,
	# so that both ways of specifying passwords are compatible.
	REAPER_JMX_CREDENTIALS+="${USERNAME}:${PASSWORD}@${CLUSTER}"
    done
fi

# we expect the jmx credentials to be a comma-separated list of 'user:password@cluster' entries
if [ ! -z "${REAPER_JMX_CREDENTIALS}" ]; then

cat <<EOT >> /etc/cassandra-reaper/cassandra-reaper.yml
jmxCredentials:
EOT

  # first we split them by commas
  for ENTRY in $(echo "${REAPER_JMX_CREDENTIALS}" | sed "s/,/ /g"); do
    # and then just cut out the fields we need
    CLUSTER=$(echo "${ENTRY}" | cut -d'@' -f2)
    USERNAME=$(echo "${ENTRY}" | cut -d'@' -f1 | cut -d':' -f1 | sed 's/"/\\"/g')
    PASSWORD=$(echo "${ENTRY}" | cut -d'@' -f1 | cut -d':' -f2 | sed 's/"/\\"/g')

    # finally, write out the YAML entries
cat <<EOT >> /etc/cassandra-reaper/cassandra-reaper.yml
  ${CLUSTER}:
    username: "${USERNAME}"
    password: "${PASSWORD}"
EOT

  done

fi


# Expect k8s credentials to be stored in:
# - ${REAPER_JMX_AUTH_K8S_PATH}/username
# - ${REAPER_JMX_AUTH_K8S_PATH}/password
if [ ! -z "${REAPER_JMX_AUTH_K8S_PATH}" ]; then
    # Override any existing env var, if using kubernetes-style secrets,
    # it overides the default behavior.
    if [ -f "${REAPER_JMX_AUTH_K8S_PATH}/username" ] ; then
        REAPER_JMX_AUTH_USERNAME=$(cat "${REAPER_JMX_AUTH_K8S_PATH}/username")
    else
	echo "k8s username file ${REAPER_JMX_AUTH_K8S_PATH}/username not found"
	exit 1
    fi
    if [ -f "${REAPER_JMX_AUTH_K8S_PATH}/password" ] ; then
        REAPER_JMX_AUTH_PASSWORD=$(cat "${REAPER_JMX_AUTH_K8S_PATH}/password")
    else
	echo "k8s password file ${REAPER_JMX_AUTH_K8S_PATH}/password not found"
	exit 1
    fi
fi

if [ ! -z "${REAPER_JMX_AUTH_USERNAME}" ]; then
cat <<EOT >> /etc/cassandra-reaper/cassandra-reaper.yml
jmxAuth:
  username: "$(echo "${REAPER_JMX_AUTH_USERNAME}" | sed 's/"/\\"/g')"
  password: "$(echo "${REAPER_JMX_AUTH_PASSWORD}" | sed 's/"/\\"/g')"
EOT

fi

# Expect k8s credentials (password only, no user) to be in:
# - ${CRYPTO_SYSTEM_PROPERTY_SECRET_K8S_PATH}/password
if [ ! -z "${CRYPTO_SYSTEM_PROPERTY_SECRET_K8S_PATH}" ]; then
    # Override existing env var, if using kubernetes-style secrets,
    # it overides the default behavior.
    if [ -f "${CRYPTO_SYSTEM_PROPERTY_SECRET_K8S_PATH}/password" ] ; then
        CRYPTO_SYSTEM_PROPERTY_SECRET=$(cat "${CRYPTO_SYSTEM_PROPERTY_SECRET_K8S_PATH}/password")
    else
        echo "k8s system property secret file ${CRYPTO_SYSTEM_PROPERTY_SECRET_K8S_PATH}/password not found"
	exit 1
    fi
fi

if [ ! -z "${CRYPTO_SYSTEM_PROPERTY_SECRET}" ]; then
cat <<EOT >> /etc/cassandra-reaper/cassandra-reaper.yml
cryptograph:
  type: symmetric
  systemPropertySecret: ${CRYPTO_SYSTEM_PROPERTY_SECRET}
EOT
fi
