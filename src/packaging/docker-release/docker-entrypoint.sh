#!/bin/bash
# Copyright 2020 DataStax, Inc.
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

if [ $# -ne 5 ]
then
  echo "Incorrect number of arguments provided. Exiting"
  echo "Usage: RELEASE_BRANCH ACCESS_TOKEN_FILE REPOSITORY_SSH_KEY"
  exit 1
fi

RELEASE_BRANCH=${1}
ACCESS_TOKEN=${2}
SSH_KEY=${3}
GIT_EMAIL_ADDRESS=${4}
GIT_USER_NAME=${5}
GPG_KEY=${6}

REPOSITORY_NAME=$(basename "$(git rev-parse --show-toplevel)")
RELEASE_VERSION=""2


# Work out release branch
if [ "$(git branch -r | grep origin/"${RELEASE_BRANCH}")" = "" ]
then
  echo "The release branch ${RELEASE_BRANCH} is missing from remote. Please specify a branch that exists in '${REPOSITORY_NAME}'."
  exit 1
fi


# Setup git and ssh
git config --global user.email "${GIT_EMAIL_ADDRESS}"
git config --global user.name "${GIT_USER_NAME}"

if [ -n "${GPG_KEY}" ]
then
  gpg --import "${GPG_KEY}"
  GPG_KEY_ID=$(gpg --list-secret-keys --keyid-format LONG | grep sec  | tr -s ' ' | cut -d' ' -f2 | cut -d'/' -f2)
  git config --global user.signingkey "${GPG_KEY_ID}"
  git config --global gpg.program gpg
  git config --global commit.gpgsign true
  export GPG_TTY=$(tty)
fi

git config --list

eval $(ssh-agent)
ssh-add ${SSH_KEY}
ssh-add -L


# Update and test branch before we commit anything
echo "Updating branches before we begin"
if [ "${RELEASE_BRANCH}" != "master" ]
then
  git checkout master
  git pull --rebase --prune
fi

git checkout ${RELEASE_BRANCH}
git pull --rebase --prune

echo "Checking licenses, build and unit tests prior to creating release"
mvn apache-rat:check
exe_result=$?
if [ ${exe_result} -ne 0 ]
then
  echo "License check failed. Aborting. Check the RAT report in ./target/rat.txt of the repository."
  exit 1
fi

rm -fr src/ui/build
rm -fr src/ui/node_modules
rm -fr src/ui/bower_components
rm -f src/ui/package-lock.json
rm -f src/ui/app/jsx/navbar.jsx
# FIXME: For now we need to add -DskipTests when calling mvn package
#  This is because the PostgresStorageTest.testUpdateLeaderEntry fails in the container even though it passes in CI.
#  Generally, we would never perform a release if CI is failing on the branch we want to release on. Hence, checking
#  if we can build is sufficient for now.
mvn clean package -DskipTests
exe_result=$?
if [ ${exe_result} -ne 0 ]
then
  echo "Build and test failed. Aborting."
  exit 1
fi


# Work out release version and confirm that we should go ahead with the release
echo "Getting the release version from pom.xml"
RELEASE_VERSION="$(grep -m 1 "<version>" pom.xml | sed 's/[\ <version\/>]*//g' | cut -d '-' -f1)"

branch_action=""
if [ "${RELEASE_BRANCH}" = "master" ]
then
  RELEASE_BRANCH="$( echo "${RELEASE_VERSION}" | cut -d'.' -f1,2)"
  branch_action="git checkout -b ${RELEASE_BRANCH}"
fi

while true
do
  read -p "I will create release ${RELEASE_VERSION} from ${RELEASE_BRANCH}. Are you happy to proceed with the release (yes/no)? " yn
  case $yn in
      yes )
        break ;;
      no )
        echo "Aborting"
        exit 0 ;;
      * ) echo "Please answer 'yes' or 'no'.";;
  esac
done

eval "${branch_action}"


# Generate changelog for the release
echo "Generating changelog for the release"

CHANGELOG="CHANGELOG.md"
CHANGELOG_NEW="CHANGELOG.md.new"
CHANGELOG_TMP="changelog.tmp"
RELEASE_COMMIT_SHA=$(git rev-parse HEAD)
github-changes \
    -o thelastpickle \
    -r "${REPOSITORY_NAME}" \
    --use-commit-body \
    -f changelog.tmp \
    -k "${ACCESS_TOKEN}" \
    -b "${RELEASE_COMMIT_SHA}" \
    -v

BLOCK_START=$(grep -n -m 1 "###" ${CHANGELOG_TMP} | cut -d':' -f1)
BLOCK_END=$(grep -n -m 2 "###" ${CHANGELOG_TMP} | cut -d':' -f1 | tail -n 1)
CHANGE_ENTRIES="$(head -n $((${BLOCK_END}-1)) ${CHANGELOG_TMP} \
    | tail -n $((${BLOCK_END}-${BLOCK_START})) \
    | sed "s/^###\ upcoming/###\ ${RELEASE_VERSION}/g")"

CHANGELOG_SIZE=$(( $(wc -l ${CHANGELOG} | cut -d' ' -f1) - 2 ))

{
  echo "## Change Log"
  echo ""
  echo -e "${CHANGE_ENTRIES}"
  echo ""
  tail -n ${CHANGELOG_SIZE} ${CHANGELOG}
} >> "${CHANGELOG_NEW}"

rm "${CHANGELOG}"
mv "${CHANGELOG_NEW}" "${CHANGELOG}"
rm "${CHANGELOG_TMP}"

changelog_status="modified"

echo "I will be committing the following changes to the ${CHANGELOG}:"
echo
git diff "${CHANGELOG}"

while true
do
  read -p "Do these changes look correct (yes/no)? " yn
  case $yn in
      yes )
        changelog_status="changes_confirmed"
        break ;;
      no )
        changelog_status="needs_editing"
        break ;;
      * ) echo "Please answer 'yes' or 'no'.";;
  esac
done

if [ "${changelog_status}" = "needs_editing" ]
then
  changelog_hash_before="$(md5sum "${CHANGELOG} "| cut -c -32)"
  vim ${CHANGELOG}

  if [ "$(md5sum "${CHANGELOG}" | cut -c -32)" = "${changelog_hash_before}" ]
  then
    echo "Changelog was not updated. Aborting."
    exit 1
  else
    changelog_status="changes_confirmed"
  fi
fi

if [ "${changelog_status}" != "changes_confirmed" ]
then
  echo "Changelog changes are unconfirmed. Aborting."
fi

CHANGE_ENTRIES="$(git diff "${CHANGELOG}" | grep -e "^\+" | sed 's/^\+//g' | grep -v "++")"
git add "${CHANGELOG}"
git commit -m "Updated changelog for ${RELEASE_VERSION}"
MERGE_COMMIT_SHA="$(git rev-parse HEAD)"

git checkout master
git merge "${RELEASE_BRANCH}" -s ours -m "Forward merge ${RELEASE_BRANCH} to master for release ${RELEASE_VERSION}"

echo "Cherry picking changelog commit ${MERGE_COMMIT_SHA} to master"
git cherry-pick -n "${MERGE_COMMIT_SHA}"
git commit -a --amend -m "Forward merge ${RELEASE_BRANCH} to master for release ${RELEASE_VERSION}"
git push origin "${RELEASE_BRANCH}" master --atomic


# Create the tag and update the project.
git checkout "${RELEASE_BRANCH}"
mvn -DskipTests -Darguments=-DskipTests release:prepare -Dtag="${RELEASE_VERSION}"

cat << EOF
I have created the $RELEASE_VERSION changelog and tag. You can now publish the release.

Go to https://github.com/thelastpickle/$REPOSITORY_NAME/tags to publish a release from the $RELEASE_VERSION tag.

You will need the following information when filling out the form to publish the release.

Existing tag: $RELEASE_VERSION
Release title: $RELEASE_VERSION
Release description:
$CHANGE_ENTRIES

EOF

echo -n "Waiting for release to be published "

publish_check=""
while [ "${publish_check}" = "" ]
do
  sleep 10
  echo -n "."
  publish_check=$(curl -s https://api.github.com/repos/thelastpickle/${REPOSITORY_NAME}/releases | grep tag_name | grep ${RELEASE_VERSION})
done
echo " Release ${RELEASE_VERSION} published!"

cat << EOF
Now tha the release is published please perform a forward merge from the release branch as described here: http://cassandra-reaper.io/docs/development/

The commands for the forward merge will look something like the following:

$ git checkout master
$ git merge $RELEASE_BRANCH -s ours -m "Forward merge $RELEASE_BRANCH to master for release $RELEASE_VERSION record keeping"
$ git push origin master
EOF
