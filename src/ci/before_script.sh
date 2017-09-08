#!/bin/bash

echo "Starting Before Script step..."

set -xe

case "${TEST_TYPE}" in
    "")
        echo "ERROR: Environment variable TEST_TYPE is unspecified."
        exit 1
        ;;
    "ccm")
        psql -c 'create database reaper;' -U postgres
        ;;
    *)
        echo "Skipping, no actions for TEST_TYPE=${TEST_TYPE}."
esac