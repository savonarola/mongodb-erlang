#!/usr/bin/env bash

set -euxo pipefail

docker run -p 27017:27017 --rm -d --name mongo_test mongo:5.0.14

echo 'Running all tests suites with the legacy protocol enabled'

ERL_FLAGS='-mongodb use_legacy_protocol true' rebar3 ct

echo 'Running mc_worker_api_SUITE with the modern protocol enabled'

ERL_FLAGS='-mongodb use_legacy_protocol false' rebar3 ct --suite test/mc_worker_api_SUITE.erl

docker stop mongo_test

