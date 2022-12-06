#!/usr/bin/env bash

set -euxo pipefail

test_with_legacy_on () {
    docker run -p 27017:27017 --rm -d --name mongo_test mongo:$2
    ERL_FLAGS="-mongodb use_legacy_protocol $1" rebar3 ct
    ERL_FLAGS="-mongodb use_legacy_protocol $1" rebar3 eunit
    docker stop mongo_test
}

# Test with and without legacy API in a version of MongoDB that supports both APIs

test_with_legacy_on true 5.0.14
test_with_legacy_on false 5.0.14

# Test with the modern API in a version that only support the modern API

test_with_legacy_on false 6.0

# Test with the legacy API in a version that only support the legacy API

test_with_legacy_on true 3.2
