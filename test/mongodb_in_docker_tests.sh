#!/usr/bin/env bash

set -euxo pipefail

LOGIN='myuser'
PASSWORD='mypassword'

test_with_legacy_on () {
    if [ -n "$3" ]
    then
        docker run \
            -p 27017:27017 \
            --rm -d \
            --name mongo_test \
            -e MONGO_INITDB_ROOT_USERNAME="$LOGIN" \
            -e MONGO_INITDB_ROOT_PASSWORD="$PASSWORD" \
            mongo:$2
        # Apparently it takes a while for the password to be installed
        sleep 4
    else
        docker run -p 27017:27017 --rm -d --name mongo_test mongo:$2
        sleep 1
    fi
    export ERL_FLAGS="-mongodb use_legacy_protocol $1 $3"
    rebar3 ct
    rebar3 eunit
    docker stop mongo_test
}

# Test with and without legacy API in a version of MongoDB that supports both APIs

test_with_legacy_on true 5.0.14 ""
test_with_legacy_on false 5.0.14 ""

# Test with the modern API in a version that only support the modern API

test_with_legacy_on false 6.0 ""

# Test with the legacy API in a version that only support the legacy API

test_with_legacy_on true 3.2 ""

# Test automatic protocol detection with several different versisons

for VERSION in 3.0 3.2 5.0.14 6.0
do
    test_with_legacy_on auto "$VERSION" ""
    test_with_legacy_on auto "$VERSION" "-mongodb test_auth_login $LOGIN -mongodb test_auth_password $PASSWORD"
done

# Test that we can connect to a replica set and run mongo_api_SUITE

( rm test/replica_set_setup/data/replica_set_initialized || true )

( cd test/replica_set_setup/ && docker-compose up -d )

while [ ! -f test/replica_set_setup/data/replica_set_initialized ]
do
    sleep 1
done

sleep 1

export ERL_FLAGS='-mongodb test_mongo_api_connection_type replica_set'

ERL_FLAGS="$ERL_FLAGS" rebar3 ct --suite mongo_api_SUITE --case count_test,find_one_test,find_test,upsert_and_update_test

( cd test/replica_set_setup/ && docker-compose down )
