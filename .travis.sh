#!/usr/bin/env bash

set -o errexit
set -o nounset

if [[ -z $RIAK_FLAVOR ]]
then
    echo 'RIAK_FLAVOR must be set' 1>&2
    exit 1
fi

if [[ $RIAK_DOWNLOAD_URL == 'docker' ]]
then
    if [[ $RIAK_FLAVOR == 'riak-ts' ]]
    then
        TAG='1.4.0'
    else
        TAG='latest'
    fi
    docker run --name "$RIAK_FLAVOR" -d -p 8087:8087 -p 8098:8098 "basho/$RIAK_FLAVOR:$TAG"
    docker exec "$RIAK_FLAVOR" riak-admin wait-for-service riak_kv
    ./tools/devrel/riak-cluster-config "docker exec $RIAK_FLAVOR riak-admin" 8098 false false
else
    sudo ./tools/travis-ci/riak-install -d "$RIAK_DOWNLOAD_URL"
    sudo ./tools/devrel/riak-cluster-config "$(which riak-admin)" 8098 false false
fi
