#!/usr/bin/env bash

set -o errexit
set -o nounset

function before_script
{
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
        ./tools/riak-cluster-config "docker exec $RIAK_FLAVOR riak-admin" 8098 false false
    else
        sudo ./tools/travis-ci/riak-install -d "$RIAK_DOWNLOAD_URL"
        sudo ./tools/setup-riak -s
    fi
}

function script
{
    case $RIAK_FLAVOR in
        'riak-kv')
            local yz='true'
            if [[ $RIAK_VERSION == '2.0.7' ]]
            then
                # NB: not running YZ due to unpredictable behavior
                # when creating indexes
                yz='false'
            fi
            sudo riak-admin security disable
            make RUN_YOKOZUNA="$yz" test
            sudo riak-admin security enable
            make RUN_YOKOZUNA='false' integration-test-security
            ;;
        'riak-ts')
            make RUN_YOKOZUNA='false' integration-test-timeseries
            ;;
        *)
            echo '$RIAK_FLAVOR must be riak-kv or riak-ts' 1>&2
            exit 1
    esac
}

function after_script
{
    docker rm -f $RIAK_FLAVOR
}

set +o nounset
if [[ -z $RIAK_FLAVOR ]]
then
    echo 'RIAK_FLAVOR must be set' 1>&2
    exit 1
fi
if [[ -z $RIAK_DOWNLOAD_URL ]]
then
    echo 'RIAK_DOWNLOAD_URL must be set' 1>&2
    exit 1
fi
set -o nounset

declare -r arg="${1:-'unset'}"
case "$arg" in
    'before_script')
        before_script;;
    'script')
        script;;
    'after_script')
        after_script;;
    *)
        echo 'arg must be before_script, script or after_script' 1>&2
        exit 1;;
esac
