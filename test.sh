#!/usr/bin/env bash

set -e

# Stop all child processes on exit
trap "pkill -P $$" EXIT

export GOBIN="$PWD/bin"

if [ -z "$SIMPLEBLOB_TEST_S3_CONFIG" ]; then
    echo "* Using MinIO for S3 tests"
    export SIMPLEBLOB_TEST_S3_CONFIG="$PWD/test-minio.json"

    # Check for existing minio
    minio=$(which minio || true)

    # Fetch minio if not found
    if [ -z "$minio" ]; then
        #minioversion="github.com/minio/minio@v0.0.0-20220420232007-ddf84f8257c9"
        #echo "+ go install $minioversion" > /dev/stderr
        #go install "$minioversion"
        source <(go env)
        curl -v -o "$GOBIN/minio" "https://dl.min.io/server/minio/release/$GOOS-$GOARCH/minio"
        minio=./bin/minio
        chmod 6755 "$minio"
    fi

    # Start MinIO
    echo "* Starting $minio on port 34730"
    tmpdir=$(mktemp -d -t minio)
    "$minio" server --address 127.0.0.1:34730 --console-address 127.0.0.1:34731 --quiet "$tmpdir" &
    sleep 3
fi

echo "* SIMPLEBLOB_TEST_S3_CONFIG=$SIMPLEBLOB_TEST_S3_CONFIG"

set -ex

go test -count=1 "$@" ./...

# Configure linters in .golangci.yml
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.45.2
./bin/golangci-lint run

