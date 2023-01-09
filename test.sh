#!/usr/bin/env bash

set -e

# Stop all child processes on exit
trap cleanup EXIT

cleanup() {
    pkill -P $$
    if [ -n "$tmpdir" ] && [ -d "$tmpdir" ]; then
        rm -r "$tmpdir"
    fi
}

export GOBIN="$PWD/bin"
export PATH="$GOBIN:$PATH"
tmpdir=

mkdir -p "$GOBIN"

if [ -z "$SIMPLEBLOB_TEST_S3_CONFIG" ]; then
    echo "* Using MinIO for S3 tests"
    export SIMPLEBLOB_TEST_S3_CONFIG="$PWD/test-minio.json"

    # Fetch minio if not found
    if ! command -v minio >/dev/null; then
        dst="$GOBIN/minio"
        curl -v -o "$dst" "https://dl.min.io/server/minio/release/$(go env GOOS)-$(go env GOARCH)/minio"
        chmod u+x "$dst"
    fi

    # Start MinIO
    echo "* Starting minio at address 127.0.0.1:34730"
    tmpdir=$(mktemp -d -t minio.XXXXXX)
    minio server --address 127.0.0.1:34730 --console-address 127.0.0.1:34731 --quiet "$tmpdir" &
    # Wait for minio server to be ready
    i=0
    while ! curl -s -I "127.0.0.1:34730/minio/health/ready" | grep '200 OK' >/dev/null; do
        i=$((i+1))
        if [ "$i" -ge 10 ]; then
            # We have been waiting for server to start for 10 seconds
            echo "Minio could not start properly"
            curl -s -I "127.0.0.1:34730/minio/health/ready"
            exit 1
        fi
        sleep 1
    done
fi

echo "* SIMPLEBLOB_TEST_S3_CONFIG=$SIMPLEBLOB_TEST_S3_CONFIG"

set -ex

go test -count=1 "$@" ./...

# Configure linters in .golangci.yml
expected_version=v1.50.1
go install github.com/golangci/golangci-lint/cmd/golangci-lint@"$expected_version"
golangci-lint run
