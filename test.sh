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
export PATH="$PATH:$GOBIN"
tmpdir=

mkdir -p "$GOBIN"

if [ -z "$SIMPLEBLOB_TEST_S3_CONFIG" ]; then
    echo "* Using MinIO for S3 tests"
    export SIMPLEBLOB_TEST_S3_CONFIG="$PWD/test-minio.json"

    # Fetch minio if not found
    if ! command -v minio >/dev/null; then
        source <(go env)
        dst="$GOBIN/minio"
        curl -v -o "$dst" "https://dl.min.io/server/minio/release/$GOOS-$GOARCH/minio"
        chmod u+x "$dst"
    fi

    # Start MinIO
    echo "* Starting minio at address 127.0.0.1:34730"
    tmpdir=$(mktemp --directory --tmpdir=. .minio.XXXXXX)
    minio server --address 127.0.0.1:34730 --console-address 127.0.0.1:34731 --quiet "$tmpdir" &
    # Wait for minio server to be ready
    while ! curl -s -I "127.0.0.1:34730/minio/health/ready" | grep '200 OK' >/dev/null; do
        sleep .1
    done
fi

echo "* SIMPLEBLOB_TEST_S3_CONFIG=$SIMPLEBLOB_TEST_S3_CONFIG"

set -ex

go test -count=1 "$@" ./...

# Configure linters in .golangci.yml
if ! command -v golangci-lint >/dev/null; then
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.45.2
fi
golangci-lint run

