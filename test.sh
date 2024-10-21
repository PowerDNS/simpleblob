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

# Run linters
# Configure linters in .golangci.yml
expected_version=1.61.0
expected_version_full=v"$expected_version"
golangci_lint_bin=./bin/golangci-lint
if ! "$golangci_lint_bin" version | grep -wq "version $expected_version"; then
    # This is the recommended installation process.
    # The installer downloads to ./bin/ by default.
    # see https://golangci-lint.run/usage/install/
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s "$expected_version_full"
fi
"$golangci_lint_bin" run
