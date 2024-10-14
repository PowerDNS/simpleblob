#!/usr/bin/env sh

set -ex

go test -count=1 "$@" ./...

# Configure linters in .golangci.yml
expected_version=v1.50.1
go install github.com/golangci/golangci-lint/cmd/golangci-lint@"$expected_version"
golangci-lint run
