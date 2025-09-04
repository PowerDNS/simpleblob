#!/usr/bin/env sh

set -ex

go test -count=1 "$@" ./...

# Run linters
# Configure linters in .golangci.yml
expected_version=2.4.0
expected_version_full=v"$expected_version"
golangci_lint_bin=./bin/golangci-lint
if ! "$golangci_lint_bin" version | grep -wq "version $expected_version"; then
    # This is the recommended installation process.
    # The installer downloads to ./bin/ by default.
    # see https://golangci-lint.run/usage/install/
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s "$expected_version_full"
fi
"$golangci_lint_bin" run
