#!/usr/bin/env sh
# Run all tests for simpleblob.
# The arguments passed to this script will be passed to the `go test` command.
#
# Note that the tests rely on testcontainers.
# In case Podman is available, it is used.
# You can bypass Podman or force the use of a specific backend by setting DOCKER_HOST,
# with a command such as:
# 	docker context ls --format '{{if .Current}}{{.DockerEndpoint}}{{end}}'
set -eu

# Try to use Podman by default.
if [ -n "${DOCKER_HOST:-}" ]; then
    printf 'DOCKER_HOST is set in environment, %s will be used.\n' "$DOCKER_HOST"
elif command -v podman >/dev/null; then
    # See https://podman-desktop.io/tutorial/testcontainers-with-podman#setup-testcontainers-with-podman
    kernel_name="$(uname -s)"
    case "$kernel_name" in
        Linux)  socket_path="${XDG_RUNTIME_DIR:-/run/user/$(id --user)}/podman/podman.sock"
            ;;
        Darwin) socket_path="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
            ;;
        *) printf '%s is not supported.\n' "$kernel_name"; exit 1
            ;;
    esac
    if [ -e "$socket_path" ]; then
        export DOCKER_HOST="unix://${socket_path}"
        # TESTCONTAINERS_RYUK_DISABLED improves stability because the Ryuk container,
        # used for cleanup, does not always work with Podman.
        export TESTCONTAINERS_RYUK_DISABLED=true
    fi
fi

echo 'Running tests'
go test -count=1 "$@" ./...

echo
echo 'Running linters'
expected_version=2.8.0
golangci_lint_bin=./bin/golangci-lint
if ! "$golangci_lint_bin" version | grep -wq "version $expected_version"; then
    # This is the recommended installation process.
    # The installer downloads to ./bin/ by default.
    # see https://golangci-lint.run/usage/install/
    echo "Downloading golangci-lint v$expected_version to ./bin/"
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v"$expected_version"
fi
exec "$golangci_lint_bin" run
