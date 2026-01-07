#!/usr/bin/env sh
# Run all tests for simpleblob.
# The arguments passed to this script will be passed to the `go test` command.
#
# Note that the tests rely on testcontainers.
# In case no Docker daemon is configured, we fallback to Podman.
# Using Podman can be forced by setting the SB_TESTS_FORCE_PODMAN to a non-empty value.
set -e

# Default to Docker but use Podman if any of those is true:
#  - DOCKER_HOST is not set.
#  - the `docker` command does not exist.
#  - the Docker daemon is not running.
#  - SB_TESTS_FORCE_PODMAN is not empty.
if [ -z "${SB_TESTS_FORCE_PODMAN}${DOCKER_HOST}" ] && command -v docker >/dev/null; then
    docker_endpoint="$(docker context ls --format '{{if .Current}}{{.DockerEndpoint}}{{end}}' 2>/dev/null || true)"
fi
if [ "$SB_TESTS_FORCE_PODMAN" ] || [ -z "${DOCKER_HOST}${docker_endpoint}" ]; then
    # See https://podman-desktop.io/tutorial/testcontainers-with-podman#setup-testcontainers-with-podman
    if ! command -v podman >/dev/null; then
        echo "Neither Podman CLI or a Docker daemon were found."
        exit 1
    fi
    case "$(uname -s)" in
        Linux)
            DOCKER_HOST="unix://${XDG_RUNTIME_DIR:-/run/user/$(id --user)}/podman/podman.sock"
            ;;
        Darwin)
            DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
            ;;
        *)
            echo "$(uname -s) is not supported."
            exit 1
            ;;
    esac
    if [ ! -e "${DOCKER_HOST#unix://}" ]; then
        echo "Podman does not seem to be ready to accept connections."
        echo "Please ensure you have started a Podman machine or system service."
        # On macOS, this is most likely to be `podman machine start`.
        # On Linux, you may have a service available, or you can run `podman system service --time=0`.
        exit 1
    fi
    # TESTCONTAINERS_RYUK_DISABLED improves stability because the Ryuk container,
    # used for cleanup, does not always work with Podman.
    export TESTCONTAINERS_RYUK_DISABLED=true
    export DOCKER_HOST
fi

echo 'Running tests'
go test -count=1 "$@" ./...

echo
echo 'Running linters'
expected_version=2.4.0
golangci_lint_bin=./bin/golangci-lint
if ! "$golangci_lint_bin" version | grep -wq "version $expected_version"; then
    # This is the recommended installation process.
    # The installer downloads to ./bin/ by default.
    # see https://golangci-lint.run/usage/install/
    echo "Downloading golangci-lint v$expected_version to ./bin/"
    curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v"$expected_version"
fi
exec "$golangci_lint_bin" run
