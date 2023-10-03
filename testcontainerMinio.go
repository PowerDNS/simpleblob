package s3

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type S3Testcontainer struct {
	testcontainers.Container
	URI string
}

func StartS3Container(ctx context.Context) (*S3Testcontainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "quay.io/minio/minio",
		ExposedPorts: []string{"9000/tcp", "9001/tcp"},
		Cmd:          []string{"server", "/data", "--console-address", ":9001"},
		WaitingFor:   wait.ForHTTP("/minio/health/ready").WithPort("9000"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	mappedPort, err := container.MappedPort(ctx, "9000")
	if err != nil {
		return nil, err
	}
	hostIP, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}
	uri := fmt.Sprintf("http://%s:%s", hostIP, mappedPort.Port())
	return &S3Testcontainer{Container: container, URI: uri}, nil
}
