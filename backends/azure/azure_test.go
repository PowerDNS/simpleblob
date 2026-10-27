package azure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/PowerDNS/simpleblob/tester"
)

func getBackend(ctx context.Context, t *testing.T) (b *Backend) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	const (
		blobPort = "10000"
		port     = blobPort + "/tcp"
		// accountName and accountKey are the default testing account name and key used by Azurite
		// Those were copied from github.com/testcontainers/testcontainers-go/modules/azure/azurite.
		accountName = "devstoreaccount1"
		accountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	)

	azuriteContainer, err := testcontainers.Run(t.Context(), "mcr.microsoft.com/azure-storage/azurite",
		testcontainers.WithEntrypoint("azurite"),
		testcontainers.WithCmd("--blobPort", blobPort, "--blobHost", "0.0.0.0", "--skipApiVersionCheck"),
		testcontainers.WithExposedPorts(port),
		testcontainers.WithWaitStrategy(wait.ForListeningPort(port)),
	)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			t.Errorf("failed to terminate container: %s", err)
		}
	})

	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	serviceURL, err := azuriteContainer.PortEndpoint(ctx, port, "http")
	require.NoError(t, err)

	// create an azblob.Client for the specified storage account that uses the above credentials
	blobServiceURL := fmt.Sprintf("%s/%s", serviceURL, accountName)

	b, err = New(ctx, Options{
		EndpointURL:     blobServiceURL,
		UseSharedKey:    true,
		AccountName:     accountName,
		AccountKey:      accountKey,
		Container:       "test-container",
		CreateContainer: true,
	})
	require.NoError(t, err)

	cleanStorage := func(ctx context.Context) {
		blobs, err := b.List(ctx, "")
		if err != nil {
			t.Fatalf("Blobs list error: %v", err)
		}

		for _, blob := range blobs {
			err := b.Delete(ctx, blob.Name)
			if err != nil {
				t.Fatalf("Object delete error: %v", err)
			}
		}

		require.NoError(t, err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanStorage(ctx)
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			t.Fatalf("failed to terminate container: %v", err)
		}
	})
	cleanStorage(ctx)

	return b
}

func TestBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := getBackend(ctx, t)
	tester.DoBackendTests(t, b)
	assert.Len(t, b.lastMarker, 0)
}

func TestBackend_marker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := getBackend(ctx, t)
	b.opt.UseUpdateMarker = true

	tester.DoBackendTests(t, b)
	assert.Regexp(t, "^foo-1:[A-Za-z0-9]*:[0-9]+:true$", b.lastMarker)
	// ^ reflects last write operation of tester.DoBackendTestsAzure
	//   i.e. deleting "foo-1"

	// Marker file should have been written accordingly
	markerFileContent, err := b.Load(ctx, UpdateMarkerFilename)
	assert.NoError(t, err)
	assert.EqualValues(t, b.lastMarker, markerFileContent)
}

func TestBackend_globalprefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := getBackend(ctx, t)
	b.setGlobalPrefix("v5/")

	tester.DoBackendTests(t, b)
	assert.Empty(t, b.lastMarker)
}

func TestBackend_globalPrefixAndMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start the backend over
	b := getBackend(ctx, t)
	b.setGlobalPrefix("v6/")
	b.opt.UseUpdateMarker = true

	tester.DoBackendTests(t, b)
	assert.NotEmpty(t, b.lastMarker)
}

func TestMetrics(t *testing.T) {
	metricName := "storage_azure_call_error_by_type_total"
	metricCallErrorsType.Reset() // Ensure no metrics are left from other tests
	b := getBackend(t.Context(), t)

	// Operations on a healthy backend do not collect any metrics.
	assert.Equal(t, 0, testutil.CollectAndCount(metricCallErrorsType, metricName))
	var (
		_    = b.Store(t.Context(), "my-key", []byte{})
		_, _ = b.Load(t.Context(), "my-key")
		_    = b.Delete(t.Context(), "my-key")
		_, _ = b.Load(t.Context(), "no-such-key") // ErrNotExist is not collected
		_    = b.Delete(t.Context(), "no-such-key")
		_, _ = b.List(t.Context(), "")
	)
	assert.Equal(t, 0, testutil.CollectAndCount(metricCallErrorsType, metricName))

	// A failed operation generates metrics.
	savedContainer := b.opt.Container
	b.opt.Container = "non-existent-container"
	assert.Error(t, b.Store(t.Context(), "file-in-non-existent-container", []byte{})) // Fails due to missing bucket
	b.opt.Container = savedContainer
	assert.Equal(t, 1, testutil.CollectAndCount(metricCallErrorsType, metricName))
	p, err := testutil.CollectAndFormat(metricCallErrorsType, expfmt.TypeProtoCompact, metricName)
	require.NoError(t, err)
	assert.Regexp(t, `label:{name:"method"\s+value:"store"}`, string(p))
	assert.Regexp(t, `label:{name:"error"\s+value:"ContainerNotFound"}`, string(p))
}
