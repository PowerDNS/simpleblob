package azure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/azurite"

	"github.com/PowerDNS/simpleblob/tester"
)

var azuriteContainer *azurite.Container

func getBackend(ctx context.Context, t *testing.T) (b *Backend) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	azuriteContainer, err := azurite.Run(ctx, "mcr.microsoft.com/azure-storage/azurite:3.35.0", azurite.WithEnabledServices(azurite.BlobService))
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			t.Errorf("failed to terminate container: %s", err)
		}
	})

	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	state, err := azuriteContainer.State(ctx)
	if err != nil {
		t.Fatalf("failed to get container state: %v", err)
	}

	t.Log(state.Running)

	serviceURL, err := azuriteContainer.BlobServiceURL(ctx)
	require.NoError(t, err)

	// create an azblob.Client for the specified storage account that uses the above credentials
	blobServiceURL := fmt.Sprintf("%s/%s", serviceURL, azurite.AccountName)

	b, err = New(ctx, Options{
		EndpointURL:     blobServiceURL,
		UseSharedKey:    true,
		AccountName:     azurite.AccountName,
		AccountKey:      azurite.AccountKey,
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
	})
	cleanStorage(ctx)

	return b
}

func tearDown(t *testing.T) {
	if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
		t.Fatalf("failed to terminate container: %v", err)
	}
}

func TestBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer tearDown(t)

	b := getBackend(ctx, t)
	tester.DoBackendTests(t, b)
	assert.Len(t, b.lastMarker, 0)
}

func TestBackend_marker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer tearDown(t)

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
	defer tearDown(t)

	b := getBackend(ctx, t)
	b.setGlobalPrefix("v5/")

	tester.DoBackendTests(t, b)
	assert.Empty(t, b.lastMarker)
}

func TestBackend_globalPrefixAndMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	defer tearDown(t)

	// Start the backend over
	b := getBackend(ctx, t)
	b.setGlobalPrefix("v6/")
	b.opt.UseUpdateMarker = true

	tester.DoBackendTests(t, b)
	assert.NotEmpty(t, b.lastMarker)
}
