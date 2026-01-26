package azure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azurite"

	"github.com/PowerDNS/simpleblob/tester"
)

var azuriteContainer *azurite.AzuriteContainer

func getBackend(ctx context.Context, t *testing.T) (b *Backend) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	azuriteContainer, err := azurite.Run(ctx, "mcr.microsoft.com/azure-storage/azurite")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(azuriteContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
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

	// using the built-in shared key credential type
	cred, err := azblob.NewSharedKeyCredential(azurite.AccountName, azurite.AccountKey)
	if err != nil {
		t.Fatalf("failed to create shared key credential: %v", err)
	}

	// create an azblob.Client for the specified storage account that uses the above credentials
	blobServiceURL := fmt.Sprintf("%s/%s", azuriteContainer.MustServiceURL(ctx, azurite.BlobService), azurite.AccountName)

	client, err := azblob.NewClientWithSharedKeyCredential(blobServiceURL, cred, nil)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	b, err = New(ctx, Options{
		EndpointURL:     blobServiceURL,
		AccountName:     azurite.AccountName,
		AccountKey:      azurite.AccountKey,
		Container:       "test-container",
		CreateContainer: true,
	})

	b.client = client
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
