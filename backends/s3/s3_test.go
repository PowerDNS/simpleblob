package s3

import (
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainersminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/PowerDNS/simpleblob/tester"
)

func getBackend(ctx context.Context, t *testing.T) (b *Backend) {
	container, err := testcontainersminio.Run(ctx, "quay.io/minio/minio")
	if err != nil {
		t.Fatal(err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}

	b, err = New(ctx, Options{
		EndpointURL:  "http://" + url,
		AccessKey:    container.Username,
		SecretKey:    container.Password,
		Bucket:       "test-bucket",
		CreateBucket: true,
	})
	require.NoError(t, err)

	cleanStorage := func(ctx context.Context) {
		blobs, err := b.List(ctx, "")
		if err != nil {
			t.Logf("Blobs list error: %s", err)
			return
		}
		for _, blob := range blobs {
			err := b.Delete(ctx, blob.Name)
			if err != nil {
				t.Logf("Object delete error: %s", err)
			}
		}
		// This one is not returned by the List command
		err = b.client.RemoveObject(ctx, b.opt.Bucket, b.markerName, minio.RemoveObjectOptions{})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanStorage(ctx)
		if err := container.Terminate(ctx); err != nil {
			t.Log(err)
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
	// ^ reflects last write operation of tester.DoBackendTests
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

func TestBackend_recursive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := getBackend(ctx, t)

	// Starts empty
	ls, err := b.List(ctx, "")
	assert.NoError(t, err)
	assert.Len(t, ls, 0)

	// Add items
	err = b.Store(ctx, "bar-1", []byte("bar1"))
	assert.NoError(t, err)
	err = b.Store(ctx, "bar-2", []byte("bar2"))
	assert.NoError(t, err)
	err = b.Store(ctx, "foo/bar-3", []byte("bar3"))
	assert.NoError(t, err)

	// List all - PrefixFolders disabled (default)
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2", "foo/bar-3"})

	// List all - PrefixFolders enabled
	b.opt.PrefixFolders = true

	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2", "foo/"})

	// List all - PrefixFolders disabled
	b.opt.PrefixFolders = false

	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2", "foo/bar-3"})

	assert.Len(t, b.lastMarker, 0)
}
