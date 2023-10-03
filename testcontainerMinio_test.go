package s3

import (
	"context"
	"github.com/PowerDNS/simpleblob/tester"
	"github.com/stretchr/testify/assert"
	"testing"
)

const testMinioUser = "minioadmin"
const testMinioPass = "minioadmin"
const testMinioBucket = "test"

func getWithContainerBackend(ctx context.Context, t *testing.T) *Backend {
	mioContainer, err := StartS3Container(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err = mioContainer.Terminate(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
	opt := Options{
		AccessKey:    testMinioUser,
		SecretKey:    testMinioPass,
		Bucket:       testMinioBucket,
		CreateBucket: true,
		EndpointURL:  mioContainer.URI,
	}
	b, err := New(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWithContainerBackend(t *testing.T) {
	ctx := context.Background()

	b := getWithContainerBackend(ctx, t)
	tester.DoBackendTests(t, b)
	assert.Len(t, b.lastMarker, 0)
}

func TestWithContainerBackend_marker(t *testing.T) {
	ctx := context.Background()

	b := getWithContainerBackend(ctx, t)
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

func TestWithContainerBackend_globalprefix(t *testing.T) {
	ctx := context.Background()

	b := getWithContainerBackend(ctx, t)
	b.setGlobalPrefix("v5/")

	tester.DoBackendTests(t, b)
	assert.Empty(t, b.lastMarker)
}

func TestWithContainerBackend_globalPrefixAndMarker(t *testing.T) {
	ctx := context.Background()
	// Start the backend over
	b := getWithContainerBackend(ctx, t)
	b.setGlobalPrefix("v6/")
	b.opt.UseUpdateMarker = true

	tester.DoBackendTests(t, b)
	assert.NotEmpty(t, b.lastMarker)
}

func TestWithContainerBackend_recursive(t *testing.T) {
	ctx := context.Background()

	b := getWithContainerBackend(ctx, t)

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
