package s3

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/PowerDNS/simpleblob/tester"
)

// TestConfigPathEnv is the path to a YAML file with the Options
// with an S3 bucket configuration that can be used for testing.
// The bucket will be emptied before every run!!!
//
// To run a Minio for this :
//
//	env MINIO_ROOT_USER=test MINIO_ROOT_PASSWORD=secret minio server /tmp/test-data/
//
// Example test config:
//
//	{
//	  "access_key": "test",
//	  "secret_key": "verysecret",
//	  "region": "us-east-1",
//	  "bucket": "test-bucket",
//	  "endpoint_url": "http://127.0.0.1:9000"
//	}
const TestConfigPathEnv = "SIMPLEBLOB_TEST_S3_CONFIG"

func getBackend(ctx context.Context, t *testing.T) (b *Backend) {
	cfgPath := os.Getenv(TestConfigPathEnv)
	if cfgPath == "" {
		t.Skipf("S3 tests skipped, set the %s env var to run these", TestConfigPathEnv)
		return
	}

	cfgContents, err := os.ReadFile(cfgPath)
	require.NoError(t, err)

	var opt Options
	err = yaml.Unmarshal(cfgContents, &opt)
	require.NoError(t, err)

	b, err = New(ctx, opt)
	require.NoError(t, err)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

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
	t.Cleanup(cleanup)
	cleanup()

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
    	// NB: Those tests are for PrefixFolders, a deprecated options.

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

	// List all - HideFolders enabled
	b.opt.HideFolders = true
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar-1", "bar-2"}, ls.Names())

	assert.Len(t, b.lastMarker, 0)
}
