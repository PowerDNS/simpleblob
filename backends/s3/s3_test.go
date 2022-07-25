package s3

import (
	"context"
	"io/ioutil"
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
//     env MINIO_ROOT_USER=test MINIO_ROOT_PASSWORD=secret minio server /tmp/test-data/
//
// Example test config:
//
//     {
//       "access_key": "test",
//       "secret_key": "verysecret",
//       "region": "us-east-1",
//       "bucket": "test-bucket",
//       "endpoint_url": "http://127.0.0.1:9000"
//     }
//
const TestConfigPathEnv = "SIMPLEBLOB_TEST_S3_CONFIG"

func getBackend(ctx context.Context, t *testing.T) (b *Backend) {
	cfgPath := os.Getenv(TestConfigPathEnv)
	if cfgPath == "" {
		t.Skipf("S3 tests skipped, set the %s env var to run these", TestConfigPathEnv)
		return
	}

	cfgContents, err := ioutil.ReadFile(cfgPath)
	require.NoError(t, err)

	var opt Options
	err = yaml.Unmarshal(cfgContents, &opt)
	require.NoError(t, err)

	b, err = New(ctx, opt)
	require.NoError(t, err)

	cleanup := func() {
    		ctx, cancel := context.WithCancel(context.Background())
    		defer cancel()

		blobs, err := b.doList(ctx, "")
		if err != nil {
			t.Logf("Blobs list error: %s", err)
			return
		}
		for _, blob := range blobs {
    			err := b.client.RemoveObject(ctx, b.opt.Bucket, blob.Name, minio.RemoveObjectOptions{})
			if err != nil {
				t.Logf("Object delete error: %s", err)
			}
		}
		// This one is not returned by the List command
    		err = b.client.RemoveObject(ctx, b.opt.Bucket, UpdateMarkerFilename, minio.RemoveObjectOptions{})
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
	assert.Equal(t, "", b.lastMarker)
}

func TestBackend_marker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := getBackend(ctx, t)
	b.opt.UseUpdateMarker = true

	tester.DoBackendTests(t, b)
	assert.Equal(t, "foo-1", b.lastMarker)

	data, err := b.Load(ctx, UpdateMarkerFilename)
	assert.NoError(t, err)
	assert.Equal(t, b.lastMarker, string(data))
}
