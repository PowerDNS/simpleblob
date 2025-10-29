package s3

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainersminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/PowerDNS/simpleblob/tester"
)

func getBackend(ctx context.Context, t *testing.T) (b *Backend) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
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

func getBadBackend(ctx context.Context, url string, t *testing.T) (b *Backend) {
	b, err := New(ctx, Options{
		EndpointURL:  url,
		AccessKey:    "foo",
		SecretKey:    "bar",
		Bucket:       "test-bucket",
		CreateBucket: false,
		DialTimeout:  1 * time.Second,
	})
	require.NoError(t, err)
	return b
}

func TestBackend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b := getBackend(ctx, t)
	tester.DoBackendTests(t, b)
	assert.Len(t, b.lastMarker, 0)
}

func TestMetrics(t *testing.T) {
	bTimeout := getBadBackend(context.Background(), "http://1.2.3.4:1234", t)

	_, err := bTimeout.List(context.Background(), "")
	assert.Error(t, err)

	expectedMetric := "# HELP storage_s3_call_error_by_type_total S3 API call errors by method and error type\n# TYPE storage_s3_call_error_by_type_total counter\nstorage_s3_call_error_by_type_total{error=\"Timeout\",method=\"list\"} 1\nstorage_s3_call_error_by_type_total{error=\"NotFound\",method=\"load\"} 3\n"

	err = testutil.CollectAndCompare(metricCallErrorsType, strings.NewReader(expectedMetric), "storage_s3_call_error_by_type_total")
	assert.NoError(t, err)

	bBadHost := getBadBackend(context.Background(), "http://nosuchhost:1234", t)

	_, err = bBadHost.List(context.Background(), "")
	assert.Error(t, err)

	expectedMetric += "storage_s3_call_error_by_type_total{error=\"DNSError\",method=\"list\"} 1\n"

	err = testutil.CollectAndCompare(metricCallErrorsType, strings.NewReader(expectedMetric), "storage_s3_call_error_by_type_total")
	assert.NoError(t, err)
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
	// NB: Those tests are for PrefixFolders, a deprecated option.

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

func TestHideFolders(t *testing.T) {
	// NB: working with folders with S3 is flaky, because a `foo` key
	// will shadow all `foo/*` keys while listing,
	// even though those `foo/*` keys exist and they hold the values they're expected to.

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := getBackend(ctx, t)
	b.opt.HideFolders = true

	p := []byte("123")
	err := b.Store(ctx, "foo", p)
	assert.NoError(t, err)
	err = b.Store(ctx, "bar/baz", p)
	assert.NoError(t, err)

	t.Run("at root", func(t *testing.T) {
		ls, err := b.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, []string{"foo"}, ls.Names())
	})

	t.Run("with List prefix", func(t *testing.T) {
		ls, err := b.List(ctx, "bar/")
		assert.NoError(t, err)
		assert.Equal(t, []string{"bar/baz"}, ls.Names())
	})

	t.Run("with global prefix", func(t *testing.T) {
		b.setGlobalPrefix("bar/")
		ls, err := b.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, []string{"baz"}, ls.Names())
	})
}
