package s3

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
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
		if exists, _ := b.client.BucketExists(ctx, b.opt.Bucket); !exists {
			return
		}
		blobs, _ := b.List(ctx, "")
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

func TestMetrics(t *testing.T) {
	metricName := "storage_s3_call_error_by_type_total"
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
	_ = b.client.RemoveBucket(t.Context(), b.opt.Bucket)
	assert.Error(t, b.Store(t.Context(), "no-more-bucket", []byte{})) // Fails due to missing bucket
	assert.Equal(t, 1, testutil.CollectAndCount(metricCallErrorsType, metricName))
	p, err := testutil.CollectAndFormat(metricCallErrorsType, expfmt.TypeProtoCompact, metricName)
	require.NoError(t, err)
	assert.Regexp(t, `label:{name:"method"\s+value:"store"}`, string(p))
	assert.Regexp(t, `label:{name:"error"\s+value:"NoSuchBucket"}`, string(p))
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

// TestClientTimeout makes sure the ClientTimeout option is taken into consideration.
func TestClientTimeout(t *testing.T) {
	b := getBackend(t.Context(), t)
	t.Run("basic", func(t *testing.T) {
		b.opt.ClientTimeout = time.Microsecond
		ctx, _ := b.clientTimeoutContext(t.Context())
		time.Sleep(time.Second)
		require.ErrorIs(t, context.Cause(ctx), ErrClientTimeout)
	})
	t.Run("crud", func(t *testing.T) {
		b.opt.ClientTimeout = time.Microsecond
		ctx := t.Context()
		assert.ErrorIs(t, b.Store(ctx, "crudTest", []byte("123")), ErrClientTimeout)
		_, err := b.Load(ctx, "crudTest")
		assert.ErrorIs(t, err, ErrClientTimeout)
		assert.ErrorIs(t, b.Delete(ctx, "delete"), ErrClientTimeout)
		_, err = b.List(ctx, "")
		assert.ErrorIs(t, err, ErrClientTimeout)
	})
	t.Run("stream write", func(t *testing.T) {
		b.opt.ClientTimeout = time.Millisecond
		ctx := t.Context()
		w, err := b.NewWriter(ctx, "failOnWriteTest")
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
		_, err = w.Write([]byte("123"))
		require.ErrorIs(t, err, ErrClientTimeout)
		require.ErrorIs(t, w.Close(), ErrClientTimeout)
	})
	t.Run("stream read", func(t *testing.T) {
		b.opt.ClientTimeout = 0 // Reset
		ctx := t.Context()
		require.NoError(t, b.Store(ctx, "failOnReadTest", []byte("123"))) // avoid ErrNoExist

		b.opt.ClientTimeout = time.Millisecond
		r, err := b.NewReader(ctx, "failOnReadTest")
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
		_, err = r.Read(make([]byte, 1))
		require.ErrorIs(t, err, ErrClientTimeout)
		require.ErrorIs(t, r.Close(), ErrClientTimeout)
	})
}

// Ensure that in Minio Client, context cancellation aborts
// an ongoing PutObject or GetObject operation.
func TestMinioKeepsContext(t *testing.T) {
	t.Parallel()
	t.Run("putObject", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		b := getBackend(ctx, t)
		_, err := b.client.PutObject(ctx, b.opt.Bucket, "putObject", readerOnce(cancel), -1, minio.PutObjectOptions{})
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("getObject", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		b := getBackend(ctx, t)
		require.NoError(t, b.Store(ctx, "getObject", []byte("123"))) // avoid ErrNoExist
		obj, err := b.client.GetObject(ctx, b.opt.Bucket, "getObject", minio.GetObjectOptions{})
		require.NoError(t, err)
		cancel()
		_, err = io.Copy(io.Discard, obj)
		require.ErrorIs(t, err, context.Canceled)
	})
}

type readerOnce context.CancelFunc

func (f readerOnce) Read(p []byte) (int, error) { f(); return len(p), nil }
