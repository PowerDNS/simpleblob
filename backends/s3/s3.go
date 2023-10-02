package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PowerDNS/go-tlsconfig"
	"github.com/go-logr/logr"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/PowerDNS/simpleblob"
)

const (
	// DefaultEndpointURL is the default S3 endpoint to use if none is set.
	// Here, no custom endpoint assumes AWS endpoint.
	DefaultEndpointURL = "https://s3.amazonaws.com"
	// DefaultRegion is the default S3 region to use, if none is configured
	DefaultRegion = "us-east-1"
	// DefaultInitTimeout is the time we allow for initialisation, like credential
	// checking and bucket creation. We define this here, because we do not
	// pass a context when initialising a plugin.
	DefaultInitTimeout = 20 * time.Second
	// UpdateMarkerFilename is the filename used for the update marker functionality
	UpdateMarkerFilename = "update-marker"
	// DefaultUpdateMarkerForceListInterval is the default value for
	// UpdateMarkerForceListInterval.
	DefaultUpdateMarkerForceListInterval = 5 * time.Minute
)

// Options describes the storage options for the S3 backend
type Options struct {
	// AccessKey and SecretKey are statically defined here.
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`

	// Region defaults to "us-east-1", which also works for Minio
	Region string `yaml:"region"`
	Bucket string `yaml:"bucket"`
	// CreateBucket tells us to try to create the bucket
	CreateBucket bool `yaml:"create_bucket"`

	// GlobalPrefix is a prefix applied to all operations, allowing work within a prefix
	// seamlessly
	GlobalPrefix string `yaml:"global_prefix"`

	// PrefixFolders can be enabled to make List operations show nested prefixes as folders
	// instead of recursively listing all contents of nested prefixes
	PrefixFolders bool `yaml:"prefix_folders"`

	// EndpointURL can be set to something like "http://localhost:9000" when using Minio
	// or "https://s3.amazonaws.com" for AWS S3.
	EndpointURL string `yaml:"endpoint_url"`

	// TLS allows customising the TLS configuration
	// See https://github.com/PowerDNS/go-tlsconfig for the available options
	TLS tlsconfig.Config `yaml:"tls"`

	// InitTimeout is the time we allow for initialisation, like credential
	// checking and bucket creation. It defaults to DefaultInitTimeout, which
	// is currently 20s.
	InitTimeout time.Duration `yaml:"init_timeout"`

	// UseUpdateMarker makes the backend write and read a file to determine if
	// it can cache the last List command. The file contains the name of the
	// last file stored or deleted.
	// This can reduce the number of LIST commands sent to S3, replacing them
	// with GET commands that are about 12x cheaper.
	// If enabled, it MUST be enabled on all instances!
	// CAVEAT: This will NOT work correctly if the bucket itself is replicated
	//         in an active-active fashion between data centers! In that case
	//         do not enable this option.
	UseUpdateMarker bool `yaml:"use_update_marker"`
	// UpdateMarkerForceListInterval is used when UseUpdateMarker is enabled.
	// A LIST command will be sent when this interval has passed without a
	// change in marker, to ensure a full sync even if the marker would for
	// some reason get out of sync.
	UpdateMarkerForceListInterval time.Duration `yaml:"update_marker_force_list_interval"`

	// Not loaded from YAML
	Logger logr.Logger `yaml:"-"`
}

func (o Options) Check() error {
	if o.AccessKey == "" {
		return fmt.Errorf("s3 storage.options: access_key is required")
	}
	if o.SecretKey == "" {
		return fmt.Errorf("s3 storage.options: secret_key is required")
	}
	if o.Bucket == "" {
		return fmt.Errorf("s3 storage.options: bucket is required")
	}
	return nil
}

type Backend struct {
	opt    Options
	config *minio.Options
	client *minio.Client
	log    logr.Logger

	mu         sync.Mutex
	lastMarker string
	lastList   simpleblob.BlobList
	lastTime   time.Time
}

func (b *Backend) List(ctx context.Context, prefix string) (simpleblob.BlobList, error) {
	// Prepend global prefix
	prefix = b.prependGlobalPrefix(prefix)

	if !b.opt.UseUpdateMarker {
		return b.doList(ctx, prefix)
	}

	m, err := b.Load(ctx, UpdateMarkerFilename)
	exists := !errors.Is(err, os.ErrNotExist)
	if err != nil && exists {
		return nil, err
	}
	upstreamMarker := string(m)

	b.mu.Lock()
	mustUpdate := b.lastList == nil ||
		upstreamMarker != b.lastMarker ||
		time.Since(b.lastTime) >= b.opt.UpdateMarkerForceListInterval ||
		!exists
	blobs := b.lastList
	b.mu.Unlock()

	if !mustUpdate {
		return blobs.WithPrefix(prefix), nil
	}

	blobs, err = b.doList(ctx, "") // We want to cache all, so no prefix
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.lastMarker = upstreamMarker
	b.lastList = blobs
	b.lastTime = time.Now()
	b.mu.Unlock()

	return blobs.WithPrefix(prefix), nil
}

func (b *Backend) doList(ctx context.Context, prefix string) (simpleblob.BlobList, error) {
	var blobs simpleblob.BlobList

	// Runes to strip from blob names for GlobalPrefix
	gpEndRune := len(b.opt.GlobalPrefix)

	objCh := b.client.ListObjects(ctx, b.opt.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: !b.opt.PrefixFolders,
	})
	for obj := range objCh {
		// Handle error returned by MinIO client
		if err := convertMinioError(obj.Err); err != nil {
			metricCallErrors.WithLabelValues("list").Inc()
			return nil, err
		}

		metricCalls.WithLabelValues("list").Inc()
		metricLastCallTimestamp.WithLabelValues("list").SetToCurrentTime()
		if obj.Key == UpdateMarkerFilename {
			continue
		}

		// Strip global prefix from blob
		blobName := obj.Key
		if gpEndRune > 0 {
			blobName = blobName[gpEndRune:]
		}

		blobs = append(blobs, simpleblob.Blob{Name: blobName, Size: obj.Size})
	}

	// Minio appears to return them sorted, but maybe not all implementations
	// will, so we sort explicitly.
	sort.Sort(blobs)

	return blobs, nil
}

// Load retrieves the content of the object identified by name from S3 Bucket
// configured in b.
func (b *Backend) Load(ctx context.Context, name string) ([]byte, error) {
	metricCalls.WithLabelValues("load").Inc()
	metricLastCallTimestamp.WithLabelValues("load").SetToCurrentTime()

	r, err := b.doLoadReader(ctx, name)
	if err != nil {
		metricCallErrors.WithLabelValues("load").Inc()
		return nil, err
	}
	defer r.Close()

	p, err := io.ReadAll(r)
	if err = convertMinioError(err); err != nil {
		metricCallErrors.WithLabelValues("load").Inc()
		return nil, err
	}
	return p, nil
}

func (b *Backend) doLoadReader(ctx context.Context, name string) (io.ReadCloser, error) {
	name = b.prependGlobalPrefix(name)

	obj, err := b.client.GetObject(ctx, b.opt.Bucket, name, minio.GetObjectOptions{})
	if err = convertMinioError(err); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, os.ErrNotExist
	}
	info, err := obj.Stat()
	if err = convertMinioError(err); err != nil {
		return nil, err
	}
	if info.Key == "" {
		// minio will return an object with empty fields when name
		// is not present in bucket.
		return nil, os.ErrNotExist
	}
	return obj, nil
}

// Store sets the content of the object identified by name to the content
// of data, in the S3 Bucket configured in b.
func (b *Backend) Store(ctx context.Context, name string, data []byte) error {
	metricCalls.WithLabelValues("store").Inc()
	metricLastCallTimestamp.WithLabelValues("store").SetToCurrentTime()

	info, err := b.doStore(ctx, name, data)
	if err != nil {
		metricCallErrors.WithLabelValues("store").Inc()
		return err
	}
	return b.setMarker(ctx, name, info.ETag, false)
}

func (b *Backend) doStore(ctx context.Context, name string, data []byte) (minio.UploadInfo, error) {
	return b.doStoreReader(ctx, name, bytes.NewReader(data), int64(len(data)))
}

// doStoreReader stores data with key name in S3, using r as a source for data.
// The value of size may be -1, in case the size is not known.
func (b *Backend) doStoreReader(ctx context.Context, name string, r io.Reader, size int64) (minio.UploadInfo, error) {
	name = b.prependGlobalPrefix(name)

	// minio accepts size == -1, meaning the size is unknown.
	info, err := b.client.PutObject(ctx, b.opt.Bucket, name, r, size, minio.PutObjectOptions{
		NumThreads: 3,
	})
	err = convertMinioError(err)
	return info, err
}

// Delete removes the object identified by name from the S3 Bucket
// configured in b.
func (b *Backend) Delete(ctx context.Context, name string) error {
	// Prepend global prefix
	name = b.prependGlobalPrefix(name)

	if err := b.doDelete(ctx, name); err != nil {
		return err
	}
	return b.setMarker(ctx, name, "", true)
}

func (b *Backend) doDelete(ctx context.Context, name string) error {
	metricCalls.WithLabelValues("delete").Inc()
	metricLastCallTimestamp.WithLabelValues("delete").SetToCurrentTime()

	err := b.client.RemoveObject(ctx, b.opt.Bucket, name, minio.RemoveObjectOptions{})
	if err = convertMinioError(err); err != nil {
		metricCallErrors.WithLabelValues("delete").Inc()
	}
	return err
}

// New creates a new backend instance.
// The lifetime of the context passed in must span the lifetime of the whole
// backend instance, not just the init time, so do not set any timeout on it!
func New(ctx context.Context, opt Options) (*Backend, error) {
	if opt.Region == "" {
		opt.Region = DefaultRegion
	}
	if opt.InitTimeout == 0 {
		opt.InitTimeout = DefaultInitTimeout
	}
	if opt.UpdateMarkerForceListInterval == 0 {
		opt.UpdateMarkerForceListInterval = DefaultUpdateMarkerForceListInterval
	}
	if opt.EndpointURL == "" {
		opt.EndpointURL = DefaultEndpointURL
	}
	if err := opt.Check(); err != nil {
		return nil, err
	}

	log := opt.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("s3")

	// Automatic TLS handling
	// This MUST receive a longer running context to be able to automatically
	// reload certificates, so we use the original ctx, not one with added
	// InitTimeout.
	tlsmgr, err := tlsconfig.NewManager(ctx, opt.TLS, tlsconfig.Options{
		IsClient: true,
		Logr:     log.WithName("tls-manager"),
	})
	if err != nil {
		return nil, err
	}
	// Get an opinionated HTTP client that:
	// - Uses a custom tls.Config
	// - Sets proxies from the environment
	// - Sets reasonable timeouts on various operations
	// Check the implementation for details.
	hc, err := tlsmgr.HTTPClient()
	if err != nil {
		return nil, err
	}

	// Some of the following calls require a short running context
	ctx, cancel := context.WithTimeout(ctx, opt.InitTimeout)
	defer cancel()

	// Minio takes only the Host value as the endpoint URL.
	// The connection being secure depends on a key in minio.Option.
	u, err := url.Parse(opt.EndpointURL)
	if err != nil {
		return nil, err
	}
	var useSSL bool
	switch u.Scheme {
	case "http":
		// Ok, no SSL
	case "https":
		useSSL = true
	case "":
		return nil, fmt.Errorf("no scheme provided for endpoint URL %q, use http or https", opt.EndpointURL)
	default:
		return nil, fmt.Errorf("unsupported scheme for S3: %q, use http or https", u.Scheme)
	}

	cfg := &minio.Options{
		Creds:     credentials.NewStaticV4(opt.AccessKey, opt.SecretKey, ""),
		Secure:    useSSL,
		Transport: hc.Transport,
		Region:    opt.Region,
	}

	// Remove scheme from URL.
	// Leave remaining validation to Minio client.
	endpoint := opt.EndpointURL[len(u.Scheme)+1:] // Remove scheme and colon
	endpoint = strings.TrimLeft(endpoint, "/")    // Remove slashes if any

	client, err := minio.New(endpoint, cfg)
	if err != nil {
		return nil, err
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		client.SetAppInfo("simpleblob", info.Main.Version)
	}

	if opt.CreateBucket {
		// Create bucket if it does not exist
		metricCalls.WithLabelValues("create-bucket").Inc()
		metricLastCallTimestamp.WithLabelValues("create-bucket").SetToCurrentTime()

		err := client.MakeBucket(ctx, opt.Bucket, minio.MakeBucketOptions{Region: opt.Region})
		if err != nil {
			if err := convertMinioError(err); err != nil {
				return nil, err
			}
		}
	}

	b := &Backend{
		opt:    opt,
		config: cfg,
		client: client,
		log:    log,
	}

	return b, nil
}

// NewReader satisfies ReaderStorage and provides a read streaming interface to
// a blob located on an S3 server.
func (b *Backend) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	r, err := b.doLoadReader(ctx, name)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// NewReader satisfies ReaderStorage and provides a write streaming interface to
// a blob located on an S3 server.
func (b *Backend) NewWriter(ctx context.Context, name string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	wrap := &writerWrapper{
		ctx:     ctx,
		backend: b,
		name:    name,
		pw:      pw,
		done:    make(chan struct{}, 1),
	}
	go func() {
		var err error
		wrap.info, err = b.doStoreReader(ctx, name, pr, -1)
		_ = pr.CloseWithError(err)
		wrap.done <- struct{}{}
	}()
	return wrap, nil
}

// A writerWrapper implements io.WriteCloser and is returned by (*Backend).NewWriter.
type writerWrapper struct {
	backend *Backend
	done    chan struct{}

	// We need to keep these around
	// to write the marker in Close.
	ctx  context.Context
	info minio.UploadInfo
	name string

	// Writes are sent to this pipe
	// and then written to S3 in a background goroutine.
	pw *io.PipeWriter
}

func (w *writerWrapper) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *writerWrapper) Close() error {
	err := w.pw.Close()
	if err != nil {
		return err
	}
	select {
	case <-w.done:
	case <-w.ctx.Done():
		err := w.ctx.Err()
		_ = w.pw.CloseWithError(err)
		return err
	}
	return w.backend.setMarker(w.ctx, w.name, w.info.ETag, false)
}

// convertMinioError takes an error, possibly a minio.ErrorResponse
// and turns it into a well known error when possible.
// If error is not well known, it is returned as is.
// If error is considered to be ignorable, nil is returned.
func convertMinioError(err error) error {
	if err == nil {
		return nil
	}
	errResp := minio.ToErrorResponse(err)
	if errResp.StatusCode == 404 {
		return os.ErrNotExist
	}
	if errResp.Code != "BucketAlreadyOwnedByYou" {
		return err
	}
	return nil
}

// prependGlobalPrefix prepends the GlobalPrefix to the name/prefix
// passed as input
func (b *Backend) prependGlobalPrefix(name string) string {
	return b.opt.GlobalPrefix + name
}

func init() {
	simpleblob.RegisterBackend("s3", func(ctx context.Context, p simpleblob.InitParams) (simpleblob.Interface, error) {
		var opt Options
		if err := p.OptionsThroughYAML(&opt); err != nil {
			return nil, err
		}
		opt.Logger = p.Logger
		return New(ctx, opt)
	})
}
