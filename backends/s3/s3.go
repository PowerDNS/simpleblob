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
	"github.com/minio/minio-go/v7"
	"github.com/go-logr/logr"
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

	objCh := b.client.ListObjects(ctx, b.opt.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})
	for obj := range objCh {
		metricCalls.WithLabelValues("list").Inc()
		metricLastCallTimestamp.WithLabelValues("list").SetToCurrentTime()
		if obj.Key == UpdateMarkerFilename {
			continue
		}
		blobs = append(blobs, simpleblob.Blob{Name: obj.Key, Size: obj.Size})
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

	obj, err := b.client.GetObject(ctx, b.opt.Bucket, name, minio.GetObjectOptions{})
	if err = convertMinioError(err); err != nil {
		return nil, err
	} else if obj == nil {
		return nil, os.ErrNotExist
	}

	p, err := io.ReadAll(obj)
	if err = convertMinioError(err); err != nil {
		return nil, err
	}
	return p, nil
}

// Store sets the content of the object identified by name to the content
// of data, in the S3 Bucket configured in b.
func (b *Backend) Store(ctx context.Context, name string, data []byte) error {
	info, err := b.doStore(ctx, name, data)
	if err != nil {
		return err
	}
	return b.setMarker(ctx, name, info.ETag, false)
}

func (b *Backend) doStore(ctx context.Context, name string, data []byte) (*minio.UploadInfo, error) {
	metricCalls.WithLabelValues("store").Inc()
	metricLastCallTimestamp.WithLabelValues("store").SetToCurrentTime()

	info, err := b.client.PutObject(ctx, b.opt.Bucket, name, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		NumThreads: 3,
	})
	if err != nil {
		metricCallErrors.WithLabelValues("store").Inc()
	}
	return &info, err
}

// Delete removes the object identified by name from the S3 Bucket
// configured in b.
func (b *Backend) Delete(ctx context.Context, name string) error {
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
		return nil, fmt.Errorf("no scheme provided for endpoint URL '%s', use http or https.", opt.EndpointURL)
	default:
		return nil, fmt.Errorf("unsupported scheme for S3: '%s', use http or https.", u.Scheme)
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
