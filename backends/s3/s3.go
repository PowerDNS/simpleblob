package s3

import (
	"bytes"
	"context"
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
	minio "github.com/minio/minio-go/v7"
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

	mu         sync.Mutex
	lastMarker string
	lastList   simpleblob.BlobList
	lastTime   time.Time
}

func (b *Backend) List(ctx context.Context, prefix string) (simpleblob.BlobList, error) {
	if !b.opt.UseUpdateMarker {
		return b.doList(ctx, prefix)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Request and cache full list, and use marker file to invalidate the cache
	data, err := b.Load(ctx, UpdateMarkerFilename)
	exists := true
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		exists = false
	}
	current := string(data)

	if !exists || b.lastMarker == "" || current != b.lastMarker || time.Since(b.lastTime) >= b.opt.UpdateMarkerForceListInterval {
		// Update cache
		blobs, err := b.doList(ctx, "") // all, no prefix
		if err != nil {
			return nil, err
		}
		b.lastMarker = current
		b.lastList = blobs
		b.lastTime = time.Now()
	}
	return b.lastList.WithPrefix(prefix), nil
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

func (b *Backend) Load(ctx context.Context, name string) ([]byte, error) {
	metricCalls.WithLabelValues("load").Inc()
	metricLastCallTimestamp.WithLabelValues("load").SetToCurrentTime()

	obj, err := b.client.GetObject(ctx, b.opt.Bucket, name, minio.GetObjectOptions{})
	if err != nil {
		if err = handleErrorResponse(err); err != nil {
			return nil, err
		}
	} else if obj == nil {
		return nil, os.ErrNotExist
	}

	p, err := io.ReadAll(obj)
	if err = handleErrorResponse(err); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *Backend) Store(ctx context.Context, name string, data []byte) error {
	if err := b.doStore(ctx, name, data); err != nil {
		return err
	}
	if b.opt.UseUpdateMarker {
		b.mu.Lock()
		defer b.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(1)

		// Update cache
		go func() {
			defer wg.Done()
			// Update size or add to local list if not present
			l := b.lastList.Len()
			idx := sort.Search(l, func(i int) bool { return b.lastList[i].Name >= name })
			blob := simpleblob.Blob{Name: name, Size: int64(len(data))}
			if idx < l {
				if b.lastList[idx].Name == name {
					b.lastList[idx].Size = int64(len(data))
				} else {
					b.lastList = append(b.lastList, simpleblob.Blob{})
					copy(b.lastList[idx+1:], b.lastList[idx:])
					b.lastList[idx] = blob
				}
			} else {
				b.lastList = append(b.lastList, blob)
			}

			b.lastMarker = name
		}()

		if err := b.doStore(ctx, UpdateMarkerFilename, []byte(name)); err != nil {
			return err
		}

		wg.Wait()
	}
	return nil
}

func (b *Backend) doStore(ctx context.Context, name string, data []byte) error {
	metricCalls.WithLabelValues("store").Inc()
	metricLastCallTimestamp.WithLabelValues("store").SetToCurrentTime()

	_, err := b.client.PutObject(ctx, b.opt.Bucket, name, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		NumThreads: 3,
	})
	if err != nil {
		metricCallErrors.WithLabelValues("store").Inc()
	}
	return err
}

func (b *Backend) Delete(ctx context.Context, name string) error {
	metricCalls.WithLabelValues("delete").Inc()
	metricLastCallTimestamp.WithLabelValues("delete").SetToCurrentTime()

	err := b.client.RemoveObject(ctx, b.opt.Bucket, name, minio.RemoveObjectOptions{})
	if err = handleErrorResponse(err); err != nil {
		metricCallErrors.WithLabelValues("delete").Inc()
	}
	if b.opt.UseUpdateMarker {
		b.mu.Lock()
		defer b.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			// Update cached list values
			// This keeps sorted order, so no need to sort again
			l := b.lastList.Len()
			idx := sort.Search(l, func(i int) bool { return b.lastList[i].Name == name })
			if idx < l && b.lastList[idx].Name == name {
				b.lastList = b.lastList[:idx]
				if idx < l-2 {
					b.lastList = append(b.lastList, b.lastList[idx+1:]...)
				}
			}
			b.lastMarker = name
		}()

		if err := b.doStore(ctx, UpdateMarkerFilename, []byte(name)); err != nil {
			return err
		}

		wg.Wait()
	}
	return err
}

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

	// Automatic TLS handling
	// This MUST receive a longer running context to be able to automatically
	// reload certificates, so we use the original ctx, not one with added
	// InitTimeout.
	tlsmgr, err := tlsconfig.NewManager(ctx, opt.TLS, tlsconfig.Options{
		IsClient: true,
		// TODO: logging might be useful here, but we need to figure this
		//       out for other parts of simpleblob first.
		Logr: nil,
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
		return nil, fmt.Errorf("No scheme provided for endpoint URL '%s', use http or https.", opt.EndpointURL)
	default:
		return nil, fmt.Errorf("Unsupported scheme for S3: '%s', use http or https.", u.Scheme)
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
	endpoint = strings.TrimLeft(endpoint, "/") // Remove slashes if any

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
			if err := handleErrorResponse(err); err != nil {
				return nil, err
			}
		}
	}

	b := &Backend{
		opt:    opt,
		config: cfg,
		client: client,
	}

	return b, nil
}

// handleErrorResponse takes an error, possibly a minio.ErrorResponse
// and turns it into a well known error when possible.
// If error is not well known, it is returned as is.
// If error is considered to be ignorable, nil is returned.
func handleErrorResponse(err error) error {
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
		return New(ctx, opt)
	})
}
