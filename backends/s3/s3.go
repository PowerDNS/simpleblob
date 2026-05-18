package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
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
	// DefaultSecretsRefreshInterval is the default value for RefreshSecrets.
	// It should not be too high so as to retrieve secrets regularly.
	DefaultSecretsRefreshInterval = 15 * time.Second
	// DefaultDisableContentMd5 : disable sending the Content-MD5 header
	DefaultDisableContentMd5 = false
	// DefaultClientTimeout is the default value for [(Options).ClientTimeout].
	DefaultClientTimeout = 15 * time.Minute
)

// ErrClientTimeout is returned when a Store, Read, Delete or List operation
// reaches the amount set in the [(Options).ClientTimeout] option (default [DefaultClientTimeout]).
var ErrClientTimeout = errors.New("S3 client timed out")

// Options describes the storage options for the S3 backend
type Options struct {
	// AccessKey and SecretKey are statically defined here.
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`

	// Path to the file containing the access key
	// as an alternative to AccessKey and SecretKey,
	// e.g. /etc/s3-secrets/access-key.
	AccessKeyFile string `yaml:"access_key_file"`

	// Path to the file containing the secret key
	// as an alternative to AccessKey and SecretKey,
	// e.g. /etc/s3-secrets/secret-key.
	SecretKeyFile string `yaml:"secret_key_file"`

	// Time between each secrets retrieval.
	// Minimum is 1s, lower values are considered an error.
	// It defaults to DefaultSecretsRefreshInterval,
	// which is currently 15s.
	SecretsRefreshInterval time.Duration `yaml:"secrets_refresh_interval"`

	// Region defaults to "us-east-1", which also works for Minio
	Region string `yaml:"region"`
	Bucket string `yaml:"bucket"`

	// StorageClass allows setting the S3 Storage Class only when storing objects.
	StorageClass string `yaml:"storage_class"`

	// CreateBucket tells us to try to create the bucket
	CreateBucket bool `yaml:"create_bucket"`

	// GlobalPrefix is a prefix applied to all operations, allowing work within a prefix
	// seamlessly
	GlobalPrefix string `yaml:"global_prefix"`

	// PrefixFolders can be enabled to make List operations show nested prefixes as folders
	// instead of recursively listing all contents of nested prefixes
	//
	// Deprecated: This option does not reflect our desire to treat blob names as keys.
	// Please do not use it.
	PrefixFolders bool `yaml:"prefix_folders"`

	// HideFolders is an S3-specific optimization, allowing to hide all keys that
	// have a separator '/' in their names.
	// In case a prefix representing a folder is provided to List,
	// that folder will be explored, and its subfolders hidden.
	//
	// Moreover, please note that regardless of this option,
	// working with folders with S3 is flaky,
	// because a `foo` key will shadow all `foo/*` keys while listing,
	// even though those `foo/*` keys exist and they hold the values they're expected to.
	HideFolders bool `yaml:"hide_folders"`

	// EndpointURL can be set to something like "http://localhost:9000" when using Minio
	// or "https://s3.amazonaws.com" for AWS S3.
	EndpointURL string `yaml:"endpoint_url"`

	// DisableContentMd5 defines whether to disable sending the Content-MD5 header
	DisableContentMd5 bool `yaml:"disable_send_content_md5"`

	// NumMinioThreads defines the number of threads that Minio uses for its workers.
	// It defaults to the using the default value defined by the Minio client.
	NumMinioThreads uint `yaml:"num_minio_threads"`

	// TLS allows customising the TLS configuration
	// See https://github.com/PowerDNS/go-tlsconfig for the available options
	TLS tlsconfig.Config `yaml:"tls"`

	// InitTimeout is the time we allow for initialisation, like credential
	// checking and bucket creation. It defaults to DefaultInitTimeout, which
	// is currently 20s.
	InitTimeout time.Duration `yaml:"init_timeout"`

	// IdleConnTimeout is the maximum amount of time an idle
	// (keep-alive) connection will remain idle before closing
	// itself. Default if unset: 90s
	IdleConnTimeout time.Duration `yaml:"idle_conn_timeout"`

	// MaxIdleConns controls the maximum number of idle (keep-alive)
	// connections. Default if unset: 100
	MaxIdleConns int `yaml:"max_idle_conns"`

	// DialTimeout is the maximum amount of time a dial will wait for
	// a connect to complete. Default if unset: 10s
	DialTimeout time.Duration `yaml:"dial_timeout"`

	// DialKeepAlive specifies the interval between keep-alive
	// probes for an active network connection. Default if unset: 10s
	DialKeepAlive time.Duration `yaml:"dial_keep_alive"`

	// TLSHandshakeTimeout specifies the maximum amount of time to
	// wait for a TLS handshake. Default if unset: 10s
	TLSHandshakeTimeout time.Duration `yaml:"tls_handshake_timeout"`

	// ClientTimeout specifies a time limit for operations on the S3 Backend.
	// If [UseUpdateMarker] is set, saving the marker is considered part of the operation.
	// Default if unset: 15m
	ClientTimeout time.Duration `yaml:"client_timeout"`

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
	hasSecretsCreds := o.AccessKeyFile != "" && o.SecretKeyFile != ""
	hasStaticCreds := o.AccessKey != "" && o.SecretKey != ""
	if !hasSecretsCreds && !hasStaticCreds {
		return fmt.Errorf("s3 storage.options: credentials are required, fill either (access_key and secret_key) or (access_key_filename and secret_key_filename)")
	}
	if hasSecretsCreds && o.SecretsRefreshInterval < time.Second {
		return fmt.Errorf("s3 storage.options: field secrets_refresh_interval must be at least 1s")
	}
	if o.Bucket == "" {
		return fmt.Errorf("s3 storage.options: bucket is required")
	}
	return nil
}

type Backend struct {
	opt        Options
	config     *minio.Options
	client     *minio.Client
	markerName string

	mu         sync.Mutex
	lastMarker string
	lastList   simpleblob.BlobList
	lastTime   time.Time
}

func (b *Backend) List(ctx context.Context, prefix string) (blobList simpleblob.BlobList, err error) {
	// Handle global prefix
	combinedPrefix := b.prependGlobalPrefix(prefix)

	if !b.opt.UseUpdateMarker {
		return b.doList(ctx, combinedPrefix)
	}

	// Using Load, that will itself prepend the global prefix to the marker name.
	// So we're using the raw marker name here.
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

	blobs, err = b.doList(ctx, b.opt.GlobalPrefix) // We want to cache all, so no prefix
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

func recordMinioDurationMetric(method string, start time.Time) {
	elapsed := time.Since(start)
	metricCallHistogram.WithLabelValues(method).Observe(elapsed.Seconds())
}

func (b *Backend) doList(ctx context.Context, prefix string) (blobs simpleblob.BlobList, err error) {
	ctx, cancel := b.clientTimeoutContext(ctx)
	defer cancel()
	defer recordMinioDurationMetric("list", time.Now())

	// Runes to strip from blob names for GlobalPrefix
	// This is fine, because we can trust the API to only return with the prefix.
	gpEndIndex := len(b.opt.GlobalPrefix)

	objIter := b.client.ListObjectsIter(ctx, b.opt.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: !b.opt.PrefixFolders && !b.opt.HideFolders,
	})
	for obj := range objIter {
		if err = convertError(ctx, obj.Err, true); err != nil {
			metricCallErrors.WithLabelValues("list").Inc()
			metricCallErrorsType.WithLabelValues("list", errorToMetricsLabel(err)).Inc()
			return nil, err
		}

		metricCalls.WithLabelValues("list").Inc()
		metricLastCallTimestamp.WithLabelValues("list").SetToCurrentTime()
		if obj.Key == b.markerName {
			continue
		}

		if b.opt.HideFolders && strings.HasSuffix(obj.Key, "/") {
			continue
		}

		// Strip global prefix from blob
		blobName := obj.Key
		if gpEndIndex > 0 {
			blobName = blobName[gpEndIndex:]
		}

		blobs = append(blobs, simpleblob.Blob{Name: blobName, Size: obj.Size})
	}

	// Minio appears to return them sorted, but maybe not all implementations
	// will, so we sort explicitly.
	blobs.Sort()

	return blobs, nil
}

// Load retrieves the content of the object identified by name from S3 Bucket
// configured in b.
func (b *Backend) Load(ctx context.Context, name string) ([]byte, error) {
	name = b.prependGlobalPrefix(name)
	ctx, cancel := b.clientTimeoutContext(ctx)
	defer cancel()

	r, err := b.doLoadReader(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	p, err := io.ReadAll(r)
	if err = convertError(ctx, err, false); err != nil {
		return nil, err
	}
	return p, nil
}

func (b *Backend) doLoadReader(ctx context.Context, name string) (*minio.Object, error) {
	metricCalls.WithLabelValues("load").Inc()
	metricLastCallTimestamp.WithLabelValues("load").SetToCurrentTime()

	defer recordMinioDurationMetric("load", time.Now())

	obj, err := b.client.GetObject(ctx, b.opt.Bucket, name, minio.GetObjectOptions{})
	if err = convertError(ctx, err, false); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			metricCallErrors.WithLabelValues("load").Inc()
			metricCallErrorsType.WithLabelValues("load", errorToMetricsLabel(err)).Inc()
		}
		return nil, err
	}
	if obj == nil {
		return nil, os.ErrNotExist
	}
	info, err := obj.Stat()
	if err = convertError(ctx, err, false); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			metricCallErrors.WithLabelValues("load").Inc()
			metricCallErrorsType.WithLabelValues("load", errorToMetricsLabel(err)).Inc()
		}
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
	name = b.prependGlobalPrefix(name)
	ctx, cancel := b.clientTimeoutContext(ctx)
	defer cancel()
	info, err := b.doStore(ctx, name, data)
	if err != nil {
		return err
	}
	return b.setMarker(ctx, name, info.ETag, false)
}

// doStore is a convenience wrapper around doStoreReader.
func (b *Backend) doStore(ctx context.Context, name string, data []byte) (minio.UploadInfo, error) {
	return b.doStoreReader(ctx, name, bytes.NewReader(data), int64(len(data)))
}

// doStoreReader stores data with key name in S3, using r as a source for data.
// The value of size may be -1, in case the size is not known.
func (b *Backend) doStoreReader(ctx context.Context, name string, r io.Reader, size int64) (minio.UploadInfo, error) {
	metricCalls.WithLabelValues("store").Inc()
	metricLastCallTimestamp.WithLabelValues("store").SetToCurrentTime()
	defer recordMinioDurationMetric("store", time.Now())

	putObjectOptions := minio.PutObjectOptions{
		NumThreads:     b.opt.NumMinioThreads,
		SendContentMd5: !b.opt.DisableContentMd5,
		StorageClass:   b.opt.StorageClass,
	}

	// minio accepts size == -1, meaning the size is unknown.
	info, err := b.client.PutObject(ctx, b.opt.Bucket, name, r, size, putObjectOptions)
	if err = convertError(ctx, err, false); err != nil {
		metricCallErrors.WithLabelValues("store").Inc()
		metricCallErrorsType.WithLabelValues("store", errorToMetricsLabel(err)).Inc()
		return info, err
	}
	return info, nil
}

// Delete removes the object identified by name from the S3 Bucket
// configured in b.
func (b *Backend) Delete(ctx context.Context, name string) error {
	name = b.prependGlobalPrefix(name)
	ctx, cancel := b.clientTimeoutContext(ctx)
	defer cancel()
	if err := b.doDelete(ctx, name); err != nil {
		return err
	}
	return b.setMarker(ctx, name, "", true)
}

func (b *Backend) doDelete(ctx context.Context, name string) error {
	metricCalls.WithLabelValues("delete").Inc()
	metricLastCallTimestamp.WithLabelValues("delete").SetToCurrentTime()
	defer recordMinioDurationMetric("delete", time.Now())

	err := b.client.RemoveObject(ctx, b.opt.Bucket, name, minio.RemoveObjectOptions{})
	if err = convertError(ctx, err, false); err != nil {
		metricCallErrors.WithLabelValues("delete").Inc()
		metricCallErrorsType.WithLabelValues("delete", errorToMetricsLabel(err)).Inc()
		return err
	}
	return nil
}

// clientTimeoutContext wraps [context.WithTimeoutCause] with the values and options that caracterise a client timeout.
func (b *Backend) clientTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, getOpt(b.opt.ClientTimeout, DefaultClientTimeout), ErrClientTimeout)
}

// convertError returns a more informative error from err.
// It may be converted to a [minio.ErrorResponse],
// or an [ErrClientTimeout] when ctx was issued by [(*Backend).contextWithTimeout].
func convertError(ctx context.Context, err error, isList bool) error {
	if err == nil {
		return nil
	}
	// Try to get a more specific error.
	if ctx.Err() != nil {
		err = context.Cause(ctx)
	} else {
		errRes := minio.ToErrorResponse(err)
		switch errRes.Code {
		case "BucketAlreadyOwnedByYou":
			// This is the desired outcome if we work on already existing bucket.
			return nil
		case "NoSuchKey":
			// NoSuchKey in a list means the marker is missing.
			if !isList {
				// This error does not reflect an upstream issue, so no metrics.
				return fmt.Errorf("%w: %s", os.ErrNotExist, err.Error())
			}
		}
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
	if opt.SecretsRefreshInterval == 0 {
		opt.SecretsRefreshInterval = DefaultSecretsRefreshInterval
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

	// Create an opinionated HTTP client that:
	// - Uses a custom tls.Config
	// - Sets proxies from the environment
	// - Sets reasonable timeouts on various operations
	// Based on tlsConfig.HTTPClient(), copied to allow overrides.
	tlsConfig, err := tlsmgr.TLSConfig()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   getOpt(opt.DialTimeout, 10*time.Second),
			KeepAlive: getOpt(opt.DialKeepAlive, 10*time.Second),
		}).DialContext,
		MaxIdleConns:          getOpt(opt.MaxIdleConns, 100),
		IdleConnTimeout:       getOpt(opt.IdleConnTimeout, 90*time.Second),
		TLSHandshakeTimeout:   getOpt(opt.TLSHandshakeTimeout, 10*time.Second),
		ExpectContinueTimeout: 10 * time.Second,
		TLSClientConfig:       tlsConfig,
		ForceAttemptHTTP2:     true,
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

	creds := credentials.NewStaticV4(opt.AccessKey, opt.SecretKey, "")
	if opt.AccessKeyFile != "" {
		creds = credentials.New(&FileSecretsCredentials{
			AccessKeyFile:   opt.AccessKeyFile,
			SecretKeyFile:   opt.SecretKeyFile,
			RefreshInterval: opt.SecretsRefreshInterval,
		})
	}

	cfg := &minio.Options{
		Creds:     creds,
		Secure:    useSSL,
		Transport: transport,
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
			if err := convertError(ctx, err, false); err != nil {
				return nil, err
			}
		}
	}

	b := &Backend{
		opt:    opt,
		config: cfg,
		client: client,
	}
	b.setGlobalPrefix(opt.GlobalPrefix)

	return b, nil
}

// setGlobalPrefix updates the global prefix in b and the cached marker name,
// so it can be dynamically changed in tests.
func (b *Backend) setGlobalPrefix(prefix string) {
	b.opt.GlobalPrefix = prefix
	b.markerName = b.prependGlobalPrefix(UpdateMarkerFilename)
}

// errorToMetricsLabel converts an error into a prometheus label.
// If error is a NotExist error, "NotFound" is returned.
// If error is a timeout, "Timeout" is returned.
// If error is a DNS error, the DNS error is returned.
// If error is a URL error, the URL error is returned.
// If error is a MinIO error, the MinIO error code is returned.
// Otherwise "Unknown" is returned.
func errorToMetricsLabel(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, os.ErrNotExist) {
		return "NotFound"
	}
	var netError *net.OpError
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrClientTimeout) ||
		(errors.As(err, &netError) && netError.Timeout()) {
		return "Timeout"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "DNSError"
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return "URLError"
	}
	errRes := minio.ToErrorResponse(err)
	if errRes.Code != "" {
		return errRes.Code
	}
	return "Unknown"
}

func getOpt[T comparable](optVal, defaultVal T) T {
	var zero T
	if optVal == zero {
		return defaultVal
	}
	return optVal
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
