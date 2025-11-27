package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/PowerDNS/go-tlsconfig"
	"github.com/PowerDNS/simpleblob"
	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
)

// Azure blob implementation examples can be found here:
//https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/storage/azblob/examples_test.go

const (
	// DefaultEndpointURL is the default the local Azurite default endpoint
	DefaultEndpointURL = "https://mycontainer.blob.core.windows.net/"
	// DefaultInitTimeout is the time we allow for initialisation, like credential
	// checking and bucket creation. We define this here, because we do not
	// pass a context when initialising a plugin.
	DefaultInitTimeout = 20 * time.Second
	// UpdateMarkerFilename is the filename used for the update marker functionality
	UpdateMarkerFilename = "update-marker"
	// DefaultUpdateMarkerForceListInterval is the default value for
	// UpdateMarkerForceListInterval.
	DefaultUpdateMarkerForceListInterval = 5 * time.Minute
	// DefaultDisableContentMd5 : disable sending the Content-MD5 header
	DefaultDisableContentMd5 = false
	// Max number of concurrent uploads to be performed to upload the file
	DefaultConcurrency = 1
)

type Options struct {

	// AccountName and AccountKey are statically defined here.

	AccountName string `yaml:"account_name"`
	AccountKey  string `yaml:"account_key"`

	UseEnvCreds bool `yaml:"use_env_creds"`

	// Azure blob container name. If it doesn't exist it will be automatically created if `CreateContainer` is true.
	Container string `yaml:"container"`

	// CreateBucket tells us to try to create the bucket
	CreateContainer bool `yaml:"create_container"`

	// GlobalPrefix is a prefix applied to all operations, allowing work within a prefix
	// seamlessly
	GlobalPrefix string `yaml:"global_prefix"`

	// EndpointURL can be set to something like "http://localhost:9000" for local testing
	EndpointURL string `yaml:"endpoint_url"`

	// DisableContentMd5 defines whether to disable sending the Content-MD5 header
	DisableContentMd5 bool `yaml:"disable_send_content_md5"`

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
	// This can reduce the number of LIST commands sent to Azure, replacing them
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

	// Concurrency defines the max number of concurrent uploads to be performed to upload the file.
	// Each concurrent upload will create a buffer of size BlockSize.  The default value is one.
	// https://github.com/Azure/azure-sdk-for-go/blob/e5c902ce7aca5aa0f4c7bb7e46c18c8fc91ad458/sdk/storage/azblob/blockblob/models.go#L264
	Concurrency int `yaml:"concurrency"`

	// Not loaded from YAML
	Logger logr.Logger `yaml:"-"`
}

type Backend struct {
	opt        Options
	client     *azblob.Client
	log        logr.Logger
	markerName string

	mu         sync.Mutex
	lastMarker string
	lastList   simpleblob.BlobList
	lastTime   time.Time
}

func (o Options) Check() error {
	if o.UseEnvCreds {
		return nil
	}

	hasSecretsCreds := o.AccountName != "" && o.AccountKey != ""

	if !hasSecretsCreds {
		return fmt.Errorf("azure storage.options: account_name and account_key are required")
	}

	if o.Container == "" {
		return fmt.Errorf("azure storage.options: container is required")
	}
	return nil
}

// New creates a new backend instance.
// The lifetime of the context passed in must span the lifetime of the whole
// backend instance, not just the init time, so do not set any timeout on it!
func New(ctx context.Context, opt Options) (*Backend, error) {
	if opt.InitTimeout == 0 {
		opt.InitTimeout = DefaultInitTimeout
	}

	if opt.UpdateMarkerForceListInterval == 0 {
		opt.UpdateMarkerForceListInterval = DefaultUpdateMarkerForceListInterval
	}
	if opt.EndpointURL == "" {
		opt.EndpointURL = DefaultEndpointURL
	}
	if opt.Concurrency == 0 {
		opt.Concurrency = DefaultConcurrency
	}

	if err := opt.Check(); err != nil {
		return nil, err
	}

	log := opt.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("azure")

	var endpoint string

	accountName := opt.AccountName

	if opt.EndpointURL != "" {
		endpoint = opt.EndpointURL
	} else {
		endpoint = fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	var client *azblob.Client

	// If UseEnvCreds is set, we will attempt to use the environment variables and the Azure service principle based `azidentity.NewDefaultAzureCredential()` method
	// https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/azidentity/README.md#service-principal-with-secret
	if opt.UseEnvCreds {
		// Test if the environment variables are set
		_, ok := os.LookupEnv("AZURE_CLIENT_ID")
		if !ok {
			return nil, errors.New("AZURE_CLIENT_ID could not be found")
		}

		_, ok = os.LookupEnv("AZURE_TENANT_ID")
		if !ok {
			return nil, errors.New("AZURE_TENANT_ID could not be found")
		}

		_, ok = os.LookupEnv("AZURE_CLIENT_SECRET")
		if !ok {
			return nil, errors.New("AZURE_CLIENT_SECRET could not be found")
		}

		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}

		client, err = azblob.NewClient(endpoint, cred, nil)

		if err != nil {
			return nil, err
		}
	} else {
		cred, err := azblob.NewSharedKeyCredential(accountName, opt.AccountKey)
		if err != nil {
			return nil, err
		}

		client, err = azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)

		if err != nil {
			return nil, err
		}
	}
	if opt.CreateContainer {
		// Create bucket if it does not exist
		metricCalls.WithLabelValues("create-container").Inc()
		metricLastCallTimestamp.WithLabelValues("create-container").SetToCurrentTime()

		_, err := client.CreateContainer(ctx, opt.Container, &azblob.CreateContainerOptions{})

		if err != nil {
			if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
				logrus.WithField("storage_type", "Azure").Infof("Container already exists: %s", opt.Container)
			} else {
				return nil, err
			}
		}
	}

	b := &Backend{
		opt:    opt,
		client: client,
		log:    log,
	}

	b.setGlobalPrefix(opt.GlobalPrefix)

	return b, nil
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

	notFound := errors.Is(err, os.ErrNotExist)

	if err != nil && !notFound {
		return nil, err
	}

	upstreamMarker := string(m)

	b.mu.Lock()
	mustUpdate := b.lastList == nil ||
		upstreamMarker != b.lastMarker ||
		time.Since(b.lastTime) >= b.opt.UpdateMarkerForceListInterval ||
		notFound
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

// convertAzureError takes an error from Azure SDK and converts it to
// os.ErrNotExist when appropriate (BlobNotFound, ContainerNotFound, ResourceNotFound).
// If the error is not a "not found" error, it is returned as is.
func convertAzureError(err error) error {
	if err == nil {
		return nil
	}
	if bloberror.HasCode(err,
		bloberror.BlobNotFound,
		bloberror.ContainerNotFound,
		bloberror.ResourceNotFound,
	) {
		return fmt.Errorf("%w: %s", os.ErrNotExist, err.Error())
	}
	return err
}

func (b *Backend) doList(ctx context.Context, prefix string) (simpleblob.BlobList, error) {
	var blobs simpleblob.BlobList

	// Runes to strip from blob names for GlobalPrefix
	// This is fine, because we can trust the API to only return with the prefix.
	gpEndIndex := len(b.opt.GlobalPrefix)

	// Use Azure SDK to get blobs from container
	blobPager := b.client.NewListBlobsFlatPager(b.opt.Container, nil)

	for blobPager.More() {
		resp, err := blobPager.NextPage(ctx)

		if err != nil {
			return nil, err
		}

		// if empty...
		if resp.Segment == nil {
			// Container is empty
			return blobs, nil
		}

		for _, v := range resp.Segment.BlobItems {
			blobName := *v.Name

			// We have to manually check for prefix since Azure doesn't support querying by prefix
			if !strings.HasPrefix(blobName, prefix) {
				continue
			}

			if blobName == b.markerName {
				continue
			}

			size := *v.Properties.ContentLength

			if gpEndIndex > 0 {
				blobName = blobName[gpEndIndex:]
			}

			blobs = append(blobs, simpleblob.Blob{Name: blobName, Size: size})
		}
	}

	// Sort explicitly.
	sort.Sort(blobs)

	return blobs, nil
}

// Load retrieves the content of the object identified by name from the Azure container
// configured in b.
func (b *Backend) Load(ctx context.Context, name string) ([]byte, error) {
	name = b.prependGlobalPrefix(name)

	r, err := b.doLoadReader(ctx, name)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	p, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (b *Backend) doLoadReader(ctx context.Context, name string) (io.ReadCloser, error) {
	metricCalls.WithLabelValues("load").Inc()
	metricLastCallTimestamp.WithLabelValues("load").SetToCurrentTime()

	// Download the blob's contents and ensure that the download worked properly
	blobDownloadResponse, err := b.client.DownloadStream(ctx, b.opt.Container, name, nil)
	if err = convertAzureError(err); err != nil {
		metricCallErrors.WithLabelValues("load").Inc()
		return nil, err
	}

	// Use the bytes.Buffer object to read the downloaded data.
	// RetryReaderOptions has a lot of in-depth tuning abilities, but for the sake of simplicity, we'll omit those here.
	// Convert the response body to a Reader
	reader := io.Reader(blobDownloadResponse.Body)

	return io.NopCloser(reader), nil
}

// Store sets the content of the object identified by name to the content
// of data, in the Azure container configured in b.
func (b *Backend) Store(ctx context.Context, name string, data []byte) error {
	// Prepend global prefix
	name = b.prependGlobalPrefix(name)

	info, err := b.doStore(ctx, name, data)

	if err != nil {
		return err
	}

	return b.setMarker(ctx, name, string(*info.ETag), false)
}

// doStore is a convenience wrapper around doStoreReader.
func (b *Backend) doStore(ctx context.Context, name string, data []byte) (azblob.UploadStreamResponse, error) {
	return b.doStoreReader(ctx, name, bytes.NewReader(data), int64(len(data)))
}

// doStoreReader stores data with key name in Azure blob, using r as a source for data.
// The value of size may be -1, in case the size is not known.
func (b *Backend) doStoreReader(ctx context.Context, name string, r io.Reader, size int64) (azblob.UploadStreamResponse, error) {
	metricCalls.WithLabelValues("store").Inc()
	metricLastCallTimestamp.WithLabelValues("store").SetToCurrentTime()

	uploadStreamOptions := &azblob.UploadStreamOptions{
		Concurrency: b.opt.Concurrency,
	}

	// Perform UploadStream
	resp, err := b.client.UploadStream(ctx, b.opt.Container, name, r, uploadStreamOptions)

	if err != nil {
		metricCallErrors.WithLabelValues("store").Inc()
		return azblob.UploadStreamResponse{}, err
	}

	return resp, err
}

// Delete removes the object identified by name from the Azure Container
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

	_, err := b.client.DeleteBlob(ctx, b.opt.Container, name, nil)

	if err = convertAzureError(err); err != nil {
		// Delete is idempotent - if blob doesn't exist, that's fine
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		metricCallErrors.WithLabelValues("delete").Inc()
		return err
	}

	return nil
}

// setGlobalPrefix updates the global prefix in b and the cached marker name,
// so it can be dynamically changed in tests.
func (b *Backend) setGlobalPrefix(prefix string) {
	b.opt.GlobalPrefix = prefix
	b.markerName = b.prependGlobalPrefix(UpdateMarkerFilename)
}

// prependGlobalPrefix prepends the GlobalPrefix to the name/prefix
// passed as input
func (b *Backend) prependGlobalPrefix(name string) string {
	return b.opt.GlobalPrefix + name
}

func init() {
	simpleblob.RegisterBackend("azure", func(ctx context.Context, p simpleblob.InitParams) (simpleblob.Interface, error) {
		var opt Options
		if err := p.OptionsThroughYAML(&opt); err != nil {
			return nil, err
		}
		opt.Logger = p.Logger

		return New(ctx, opt)
	})
}
