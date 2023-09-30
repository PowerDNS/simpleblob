package nats

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/PowerDNS/simpleblob"
	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultMaxReconnects - Max Reconnect attempts
	DefaultMaxReconnects = 5
	// DefaultReconnectWaitSeconds - Time to wait before attempting reconnect
	DefaultReconnectWaitSeconds = 1
	// DefaultNatsBucketDescription - Default description for bucket
	DefaultNatsBucketDescription = "NATS bucket for simpleblob"
	// DefaultNatsBucketReplicas - Default replicas for bucket
	DefaultNatsBucketReplicas = 1
	// DefaultMaxWaitSeconds - Default max wait for Jetstream replies
	DefaultMaxWaitSeconds = 10
)

// internal enum to track auth type
type authType int

const (
	username authType = iota
	token
	nkey
	credentials
	nkeyJwt
	jwt
	invalid
)

// Options enumerates various configuration options
type Options struct {
	// Params related to reconnection attempts
	DisableRetryOnFailedConnect bool `yaml:"disableRetryOnFailedConnect"`
	MaxReconnects               int  `yaml:"maxReconnects"`
	ReconnectWaitSeconds        int  `yaml:"reconnectWait"`
	MaxWaitSeconds              int  `yaml:"maxWaitSeconds"`
	// Authentication options
	NatsUsername            string `yaml:"natsUsername"`
	NatsPassword            string `yaml:"natsPassword"`
	NatsToken               string `yaml:"natsToken"`
	NatsNkeySeedFilePath    string `yaml:"natsNkeySeedFilePath"`
	NatsCredentialsFilePath string `yaml:"natsCredentialsFilePath"`
	NatsJWTKeyFilePath      string `yaml:"natsJWTKeyFilePath"`
	// TLS Options
	// NatsTLSRootCA Specify a custom root CA file (e.g. for self-signed certs)
	NatsTLSRootCA string `yaml:"natsTLSRootCA"`
	// NatsTLSClientCert Specify a client cert file
	NatsTLSClientCert string `yaml:"natsTLSClientCert"`
	// NatsTLSClientKey Specify a client key file
	NatsTLSClientKey string `yaml:"natsTLSClientKey"`
	// Connection options
	NatsURL string `yaml:"natsURL"`
	// Storage Options
	// NatsBucket defines the bucket name
	NatsBucket string `yaml:"natsBucket"`
	// NatsBucketReplicas defines number of replicas
	NatsBucketReplicas int `yaml:"natsBucketReplicas"`
	// NatsBucketDescription description for NatsBucket
	NatsBucketDescription string `yaml:"natsBucketDescription"`
	// CreateBucket tells us to try to create the bucket
	CreateBucket bool `yaml:"create_bucket"`
	// CreateBucketPlacementCluster optionally set the cluster placement value when creating a bucket
	CreateBucketPlacementCluster string `yaml:"createBucketPlacementCluster"`
	// CreateBucketPlacementTagList comma seperated list of optional placement tags when creating a bucket
	CreateBucketPlacementTagList string `yaml:"createBucketTagList"`
	// GlobalPrefix is a prefix applied to all operations, allowing work within a prefix
	GlobalPrefix string `yaml:"global_prefix"`
	// EncryptionKey -  key (hex format,256 bit) for encryption at rest
	EncryptionKey string `yaml:"encryptionKey"`
	// Internally managed options
	// Auth type flag set internally by checkCredentialsAvailability()
	internalUseAuthType authType
	// Converted type for wait seconds
	internalReconnectWaitSeconds time.Duration
	// Converted type for max wait
	internalMaxWaitSeconds time.Duration
	// Converted key bytes
	internalEncryptionKeyBytes []byte
	// Not loaded from YAML
	Logger logr.Logger `yaml:"-"`
}

type Backend struct {
	opt Options
	//config     *minio.Options
	nc         *nats.Conn
	log        logr.Logger
	markerName string

	mu         sync.Mutex
	lastMarker string
	lastList   simpleblob.BlobList
	lastTime   time.Time
}

// Check TLS params are available
func (o Options) checkTLS() error {
	if o.NatsTLSRootCA != "" {
		err := fileExistsAndIsReadable(o.NatsTLSRootCA)
		if err != nil {
			return err
		}
	}
	if (o.NatsTLSClientCert != "" && o.NatsTLSClientKey == "") || (o.NatsTLSClientKey != "" && o.NatsTLSClientCert == "") {
		return errors.New("you appear to want to use client TLS certs but have not provided either cert or key")
	}
	if o.NatsTLSClientCert != "" {
		err := fileExistsAndIsReadable(o.NatsTLSClientCert)
		if err != nil {
			return err
		}
	}
	if o.NatsTLSClientKey != "" {
		err := fileExistsAndIsReadable(o.NatsTLSClientKey)
		if err != nil {
			return err
		}
	}
	return nil
}

// Check some credentials have been supplied and set the flag in the options struct
func (o Options) checkCredentialsAvailability() error {
	// Found a token
	if o.NatsToken != "" {
		o.internalUseAuthType = token
		return nil
	}
	// Found a username
	if o.NatsUsername != "" && o.NatsPassword != "" {
		o.internalUseAuthType = username
		return nil
	}
	if (o.NatsUsername != "" && o.NatsPassword == "") || (o.NatsPassword != "" && o.NatsUsername == "") {
		o.internalUseAuthType = invalid
		return errors.New("you appear to want to authenticate using username and password but have not supplied a username or a password ")
	}
	// Nkey specified
	if o.NatsNkeySeedFilePath != "" {
		err := fileExistsAndIsReadable(o.NatsNkeySeedFilePath)
		if err != nil {
			o.internalUseAuthType = invalid
			return err
		}
		if o.NatsJWTKeyFilePath == "" {
			o.internalUseAuthType = nkey
			return nil
		}
		err = fileExistsAndIsReadable(o.NatsJWTKeyFilePath)
		if err != nil {
			o.internalUseAuthType = invalid
			return err
		}
		o.internalUseAuthType = nkeyJwt
		return nil
	}
	// JWT only
	if o.NatsJWTKeyFilePath != "" {
		err := fileExistsAndIsReadable(o.NatsJWTKeyFilePath)
		if err != nil {
			o.internalUseAuthType = invalid
			return err
		}
		o.internalUseAuthType = jwt
		return nil
	}
	// Credentials specified
	if o.NatsCredentialsFilePath != "" {
		err := fileExistsAndIsReadable(o.NatsCredentialsFilePath)
		if err != nil {
			o.internalUseAuthType = invalid
			return err
		}
		o.internalUseAuthType = credentials
		return nil
	}
	o.internalUseAuthType = invalid
	return errors.New("unable to detect suitable credentials")
}

// prependGlobalPrefix prepends the GlobalPrefix to the name/prefix
// passed as input
func (b *Backend) prependGlobalPrefix(name string) string {
	return b.opt.GlobalPrefix + name
}

// setGlobalPrefix updates the global prefix in b and the cached marker name,
// so it can be dynamically changed in tests.
func (b *Backend) setGlobalPrefix(prefix string) {
	b.opt.GlobalPrefix = prefix
}

// Load retrieves the content of the object identified by name from S3 Bucket
// configured in b.
func (b *Backend) Load(ctx context.Context, name string) ([]byte, error) {
	name = b.prependGlobalPrefix(name)
	js, err := b.nc.JetStream(nats.MaxWait(b.opt.internalMaxWaitSeconds))
	if err != nil {
		return nil, err
	}
	obj, err := js.ObjectStore(b.opt.NatsBucket)
	if err != nil {
		return nil, err
	}
	if len(b.opt.internalEncryptionKeyBytes) == 0 {
		return obj.GetBytes(name)
	} else {
		ciphertext, err := obj.GetBytes(name)
		if err != nil {
			return nil, err
		}
		return helperDecrypt(b.opt.internalEncryptionKeyBytes, ciphertext)
	}
}

// Store sets the content of the object identified by name to the content
// of data, in the S3 Bucket configured in b.
func (b *Backend) Store(ctx context.Context, name string, data []byte) error {
	// Prepend global prefix
	name = b.prependGlobalPrefix(name)
	_, err := b.doStore(ctx, name, data)
	return err
}

func (b *Backend) doStore(ctx context.Context, name string, data []byte) (*nats.ObjectInfo, error) {
	// Optionally encrypt
	if len(b.opt.internalEncryptionKeyBytes) > 0 {
		ciphertext, err := helperEncrypt(b.opt.internalEncryptionKeyBytes, data)
		if err != nil {
			return nil, err
		}
		data = ciphertext
	}
	// Store data
	js, err := b.nc.JetStream(nats.MaxWait(b.opt.internalMaxWaitSeconds))
	if err != nil {
		return nil, err
	}
	obj, err := js.ObjectStore(b.opt.NatsBucket)
	if err != nil {
		return nil, err
	}
	return obj.PutBytes(name, data)
}

// Delete removes the object identified by name from the S3 Bucket
// configured in b.
func (b *Backend) Delete(ctx context.Context, name string) error {
	name = b.prependGlobalPrefix(name)
	js, err := b.nc.JetStream(nats.MaxWait(b.opt.internalMaxWaitSeconds))
	if err != nil {
		return err
	}
	obj, err := js.ObjectStore(b.opt.NatsBucket)
	if err != nil {
		return err
	}
	return obj.Delete(name)
}

// List returns BlobList
func (b *Backend) List(ctx context.Context, prefix string) (blobList simpleblob.BlobList, err error) {
	prefix = b.prependGlobalPrefix(prefix)
	return b.doList(ctx, prefix)
}

func (b *Backend) doList(ctx context.Context, prefix string) (simpleblob.BlobList, error) {
	var blobs simpleblob.BlobList
	js, err := b.nc.JetStream(nats.MaxWait(b.opt.internalMaxWaitSeconds))
	if err != nil {
		return nil, err
	}
	obj, err := js.ObjectStore(b.opt.NatsBucket)
	if err != nil {
		return nil, err
	}
	objList, err := obj.List()
	if err != nil {
		return nil, err
	}
	gpEndIndex := len(b.opt.GlobalPrefix)
	for _, objI := range objList {
		if !strings.HasPrefix(objI.Name, prefix) {
			continue
		}
		blobName := objI.Name
		if gpEndIndex > 0 {
			blobName = blobName[gpEndIndex:]
		}
		blobs = append(blobs, simpleblob.Blob{Name: blobName, Size: int64(objI.Size)})
	}
	sort.Sort(blobs)
	return blobs, nil
}

// New creates a new backend instance.
func New(ctx context.Context, opt Options) (*Backend, error) {
	// Basic validation
	err := opt.checkCredentialsAvailability()
	if err != nil {
		return nil, err
	}
	err = opt.checkTLS()
	if err != nil {
		return nil, err
	}
	if opt.NatsBucket == "" {
		return nil, errors.New("bucket name not provided")
	}
	if opt.NatsBucketDescription == "" {
		opt.NatsBucketDescription = DefaultNatsBucketDescription
	}
	if opt.NatsBucketReplicas == 0 {
		opt.NatsBucketReplicas = DefaultNatsBucketReplicas
	}
	if opt.NatsURL == "" {
		opt.NatsURL = nats.DefaultURL
	}
	if opt.MaxWaitSeconds == 0 {
		opt.MaxWaitSeconds = DefaultMaxWaitSeconds
	}
	opt.internalMaxWaitSeconds = helperSecondsToDuration(opt.MaxWaitSeconds)
	if !opt.DisableRetryOnFailedConnect {
		if opt.MaxReconnects == 0 {
			opt.MaxReconnects = DefaultMaxReconnects
		}
		if opt.ReconnectWaitSeconds == 0 {
			opt.internalReconnectWaitSeconds = helperSecondsToDuration(DefaultReconnectWaitSeconds)
		} else {
			opt.internalReconnectWaitSeconds = helperSecondsToDuration(opt.ReconnectWaitSeconds)
		}
	}
	if opt.EncryptionKey != "" {
		keyBytes, err := hex.DecodeString(opt.EncryptionKey)
		if err != nil {
			return nil, err
		}
		if len(keyBytes) < 32 {
			return nil, errors.New("provided key is too short")
		}
		opt.internalEncryptionKeyBytes = keyBytes
	}
	// Create client
	b := &Backend{opt: opt}
	var ncOptions []nats.Option
	if opt.NatsTLSRootCA != "" {
		ncOptions = append(ncOptions, nats.RootCAs(opt.NatsTLSRootCA))
	}
	if opt.NatsTLSClientCert != "" {
		ncOptions = append(ncOptions, nats.ClientCert(opt.NatsTLSClientCert, opt.NatsTLSClientKey))
	}
	if opt.internalUseAuthType == username {
		ncOptions = append(ncOptions, nats.UserInfo(opt.NatsUsername, opt.NatsPassword))
	}
	if opt.internalUseAuthType == token {
		ncOptions = append(ncOptions, nats.Token(opt.NatsToken))
	}
	if opt.internalUseAuthType == nkey {
		nk, err := nats.NkeyOptionFromSeed(opt.NatsNkeySeedFilePath)
		if err != nil {
			return nil, err
		}
		ncOptions = append(ncOptions, nk)
	}
	if opt.internalUseAuthType == nkeyJwt {
		cr := nats.UserCredentials(opt.NatsJWTKeyFilePath, opt.NatsNkeySeedFilePath)
		ncOptions = append(ncOptions, cr)
	}
	if opt.internalUseAuthType == credentials {
		cr := nats.UserCredentials(opt.NatsCredentialsFilePath)
		ncOptions = append(ncOptions, cr)
	}
	if opt.DisableRetryOnFailedConnect {
		b.nc, err = nats.Connect(opt.NatsURL, ncOptions...)
		if err != nil {
			return nil, err
		}
	} else {
		ncOptions = append(ncOptions, nats.RetryOnFailedConnect(true))
		ncOptions = append(ncOptions, nats.MaxReconnects(opt.MaxReconnects))
		ncOptions = append(ncOptions, nats.ReconnectWait(opt.internalReconnectWaitSeconds))
		b.nc, err = nats.Connect(opt.NatsURL, ncOptions...)
		if err != nil {
			return nil, err
		}
	}
	if opt.CreateBucket {
		js, err := b.nc.JetStream(nats.MaxWait(opt.internalMaxWaitSeconds))
		if err != nil {
			return nil, err
		}
		bucketConfig := nats.ObjectStoreConfig{
			Bucket:      opt.NatsBucket,
			Description: opt.NatsBucketDescription,
			Replicas:    opt.NatsBucketReplicas,
		}
		if opt.CreateBucketPlacementCluster != "" || opt.CreateBucketPlacementTagList != "" {
			placementParams := nats.Placement{}
			if opt.CreateBucketPlacementCluster != "" {
				placementParams.Cluster = opt.CreateBucketPlacementCluster
			}
			if opt.CreateBucketPlacementTagList != "" {
				tags := strings.Split(opt.CreateBucketPlacementTagList, ",")
				placementParams.Tags = tags
			}
			bucketConfig.Placement = &placementParams
		}
		_, err = js.CreateObjectStore(&bucketConfig)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func init() {
	simpleblob.RegisterBackend("nats", func(ctx context.Context, p simpleblob.InitParams) (simpleblob.Interface, error) {
		var opt Options
		if err := p.OptionsThroughYAML(&opt); err != nil {
			return nil, err
		}
		opt.Logger = p.Logger
		return New(ctx, opt)
	})
}
