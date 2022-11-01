package simpleblob

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v2"
)

// Interface defines the interface storage plugins need to implement.
// This Interface MUST NOT be extended or changed without bumping the major
// version number, because it would break any backends in external repos.
// If you want to provide additional features, you can define additional
// optional interfaces that a backend can implement.
type Interface interface {
	// List retrieves a BlobList with given prefix.
	List(ctx context.Context, prefix string) (BlobList, error)
	// Load brings a whole value, chosen by name, into memory.
	Load(ctx context.Context, name string) ([]byte, error)
	// Store sends value to storage for a given name.
	Store(ctx context.Context, name string, data []byte) error
	// Delete entry, identified by name, from storage. No error is returned if it does not exist.
	Delete(ctx context.Context, name string) error
}

// InitFunc is the type for the backend constructor function used to register
// backend.
type InitFunc func(ctx context.Context, p InitParams) (Interface, error)

// InitParams contains the parameters for the InitFunc. This allows us to pass
// extra values in the future, without breaking existing backends that do not
// expect these.
type InitParams struct {
	OptionMap OptionMap // map of key-value options for this backend
	Logger    logr.Logger
}

// OptionMap is the type for options that we pass internally to backends
type OptionMap map[string]interface{}

// OptionsThroughYAML performs a YAML roundtrip for the OptionMap to load
// them into a struct with yaml tags.
// dest: pointer to destination struct
func (ip InitParams) OptionsThroughYAML(dest interface{}) error {
	// YAML roundtrip to get the options in a nice struct
	y, err := yaml.Marshal(ip.OptionMap)
	if err != nil {
		return err
	}
	if err := yaml.UnmarshalStrict(y, dest); err != nil {
		return err
	}
	return nil
}

// backends is the internal backend registry
var (
	mu       sync.Mutex
	backends = make(map[string]InitFunc)
)

// RegisterBackend registers a new backend.
func RegisterBackend(typeName string, initFunc InitFunc) {
	mu.Lock()
	backends[typeName] = initFunc
	mu.Unlock()
}

// GetBackend creates a new backend instance of given typeName. This type must
// have been previously registered with RegisterBackend.
// The options map contains backend dependant key-value options. Some backends
// take no options, others require some specific options.
// The lifetime of the context passed in must span the lifetime of the whole
// backend instance, not just the init time, so do not set any timeout on it!
//
// Deprecated: consider switching to GetBackendWithParams
func GetBackend(ctx context.Context, typeName string, options map[string]interface{}) (Interface, error) {
	p := InitParams{OptionMap: options}
	return GetBackendWithParams(ctx, typeName, p)
}

// GetBackendWithParams creates a new backend instance of given typeName. This type must
// have been previously registered with RegisterBackend.
// Unlike the old GetBackend, this directly accepts an InitParams struct which allows
// us to add more options on the future.
//
// One notable addition is the InitParams.Logger field that passes a logr.Logger
// to the backend.
//
// The options map contains backend dependant key-value options. Some backends
// take no options, others require some specific options.
//
// The lifetime of the context passed in must span the lifetime of the whole
// backend instance, not just the init time, so do not set any timeout on it!
// TODO: the context lifetime requirement is perhaps error prone and this does
// not allow setting an init timeout. Not sure what would be a good solution.
func GetBackendWithParams(ctx context.Context, typeName string, params InitParams) (Interface, error) {
	if typeName == "" {
		return nil, fmt.Errorf("no storage.type configured")
	}
	mu.Lock()
	initFunc, exists := backends[typeName]
	mu.Unlock()
	if !exists {
		return nil, fmt.Errorf("storage.type %q not found or registered", typeName)
	}
	if params.Logger.GetSink() == nil {
		params.Logger = logr.Discard()
	}
	return initFunc(ctx, params)
}
