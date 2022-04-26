
# Simpleblob

[![Go Reference](https://pkg.go.dev/badge/github.com/PowerDNS/simpleblob.svg)](https://pkg.go.dev/github.com/PowerDNS/simpleblob)
[![CI Tests](https://github.com/PowerDNS/simpleblob/actions/workflows/go.yml/badge.svg)](https://github.com/PowerDNS/simpleblob/actions/workflows/go.yml)

Simpleblob is a Go module that simplifies the storage of arbitrary data by key from Go code. It ships with the following backends:

- `s3`: S3 bucket storage
- `fs`: File storage (one file per blob)
- `memory`: Memory storage (for tests)

The interface implemented by the backends is:

```go
type Interface interface {
	List(ctx context.Context, prefix string) (BlobList, error)
	Load(ctx context.Context, name string) ([]byte, error)
	Store(ctx context.Context, name string, data []byte) error
}
```

We plan to extend this with a `Delete` before 1.0.

To instantiate a backend, `_`-import all the backends that you want to register, and call:

```go
func GetBackend(ctx context.Context, typeName string, options map[string]any) (Interface, error)
```

An example can be found in `example_test.go`.

Every backend accepts a `map[string]any` with options and performs its own validation on the options. If you use a YAML, TOML and JSON, you could structure it like this:

```go
type Storage struct {
	Type    string         `yaml:"type"`
	Options map[string]any `yaml:"options"` // backend specific
}
```

For Go 1.17, replace `any` by `interface{}`.

## Limitations

The interface currently does not support streaming of large blobs. In the future we may provide this by implementing `fs.FS` in the backend for reading, and a similar interface for writing new blobs.

## API Stability

We support the last two stable Go versions, currently 1.17 and 1.18.

From a API consumer point of view, we do not plan any backward incompatible changes before a v1.0.

If you want to implement a storage backend, be aware that we will probably add a `Delete` method before v1.0.

Any future extensions most likely be added with optional interface, similar to the `fs.FS` design. Utility functions that return a compatible implementation will be used for backends that do not implement the interface, if possible.


