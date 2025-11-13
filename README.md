# Simpleblob

[![Go Reference](https://pkg.go.dev/badge/github.com/PowerDNS/simpleblob.svg)](https://pkg.go.dev/github.com/PowerDNS/simpleblob)
[![CI Tests](https://github.com/PowerDNS/simpleblob/actions/workflows/go.yml/badge.svg)](https://github.com/PowerDNS/simpleblob/actions/workflows/go.yml)

Simpleblob is a Go module that simplifies the storage of arbitrary data by key from Go code. It ships with the following backends:

- `s3`: S3 bucket storage
- `fs`: File storage (one file per blob)
- `memory`: Memory storage (for tests)


## Usage

The interface implemented by the backends is:

```go
type Interface interface {
	List(ctx context.Context, prefix string) (BlobList, error)
	Load(ctx context.Context, name string) ([]byte, error)
	Store(ctx context.Context, name string, data []byte) error
	Delete(ctx context.Context, name string) error
}
```

To instantiate a backend, `_`-import all the backends that you want to register, and call:

```go
func GetBackend(ctx context.Context, typeName string, options map[string]any, params ...Param) (Interface, error)
```

An example can be found in `example_test.go`.

Every backend accepts a `map[string]any` with options and performs its own validation on the options. If you use a YAML, TOML and JSON, you could structure it like this:

```go
type Storage struct {
	Type    string         `yaml:"type"`
	Options map[string]any `yaml:"options"` // backend specific
}
```


### `io` interfaces

Reading from or writing to a blob directly can be done using the `NewReader` and `NewWriter` functions.

```go
func NewReader(ctx context.Context, storage Interface, blobName string) (io.ReadCloser, error)
func NewWriter(ctx context.Context, storage Interface, blobName string) (io.WriteCloser, error)
```

The returned ReadCloser or WriteCloser is an optimized implementation if the backend being used implements the `StreamReader` or `StreamWriter` interfaces.
If not, a convenience wrapper for the storage is returned.

| Backend | StreamReader | StreamWriter |
| --- | --- | --- |
| S3 | ✔ | ✔ |
| Azure | ✔ | ✔ |
| Filesystem | ✔ | ✔ |
| Memory | ✖ | ✖ |


## API Stability

We support the last two stable Go versions, currently 1.24 and 1.25.

From a API consumer point of view, we do not plan any backward incompatible changes before a v1.0.
