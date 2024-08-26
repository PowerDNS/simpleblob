package fs

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

// NewReader provides an optimized way to read from named file.
func (b *Backend) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	if !allowedName(name) {
		return nil, os.ErrPermission
	}
	fullPath := filepath.Join(b.rootPath, name)
	return os.Open(fullPath)
}

// NewWriter provides an optimized way to write to a file.
func (b *Backend) NewWriter(ctx context.Context, name string) (io.WriteCloser, error) {
	if !allowedName(name) {
		return nil, os.ErrPermission
	}
	fullPath := filepath.Join(b.rootPath, name)
	return createAtomic(fullPath)
}
