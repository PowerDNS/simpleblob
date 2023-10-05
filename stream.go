package simpleblob

import (
	"bytes"
	"context"
	"io"
	"io/fs"
)

// A StreamReader is an Interface providing an optimized way to create an io.ReadCloser.
type StreamReader interface {
	Interface
	// NewReader returns an io.ReadCloser, allowing stream reading
	// of named value from the underlying backend.
	NewReader(ctx context.Context, name string) (io.ReadCloser, error)
}

// A StreamWriter is an Interface providing an optimized way to create an io.WriteCloser.
type StreamWriter interface {
	Interface
	// NewWriter returns an io.WriteCloser, allowing stream writing
	// to named key in the underlying backend.
	NewWriter(ctx context.Context, name string) (io.WriteCloser, error)
}

// NewReader returns an optimized io.ReadCloser for backend if available,
// else a basic buffered implementation.
func NewReader(ctx context.Context, st Interface, name string) (io.ReadCloser, error) {
	if sst, ok := st.(StreamReader); ok {
		return sst.NewReader(ctx, name)
	}
	b, err := st.Load(ctx, name)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// A fallbackWriter wraps a backend to satisfy io.WriteCloser.
// The bytes written to it are buffered, then sent to backend when closed.
type fallbackWriter struct {
	st     Interface
	ctx    context.Context
	name   string
	closed bool
	buf    bytes.Buffer
}

// ErrClosed implies that the Close function has already been called.
var ErrClosed = fs.ErrClosed

// Write appends p to the data ready to be stored.
//
// Content will only be sent to backend when w.Close is called.
func (w *fallbackWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, ErrClosed
	}
	return w.buf.Write(p)
}

// Close signifies operations on writer are over.
// The file is sent to backend when called.
func (w *fallbackWriter) Close() error {
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	return w.st.Store(w.ctx, w.name, w.buf.Bytes())
}

// NewWriter returns an optimized io.WriteCloser for backend if available,
// else a basic buffered implementation.
func NewWriter(ctx context.Context, st Interface, name string) (io.WriteCloser, error) {
	// TODO: check name is allowed
	if sst, ok := st.(StreamWriter); ok {
		return sst.NewWriter(ctx, name)
	}
	return &fallbackWriter{
		st:   st,
		ctx:  ctx,
		name: name,
	}, nil
}
