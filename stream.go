package simpleblob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
)

// A ReaderStorage is an Interface providing an optimized way to create an io.ReadCloser.
type ReaderStorage interface {
	Interface
	// NewReader returns an io.ReadCloser, allowing stream reading
	// of named value from the underlying backend.
	NewReader(context.Context, string) (io.ReadCloser, error)
}

// A WriterStorage is an Interface providing an optimized way to create an io.WriteCloser.
type WriterStorage interface {
	Interface
	// NewWriter returns an io.WriteCloser, allowing stream writing
	// to named key in the underlying backend.
	NewWriter(context.Context, string) (io.WriteCloser, error)
}

// A reader wraps a backend to satisfy io.ReadCloser.
type reader struct {
	st Interface
	*bytes.Reader
}

// Close signifies operations on reader are over.
func (*reader) Close() error {
	return nil
}

// NewReader returns an optimized io.ReadCloser for backend if available,
// else a basic buffered implementation.
func NewReader(ctx context.Context, st Interface, name string) (io.ReadCloser, error) {
	if sst, ok := st.(ReaderStorage); ok {
		return sst.NewReader(ctx, name)
	}
	b, err := st.Load(ctx, name)
	if err != nil {
		return nil, err
	}
	return &reader{st, bytes.NewReader(b)}, nil
}

// A writer wraps a backend to satisfy io.WriteCloser.
// The bytes written to it are buffered, then sent to backend when closed.
type writer struct {
	st     Interface
	ctx    context.Context
	name   string
	closed bool
	buf    bytes.Buffer
	mu     sync.Mutex
}

var errWClosed = errors.New("WriterStorage closed")

// Write appends p to the data ready to be stored.
//
// Content will only be sent to backend when w.Close is called.
func (w *writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errWClosed
	}
	return w.buf.Write(p)
}

// Close signifies operations on writer are over.
// The file is sent to backend when called.
func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errWClosed
	}
	w.closed = true
	return w.st.Store(w.ctx, w.name, w.buf.Bytes())
}

// NewWriter returns an optimized io.WriteCloser for backend if available,
// else a basic buffered implementation.
func NewWriter(ctx context.Context, st Interface, name string) (io.WriteCloser, error) {
	if sst, ok := st.(WriterStorage); ok {
		return sst.NewWriter(ctx, name)
	}
	return &writer{
		st:   st,
		ctx:  ctx,
		name: name,
	}, nil
}
