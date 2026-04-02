package s3

import (
	"context"
	"io"

	"github.com/PowerDNS/simpleblob"
)

// NewReader satisfies StreamReader and provides a read streaming interface to
// a blob located on an S3 server.
func (b *Backend) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	name = b.prependGlobalPrefix(name)
	r, err := b.doLoadReader(ctx, name)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// NewWriter satisfies StreamWriter and provides a write streaming interface to
// a blob located on an S3 server.
func (b *Backend) NewWriter(ctx context.Context, name string) (io.WriteCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	name = b.prependGlobalPrefix(name)
	pr, pw := io.Pipe()
	go func(ctx context.Context, b *Backend, name string, pr *io.PipeReader, cancel context.CancelFunc) {
		// This call returns when the pipe is closed, or when an error occurs.
		info, err := b.doStoreReader(ctx, name, pr, -1)
		if err == nil {
			_ = b.setMarker(ctx, name, info.ETag, false)
		}
		_ = pr.CloseWithError(err)
		cancel()
	}(ctx, b, name, pr, cancel)
	return &writerWrapper{backend: b, ctx: ctx, pw: pw}, nil
}

// A writerWrapper allows storing data on S3 through a io.WriteCloser.
type writerWrapper struct {
	backend *Backend
	prevErr error // never nil after Close has been called
	ctx     context.Context
	pw      *io.PipeWriter
}

// Write sends p to store as the S3 object associated with w.
// An error is returned if Write failed previously, an error occurred in S3, or w is already closed.
func (w *writerWrapper) Write(p []byte) (int, error) {
	// Not checking the status of ctx explicitly because it will be propagated
	// from the reader goroutine.
	if w.prevErr != nil {
		return 0, w.prevErr
	}
	n, err := w.pw.Write(p)
	if err != nil {
		w.prevErr = err
	}
	return n, err
}

// Close ensures that the written data is saved.
// An error is returned if Write failed previously, an error occurred in S3, or w is already closed.
func (w *writerWrapper) Close() (err error) {
	err = w.prevErr
	_ = w.pw.CloseWithError(err)
	if w.prevErr == nil {
		w.prevErr = simpleblob.ErrClosed
	}
	<-w.ctx.Done() // cancelled after writing the marker
	return err
}
