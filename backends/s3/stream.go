package s3

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
)

// NewReader satisfies StreamReader and provides a read streaming interface to
// a blob located on an S3 server.
func (b *Backend) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	ctx, cancel := b.clientTimeoutContext(ctx)
	name = b.prependGlobalPrefix(name)
	r, err := b.doLoadReader(ctx, name)
	if err != nil {
		return nil, err
	}
	return &readWrapper{r, ctx, cancel}, nil
}

// A readWrapper implements io.ReadCloser and allows keeping the context around.
type readWrapper struct {
	obj    *minio.Object
	ctx    context.Context
	cancel context.CancelFunc
}

func (r *readWrapper) Read(b []byte) (n int, err error) {
	n, err = r.obj.Read(b)
	if err == context.DeadlineExceeded {
		return n, context.Cause(r.ctx)
	}
	return n, err
}

func (r *readWrapper) Close() (err error) {
	err = r.obj.Close()
	if err == nil {
		err = context.Cause(r.ctx)
	}
	r.cancel()
	return err
}

// NewWriter satisfies StreamWriter and provides a write streaming interface to
// a blob located on an S3 server.
func (b *Backend) NewWriter(ctx context.Context, name string) (io.WriteCloser, error) {
	ctx, cancel := b.clientTimeoutContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name = b.prependGlobalPrefix(name)
	pr, pw := io.Pipe()
	w := &writerWrapper{
		ctx:     ctx,
		backend: b,
		pw:      pw,
	}
	go func(ctx context.Context, b *Backend, name string, pr *io.PipeReader, cancel context.CancelFunc) {
		// This call returns when the pipe is closed, or when an error occurs.
		// It is okay to write to w.info from this goroutine
		// because it will only be used after the WaitGroup is done.
		info, err := b.doStoreReader(ctx, name, pr, -1)
		if err == nil {
			_ = b.setMarker(ctx, name, info.ETag, false)
		}
		cancel()
	}(ctx, b, name, pr, cancel)
	return w, nil
}

// A writerWrapper implements io.WriteCloser and is returned by (*Backend).NewWriter.
type writerWrapper struct {
	backend *Backend
	closed  bool
	prevErr error

	// We keep track of the context in Write and Close.
	ctx context.Context

	// Writes are sent to this pipe
	// and then written to S3 in a background goroutine.
	pw *io.PipeWriter
}

func (w *writerWrapper) Write(p []byte) (n int, err error) {
	if w.closed {
		return 0, w.prevErr
	}
	if err := context.Cause(w.ctx); err != nil {
		_ = w.Close()
		w.prevErr = err
		return 0, err
	}
	n, w.prevErr = w.pw.Write(p)
	return n, w.prevErr
}

func (w *writerWrapper) Close() error {
	if w.closed {
		return w.prevErr
	}
	w.closed = true
	w.prevErr = context.Cause(w.ctx)
	_ = w.pw.CloseWithError(w.prevErr)
	<-w.ctx.Done()
	return w.prevErr
}
