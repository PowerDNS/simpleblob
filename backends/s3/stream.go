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
	ctx, cancel := context.WithCancel(ctx)
	name = b.prependGlobalPrefix(name)
	pr, pw := io.Pipe()
	go func() {
		// This call returns when the pipe is closed, or when an error occurs.
		ctx1, _ := b.clientTimeoutContext(ctx)
		info, err := b.doStoreReader(ctx1, name, pr, -1)
		if err == nil {
			ctx2, _ := b.clientTimeoutContext(ctx)
			_ = b.setMarker(ctx2, name, info.ETag, false)
		}
		_ = pr.CloseWithError(err)
		cancel()
	}()
	return &writerWrapper{ctx: ctx, pw: pw}, nil
}

// A writerWrapper allows storing data on S3 through a io.WriteCloser.
type writerWrapper struct {
	ctx context.Context
	pw  *io.PipeWriter
}

// Write sends p to store as the S3 object associated with w.
// An error is returned if Write failed previously, an error occurred in S3, or w is already closed.
func (w *writerWrapper) Write(p []byte) (int, error) {
	// Not checking the status of ctx explicitly because it will be propagated
	// from the reader goroutine.
	return w.pw.Write(p)
}

// Close ensures that the written data is saved.
// An error is returned if Write failed previously, an error occurred in S3, or w is already closed.
func (w *writerWrapper) Close() error {
	_, err := w.pw.Write(nil)
	_ = w.pw.Close()
	// Let the reading goroutine finish writing,
	// and write the marker if needed.
	<-w.ctx.Done() // cancelled after writing the marker
	return err
}
