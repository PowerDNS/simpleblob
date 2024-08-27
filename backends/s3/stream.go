package s3

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
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
	name = b.prependGlobalPrefix(name)
	wrap := &writerWrapper{
		ctx:      ctx,
		backend:  b,
		name:     name,
		donePipe: make(chan struct{}),
	}
	return wrap, nil
}

// A writerWrapper implements io.WriteCloser and is returned by (*Backend).NewWriter.
type writerWrapper struct {
	backend *Backend

	// We need to keep these around
	// to write the marker in Close.
	ctx  context.Context
	info minio.UploadInfo
	name string

	// Writes are sent to this pipe
	// and then written to S3 in a background goroutine.
	pw       *io.PipeWriter
	donePipe chan struct{}
}

func (w *writerWrapper) Write(p []byte) (int, error) {
	if w.pw == nil {
		pr, pw := io.Pipe()
		w.pw = pw
		go func() {
			var err error
			w.info, err = w.backend.doStoreReader(w.ctx, w.name, pr, -1)
			_ = pr.CloseWithError(err)
			close(w.donePipe)
		}()
	}
	return w.pw.Write(p)
}

func (w *writerWrapper) Close() (err error) {
	if w.pw != nil {
		err = w.pw.Close()
		if err != nil {
			return err
		}
		select {
		case <-w.donePipe:
		case <-w.ctx.Done():
			err = w.ctx.Err()
			_ = w.pw.CloseWithError(err)
			return err
		}
	}
	return w.backend.setMarker(w.ctx, w.name, w.info.ETag, false)
}
