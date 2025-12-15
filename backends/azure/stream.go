package azure

import (
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/PowerDNS/simpleblob"
)

// NewReader satisfies StreamReader and provides a read streaming interface to
// a blob located on an Azure Storage container.
func (b *Backend) NewReader(ctx context.Context, name string) (io.ReadCloser, error) {
	name = b.prependGlobalPrefix(name)
	r, err := b.doLoadReader(ctx, name)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// NewWriter satisfies StreamWriter and provides a write streaming interface to
// a blob located on an Azure Storage container.
func (b *Backend) NewWriter(ctx context.Context, name string) (io.WriteCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name = b.prependGlobalPrefix(name)
	pr, pw := io.Pipe()
	w := &writerWrapper{
		ctx:      ctx,
		backend:  b,
		name:     name,
		pw:       pw,
		donePipe: make(chan struct{}),
	}
	go func() {
		var err error
		// The following call will return only on error or
		// if the writing end of the pipe is closed.
		// It is okay to write to w.info from this goroutine
		// because it will only be used after w.donePipe is closed.
		w.info, err = w.backend.doStoreReader(w.ctx, w.name, pr, -1)
		_ = pr.CloseWithError(err) // Always returns nil.
		close(w.donePipe)
	}()
	return w, nil
}

// A writerWrapper implements io.WriteCloser and is returned by (*Backend).NewWriter.
type writerWrapper struct {
	backend *Backend

	// We need to keep these around
	// to write the marker in Close.
	ctx  context.Context
	info azblob.UploadStreamResponse
	name string

	// Writes are sent to this pipe
	// and then written to Azure Blob Storage in a background goroutine.
	pw       *io.PipeWriter
	donePipe chan struct{}
}

func (w *writerWrapper) Write(p []byte) (int, error) {
	// Not checking the status of ctx explicitly because it will be propagated
	// from the reader goroutine.
	return w.pw.Write(p)
}

func (w *writerWrapper) Close() error {
	select {
	case <-w.donePipe:
		return simpleblob.ErrClosed
	default:
	}
	_ = w.pw.Close() // Always returns nil.
	<-w.donePipe     // Wait for doStoreReader to return and w.info to be set.
	return w.backend.setMarker(w.ctx, w.name, string(*w.info.ETag), false)
}
