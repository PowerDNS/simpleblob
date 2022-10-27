package s3

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// setMarker puts name and etag into the object identified by
// UpdateMarkerFilename.
// An empty etag string means that the object identified by name was deleted.
//
// In case the UseUpdateMarker option is false, this function doesn't do
// anything and returns no error.
func (b *Backend) setMarker(ctx context.Context, name, etag string) error {
	if !b.opt.UseUpdateMarker {
		return nil
	}
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	lines := []string{name, etag, ts}
	joined := strings.Join(lines, "\x00")
	info, err := b.doStore(ctx, UpdateMarkerFilename, []byte(joined))
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastList = nil
	b.lastMarker = info.ETag
	return nil
}

// getUpstreamMarker fetches the ETag of the object identified
// by UpdateMarkerFilename.
func (b *Backend) getUpstreamMarker(ctx context.Context) (string, error) {
	info, err := b.client.StatObject(ctx, b.opt.Bucket, UpdateMarkerFilename, minio.StatObjectOptions{})
	err = convertMinioError(err)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return info.ETag, nil
}
