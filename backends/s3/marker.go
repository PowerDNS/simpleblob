package s3

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// setMarker puts name and etag into the object identified by
// UpdateMarkerFilename.
// An empty etag string means that the object identified by name was deleted.
//
// In case the UseUpdateMarker option is false, this function doesn't do
// anything and returns no error.
func (b *Backend) setMarker(ctx context.Context, name, etag string, isDel bool) error {
	if !b.opt.UseUpdateMarker {
		return nil
	}
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	lines := []string{name, etag, ts}
	if isDel {
    		lines = append(lines, "del")
	}
	joined := []byte(strings.Join(lines, "\x00"))
	_, err := b.doStore(ctx, UpdateMarkerFilename, joined)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastList = nil
	b.lastMarker = joined
	return nil
}
