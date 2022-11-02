package s3

import (
	"context"
	"fmt"
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
	nanos := time.Now().UnixNano()
	s := fmt.Sprintf("%s\x00%s\x00%d\x00%v", name, etag, nanos, isDel)
	p := []byte(s)
	_, err := b.doStore(ctx, UpdateMarkerFilename, p)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastList = nil
	b.lastMarker = p
	return nil
}
