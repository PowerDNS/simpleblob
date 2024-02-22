package simpleblob

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlobListStats(t *testing.T) {
	// Empty BlobList
	var blobs BlobList
	assert.Equal(t, blobs.Len(), 0)
	assert.Equal(t, blobs.Size(), int64(0))

	// Non-empty BlobList
	blobs = append(blobs, Blob{
		Name: "blob1",
		Size: 100,
	}, Blob{
		Name: "blob2",
		Size: 200,
	})
	assert.Equal(t, blobs.Len(), 2)
	assert.Equal(t, blobs.Size(), int64(300))
}
