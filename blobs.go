package simpleblob

import (
	"strings"
)

// Blob describes a single blob
type Blob struct {
	Name string
	Size int64
}

// BlobList is a slice of Blob structs
type BlobList []Blob

func (bl BlobList) Len() int {
	return len(bl)
}

func (bl BlobList) Less(i, j int) bool {
	return bl[i].Name < bl[j].Name
}

func (bl BlobList) Swap(i, j int) {
	bl[i], bl[j] = bl[j], bl[i]
}

// Names returns a slice of name strings for the BlobList
func (bl BlobList) Names() []string {
	var names []string
	for _, b := range bl {
		names = append(names, b.Name)
	}
	return names
}

// WithPrefix filters the BlobList to returns only Blob structs where the name
// starts with the given prefix.
func (bl BlobList) WithPrefix(prefix string) (blobs BlobList) {
	for _, b := range bl {
		if !strings.HasPrefix(b.Name, prefix) {
			continue
		}
		blobs = append(blobs, b)
	}
	return blobs
}

// Size returns the total size of all blobs in the BlobList
func (bl BlobList) Size() int64 {
	var size int64
	for _, b := range bl {
		size += b.Size
	}
	return size
}
