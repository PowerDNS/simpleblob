package simpleblob

import (
	"slices"
	"sort"
	"strconv"
	"strings"
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

func BenchmarkBlobListSort(b *testing.B) {
	var blobList BlobList
	for i := range 100 {
		blobList = append(blobList, Blob{Name: strconv.Itoa(i), Size: int64(i)})
	}
	checkSorted := func() {
		if !slices.IsSortedFunc(blobList, func(a, b Blob) int {
			return strings.Compare(a.Name, b.Name)
		}) {
			b.Fatal("did not sort")
		}
	}

	b.Run("sort.Sort", func(b *testing.B) {
		s := sortable(blobList)
		for b.Loop() {
			slices.Reverse(blobList)
			sort.Sort(s)
			checkSorted()
		}
	})
	b.Run("slices.SortFunc", func(b *testing.B) {
		for b.Loop() {
			slices.Reverse(blobList)
			blobList.Sort()
			checkSorted()
		}
	})
}

type sortable BlobList

func (s sortable) Len() int               { return len(s) }
func (s sortable) Less(i int, j int) bool { return s[i].Name < s[j].Name }
func (s sortable) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

var _ sort.Interface = (sortable)(nil)
