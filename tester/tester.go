package tester

import (
	"context"
	"crypto/rand"
	"io"
	"io/fs"
	"os"
	"testing"

	"github.com/PowerDNS/simpleblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DoBackendTests tests a backend for conformance
func DoBackendTests(t *testing.T, b simpleblob.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Starts empty
	ls, err := b.List(ctx, "")
	assert.NoError(t, err)
	assert.Len(t, ls, 0)

	// Add items
	foo := []byte("foo") // will be modified later
	err = b.Store(ctx, "foo-1", foo)
	assert.NoError(t, err)
	err = b.Store(ctx, "bar-2", []byte("bar2"))
	assert.NoError(t, err)
	err = b.Store(ctx, "bar-1", []byte("bar"))
	assert.NoError(t, err)

	// Overwrite
	err = b.Store(ctx, "bar-1", []byte("bar1"))
	assert.NoError(t, err)

	// List all
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2", "foo-1"}) // sorted

	// List with prefix
	ls, err = b.List(ctx, "foo-")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"foo-1"})
	assert.Equal(t, ls[0].Size, int64(3))
	ls, err = b.List(ctx, "bar-")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2"}) // sorted

	// Load
	data, err := b.Load(ctx, "foo-1")
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("foo"))

	// Check overwritten data
	data, err = b.Load(ctx, "bar-1")
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("bar1"))

	// Verify that Load makes a copy
	data[0] = '!'
	data, err = b.Load(ctx, "bar-1")
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("bar1"))

	// Change foo buffer to verify that Store made a copy
	foo[0] = '!'
	data, err = b.Load(ctx, "foo-1")
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("foo"))

	// Load non-existing
	_, err = b.Load(ctx, "does-not-exist")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

// DoFSWrapperTests confronts Interface to its fs.FS implementations
func DoFSWrapperTests(t *testing.T, b simpleblob.Interface) {
	// Wrap provided interface into a filesystem
	// and use the backend to check operations on the filesystem.
	// The backend is considered working from DoBackendTests.
	fsys := simpleblob.AsFS(b)

	// Filesystem is empty
	ls, err := fs.Glob(fsys, "*")
	assert.NoError(t, err)
	assert.Len(t, ls, 0)

	// Opening random thing fails
	f, err := fsys.Open("something")
	assert.Error(t, err)
	assert.Nil(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test single item
	fooData := make([]byte, 64)
	_, err = rand.Read(fooData)
	require.NoError(t, err)
	b.Store(ctx, "foo", fooData) // error is ignored, Store is tested above

	// Item isn't listed when prefix is bad
	ls, err = fs.Glob(fsys, "bar")
	assert.NoError(t, err)
	assert.Len(t, ls, 0)

	// Item is listed
	ls, err = fs.Glob(fsys, "*")
	assert.NoError(t, err)
	assert.Len(t, ls, 1)
	assert.Contains(t, ls, "foo")

	// Item can be loaded by name
	f, err = fsys.Open("foo")
	assert.NoError(t, err)
	assert.NotNil(t, f)
	defer func() {
		assert.NoError(t, f.Close())
	}()

	// Item has right content
	p, err := io.ReadAll(f)
	assert.Equal(t, p, fooData)

	// Check file info
	info, err := f.Stat()
	assert.NoError(t, err)
	assert.EqualValues(t, info.Mode(), 666)
	assert.Equal(t, info.Name(), "foo")
	assert.EqualValues(t, info.Size(), 64)
	assert.Equal(t, info.Sys(), fsys)

	// fs.ReadFileFS is satisfied
	p2, err := fs.ReadFile(fsys, "meh")
	assert.Error(t, err)
	assert.Empty(t, p2)
	p2, err = fs.ReadFile(fsys, "foo")
	assert.NoError(t, err)
	assert.Equal(t, p, p2)

	// fs.ReadDirFS is satisfied and allows only "." subdir
	direntries, err := fs.ReadDir(fsys, "meh")
	assert.Error(t, err)
	assert.Nil(t, direntries)
	direntries, err = fs.ReadDir(fsys, ".")
	assert.NoError(t, err)
	assert.Contains(t, direntries, f)

	// fs.SubFS is satisfied and allows only "." subdir
	subfsys, err := fs.Sub(fsys, "anything")
	assert.Error(t, err)
	assert.Nil(t, subfsys)
	subfsys, err = fs.Sub(fsys, ".")
	assert.NoError(t, err)
	assert.Equal(t, fsys, subfsys)
}
