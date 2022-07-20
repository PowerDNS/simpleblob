package tester

import (
	"context"
	"io"
	"io/fs"
	"os"
	"testing"

	"github.com/PowerDNS/simpleblob"
	"github.com/stretchr/testify/assert"
)

// DoBackendTests tests a backend for conformance
func DoBackendTests(t *testing.T, b simpleblob.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wrap provided interface into a filesystem
	// and use the backend to check operations on the filesystem.
	// Such operations will be performed along the ones on the backend.
	fsys := simpleblob.AsFS(context.Background(), b)

	// Starts empty
	ls, err := b.List(ctx, "")
	assert.NoError(t, err)
	assert.Len(t, ls, 0)
	// With FS
	dirls, err := fs.ReadDir(fsys, ".")
	assert.NoError(t, err)
	assert.Len(t, dirls, 0)

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
	expectedNames := []string{"bar-1", "bar-2", "foo-1"} // sorted
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), expectedNames)
	// With FS
	dirls, err = fs.ReadDir(fsys, ".")
	assert.NoError(t, err)
	assert.Len(t, dirls, len(expectedNames))
	for i, entry := range dirls {
    		assert.Equal(t, expectedNames[i], entry.Name())
	}

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
	// With FS
	f, err := fsys.Open("foo-1")
	assert.NoError(t, err)
	dataf, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, data, dataf)
	assert.NoError(t, f.Close())
	// With ReadFileFS
	datar, err := fs.ReadFile(fsys, "foo-1")
	assert.NoError(t, err)
	assert.Equal(t, data, datar)

	// Check overwritten data
	data, err = b.Load(ctx, "bar-1")
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("bar1"))
	// With FS
	f, err = fsys.Open("bar-1")
	assert.NoError(t, err)
	dataf, err = io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, data, dataf)
	assert.NoError(t, f.Close())
	// With ReadFileFS
	datar, err = fs.ReadFile(fsys, "bar-1")
	assert.NoError(t, err)
	assert.Equal(t, data, datar)

	// Verify that Load makes a copy
	data[0] = '!'
	data, err = b.Load(ctx, "bar-1")
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("bar1"))
	// With FS
	f, err = fsys.Open("bar-1")
	assert.NoError(t, err)
	dataf, err = io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, data, dataf)
	assert.NoError(t, f.Close())
	// With ReadFileFS
	datar, err = fs.ReadFile(fsys, "bar-1")
	assert.NoError(t, err)
	assert.Equal(t, data, datar)

	// Change foo buffer to verify that Store made a copy
	foo[0] = '!'
	data, err = b.Load(ctx, "foo-1")
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("foo"))
	// With FS
	f, err = fsys.Open("foo-1")
	assert.NoError(t, err)
	dataf, err = io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, data, dataf)
	assert.NoError(t, f.Close())
	// With ReadFileFS
	datar, err = fs.ReadFile(fsys, "foo-1")
	assert.NoError(t, err)
	assert.Equal(t, data, datar)

	// Load non-existing
	_, err = b.Load(ctx, "does-not-exist")
	assert.ErrorIs(t, err, os.ErrNotExist)
	// With FS
	f, err = fsys.Open("something")
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, f)
	// With ReadFileFS
	datar, err = fs.ReadFile(fsys, "does-not-exist-either")
	assert.Error(t, err)
	assert.Empty(t, datar)

	// Delete existing
	err = b.Delete(ctx, "foo-1")
	assert.NoError(t, err)

	// Delete non-existing
	err = b.Delete(ctx, "foo-1")
	assert.NoError(t, err)
}
