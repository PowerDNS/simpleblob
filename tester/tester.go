package tester

import (
	"context"
	"os"
	"testing"

	"github.com/PowerDNS/simpleblob"
	"github.com/stretchr/testify/assert"
)

// DoBackendTests tests a backend for conformance
func DoBackendTests(t *testing.T, b simpleblob.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Starts empty
	ls, err := b.List(ctx, "")
	assert.NoError(t, err)
	assert.Len(t, ls, 0)

	// Forbidden names
	data, err := b.Load(ctx, "")
	assert.Empty(t, data)
	assert.ErrorIs(t, err, simpleblob.ErrNameNotAllowed)
	err = b.Store(ctx, "", []byte{})
	assert.ErrorIs(t, err, simpleblob.ErrNameNotAllowed)
	err = b.Delete(ctx, "")
	assert.ErrorIs(t, err, simpleblob.ErrNameNotAllowed)

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
	data, err = b.Load(ctx, "foo-1")
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

	// Delete existing
	err = b.Delete(ctx, "foo-1")
	assert.NoError(t, err)

	// Delete non-existing
	err = b.Delete(ctx, "foo-1")
	assert.NoError(t, err)

	// Should not exist anymore
	_, err = b.Load(ctx, "foo-1")
	assert.ErrorIs(t, err, os.ErrNotExist)

	// Should not appear in list anymore
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.NotContains(t, ls.Names(), "foo-1")
}
