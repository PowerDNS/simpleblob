package fs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PowerDNS/simpleblob/tester"
)

func cleanup(t *testing.T, tmpDir string) func() {
	return func() {
		// Don't want to use the recursive os.RemoveAll() for safety
		if tmpDir == "" {
			return
		}
		entries, err := os.ReadDir(tmpDir)
		assert.NoError(t, err)
		for _, e := range entries {
			assert.False(t, e.IsDir())
			p := filepath.Join(tmpDir, e.Name())
			err := os.Remove(p)
			assert.NoError(t, err)
		}
		err = os.Remove(tmpDir)
		assert.NoError(t, err)
	}
}

func TestBackend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "simpleblob-test-")
	assert.NoError(t, err)
	t.Cleanup(cleanup(t, tmpDir))

	b, err := New(Options{RootPath: tmpDir})
	assert.NoError(t, err)
	tester.DoBackendTests(t, b)
}

func TestFilesystem(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "simpleblob-test-")
	assert.NoError(t, err)
	t.Cleanup(cleanup(t, tmpDir))

	b, err := New(Options{RootPath: tmpDir})
	assert.NoError(t, err)
	tester.DoFSWrapperTests(t, b)
}
