package fs

import (
	"fmt"
	"os"
	"path/filepath"
)

// createAtomic creates a new File. The given fpath is the file path of the final destination.
func createAtomic(fpath string) (*atomicFile, error) {
	fpath, err := filepath.Abs(fpath)
	if err != nil {
		return nil, fmt.Errorf("absolute path for atomic file %q: %w", fpath, err)
	}
	// Using the PID under the assumption that the same program will not be writing to
	// the same path at the same time. An overwrite later on retry is desired, if
	// not cleaned properly.
	tmp := fmt.Sprintf("%s.%d%s", fpath, os.Getpid(), ignoreSuffix)
	file, err := os.Create(tmp)
	if err != nil {
		return nil, fmt.Errorf("create atomic file %q: %w", fpath, err)
	}
	return &atomicFile{
		file: file,
		path: fpath,
		tmp:  tmp,
	}, nil
}

// atomicFile implements an io.WriteCloser that writes to a temp file and moves it
// atomically into place on Close.
type atomicFile struct {
	file *os.File // The underlying file being written to.
	path string   // The final path of the file.
	tmp  string   // The path of the file during write.
}

// Write implements io.Writer
func (f *atomicFile) Write(data []byte) (int, error) {
	return f.file.Write(data)
}

// Clean aborts the creation of the file if called before Close. If called
// after Close, it does nothing. This makes it useful in a defer.
func (f *atomicFile) Clean() {
	_ = f.file.Close()
	_ = os.Remove(f.tmp)
}

// Close closes the temp file and moves it to the final destination.
func (f *atomicFile) Close() error {
	var err error
	defer func() {
		if err != nil {
			// The rename did not happen and we're left with
			// the temporary file hanging.
			_ = os.Remove(f.tmp)
		}
	}()

	// Some of the file content may still be cached by the OS,
	// and is not guaranteed to be written to physical device on close.
	// Behaviour is inconsistent across devices and C standard libraries.
	// Syncing file AND its parent directory (here) ensure this.
	// See fsync(2) and open(2).
	if err = f.file.Sync(); err != nil {
		_ = f.file.Close()
		return err
	}
	if err = f.file.Close(); err != nil {
		return err
	}

	// Move into place
	if err = os.Rename(f.tmp, f.path); err != nil {
		return err
	}

	var dir *os.File
	dir, err = os.Open(filepath.Dir(f.path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	if err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
