package simpleblob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"time"
)

// fsInterfaceWrapper wraps an Interface and implements fs.FS.
type fsInterfaceWrapper struct{ Interface }

// fsBlobWrapper represents data upstream and implements both fs.File
// and fs.FileInfo for convenience.
type fsBlobWrapper struct {
	b      *Blob
	parent *fsInterfaceWrapper
	r      *bytes.Reader
}

// AsFS casts the provided interface to a fs.FS interface if supported,
// else it wraps it to replicate its functionalities.
func AsFS(st Interface) fs.FS {
	if fsys, ok := st.(fs.FS); ok {
		return fsys
	}
	return &fsInterfaceWrapper{st}
}

// find gets a Blob in wrapped Interface and returns it wrapped as a fsBlobWrapper.
func (stw *fsInterfaceWrapper) find(name string) (*fsBlobWrapper, error) {
	blobs, err := stw.Interface.List(context.TODO(), name)
	if err != nil {
		return nil, err
	}
	for _, b := range blobs {
		if b.Name == name {
			return &fsBlobWrapper{&b, stw, nil}, nil
		}
	}
	return nil, os.ErrNotExist
}

// Open retrieves a Blob, wrapped as a fs.File, from the underlying Interface.
func (stw *fsInterfaceWrapper) Open(name string) (fs.File, error) {
	if ret, err := stw.find(name); err != nil {
		return nil, err
	} else if ret != nil {
		return ret, nil
	}
	return nil, &fs.PathError{
		Op:   "open",
		Path: name,
		Err:  os.ErrNotExist,
	}
}

// ReadFile implements fs.ReadFileFS on top of an Interface wrapped as a fs.FS.
func (stw *fsInterfaceWrapper) ReadFile(name string) ([]byte, error) {
	return stw.Load(context.Background(), name)
}

// Stat implements fs.StatFS on top of an Interface wrapped as a fs.FS.
func (stw *fsInterfaceWrapper) Stat(name string) (fs.FileInfo, error) {
	if ret, err := stw.find(name); err != nil {
		return nil, err
	} else if ret != nil {
		return ret.Stat()
	}
	return nil, nil
}

func checkDirPath(name, op string) *fs.PathError {
	if name != "" && name != "." {
		return &fs.PathError{Op: op, Path: name, Err: errors.New("no subdirectory available")}
	}
	return nil
}

// ReadDir satisfies fs.ReadDirFS
func (stw *fsInterfaceWrapper) ReadDir(name string) ([]fs.DirEntry, error) {
	if err := checkDirPath(name, "readdir"); err != nil {
		return nil, err
	}
	ls, err := stw.List(context.Background(), "")
	if err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
	}
	ret := make([]fs.DirEntry, len(ls))
	for i, entry := range ls {
		ret[i] = &fsBlobWrapper{&entry, stw, nil}
	}
	return ret, nil
}

// Glob satisfies fs.GlobFS
func (stw *fsInterfaceWrapper) Glob(pattern string) ([]string, error) {
	var prefix string
	if pattern != "" && !strings.ContainsAny(pattern, `*?[\`) {
		prefix = pattern
	}
	ls, err := stw.List(context.Background(), prefix)
	if err != nil {
		return nil, &fs.PathError{Op: "glob", Path: pattern, Err: err}
	}
	var ret []string
	for _, b := range ls {
		if ok, _ := path.Match(pattern, b.Name); ok {
			ret = append(ret, b.Name)
		}
	}
	return ret, nil
}

// Sub satisfies fs.SubFS
func (stw *fsInterfaceWrapper) Sub(dir string) (fs.FS, error) {
	if err := checkDirPath(dir, "sub"); err != nil {
		return nil, err
	}
	return stw, nil
}

// fs.FileInfo implementation

func (*fsBlobWrapper) IsDir() bool         { return false }
func (*fsBlobWrapper) ModTime() time.Time  { return time.Time{} }
func (*fsBlobWrapper) Mode() fs.FileMode   { return 666 }
func (bw *fsBlobWrapper) Name() string     { return bw.b.Name }
func (bw *fsBlobWrapper) Sys() interface{} { return bw.parent }
func (bw *fsBlobWrapper) Size() int64      { return bw.b.Size }

// fs.File implementation

func (bw *fsBlobWrapper) Stat() (fs.FileInfo, error) {
	return bw, nil
}
func (bw *fsBlobWrapper) Read(p []byte) (int, error) {
	if bw.r == nil {
		b, err := bw.parent.Interface.Load(context.TODO(), bw.b.Name)
		if err != nil {
			return 0, err
		}
		bw.r = bytes.NewReader(b)
	}
	n, err := bw.r.Read(p)
	if err == io.EOF {
    		bw.r = nil
	}
	return n, err
}
func (bw *fsBlobWrapper) Close() error { return nil }

// fs.DirEntry implementation

func (bw *fsBlobWrapper) Type() fs.FileMode          { return bw.Mode() }
func (bw *fsBlobWrapper) Info() (fs.FileInfo, error) { return bw, nil }
