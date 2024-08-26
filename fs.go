package simpleblob

import (
	"bytes"
	"context"
	"io/fs"
	"time"
)

// fsInterfaceWrapper wraps an Interface and implements fs.FS.
type fsInterfaceWrapper struct {
	Interface
	ctx context.Context
}

// fsBlobWrapper represents data upstream and implements both fs.File
// and fs.FileInfo for convenience.
type fsBlobWrapper struct {
	b      *Blob
	parent *fsInterfaceWrapper
	r      *bytes.Reader
}

// AsFS casts the provided interface to a fs.FS interface if supported,
// else it wraps it to replicate its functionalities.
func AsFS(ctx context.Context, st Interface) fs.FS {
	if fsys, ok := st.(fs.FS); ok {
		return fsys
	}
	return &fsInterfaceWrapper{st, ctx}
}

// Open retrieves a Blob, wrapped as a fs.File, from the underlying Interface.
func (stw *fsInterfaceWrapper) Open(name string) (fs.File, error) {
	b, err := stw.Load(stw.ctx, name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return &fsBlobWrapper{&Blob{name, int64(len(b))}, stw, nil}, nil
}

// ReadDir satisfies fs.ReadDirFS
func (stw *fsInterfaceWrapper) ReadDir(name string) ([]fs.DirEntry, error) {
	ls, err := stw.List(stw.ctx, "")
	if err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
	}
	ret := make([]fs.DirEntry, len(ls))
	for i, entry := range ls {
		blob := entry
		ret[i] = &fsBlobWrapper{&blob, stw, nil}
	}
	return ret, nil
}

// ReadFile implements fs.ReadFileFS on top of an Interface wrapped as a fs.FS.
func (stw *fsInterfaceWrapper) ReadFile(name string) ([]byte, error) {
	return stw.Load(stw.ctx, name)
}

// fs.FileInfo implementation

func (*fsBlobWrapper) IsDir() bool         { return false }
func (*fsBlobWrapper) ModTime() time.Time  { return time.Time{} }
func (*fsBlobWrapper) Mode() fs.FileMode   { return 0777 }
func (bw *fsBlobWrapper) Name() string     { return bw.b.Name }
func (bw *fsBlobWrapper) Sys() interface{} { return bw.parent }
func (bw *fsBlobWrapper) Size() int64      { return bw.b.Size }

// fs.File implementation

func (bw *fsBlobWrapper) Stat() (fs.FileInfo, error) {
	return bw, nil
}
func (bw *fsBlobWrapper) Read(p []byte) (int, error) {
	if bw.r == nil {
		b, err := bw.parent.Interface.Load(bw.parent.ctx, bw.b.Name)
		if err != nil {
			return 0, err
		}
		bw.r = bytes.NewReader(b)
	}
	return bw.r.Read(p)
}
func (bw *fsBlobWrapper) Close() error {
	if bw.r != nil {
		bw.r = nil
	}
	return nil
}

// fs.DirEntry implementation

func (bw *fsBlobWrapper) Type() fs.FileMode          { return bw.Mode() }
func (bw *fsBlobWrapper) Info() (fs.FileInfo, error) { return bw, nil }
