package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/PowerDNS/simpleblob"
)

// ignoreSuffix is the suffix to use internally
// to hide a file from (*Backend).List.
const ignoreSuffix = ".tmp"

// Options describes the storage options for the fs backend
type Options struct {
	RootPath string `yaml:"root_path"`
}

type Backend struct {
	rootPath string
}

func (b *Backend) List(ctx context.Context, prefix string) (simpleblob.BlobList, error) {
	var blobs simpleblob.BlobList
	if err := ctx.Err(); err != nil {
		return blobs, err
	}

	entries, err := os.ReadDir(b.rootPath)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		name := e.Name()
		if !allowedName(name) {
			continue
		}
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue // could have been removed in the meantime
			}
			return nil, err
		}
		blobs = append(blobs, simpleblob.Blob{
			Name: name,
			Size: info.Size(),
		})
	}

	blobs.Sort()
	return blobs, nil
}

func (b *Backend) Load(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !allowedName(name) {
		return nil, os.ErrNotExist
	}
	fullPath := filepath.Join(b.rootPath, name)
	return os.ReadFile(fullPath)
}

func (b *Backend) Store(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !allowedName(name) {
		return os.ErrPermission
	}
	fullPath := filepath.Join(b.rootPath, name)
	tmpPath := fullPath + ignoreSuffix // ignored by List()
	if err := writeFile(tmpPath, data); err != nil {
		return err
	}
	if err := syncDir(b.rootPath); err != nil {
		return err
	}
	return os.Rename(tmpPath, fullPath)
}

func (b *Backend) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !allowedName(name) {
		return os.ErrPermission
	}
	err := os.Remove(filepath.Join(b.rootPath, name))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func allowedName(name string) bool {
	// TODO: Make shared and test for rejection
	if strings.Contains(name, "/") {
		return false
	}
	if strings.HasPrefix(name, ".") {
		return false
	}
	if strings.HasSuffix(name, ignoreSuffix) {
		return false // used for our temp files when writing
	}
	return true
}

func New(opt Options) (*Backend, error) {
	if opt.RootPath == "" {
		return nil, fmt.Errorf("options.root_path must be set for the fs backend")
	}
	if err := os.MkdirAll(opt.RootPath, 0o755); err != nil {
		return nil, err
	}
	b := &Backend{rootPath: opt.RootPath}
	return b, nil
}

func init() {
	simpleblob.RegisterBackend("fs", func(ctx context.Context, p simpleblob.InitParams) (simpleblob.Interface, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var opt Options
		if err := p.OptionsThroughYAML(&opt); err != nil {
			return nil, err
		}
		return New(opt)
	})
}

func writeFile(name string, data []byte) error {
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return nil
}

func syncDir(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return err
	}
	info, err := dir.Stat()
	if err != nil {
		_ = dir.Close()
		return err
	}
	if !info.IsDir() {
		_ = dir.Close()
		return fmt.Errorf("%s: not a dir", name)
	}
	if err := dir.Sync(); err != nil {
		return err
	}
	return dir.Close()
}
