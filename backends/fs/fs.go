package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/PowerDNS/simpleblob"
)

// Options describes the storage options for the fs backend
type Options struct {
	RootPath string `yaml:"root_path"`
}

type Backend struct {
	rootPath string
}

func (b *Backend) List(ctx context.Context, prefix string) (simpleblob.BlobList, error) {
	var blobs simpleblob.BlobList

	entries, err := os.ReadDir(b.rootPath)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		name := e.Name()
		if !simpleblob.AllowedName(name) {
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

	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].Name < blobs[j].Name
	})
	return blobs, nil
}

func (b *Backend) Load(ctx context.Context, name string) ([]byte, error) {
	if err := simpleblob.CheckName(name); err != nil {
		return nil, err
	}
	fullPath := filepath.Join(b.rootPath, name)
	return os.ReadFile(fullPath)
}

func (b *Backend) Store(ctx context.Context, name string, data []byte) error {
	if err := simpleblob.CheckName(name); err != nil {
		return err
	}
	fullPath := filepath.Join(b.rootPath, name)
	tmpPath := fullPath + ".tmp" // ignored by List()
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, fullPath)
}

func (b *Backend) Delete(ctx context.Context, name string) error {
	if err := simpleblob.CheckName(name); err != nil {
		return err
	}
	err := os.Remove(filepath.Join(b.rootPath, name))
	if os.IsNotExist(err) {
		return nil
	}
	return err
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
		var opt Options
		if err := p.OptionsThroughYAML(&opt); err != nil {
			return nil, err
		}
		return New(opt)
	})
}
