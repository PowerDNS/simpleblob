package s3_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PowerDNS/simpleblob/backends/s3"
	"github.com/PowerDNS/simpleblob/backends/s3/s3testing"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestFileSecretsCredentials(t *testing.T) {
	tempDir := t.TempDir()

	// Instanciate provider (what we're testing).
	provider := &s3.FileSecretsCredentials{
		AccessKeyFile: filepath.Join(tempDir, "access-key"),
		SecretKeyFile: filepath.Join(tempDir, "secret-key"),
	}

	// writeFiles creates or overwrites provider files
	// with the same content.
	writeFiles := func(content string) {
		writeContent := func(filename string) {
			f, err := os.Create(filename)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if content == "" {
				return
			}
			_, err = io.WriteString(f, content)
			if err != nil {
				t.Fatal(err)
			}
		}
		writeContent(provider.AccessKeyFile)
		writeContent(provider.SecretKeyFile)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Create server
	addr, stop, err := s3testing.ServeMinio(ctx, tempDir)
	if errors.Is(err, s3testing.ErrMinioNotFound) {
		t.Skip("minio binary not found locally, make sure it is in PATH")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()

	// First credential files creation.
	// Keep them empty for now,
	// so that calls to the server will fail.
	writeFiles("")

	// Create minio client, using our provider.
	creds := credentials.New(provider)
	clt, err := minio.New(addr, &minio.Options{
		Creds:  creds,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	assertClientSuccess := func(want bool, when string) {
		_, err = clt.BucketExists(ctx, "doesnotmatter")
		s := "fail"
		if want {
			s = "succeed"
		}
		ok := (err == nil) == want
		if !ok {
			t.Fatalf("expected call to %s %s", s, when)
		}
	}

	// The files do not hold the right values,
	// so a call to the server should fail.
	assertClientSuccess(false, "just after init")

	// Write the right keys to the files.
	// We're not testing expiry here,
	// and forcing credentials cache to update.
	writeFiles(s3testing.AdminUserOrPassword)
	creds.Expire()
	assertClientSuccess(true, "after changing files content")

	// Change content of the files.
	writeFiles("badcredentials")
	creds.Expire()
	assertClientSuccess(false, "after changing again, to bad credentials")
}
