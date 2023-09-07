package s3_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PowerDNS/simpleblob/backends/s3"
	"github.com/PowerDNS/simpleblob/backends/s3/s3testing"
	"github.com/PowerDNS/simpleblob/tester"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestFileSecretsCredentials(t *testing.T) {
	tempDir := t.TempDir()

	access, secret := secretsPaths(tempDir)

	// Instanciate provider (what we're testing).
	provider := &s3.FileSecretsCredentials{
		AccessKeyFile: access,
		SecretKeyFile: secret,
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

	// First credential files creation.
	// Keep them empty for now,
	// so that calls to the server will fail.
	writeSecrets(t, tempDir, "")

	// The files do not hold the right values,
	// so a call to the server should fail.
	assertClientSuccess(false, "just after init")

	// Write the right keys to the files.
	// We're not testing expiry here,
	// and forcing credentials cache to update.
	writeSecrets(t, tempDir, s3testing.AdminUserOrPassword)
	creds.Expire()
	assertClientSuccess(true, "after changing files content")

	// Change content of the files.
	writeSecrets(t, tempDir, "badcredentials")
	creds.Expire()
	assertClientSuccess(false, "after changing again, to bad credentials")
}

func TestBackendWithSecrets(t *testing.T) {
	tempDir := t.TempDir()

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	addr, stop, err := s3testing.ServeMinio(ctx, tempDir)
	if errors.Is(err, s3testing.ErrMinioNotFound) {
		t.Skip("minio binary not found locally, make sure it is in PATH")
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()

	// Prepare backend options to reuse.
	// These will not change.
	access, secret := secretsPaths(tempDir)
	opt := s3.Options{
		AccessKeyFile: access,
		SecretKeyFile: secret,
		Region:        "us-east-1",
		Bucket:        "test-bucket",
		CreateBucket:  true,
		EndpointURL:   "http://" + addr,
	}

	// Backend should not start if secrets files do not exist.
	_, err = s3.New(ctx, opt)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal("backend should not start without credentials")
	}

	// Now write files, but with bad content.
	writeSecrets(t, tempDir, "")
	_, err = s3.New(ctx, opt)
	if err == nil || err.Error() != "Access Denied." {
		t.Fatal("backend should not start with bad credentials")
	}

	// Write the good content.
	// Now the backend should start and be able to perform a request.
	writeSecrets(t, tempDir, s3testing.AdminUserOrPassword)

	backend, err := s3.New(ctx, opt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = backend.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	// Finally, the whole test suite should succeed.
	tester.DoBackendTests(t, backend)
}

// secretsPaths returns the file paths for the access key
// and the secret key, respectively.
// For a same dir, the returned values will always be the same.
func secretsPaths(dir string) (access, secret string) {
	access = filepath.Join(dir, "access-key")
	secret = filepath.Join(dir, "secret-key")
	return
}

// writeSecrets writes content to files called "access-key" and "secret-key"
// in dir.
// It returns
func writeSecrets(t testing.TB, dir, content string) {
	access, secret := secretsPaths(dir)
	err := os.WriteFile(access, []byte(content), 0666)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(secret, []byte(content), 0666)
	if err != nil {
		t.Fatal(err)
	}
}
