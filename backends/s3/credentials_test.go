package s3_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PowerDNS/simpleblob/backends/s3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	testcontainersminio "github.com/testcontainers/testcontainers-go/modules/minio"
)

// TestCredentialsFilesUpdate ensures that the file contents are updated
// and that the minio client catches it.
func TestCredentialsFilesUpdate(t *testing.T) {
	container, addr := setupMinioServer(t)
	tempDir := t.TempDir()

	access, secret, token := secretsPaths(tempDir)

	// Instantiate provider (what we're testing).
	provider := &s3.FileSecretsCredentials{
		AccessKeyFile:    access,
		SecretKeyFile:    secret,
		SessionTokenFile: token,
	}

	// Create minio client, using our provider.
	creds := credentials.New(provider)
	clt, err := minio.New(addr, &minio.Options{Creds: creds, Region: s3.DefaultRegion})
	if err != nil {
		t.Fatal(err)
	}

	assertClientSuccess := func(want bool, when string) {
		_, err = clt.BucketExists(t.Context(), "doesnotmatter")
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
	writeSecrets(t, tempDir, "", "", "")

	// The files do not hold the right values,
	// so a call to the server should fail.
	assertClientSuccess(false, "just after init")

	// Write the right keys to the files.
	// We're not testing expiry here,
	// and forcing credentials cache to update.
	writeSecrets(t, tempDir, container.Username, container.Password, "")
	creds.Expire()
	assertClientSuccess(true, "after changing files content")

	// Change content of the files.
	writeSecrets(t, tempDir, "bad-user", "bad-password", "")
	creds.Expire()
	assertClientSuccess(false, "after changing again, to bad credentials")

	// Switch to a method with a session token.
	sts := setupSTS(t, addr, container.Username, container.Password)
	writeSecrets(t, tempDir, sts.AccessKeyID, sts.SecretAccessKey, sts.SessionToken)
	creds.Expire()
	assertClientSuccess(true, "after switching to session token")

	// Back without session token.
	writeSecrets(t, tempDir, container.Username, container.Password, "")
	creds.Expire()
	assertClientSuccess(true, "after removing session token")
}

// TestCredentials ensures credentials are read and allow access to S3.
func TestCredentials(t *testing.T) {
	container, addr := setupMinioServer(t)

	// Setup credentials that use a session token
	sts := setupSTS(t, addr, container.Username, container.Password)

	testCases := []struct {
		label        string
		wantOK       bool
		accessKey    string
		secretKey    string
		sessionToken string
		write        bool
	}{
		{"missing files", false,
			"", "", "", false},
		{"empty files", false,
			"", "", "", true},
		{"bad files", false,
			"foo", "bar", "baz", true},
		{"good files", true,
			container.Username, container.Password, "", true},
		{"good files with newlines", true,
			container.Username + "\n", container.Password + "\n", "\n", true},
		{"good files with session token", true,
			sts.AccessKeyID, sts.SecretAccessKey, sts.SessionToken, true},
		{"good files but bad session token", false,
			sts.AccessKeyID, sts.SecretAccessKey, "bad", true},
	}
	for _, tc := range testCases {
		t.Run(tc.label, func(t *testing.T) {
			tempdir := t.TempDir()
			akf, skf, stf := secretsPaths(tempdir)
			cutName, _ := strings.CutPrefix(t.Name(), "TestCredentials/")
			bucket := strings.ReplaceAll(cutName, "_", "-") + ".test-bucket"
			if tc.write {
				writeSecrets(t, tempdir, tc.accessKey, tc.secretKey, tc.sessionToken)
			}

			st, err := s3.New(t.Context(), s3.Options{
				AccessKeyFile:    akf,
				SecretKeyFile:    skf,
				SessionTokenFile: stf,
				Bucket:           bucket,
				CreateBucket:     true,
				EndpointURL:      "http://" + addr,
			})
			ok := err == nil
			if ok != tc.wantOK {
				t.Fatalf("unexpected error %q", err)
			}
			if !ok {
				return
			}
			// Do a quick operation to ensure the backend has done something.
			// The above operation should be enough since it creates the bucket,
			// this extra check will come handy if the bucket creation moves.
			err = st.Store(t.Context(), "foo", []byte("bar"))
			ok = err == nil
			if ok != tc.wantOK {
				t.Fatalf("unexpected error %q", err)
			}
		})
	}
}

// secretsPaths returns the file paths for the access key
// and the secret key, respectively.
// For a same dir, the returned values will always be the same.
func secretsPaths(dir string) (access, secret, token string) {
	access = filepath.Join(dir, "access-key")
	secret = filepath.Join(dir, "secret-key")
	token = filepath.Join(dir, "session-token")
	return
}

// writeSecrets writes content to files called "access-key" and "secret-key"
// in dir.
func writeSecrets(t testing.TB, dir, adminUser, password, token string) {
	akf, skf, stf := secretsPaths(dir)
	err := os.WriteFile(akf, []byte(adminUser), 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(skf, []byte(password), 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(stf, []byte(token), 0600)
	if err != nil {
		t.Fatal(err)
	}
}

// setupMinioServer starts a container and returns information to connect.
func setupMinioServer(t *testing.T) (container *testcontainersminio.MinioContainer, addr string) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	container, err := testcontainersminio.Run(t.Context(), "quay.io/minio/minio")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Log(err)
		}
	})
	addr, err = container.ConnectionString(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return container, addr
}

// setupSTS returns credentials that require a session token.
func setupSTS(t *testing.T, addr, username, password string) credentials.Value {
	stsAssumeRole, err := credentials.NewSTSAssumeRole("http://"+addr, credentials.STSAssumeRoleOptions{
		AccessKey: username,
		SecretKey: password,
	})
	if err != nil {
		t.Fatal(err)
	}
	for {
		creds, err := stsAssumeRole.GetWithContext(nil)
		if err != nil {
			if strings.HasPrefix(err.Error(), "Server not initialized") {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			t.Fatal(err)
		}
		return creds
	}
}
