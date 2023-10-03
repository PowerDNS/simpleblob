package nats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"github.com/PowerDNS/simpleblob/tester"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/assert"
	"math"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"testing"
)

const testUser = "foo"
const testPassword = "foo"
const testBucketName = "test"
const testGlobalPrefix = "TestGlobalPrefix123/"
const testObjectName = "TestObjectName123"
const testObjectContents = "TestObjectContents123"
const testEncryptionKey = "5cdfc91054e7d9dc99fd295a6e27cb4fd01fa91c4e94c424595e9c3e5b5a293e"

func commonTestServer(dir string, port int) *server.Server {
	opts := test.DefaultTestOptions
	opts.StoreDir = dir
	opts.JetStream = true
	opts.Username = testUser
	opts.Password = testPassword
	opts.Port = port
	return test.RunServer(&opts)
}

func commonTestOpts(port int) Options {
	simpleOpts := Options{}
	simpleOpts.NatsUsername = testUser
	simpleOpts.NatsPassword = testPassword
	simpleOpts.CreateBucket = true
	simpleOpts.NatsBucket = testBucketName
	simpleOpts.GlobalPrefix = testGlobalPrefix
	simpleOpts.EncryptionKey = testEncryptionKey
	simpleOpts.NatsURL = "nats://127.0.0.1:" + strconv.Itoa(port)
	return simpleOpts
}

func getEphemeralPort() (int, error) {
	for {
		n, err := rand.Int(rand.Reader, big.NewInt(65534))
		if err != nil {
			return math.MinInt64, err
		}
		if n.Int64() >= 49152 {
			return int(n.Int64()), nil
		}
	}
}

func Fuzz_helperCrypto(f *testing.F) {
	keyBytes, err := hex.DecodeString(testEncryptionKey)
	if err != nil {
		f.Fatal(err)
	}
	if len(keyBytes) < 32 {
		f.Fatal("provided key is too short")
	}
	seeds := []string{"Hello", "World", " ", "Hello%World", "123"}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, orig string) {
		ciphertext, err := helperEncrypt(keyBytes, []byte(orig))
		if err != nil {
			t.Fatal(err)
		}
		plaintext, err := helperDecrypt(keyBytes, ciphertext)
		if err != nil {
			t.Fatal(err)
		}
		if string(plaintext) != orig {
			t.Fatalf("Expected: %s, got %s", orig, string(plaintext))
		}
	})
}

func TestNew(t *testing.T) {
	port, err := getEphemeralPort()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir, port)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts(port))
	if err != nil {
		t.Fatal(err)
	}
	t.Log(b.nc.Status().String())
}

func TestBackend_Store_Load_List_Delete(t *testing.T) {
	port, err := getEphemeralPort()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir, port)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts(port))
	if err != nil {
		t.Fatal(err)
	}
	err = b.Store(context.Background(), testObjectName, []byte(testObjectContents))
	if err != nil {
		t.Fatal(err)
	}
	dat, err := b.Load(context.Background(), testObjectName)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(dat))
	blobs, err := b.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, blob := range blobs {
		t.Log(blob.Name)
		t.Log(blob.Size)
	}
	err = b.Delete(context.Background(), testObjectName)
	if err != nil {
		t.Fatal(err)
	}
}

// IMPORTANT: This fuzz test does not support highly-parallel execution due to port number requirement
// Run a test with the "-parallel 1" (or a low number might work, untested ! ), e.g.
// go test -fuzz=FuzzBackend -fuzztime 30s -parallel 1
func FuzzBackend_Store_Load_List_Delete(f *testing.F) {
	validBucketRe := regexp.MustCompile(`\A[a-zA-Z0-9_-]+\z`)
	validKeyRe := regexp.MustCompile(`\A[-/_=\.a-zA-Z0-9]+\z`)
	keyBytes, err := hex.DecodeString(testEncryptionKey)
	if err != nil {
		f.Fatal(err)
	}
	if len(keyBytes) < 32 {
		f.Fatal("provided key is too short")
	}
	seeds := []string{"0000000000000000", "Hello", "World", " ", "Hello_World", "123", "/some/path", "example.com"}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, orig string) {
		// Don't test known bad names
		if !validKeyRe.MatchString(orig) {
			t.Skip()
		}
		dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		port, err := getEphemeralPort()
		if err != nil {
			t.Fatal(err)
		}
		srv := commonTestServer(dir, int(port))
		defer srv.Shutdown()
		op := commonTestOpts(int(port))
		// Don't test known bad names
		if validBucketRe.MatchString(orig) {
			op.NatsBucket = orig
		}
		b, err := New(context.Background(), op)
		if err != nil {
			t.Fatal(err)
		}
		err = b.Store(context.Background(), orig, []byte(orig))
		if err != nil {
			t.Fatal(err)
		}
		dat, err := b.Load(context.Background(), orig)
		if err != nil {
			t.Fatal(err)
		}
		if string(dat) != orig {
			t.Fatalf("expected %s,got %s", orig, string(dat))
		}
		err = b.Delete(context.Background(), orig)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBackend(t *testing.T) {
	port, err := getEphemeralPort()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir, port)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts(port))
	if err != nil {
		t.Fatal(err)
	}
	tester.DoBackendTests(t, b)
}

func TestBackend_recursive(t *testing.T) {
	port, err := getEphemeralPort()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir, port)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts(port))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Starts empty
	ls, err := b.List(ctx, "")
	assert.NoError(t, err)
	assert.Len(t, ls, 0)
	// Add items
	err = b.Store(ctx, "bar-1", []byte("bar1"))
	assert.NoError(t, err)
	err = b.Store(ctx, "bar-2", []byte("bar2"))
	assert.NoError(t, err)
	err = b.Store(ctx, "foo/bar-3", []byte("bar3"))
	assert.NoError(t, err)
	// List all - PrefixFolders disabled (default)
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2", "foo/bar-3"})
	// List all - PrefixFolders enabled
	b.opt.PrefixFolders = true
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2", "foo/"})
	// List all - PrefixFolders disabled
	b.opt.PrefixFolders = false
	ls, err = b.List(ctx, "")
	assert.NoError(t, err)
	assert.Equal(t, ls.Names(), []string{"bar-1", "bar-2", "foo/bar-3"})
}
