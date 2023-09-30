package nats

import (
	"context"
	"github.com/PowerDNS/simpleblob/tester"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const testUser = "foo"
const testPassword = "foo"
const testBucketName = "test"
const testGlobalPrefix = "TestGlobalPrefix123/"
const testObjectName = "TestObjectName123"
const testObjectContents = "TestObjectContents123"
const testEncryptionKey = "5cdfc91054e7d9dc99fd295a6e27cb4fd01fa91c4e94c424595e9c3e5b5a293e"

func commonTestServer(dir string) *server.Server {
	opts := test.DefaultTestOptions
	opts.StoreDir = dir
	opts.JetStream = true
	opts.Username = testUser
	opts.Password = testPassword
	return test.RunServer(&opts)
}

func commonTestOpts() Options {
	simpleOpts := Options{}
	simpleOpts.NatsUsername = testUser
	simpleOpts.NatsPassword = testPassword
	simpleOpts.CreateBucket = true
	simpleOpts.NatsBucket = testBucketName
	simpleOpts.GlobalPrefix = testGlobalPrefix
	simpleOpts.EncryptionKey = testEncryptionKey
	return simpleOpts
}

func TestNew(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts())
	if err != nil {
		t.Fatal(err)
	}
	t.Log(b.nc.Status().String())
}

func TestBackend_Store_Load_List_Delete(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts())
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

func TestBackend(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts())
	if err != nil {
		t.Fatal(err)
	}
	tester.DoBackendTests(t, b)
}

func TestBackend_recursive(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "simpleblob")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	srv := commonTestServer(dir)
	defer srv.Shutdown()
	b, err := New(context.Background(), commonTestOpts())
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
