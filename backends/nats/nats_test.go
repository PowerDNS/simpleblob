package nats

import (
	"context"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"os"
	"testing"
)

const testUser = "foo"
const testPassword = "foo"
const testBucketName = "test"
const testGlobalPrefix = "TestGlobalPrefix123"
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
