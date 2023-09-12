package s3

import (
	"os"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// FileSecretsCredentials is an implementation of Minio's credentials.Provider,
// allowing to read credentials from Kubernetes or Docker secrets, as described in
// https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure
// and https://docs.docker.com/engine/swarm/secrets.
type FileSecretsCredentials struct {
	credentials.Expiry

	// Path to the file containing the access key,
	// e.g. /etc/s3-secrets/access-key.
	AccessKeyFile string

	// Path to the file containing the secret key,
	// e.g. /etc/s3-secrets/secret-key.
	SecretKeyFile string

	// Time between each secrets retrieval.
	RefreshInterval time.Duration
}

// Retrieve implements credentials.Provider.
// It reads files pointed to by p.AccessKeyFilename and p.SecretKeyFilename.
func (c *FileSecretsCredentials) Retrieve() (credentials.Value, error) {
	keyId, err := os.ReadFile(c.AccessKeyFile)
	if err != nil {
		return credentials.Value{}, err
	}
	secretKey, err := os.ReadFile(c.SecretKeyFile)
	if err != nil {
		return credentials.Value{}, err
	}

	creds := credentials.Value{
		AccessKeyID:     string(keyId),
		SecretAccessKey: string(secretKey),
	}

	c.SetExpiration(time.Now().Add(c.RefreshInterval), -1)

	return creds, err
}

var _ credentials.Provider = new(FileSecretsCredentials)
