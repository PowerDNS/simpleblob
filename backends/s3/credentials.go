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

	// Path to the fiel containing the access key,
	// e.g. /etc/s3-secrets/access-key.
	AccessKeyFilename string `json:"access_key_file"`

	// Path to the fiel containing the secret key,
	// e.g. /etc/s3-secrets/secret-key.
	SecretKeyFilename string `json:"secret_key_file"`
}

// Retrieve implements credentials.Provider.
// It reads files pointed to by p.AccessKeyFilename and p.SecretKeyFilename.
func (c *FileSecretsCredentials) Retrieve() (credentials.Value, error) {
	keyId, err := os.ReadFile(c.AccessKeyFilename)
	if err != nil {
		return credentials.Value{}, err
	}
	secretKey, err := os.ReadFile(c.SecretKeyFilename)
	if err != nil {
		return credentials.Value{}, err
	}

	creds := credentials.Value{
		AccessKeyID:     string(keyId),
		SecretAccessKey: string(secretKey),
	}

	c.SetExpiration(time.Now().Add(time.Second), -1)

	return creds, err
}

// IsZero returns true if both c.AccessKeyFilename and c.SecretKeyFilename
// are empty.
func (c *FileSecretsCredentials) IsZero() bool {
    	return c.AccessKeyFilename == "" && c.SecretKeyFilename == ""
}

var _ credentials.Provider = new(FileSecretsCredentials)
