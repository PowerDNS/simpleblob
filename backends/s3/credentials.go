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
func (c *FileSecretsCredentials) Retrieve() (value credentials.Value, err error) {
	load := func(filename string, dst *string) error {
		var b []byte
		b, err = os.ReadFile(filename)
		if err != nil {
			return err
		}

		*dst = string(b)
		return nil
	}

	err = load(c.AccessKeyFilename, &value.AccessKeyID)
	if err != nil {
		return value, err
	}
	err = load(c.SecretKeyFilename, &value.SecretAccessKey)

	c.SetExpiration(time.Now().Add(time.Second), -1)

	return value, err
}

// IsZero returns true if both c.AccessKeyFilename and c.SecretKeyFilename
// are empty.
func (c *FileSecretsCredentials) IsZero() bool {
    	return c.AccessKeyFilename == "" && c.SecretKeyFilename == ""
}

var _ credentials.Provider = new(FileSecretsCredentials)
