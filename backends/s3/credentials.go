package s3

import (
	"bytes"
	"os"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// FileSecretsCredentials is an implementation of Minio's credentials.Provider,
// allowing to read credentials from Kubernetes or Docker secrets, as described in
// https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure
// and https://docs.docker.com/engine/swarm/secrets.
//
// It supports an empty or deleted SessionTokenFile.
type FileSecretsCredentials struct {
	credentials.Expiry

	// Path to the file containing the access key,
	// e.g. /etc/s3-secrets/access-key.
	AccessKeyFile string

	// Path to the file containing the secret key,
	// e.g. /etc/s3-secrets/secret-key.
	SecretKeyFile string

	// Optional path to the file containing the session token if any,
	// e.g. /etc/s3-secrets/session-token.
	SessionTokenFile string

	// Time between each secrets retrieval.
	RefreshInterval time.Duration
}

// Retrieve implements credentials.Provider.
// It reads files pointed to by p.AccessKeyFilename and p.SecretKeyFilename.
func (c *FileSecretsCredentials) Retrieve() (credentials.Value, error) {
	return c.RetrieveWithCredContext(nil)
}

// RetrieveWithCredContext implements credentials.Provider.
// It reads files pointed to by c.AccessKeyFile, c.SecretKeyFile and c.SessionTokenFile.
// The [*credentials.CredContext] argument is ignored.
func (c *FileSecretsCredentials) RetrieveWithCredContext(*credentials.CredContext) (credentials.Value, error) {
	keyId, err := os.ReadFile(c.AccessKeyFile)
	if err != nil {
		return credentials.Value{}, err
	}
	secretKey, err := os.ReadFile(c.SecretKeyFile)
	if err != nil {
		return credentials.Value{}, err
	}

	var sessionToken []byte
	if c.SessionTokenFile != "" {
		sessionToken, err = os.ReadFile(c.SessionTokenFile)
		if err != nil {
			return credentials.Value{}, err
		}
	}

	creds := credentials.Value{
		AccessKeyID:     string(bytes.TrimSpace(keyId)),
		SecretAccessKey: string(bytes.TrimSpace(secretKey)),
		SessionToken:    string(bytes.TrimSpace(sessionToken)),
	}

	c.SetExpiration(time.Now().Add(c.RefreshInterval), -1)

	return creds, err
}

var _ credentials.Provider = new(FileSecretsCredentials)
