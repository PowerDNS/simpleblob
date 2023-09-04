package s3

import (
	"errors"
	"os"

	"github.com/minio/minio-go/v7/pkg/credentials"
)

// K8sSecretProvider is an implementation of Minio's credentials.Provider,
// allowing to read credentials from Kubernetes secrets, as described in
// https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure.
type K8sSecretProvider struct {
	// Path to the fiel containing the access key,
	// e.g. /etc/s3-secrets/access-key.
	AccessKeyFilename string `json:"access_key_file"`

	// Path to the fiel containing the secret key,
	// e.g. /etc/s3-secrets/secret-key.
	SecretKeyFilename string `json:"secret_key_file"`
}

// IsExpired implements credentials.Provider.
// As there is no totally reliable way to tell
// if a file was modified accross all filesystems except opening it,
// we always return true, and p.Retrieve will open it regardless.
func (*K8sSecretProvider) IsExpired() bool {
	return true
}

// Retrieve implements credentials.Provider.
// It reads files pointed to by p.AccessKeyFilename and p.SecretKeyFilename.
func (p *K8sSecretProvider) Retrieve() (value credentials.Value, err error) {
	load := func(filename string, dst *string) {
		b, err1 := os.ReadFile(filename)
		if err1 != nil {
			err = errors.Join(err, err1)
			return
		}

		*dst = string(b)
	}

	load(p.AccessKeyFilename, &value.AccessKeyID)
	load(p.SecretKeyFilename, &value.SecretAccessKey)

	return value, err
}

var _ credentials.Provider = new(K8sSecretProvider)
