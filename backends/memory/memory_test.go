package memory

import (
	"testing"

	"github.com/PowerDNS/simpleblob/tester"
)

func TestBackend(t *testing.T) {
	b := New()
	tester.DoBackendTests(t, b)
}
