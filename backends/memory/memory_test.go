package memory

import (
	"testing"

	"github.com/PowerDNS/simpleblob/tester"
)

func TestBackend(t *testing.T) {
	tester.DoBackendTests(t, New())
	tester.DoFSWrapperTests(t, New())
}
