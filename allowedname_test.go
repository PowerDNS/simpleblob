package simpleblob_test

import (
	"testing"

	"github.com/PowerDNS/simpleblob"
)

func TestAllowedNames(t *testing.T) {
	names := []string{
		"with space",
		" notrim ",
		" .not-really-hidden",       // Notice space before '.'
		"not-really-temporary.tmp ", // Notice space after '.tmp'
	}
	for _, name := range names {
		if !simpleblob.AllowedName(name) {
			t.Logf("%q: should be allowed", name)
			t.Fail()
		}
	}
}

func TestForbiddenNames(t *testing.T) {
	names := []string{
		".hidden",
		"temporary.tmp",
		"has\x00NUL",
		"has/slash",
		"",
	}
	for _, name := range names {
		if simpleblob.AllowedName(name) {
			t.Logf("%q: should be forbidden", name)
			t.Fail()
		}
	}
}
