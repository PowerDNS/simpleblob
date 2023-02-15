package simpleblob

import (
	"fmt"
	"strings"
)

// AllowedName is used to determine if name is suitable to be saved
// by any backend.
func AllowedName(s string) bool {
	return CheckName(s) == nil
}

// CheckName returns nil if s is a suitable name for a blob.
// Otherwise, an error is returned, describing why the name is rejected.
func CheckName(s string) error {
	if s == "" {
		return &NameError{Kind: "empty"}
	}
	contents := []string{"/", "\x00"}
	prefixes := []string{"."}
	suffixes := []string{".tmp"}
	for _, t := range contents {
		if strings.Contains(s, t) {
			return &NameError{s, "content", t}
		}
	}
	for _, t := range prefixes {
		if strings.HasPrefix(s, t) {
			return &NameError{s, "prefix", t}
		}
	}
	for _, t := range suffixes {
		if strings.HasSuffix(s, t) {
			return &NameError{s, "suffix", t}
		}
	}
	return nil
}

// A NameError contains information about a string, which use is forbidden
// as the name of a blob.
type NameError struct {
	Name      string // The full name that prompted the error.
	Kind      string // "prefix", "suffix", or "content".
	Forbidden string // The problematic part of name.
}

func (e *NameError) Error() string {
	if e.Kind == "empty" {
		return fmt.Sprintf("%q: name not allowed, cannot be empty", e.Name)
	}
	return fmt.Sprintf("%q: name not allowed, cannot have %s %q", e.Name, e.Kind, e.Forbidden)
}
