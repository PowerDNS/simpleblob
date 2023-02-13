package simpleblob

import (
	"errors"
	"fmt"
	"strings"
)

var ErrNameNotAllowed = errors.New("name not allowed")

// AllowedName is used to determine if name is suitable to be saved
// by any backend.
func AllowedName(s string) bool {
	return CheckName(s) == nil
}

// CheckName returns nil if s is a suitable name for a blob.
// Otherwise, an error is returned, describing why the name is rejected.
func CheckName(s string) error {
	if s == "" {
		return fmt.Errorf("%q: %w (empty)", s, ErrNameNotAllowed)
	}
	contents := []string{"/", "\x00"}
	prefices := []string{"."}
	suffices := []string{".tmp"}
	for _, t := range contents {
		if strings.Contains(s, t) {
			return fmt.Errorf("%q: %w (cannot contain %q)", s, ErrNameNotAllowed, t)
		}
	}
	for _, t := range prefices {
		if strings.HasPrefix(s, t) {
			return fmt.Errorf("%q: %w (cannot have prefix %q)", s, ErrNameNotAllowed, t)
		}
	}
	for _, t := range suffices {
		if strings.HasSuffix(s, t) {
			return fmt.Errorf("%q: %w (cannot have suffix %q)", s, ErrNameNotAllowed, t)
		}
	}
	return nil
}
