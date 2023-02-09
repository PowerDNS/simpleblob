package simpleblob

import (
	"errors"
	"fmt"
	"strings"
)

var ErrNameNotAllowed = errors.New("name not allowed")

// AllowedName is used to determine if name is suitable to be saved
// by any backend.
func AllowedName(name string) bool {
    	if name == "" {
        	return false
    	}
	if strings.Contains(name, "/") {
		return false
	}
	if strings.Contains(name, "\x00") {
		return false
	}
	if strings.HasPrefix(name, ".") {
		return false
	}
	if strings.HasSuffix(name, ".tmp") {
		return false // used for our temp files when writing
	}
	return true
}

// CheckName calls AllowedName on name, and if false returns an error
// wrapping ErrNameNotAllowed.
func CheckName(name string) error {
	if AllowedName(name) {
		return nil
	}
	return fmt.Errorf("%q: %w", name, ErrNameNotAllowed)
}
