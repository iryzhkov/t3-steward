//go:build !unix

package privatefile

import (
	"errors"
	"os"
)

// WriteBelow refuses on platforms without descriptor-relative, no-follow
// opens, because it cannot keep a repository's links from redirecting it.
func WriteBelow(string, string, []byte, os.FileMode) error {
	return errors.New("writing below an untrusted directory requires Unix no-follow opens")
}
