//go:build !linux

package directoryresource

import (
	"errors"
	"os"
)

func Open(Registration) (*os.File, Identity, error) {
	return nil, Identity{}, errors.New("directory identity containment is unsupported on this platform")
}
func Reopen(Identity, Registration) (*os.File, error) {
	return nil, errors.New("directory identity containment is unsupported on this platform")
}
