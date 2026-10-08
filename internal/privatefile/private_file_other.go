//go:build !unix

package privatefile

import (
	"errors"
	"os"
)

// Read refuses on platforms without Unix ownership validation, because the
// content of these files is authority-bearing and cannot be trusted without it.
func Read(string, int64) ([]byte, error) {
	return nil, errors.New("private files require Unix ownership validation")
}

// OpenNoFollow refuses for the same reason as Read.
func OpenNoFollow(string) (*os.File, error) {
	return nil, errors.New("private files require Unix ownership validation")
}

// Owner reports no owner, so every ownership check fails closed.
func Owner(os.FileInfo) (int, bool) {
	return 0, false
}
