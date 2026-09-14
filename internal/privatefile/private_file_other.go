//go:build !unix

package privatefile

import "errors"

// Read refuses on platforms without Unix ownership validation, because the
// content of these files is authority-bearing and cannot be trusted without it.
func Read(string, int64) ([]byte, error) {
	return nil, errors.New("private files require Unix ownership validation")
}
