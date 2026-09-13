//go:build !linux && !darwin

package t3api

import (
	"errors"
	"os"
)

func openTokenFile(*os.Root) (*os.File, error) {
	return nil, errors.New("contained token reader unsupported on this platform")
}
