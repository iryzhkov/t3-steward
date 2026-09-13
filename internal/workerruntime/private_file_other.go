//go:build !unix

package workerruntime

import "errors"

func readPrivateFile(string, int64) ([]byte, error) {
	return nil, errors.New("private worker files require Unix ownership validation")
}
