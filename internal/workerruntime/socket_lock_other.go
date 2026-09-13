//go:build !unix

package workerruntime

import (
	"errors"
	"os"
)

func LockWorkerSocket(string) (*os.File, error) {
	return nil, errors.New("persistent workers require Unix socket ownership")
}
