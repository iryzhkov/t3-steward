package workerruntime

import "github.com/iryzhkov/t3-steward/internal/privatefile"

// readPrivateFile keeps the worker runtime's call sites unchanged while the
// rules themselves live in one place, shared with the coordinator client
// bootstrap.
func readPrivateFile(path string, limit int64) ([]byte, error) {
	return privatefile.Read(path, limit)
}
