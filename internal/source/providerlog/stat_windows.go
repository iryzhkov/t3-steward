//go:build windows

package providerlog

import (
	"os"
)

// statFile returns a stable identity and the size of a file. Windows has
// no inode; the identity is derived from the creation time so that a
// rotated file (renamed away, new file created) is detected.
// statFile returns the size of a file. Windows has no inode, so the identity
// is always zero: rotation is then detected only when the new file is
// shorter than the stored offset, and the unread tail of the rotated file
// is not drained. That is one reason Windows support is experimental.
func statFile(path string) (uint64, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	return 0, info.Size(), nil
}
