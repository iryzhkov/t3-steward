//go:build !windows

package providerlog

import (
	"os"
	"syscall"
)

// statFile returns the inode and size of a file.
func statFile(path string) (uint64, int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	var inode uint64
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		inode = uint64(st.Ino)
	}
	return inode, info.Size(), nil
}
