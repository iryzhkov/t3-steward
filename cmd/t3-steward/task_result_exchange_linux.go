//go:build linux

package main

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// exchangeDirectories swaps from and to in one rename, so each path names
// either what it named before or what the other did, never nothing. It fails
// with fs.ErrNotExist when either path is missing and with
// errExchangeUnsupported where the kernel or filesystem has no
// RENAME_EXCHANGE. Older container seccomp profiles refuse renameat2 with
// EPERM. A nonempty previous collection is preserved when exchange is refused.
func exchangeDirectories(from, to string) error {
	return exchangeDirectoriesWithRenameat2(from, to, unix.Renameat2)
}

// exchangeDirectoriesWithRenameat2 keeps errno classification on the production
// path while allowing tests to exercise kernels that refuse the syscall.
func exchangeDirectoriesWithRenameat2(from, to string, renameat2 func(int, string, int, string, uint) error) error {
	err := renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_EXCHANGE)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EPERM) {
		return errExchangeUnsupported
	}
	if err != nil {
		return &os.LinkError{Op: "exchange", Old: from, New: to, Err: err}
	}
	return nil
}
