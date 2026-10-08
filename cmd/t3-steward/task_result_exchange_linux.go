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
// RENAME_EXCHANGE.
func exchangeDirectories(from, to string) error {
	err := unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_EXCHANGE)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
		return errExchangeUnsupported
	}
	if err != nil {
		return &os.LinkError{Op: "exchange", Old: from, New: to, Err: err}
	}
	return nil
}
