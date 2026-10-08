//go:build linux

package main

import (
	"golang.org/x/sys/unix"
	"testing"
)

func TestLinuxEPERMExchangePreservesPreviousCollection(t *testing.T) {
	calls := 0
	testUnsupportedCollectionPreservesPrevious(t, func(from, to string) error {
		return exchangeDirectoriesWithRenameat2(from, to, func(fromFD int, old string, toFD int, new string, flags uint) error {
			calls++
			if fromFD != unix.AT_FDCWD || toFD != unix.AT_FDCWD || old != from || new != to || flags != unix.RENAME_EXCHANGE {
				t.Fatalf("wrong exchange syscall arguments")
			}
			return unix.EPERM
		})
	})
	if calls != 101 {
		t.Fatalf("renameat2 calls = %d, want first publication plus 100 refusals", calls)
	}
}
