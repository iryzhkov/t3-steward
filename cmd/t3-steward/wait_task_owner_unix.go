//go:build unix

package main

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// taskIdentityFileIsOwned refuses an identity record this user does not own.
//
// The file decides which attempt a command speaks for. A file written by
// somebody else, in a directory this process happens to be under, is not
// evidence of anything.
func taskIdentityFileIsOwned(info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("file ownership could not be determined; refusing to read a task identity from it")
	}
	if uid := os.Getuid(); int(stat.Uid) != uid {
		return errors.New("file is owned by another user; refusing to read a task identity from it")
	}
	return nil
}
