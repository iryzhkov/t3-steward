//go:build linux

package config

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// stagedInput is a read-only observation, including the identity of every
// ancestor. Traversal is descriptor-relative and never follows symbolic links.
// Absence is observed too, so a newly introduced applicable projection refuses.
type stagedInput struct {
	path     string
	limit    int64
	optional bool
	raw      []byte
	dirs     []unix.Stat_t
	file     unix.Stat_t
	missing  int
}

func sameStagedDir(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid
}

func sameStagedFile(a, b unix.Stat_t) bool {
	return sameStagedDir(a, b) && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func observeStagedInput(path string, limit int64, optional bool) (*stagedInput, error) {
	// Cleaning must not erase a symlink-bearing component such as link/../x.
	if path != filepath.Clean(path) {
		return nil, errors.New("unclean input path")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimPrefix(absolute, "/"), "/")
	s := &stagedInput{path: absolute, limit: limit, optional: optional, missing: -1}
	dir, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(dir) }()
	for i, part := range parts {
		var parent unix.Stat_t
		if err := unix.Fstat(dir, &parent); err != nil {
			return nil, err
		}
		// Root-owned sticky temporary directories permit private owner-only files.
		if parent.Uid != 0 && parent.Uid != uint32(os.Getuid()) ||
			parent.Mode&0022 != 0 && !(parent.Uid == 0 && parent.Mode&unix.S_ISVTX != 0) {
			return nil, errors.New("unsafe input ancestor")
		}
		s.dirs = append(s.dirs, parent)
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
		if i < len(parts)-1 {
			flags |= unix.O_DIRECTORY
		}
		fd, err := unix.Openat(dir, part, flags, 0)
		if err != nil {
			if optional && errors.Is(err, unix.ENOENT) {
				s.missing = i
				return s, nil
			}
			return nil, err
		}
		if i < len(parts)-1 {
			_ = unix.Close(dir)
			dir = fd
			continue
		}
		f := os.NewFile(uintptr(fd), absolute)
		defer f.Close()
		if err := unix.Fstat(fd, &s.file); err != nil {
			return nil, err
		}
		if s.file.Mode&unix.S_IFMT != unix.S_IFREG || s.file.Mode&07777 != 0600 ||
			s.file.Uid != uint32(os.Getuid()) || s.file.Nlink != 1 || s.file.Size < 1 || s.file.Size > limit {
			return nil, errors.New("unsafe private input")
		}
		s.raw, err = io.ReadAll(io.LimitReader(f, limit+1))
		if err != nil || int64(len(s.raw)) != s.file.Size {
			return nil, errors.New("changed private input")
		}
		var after unix.Stat_t
		if err := unix.Fstat(fd, &after); err != nil || !sameStagedFile(s.file, after) {
			return nil, errors.New("changed private input")
		}
	}
	return s, nil
}

func (s *stagedInput) check() error {
	now, err := observeStagedInput(s.path, s.limit, s.optional)
	if err != nil {
		return err
	}
	if now.missing != s.missing || len(now.dirs) != len(s.dirs) || !bytes.Equal(now.raw, s.raw) {
		return errors.New("changed private input")
	}
	// A create/remove cycle at the first absent component changes its private
	// parent even if the projection is absent again. Do not compare timestamps
	// on shared ancestors such as /tmp, where unrelated writes are legitimate.
	if s.missing >= 0 {
		i := len(s.dirs) - 1
		if s.dirs[i].Mtim != now.dirs[i].Mtim || s.dirs[i].Ctim != now.dirs[i].Ctim {
			return errors.New("changed absent input")
		}
	}
	for i := range s.dirs {
		if !sameStagedDir(s.dirs[i], now.dirs[i]) {
			return errors.New("changed input ancestor")
		}
	}
	if s.missing < 0 && !sameStagedFile(s.file, now.file) {
		return errors.New("changed private input")
	}
	return nil
}
