//go:build unix

package privatefile

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// WriteBelow writes content to relative, a slash-separated path below root,
// and leaves it a regular file with exactly mode perm.
//
// root is trusted; nothing below it is, because it may be a repository
// checkout whose author chose its entries. Each directory on the way is opened
// relative to its parent without following a symbolic link, and created 0700
// when it is missing. A symbolic link or anything but a directory on the way,
// or anything but a regular file at the final name, is refused with an error
// that names it. The content is staged under a fresh name in the final
// directory and renamed over the final name, so an existing regular file is
// replaced whole and never written through.
func WriteBelow(root, relative string, content []byte, perm os.FileMode) error {
	parts := strings.Split(relative, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("%q is not a clean relative path", relative)
		}
	}
	dir, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", root, err)
	}
	defer func() { _ = unix.Close(dir) }()
	for index, part := range parts[:len(parts)-1] {
		next, err := openDirectoryAt(dir, part, strings.Join(parts[:index+1], "/"))
		if err != nil {
			return err
		}
		_ = unix.Close(dir)
		dir = next
	}
	name := parts[len(parts)-1]
	var stat unix.Stat_t
	if err := unix.Fstatat(dir, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		if err := requireKind(uint32(stat.Mode), unix.S_IFREG, relative, "a regular file"); err != nil {
			return err
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("inspect %s: %w", relative, err)
	}
	staged, err := stageBelow(dir, name, content, perm)
	if err != nil {
		return fmt.Errorf("stage %s: %w", relative, err)
	}
	// rename replaces the directory entry itself: a link that appeared since
	// the check above is replaced, never followed.
	if err := unix.Renameat(dir, staged, dir, name); err != nil {
		_ = unix.Unlinkat(dir, staged, 0)
		return fmt.Errorf("replace %s: %w", relative, err)
	}
	if err := unix.Fsync(dir); err != nil {
		return fmt.Errorf("sync the directory of %s: %w", relative, err)
	}
	return nil
}

// openDirectoryAt opens, creating it when missing, the directory part of the
// directory parent. name is the path below the root, for errors.
func openDirectoryAt(parent int, part, name string) (int, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(parent, part, &stat, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
		if err := unix.Mkdirat(parent, part, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
			return -1, fmt.Errorf("create %s: %w", name, err)
		}
	} else if err != nil {
		return -1, fmt.Errorf("inspect %s: %w", name, err)
	} else if err := requireKind(uint32(stat.Mode), unix.S_IFDIR, name, "a directory"); err != nil {
		return -1, err
	}
	// O_NOFOLLOW holds even if the entry was swapped for a link since the check.
	fd, err := unix.Openat(parent, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open %s without following links: %w", name, err)
	}
	return fd, nil
}

func requireKind(mode, want uint32, name, wanted string) error {
	switch mode & unix.S_IFMT {
	case want:
		return nil
	case unix.S_IFLNK:
		return fmt.Errorf("%s is a symbolic link; refusing to write through it", name)
	default:
		return fmt.Errorf("%s is not %s", name, wanted)
	}
}

// stageBelow writes content to a new file beside name in dir and returns the
// staged file's name. The file is created exclusively, so it cannot be
// anything a repository put there.
func stageBelow(dir int, name string, content []byte, perm os.FileMode) (string, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	staged := "." + name + ".tmp-" + hex.EncodeToString(random[:])
	fd, err := unix.Openat(dir, staged, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return "", err
	}
	file := os.NewFile(uintptr(fd), staged)
	_, err = file.Write(content)
	if err == nil {
		// The mode is set on the descriptor, so the umask cannot narrow it
		// and nothing at a path can widen it.
		err = file.Chmod(perm.Perm())
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = unix.Unlinkat(dir, staged, 0)
		return "", err
	}
	return staged, nil
}
