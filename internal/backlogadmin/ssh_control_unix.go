//go:build unix

package backlogadmin

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// PrepareControlDir returns the directory for the admin client's ssh master
// sockets, <runtimeDir>/t3-steward/ssh, creating it owner-only. It refuses a
// directory that is a symlink, is not owned by this user or is reachable by
// anyone else, because whoever can place a socket there could answer as the
// coordinator's transport. An empty runtimeDir returns "" and no error: the
// caller then runs without connection reuse.
func PrepareControlDir(runtimeDir string) (string, error) {
	if runtimeDir == "" {
		return "", nil
	}
	if !filepath.IsAbs(runtimeDir) {
		return "", fmt.Errorf("ssh control directory: runtime directory %q is not absolute", runtimeDir)
	}
	parent := filepath.Join(runtimeDir, "t3-steward")
	dir := filepath.Join(parent, "ssh")
	for _, path := range []string{parent, dir} {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("ssh control directory: %w", err)
		}
		if err := checkPrivateDir(path); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func checkPrivateDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("ssh control directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("ssh control directory: %s is not a directory", path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("ssh control directory: %s is accessible to other users (mode %o)", path, info.Mode().Perm())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() {
		return fmt.Errorf("ssh control directory: %s is not owned by this user", path)
	}
	return nil
}
