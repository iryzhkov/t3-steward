// Package symlinkpath names the symbolic link that makes a path fail a check
// that refuses symlinks, so that a refusal can say which component was a link
// and where it pointed instead of only that the path was not real.
package symlinkpath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// First walks path from the root and returns the first existing component
// that is a symbolic link, together with the target that link names as
// written. ok is false when no component is a link, including when the walk
// reaches a component that does not exist or cannot be inspected first.
func First(path string) (link, target string, ok bool) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", "", false
	}
	current := string(filepath.Separator)
	for _, part := range strings.Split(absolute, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return "", "", false
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(current)
			if err != nil {
				return "", "", false
			}
			return current, target, true
		}
	}
	return "", "", false
}

// Describe returns "LINK is a symlink to TARGET" for the first symbolic link
// in path, or "" when path contains none.
func Describe(path string) string {
	link, target, ok := First(path)
	if !ok {
		return ""
	}
	return Link(link, target)
}

// Link formats one symbolic link and its target the way Describe does.
func Link(link, target string) string {
	return fmt.Sprintf("%s is a symlink to %s", link, target)
}
