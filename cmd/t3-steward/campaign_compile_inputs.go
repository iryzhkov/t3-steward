package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// compileInputReader reads the extra inputs a plan names, relative to the
// plan's directory. An input that resolves, through a symbolic link, outside
// that directory is refused, as is one that is not a regular file or is larger
// than a campaign may carry.
func compileInputReader(planPath string, limit int64) func(string) ([]byte, error) {
	dir := filepath.Dir(planPath)
	return func(name string) ([]byte, error) {
		base, err := filepath.EvalSymlinks(dir)
		if err != nil {
			return nil, err
		}
		resolved, err := filepath.EvalSymlinks(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			return nil, err
		}
		if rel, err := filepath.Rel(base, resolved); err != nil || !filepath.IsLocal(rel) {
			return nil, errors.New("it resolves outside the plan's directory")
		}
		// The mode is checked before the open, as for the plan: opening a FIFO
		// blocks until a writer appears.
		if info, err := os.Stat(resolved); err != nil {
			return nil, err
		} else if !info.Mode().IsRegular() {
			return nil, errors.New("it is not a regular file")
		}
		file, err := os.Open(resolved)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		reader := io.Reader(file)
		if limit > 0 {
			reader = io.LimitReader(file, limit+1)
		}
		raw, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}
		if limit > 0 && int64(len(raw)) > limit {
			return nil, fmt.Errorf("it is larger than the %d bytes a campaign may carry", limit)
		}
		return raw, nil
	}
}
