package backlog

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/iryzhkov/t3-steward/internal/symlinkpath"
)

// TempDirectory records the temporary directory one environment variable gave
// a verification or gate command: the value the worker was configured with and
// the real path, free of symbolic links, that the command was given instead.
type TempDirectory struct {
	Variable string `json:"variable"`
	// Configured is the worker's own value; empty means the variable was unset
	// and the platform default applied.
	Configured string `json:"configured,omitempty"`
	Resolved   string `json:"resolved"`
}

// defaultTempDirectory is what Go and the C library use when TMPDIR is unset.
const defaultTempDirectory = "/tmp"

// ResolveTempDirectories resolves TMPDIR, and GOTMPDIR when it is set, to real
// paths, creating a missing directory first. A verification or gate command
// runs with the resolved values, so that a host whose temporary directory is
// a symbolic link to another disk cannot fail a task's checks that refuse
// symlinked paths, as Go's own tests of such checks do. lookup reads the
// worker's environment; nil means os.LookupEnv.
func ResolveTempDirectories(lookup func(string) (string, bool)) ([]TempDirectory, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	var directories []TempDirectory
	for _, variable := range []string{"TMPDIR", "GOTMPDIR"} {
		configured, _ := lookup(variable)
		path := configured
		if path == "" {
			if variable == "GOTMPDIR" {
				// Go falls back to TMPDIR, which is already resolved.
				continue
			}
			path = defaultTempDirectory
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve %s %s: %w", variable, path, err)
		}
		if err := os.MkdirAll(absolute, 0o700); err != nil {
			return nil, fmt.Errorf("create %s %s: %w", variable, absolute, err)
		}
		resolved, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return nil, fmt.Errorf("resolve %s %s: %w", variable, absolute, err)
		}
		directories = append(directories, TempDirectory{Variable: variable, Configured: configured, Resolved: resolved})
	}
	return directories, nil
}

// tempEnvironment is the environment a command gets from directories.
func tempEnvironment(directories []TempDirectory) []string {
	environment := make([]string, 0, len(directories))
	for _, directory := range directories {
		environment = append(environment, directory.Variable+"="+directory.Resolved)
	}
	return environment
}

// TempDirectoryWarnings names each temporary directory variable of the
// worker's environment whose path contains a symbolic link, with the link and
// its target, for the worker's health checks. Verification still works,
// because it is given the resolved path, but a task's own tools that read the
// variable from elsewhere would see the link. lookup nil means os.LookupEnv.
func TempDirectoryWarnings(lookup func(string) (string, bool)) []string {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	var warnings []string
	for _, variable := range []string{"TMPDIR", "GOTMPDIR"} {
		value, _ := lookup(variable)
		if value == "" {
			continue
		}
		link, target, ok := symlinkpath.First(value)
		if !ok || systemOwnedLink(link) {
			// A link only root can change, such as macOS's /var, is part of
			// the platform's layout rather than a host's configuration.
			continue
		}
		if link == filepath.Clean(value) {
			warnings = append(warnings, fmt.Sprintf("%s %s", variable, symlinkpath.Link(link, target)))
		} else {
			warnings = append(warnings, fmt.Sprintf("%s %s: %s", variable, value, symlinkpath.Link(link, target)))
		}
	}
	return warnings
}

func systemOwnedLink(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0 && info.Mode().Perm()&0o022 == 0
}
