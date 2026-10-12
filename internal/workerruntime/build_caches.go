package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/config"
	"golang.org/x/sys/unix"
)

const cacheVersionLimit = 4096
const cacheVersionTimeout = 10 * time.Second

// CacheVersionRunner must honour ctx; the production runner also bounds combined output.
type CacheVersionRunner func(ctx context.Context, command string, args []string) ([]byte, error)

func BuildCacheRoot(workspaces string) string { return filepath.Join(workspaces, "build-caches") }

type cacheOutput struct{ bytes.Buffer }

func (b *cacheOutput) Write(p []byte) (int, error) {
	if len(p) > cacheVersionLimit-b.Len() {
		return 0, fmt.Errorf("version output exceeds %d bytes", cacheVersionLimit)
	}
	return b.Buffer.Write(p)
}

func runCacheVersion(ctx context.Context, command string, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.WaitDelay = time.Second
	output := &cacheOutput{}
	cmd.Stdout, cmd.Stderr = output, output
	err := cmd.Run()
	return output.Bytes(), err
}

var cacheKeyUnsafe = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

// ResolveBuildCaches is inert until a caller explicitly applies its environment.
// Cache problems are reported individually and never prevent task launch.
func ResolveBuildCaches(ctx context.Context, root string, caches []config.V2Cache, runner CacheVersionRunner) (map[string]string, map[string]string) {
	env, skipped := map[string]string{}, map[string]string{}
	if runner == nil {
		runner = runCacheVersion
	}
	for _, cache := range caches {
		if err := config.ValidateWorkerCaches("local", []config.V2Cache{cache}); err != nil {
			skipped[cache.Name] = err.Error()
			continue
		}
		queryCtx, cancel := context.WithTimeout(ctx, cacheVersionTimeout)
		raw, err := runner(queryCtx, cache.Toolchain.Command, cache.Toolchain.VersionArgs)
		if err == nil {
			err = queryCtx.Err()
		}
		cancel()
		version := strings.TrimSpace(string(raw))
		if err == nil && (version == "" || len(raw) > cacheVersionLimit) {
			err = fmt.Errorf("empty or oversized version output")
		}
		if err != nil {
			skipped[cache.Name] = err.Error()
			continue
		}
		label := cacheKeyUnsafe.ReplaceAllString(version, "-")
		if len(label) > 64 {
			label = label[:64]
		}
		sum := sha256.Sum256([]byte(version))
		key := fmt.Sprintf("%s-%x", label, sum[:8])
		resolved := map[string]string{}
		for variable, subpath := range cache.Env {
			relative := filepath.Join(cache.Name, key, subpath)
			if err = makeCacheDirectories(root, relative); err != nil {
				break
			}
			resolved[variable] = filepath.Join(root, relative)
		}
		if err != nil {
			skipped[cache.Name] = err.Error()
			continue
		}
		for variable, path := range resolved {
			env[variable] = path
		}
	}
	return env, skipped
}

// Open each component without following symlinks, including during concurrent
// replacement. Descriptor-relative creation keeps every descendant below root.
func makeCacheDirectories(root, relative string) error {
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if err := unix.Mkdirat(fd, part, 0700); err != nil && err != unix.EEXIST {
			return err
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		_ = unix.Close(fd)
		fd = next
	}
	return nil
}
