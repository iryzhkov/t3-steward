package config

import (
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

type V2Cache struct {
	Name      string            `yaml:"name" json:"name"`
	Toolchain V2CacheToolchain  `yaml:"toolchain" json:"toolchain"`
	Env       map[string]string `yaml:"env" json:"env"`
	MaxMB     int               `yaml:"max_mb" json:"max_mb"`
}

type V2CacheToolchain struct {
	Command     string   `yaml:"command" json:"command"`
	VersionArgs []string `yaml:"version_args" json:"version_args"`
}

var cacheName = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
var cacheEnv = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ValidateWorkerCaches also guards standalone resolver callers.
func ValidateWorkerCaches(worker string, caches []V2Cache) error {
	names, variables := map[string]bool{}, map[string]bool{}
	for _, c := range caches {
		fail := func(reason string) error { return fmt.Errorf("worker %q cache %q: %s", worker, c.Name, reason) }
		if !cacheName.MatchString(c.Name) || names[c.Name] {
			return fail("name must be a unique lowercase directory-safe identifier")
		}
		names[c.Name] = true
		command := c.Toolchain.Command
		if strings.TrimSpace(command) == "" || strings.TrimSpace(command) != command || strings.ContainsRune(command, 0) || (!filepath.IsAbs(command) && (filepath.Base(command) != command || strings.ContainsAny(command, " \t\r\n") || command == "." || command == "..")) {
			return fail("toolchain command must be a bare program name or absolute path")
		}
		if c.MaxMB <= 0 {
			return fail("max_mb must be positive")
		}
		if len(c.Env) == 0 {
			return fail("env must not be empty")
		}
		paths := []string{}
		for variable, path := range c.Env {
			if !cacheEnv.MatchString(variable) || variables[variable] || slices.Contains(domain.TaskWaitEnvironmentNames(), variable) {
				return fail("invalid, duplicate or reserved environment variable " + variable)
			}
			variables[variable] = true
			if path == "." || path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || slices.Contains(strings.Split(path, string(filepath.Separator)), "..") {
				return fail("subpath must be relative, clean and without ..")
			}
			for _, other := range paths {
				if path == other || strings.HasPrefix(path, other+string(filepath.Separator)) || strings.HasPrefix(other, path+string(filepath.Separator)) {
					return fail("subpaths must not be equal or nested")
				}
			}
			paths = append(paths, path)
		}
	}
	return nil
}
