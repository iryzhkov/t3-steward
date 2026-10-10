//go:build linux

package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

func stagedTestHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	return home
}

func stagedWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func stagedConfig(t *testing.T, c Config) (string, []byte) {
	t.Helper()
	raw, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "staged.yaml")
	stagedWrite(t, path, raw)
	return path, raw
}

func stagedRefused(t *testing.T, path string) {
	t.Helper()
	digest, err := ValidateStagedFile(path)
	if digest != "" || !errors.Is(err, ErrStagedConfig) || err.Error() != "configuration validation refused" {
		t.Fatalf("refusal digest=%q err=%v", digest, err)
	}
}

func TestStagedConfigExactBytesAndRuntimeSemantics(t *testing.T) {
	stagedTestHome(t)
	t.Setenv("T3_STEWARD_BACKLOG_V2_MODE", "invalid")
	t.Setenv("T3_STEWARD_T3_URL", "secret-value-invalid")
	t.Setenv("T3_STEWARD_DRY_RUN", "invalid")
	path, raw := stagedConfig(t, validBacklogV2Config(t))
	if _, err := LoadFile(path); err != nil {
		t.Fatalf("runtime fixture: %v", err)
	}
	digest, err := ValidateStagedFile(path)
	want := sha256.Sum256(raw)
	if err != nil || digest != hex.EncodeToString(want[:]) {
		t.Fatalf("digest=%s err=%v", digest, err)
	}
}

func TestStagedConfigSizeBoundary(t *testing.T) {
	stagedTestHome(t)
	raw := []byte("{}\n#" + strings.Repeat("x", (4<<20)-4))
	path := filepath.Join(t.TempDir(), "boundary.yaml")
	stagedWrite(t, path, raw)
	digest, err := ValidateStagedFile(path)
	want := sha256.Sum256(raw)
	if err != nil || digest != hex.EncodeToString(want[:]) {
		t.Fatalf("4MiB boundary %s %v", digest, err)
	}
	stagedWrite(t, path, append(raw, 'x'))
	stagedRefused(t, path)
}

func TestStagedConfigValidationRefusals(t *testing.T) {
	stagedTestHome(t)
	for name, mutate := range map[string]func(*Config){
		"identity": func(c *Config) { c.BacklogV2.Coordinator.ID = "" },
		"storage":  func(c *Config) { c.BacklogV2.Storage.Artifacts = "" },
		"catalog":  func(c *Config) { c.BacklogV2.Workers["normandy"] = V2Worker{} },
		"route":    func(c *Config) { c.BacklogV2.QuotaPools = nil },
		"credential namespace": func(c *Config) {
			*c = clientHostConfig()
			c.BacklogV2.CoordinatorClient.Credential = "secretref:f03-worker/SECRET"
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validBacklogV2Config(t)
			mutate(&cfg)
			path, _ := stagedConfig(t, cfg)
			if _, err := LoadFile(path); err == nil {
				t.Fatal("invalid fixture is accepted by runtime")
			}
			stagedRefused(t, path)
		})
	}
	for name, raw := range map[string]string{
		"unknown":   "SECRET_UNKNOWN_KEY: SECRET_VALUE\n",
		"duplicate": "log_level: info\nlog_level: SECRET_VALUE\n",
		"multiple":  "{}\n---\n{}\n",
		"parse":     "providers: [SECRET_VALUE\n",
		"empty":     "",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "SECRET_PATH.yaml")
			stagedWrite(t, path, []byte(raw))
			stagedRefused(t, path)
		})
	}
	stagedRefused(t, "")
	stagedRefused(t, filepath.Join(t.TempDir(), "missing"))
}

func TestStagedConfigUnsafeInputs(t *testing.T) {
	stagedTestHome(t)
	for _, kind := range []string{"mode", "symlink", "ancestor symlink", "unclean traversal", "hardlink", "directory", "fifo", "oversize", "unsafe ancestor"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "staged.yaml")
			stagedWrite(t, path, []byte("{}\n"))
			switch kind {
			case "mode":
				if err := os.Chmod(path, 0640); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := path + ".link"
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			case "ancestor symlink":
				link := filepath.Join(t.TempDir(), "link")
				if err := os.Symlink(filepath.Dir(path), link); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(link, filepath.Base(path))
			case "unclean traversal":
				path = filepath.Dir(path) + "/ignored/../" + filepath.Base(path)
			case "hardlink":
				if err := os.Link(path, path+".link"); err != nil {
					t.Fatal(err)
				}
			case "directory":
				path = filepath.Dir(path)
			case "fifo":
				path += ".fifo"
				if err := unix.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				stagedWrite(t, path, []byte(strings.Repeat(" ", (4<<20)+1)))
			case "unsafe ancestor":
				if err := os.Chmod(filepath.Dir(path), 0777); err != nil {
					t.Fatal(err)
				}
			}
			stagedRefused(t, path)
		})
	}
	stagedRefused(t, "/dev/null")
	t.Run("foreign owner", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "foreign")
		stagedWrite(t, path, []byte("{}\n"))
		if err := os.Chown(path, os.Getuid()+1, -1); err != nil {
			t.Skip("foreign ownership requires chown privilege")
		}
		stagedRefused(t, path)
	})
}

func TestStagedConfigChangeRefusals(t *testing.T) {
	stagedTestHome(t)
	for _, kind := range []string{"rewrite", "replace same bytes", "remove", "mode", "hardlink", "ancestor replace"} {
		t.Run(kind, func(t *testing.T) {
			path, raw := stagedConfig(t, Default())
			digest, err := validateStagedFile(path, func() {
				switch kind {
				case "rewrite":
					stagedWrite(t, path, []byte("log_level: debug\n"))
				case "replace same bytes":
					stagedWrite(t, path+".new", raw)
					if err := os.Rename(path+".new", path); err != nil {
						t.Fatal(err)
					}
				case "remove":
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				case "mode":
					if err := os.Chmod(path, 0644); err != nil {
						t.Fatal(err)
					}
				case "hardlink":
					if err := os.Link(path, path+".link"); err != nil {
						t.Fatal(err)
					}
				case "ancestor replace":
					dir := filepath.Dir(path)
					if err := os.Rename(dir, dir+".old"); err != nil {
						t.Fatal(err)
					}
					stagedWrite(t, path, raw)
				}
			})
			if digest != "" || !errors.Is(err, ErrStagedConfig) {
				t.Fatalf("changed file accepted: %s %v", digest, err)
			}
		})
	}
}

func TestStagedConfigOwnedProjections(t *testing.T) {
	home := stagedTestHome(t)
	fleetPath := filepath.Join(home, CoordinatorFleetPath)
	// Empty owned catalog is valid and deliberately replaces staged catalog.
	fleet := []byte(`{"kind":"steward-coordinator-catalog-input","schema_version":1,"coordinator_id":"normandy","workers":{},"projects":{}}`)
	path, _ := stagedConfig(t, validBacklogV2Config(t))
	stagedWrite(t, fleetPath, fleet)
	if _, err := LoadFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateStagedFile(path); err != nil {
		t.Fatal(err)
	}
	stagedWrite(t, fleetPath, []byte(strings.ReplaceAll(string(fleet), "normandy", "other")))
	stagedRefused(t, path)
	stagedWrite(t, fleetPath, fleet)
	for _, kind := range []string{"change", "replace", "remove"} {
		t.Run("fleet "+kind, func(t *testing.T) {
			stagedWrite(t, fleetPath, fleet)
			digest, err := validateStagedFile(path, func() {
				switch kind {
				case "change":
					stagedWrite(t, fleetPath, append(fleet, ' '))
				case "replace":
					stagedWrite(t, fleetPath+".new", fleet)
					if err := os.Rename(fleetPath+".new", fleetPath); err != nil {
						t.Fatal(err)
					}
				case "remove":
					if err := os.Remove(fleetPath); err != nil {
						t.Fatal(err)
					}
				}
			})
			if digest != "" || !errors.Is(err, ErrStagedConfig) {
				t.Fatalf("projection changed: %s %v", digest, err)
			}
		})
	}
	_ = os.Remove(fleetPath)
	digest, err := validateStagedFile(path, func() { stagedWrite(t, fleetPath, fleet) })
	if digest != "" || !errors.Is(err, ErrStagedConfig) {
		t.Fatal("new projection accepted")
	}
	_ = os.Remove(fleetPath)
	digest, err = validateStagedFile(path, func() {
		stagedWrite(t, fleetPath, fleet)
		if err := os.Remove(fleetPath); err != nil {
			t.Fatal(err)
		}
	})
	if digest != "" || !errors.Is(err, ErrStagedConfig) {
		t.Fatal("transient new projection accepted")
	}

	clientPath := filepath.Join(home, CoordinatorClientBootstrapPath)
	stagedWrite(t, clientPath, []byte(validBootstrapDocument))
	// A coordinator cannot silently acquire a remote client.
	stagedRefused(t, path)
	path, _ = stagedConfig(t, Default())
	if _, err := LoadFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateStagedFile(path); err != nil {
		t.Fatal(err)
	}
	digest, err = validateStagedFile(path, func() { stagedWrite(t, clientPath, []byte("{}")) })
	if digest != "" || !errors.Is(err, ErrStagedConfig) {
		t.Fatal("client projection changed")
	}
	// Explicit operator client wins and the owned file is not even read.
	path, _ = stagedConfig(t, clientHostConfig())
	if err := os.Chmod(clientPath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateStagedFile(path); err != nil {
		t.Fatal("explicit precedence", err)
	}
}
