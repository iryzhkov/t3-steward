package platform

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Upgrades must remove the user mount namespace that remaps root-owned SSH
// configuration while retaining the privilege-escalation boundary.
func TestSystemdUserUpgradePreservesSSHConfigOwnership(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	// Installer must not reach this host's systemd manager.
	t.Setenv("PATH", t.TempDir())
	service := &systemdUser{}
	path, err := service.unitPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	before := generatedMarker + "\n[Service]\nNoNewPrivileges=true\nProtectSystem=full\n"
	if err := os.WriteFile(path, []byte(before), 0644); err != nil {
		t.Fatal(err)
	}
	opts := InstallOptions{Binary: "/usr/bin/t3-steward", ConfigPath: "/tmp/config.yaml"}
	if _, err := service.Install(opts); err == nil {
		t.Fatal("changed unit overwritten without force")
	}
	opts.Force = true
	if _, err := service.Install(opts); err != nil {
		t.Fatal(err)
	}
	generated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	example, err := os.ReadFile("../../packaging/systemd/t3-steward.service")
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string][]byte{"regenerated": generated, "example": example} {
		values := map[string]string{}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "#") {
				continue
			}
			key, value, ok := strings.Cut(line, "=")
			if ok {
				values[key] = value
			}
		}
		if values["ProtectSystem"] != "false" {
			t.Errorf("%s retains SSH-breaking user mount namespace: %q", name, values["ProtectSystem"])
		}
		if values["NoNewPrivileges"] != "true" {
			t.Errorf("%s lost NoNewPrivileges", name)
		}
		for _, key := range []string{"PrivateUsers", "ProtectHome", "PrivateTmp"} {
			if values[key] != "" && values[key] != "false" {
				t.Errorf("%s enables incompatible %s=%s", name, key, values[key])
			}
		}
	}
}
