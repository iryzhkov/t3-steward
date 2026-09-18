package platform

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// F-10: every role (watchdog, worker, coordinator) runs in t3-steward.service,
// and the rendered unit pins the journal identifier so that
// "journalctl --user -t t3-steward" finds the process however it is started.
func TestRenderUnitPinsSyslogIdentifier(t *testing.T) {
	unit := RenderUnit(InstallOptions{Binary: "/usr/local/bin/t3-steward", ConfigPath: "/home/u/.config/t3-steward/config.yaml"})
	if !strings.Contains(unit, "\nSyslogIdentifier=t3-steward\n") {
		t.Fatalf("rendered unit lacks SyslogIdentifier=t3-steward:\n%s", unit)
	}
	service := strings.Index(unit, "[Service]")
	install := strings.Index(unit, "[Install]")
	identifier := strings.Index(unit, "SyslogIdentifier=")
	if !(service < identifier && identifier < install) {
		t.Fatalf("SyslogIdentifier is outside the [Service] section:\n%s", unit)
	}
	if strings.Contains(unit, "_FILE=") {
		t.Fatalf("a unit without credential files renders a credential line:\n%s", unit)
	}
}

// F-10: --credential-file REF=PATH renders one Environment= line per file, in
// the [Service] section, with the home directory as %h, so the generated unit
// replaces a hand-written wrapper that exported the value.
func TestRenderUnitRendersCredentialFiles(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home directory")
	}
	unit := RenderUnit(InstallOptions{
		Binary: "/usr/local/bin/t3-steward", ConfigPath: "/home/u/.config/t3-steward/config.yaml",
		CredentialFiles: []CredentialFile{
			{Reference: "secretref:f03-admin/normandy", Path: "/etc/t3-steward/admin credential.json"},
			{Reference: "F02_PROTOCOL", Path: filepath.Join(home, ".config/t3-steward/f02/protocol-credential.json")},
		},
	})
	for _, want := range []string{
		"\nEnvironment=T3_STEWARD_CREDENTIAL_F02_PROTOCOL_FILE=%h/.config/t3-steward/f02/protocol-credential.json\n",
		"\nEnvironment=\"T3_STEWARD_CREDENTIAL_SECRETREF_F03_ADMIN_NORMANDY_FILE=/etc/t3-steward/admin credential.json\"\n",
		"\nSyslogIdentifier=t3-steward\n",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("rendered unit lacks %q:\n%s", want, unit)
		}
	}
	service := strings.Index(unit, "[Service]")
	install := strings.Index(unit, "[Install]")
	first := strings.Index(unit, "T3_STEWARD_CREDENTIAL_F02_PROTOCOL_FILE=")
	second := strings.Index(unit, "T3_STEWARD_CREDENTIAL_SECRETREF_F03_ADMIN_NORMANDY_FILE=")
	if !(service < first && first < second && second < install) {
		t.Fatalf("credential lines are outside [Service] or unsorted:\n%s", unit)
	}
	if strings.Contains(unit, home) {
		t.Fatalf("the home directory is rendered literally:\n%s", unit)
	}
	// The same options render the same unit whatever order they came in.
	again := RenderUnit(InstallOptions{
		Binary: "/usr/local/bin/t3-steward", ConfigPath: "/home/u/.config/t3-steward/config.yaml",
		CredentialFiles: []CredentialFile{
			{Reference: "F02_PROTOCOL", Path: filepath.Join(home, ".config/t3-steward/f02/protocol-credential.json")},
			{Reference: "secretref:f03-admin/normandy", Path: "/etc/t3-steward/admin credential.json"},
		},
	})
	if again != unit {
		t.Fatal("credential file order changed the rendered unit")
	}
}
