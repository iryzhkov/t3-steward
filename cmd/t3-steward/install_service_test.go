package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// F-10: install-service --credential-file REF=PATH is parsed once per
// occurrence, the path is made absolute and checked the way the runtime will
// check it, and a malformed or unusable argument is refused before any unit
// is written.
func TestInstallServiceCredentialFilesAreParsedAndChecked(t *testing.T) {
	dir := t.TempDir()
	protocol := filepath.Join(dir, "protocol-credential.json")
	if err := os.WriteFile(protocol, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	admin := filepath.Join(dir, "admin.json")
	if err := os.WriteFile(admin, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	files, err := parseCredentialFiles([]string{"F02_PROTOCOL=" + protocol, "secretref:f03-admin/normandy=" + admin})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Reference != "F02_PROTOCOL" || files[0].Path != protocol || files[1].Reference != "secretref:f03-admin/normandy" || files[1].Path != admin {
		t.Fatalf("files = %+v", files)
	}

	for argument, want := range map[string]string{
		"F02_PROTOCOL": "expected REF=PATH",
		"=" + protocol: "expected REF=PATH",
		"F02_PROTOCOL=" + filepath.Join(dir, "absent"): "no such file",
	} {
		if _, err := parseCredentialFiles([]string{argument}); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%q: error = %v, want %q", argument, err, want)
		}
	}
	if _, err := parseCredentialFiles([]string{"F02_PROTOCOL=" + protocol, "f02-protocol=" + admin}); err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("two references for one variable were accepted: %v", err)
	}
	if err := os.Chmod(admin, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := parseCredentialFiles([]string{"ADMIN=" + admin}); err == nil || !strings.Contains(err.Error(), "world-readable") {
		t.Fatalf("a world-readable file was accepted: %v", err)
	}

	// The flag is repeatable on the command line.
	var repeated repeatableFlag
	for _, value := range []string{"a=b", "c=d"} {
		if err := repeated.Set(value); err != nil {
			t.Fatal(err)
		}
	}
	if len(repeated) != 2 || repeated.String() != "a=b,c=d" {
		t.Fatalf("repeated = %v", repeated)
	}
}
