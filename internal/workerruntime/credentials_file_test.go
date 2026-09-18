package workerruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// environmentFrom builds a lookup over a fixed environment.
func environmentFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

// F-10: every resolver that reads T3_STEWARD_CREDENTIAL_<REF> also accepts
// T3_STEWARD_CREDENTIAL_<REF>_FILE=<path>, read at use with one trailing
// newline trimmed, so a generated unit can name the file and no wrapper script
// has to export the value.
func TestCredentialCheckerReadsTheFileForm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "github-token")
	if err := os.WriteFile(path, []byte("super-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	checker := EnvironmentCredentialChecker{Lookup: environmentFrom(map[string]string{
		"T3_STEWARD_CREDENTIAL_GITHUB_TOKEN_FILE": path,
	})}
	if err := checker.Require(context.Background(), []string{"github-token"}); err != nil {
		t.Fatalf("the file form was not accepted: %v", err)
	}
	value, found, err := ResolveCredentialVariable(checker.Lookup, "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN")
	if err != nil || !found || value != "super-secret" {
		t.Fatalf("value = %q, found = %t, err = %v; one trailing newline is trimmed and nothing else", value, found, err)
	}
	// Only one newline goes: a value that ends in a blank line keeps it.
	if err := os.WriteFile(path, []byte("two\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if value, _, _ := ResolveCredentialVariable(checker.Lookup, "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN"); value != "two\n" {
		t.Fatalf("value = %q, want %q", value, "two\n")
	}
}

// The inline variable wins when both forms are set, and it is never read from
// the file then, so a file that is unreadable does not matter.
func TestCredentialInlineVariableWinsOverTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent")
	value, found, err := ResolveCredentialVariable(environmentFrom(map[string]string{
		"T3_STEWARD_CREDENTIAL_GITHUB_TOKEN":      "inline-secret",
		"T3_STEWARD_CREDENTIAL_GITHUB_TOKEN_FILE": path,
	}), "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN")
	if err != nil || !found || value != "inline-secret" {
		t.Fatalf("value = %q, found = %t, err = %v", value, found, err)
	}
	// Neither form set: unavailable, not an error.
	if _, found, err := ResolveCredentialVariable(environmentFrom(nil), "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN"); found || err != nil {
		t.Fatalf("found = %t, err = %v", found, err)
	}
}

// A world-readable or missing file is refused with an error that names the
// variable and the path and never the content.
func TestCredentialFileIsRefusedWhenWorldReadableOrMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("leaked-content\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	lookup := environmentFrom(map[string]string{"T3_STEWARD_CREDENTIAL_GITHUB_TOKEN_FILE": path})
	for _, test := range []struct {
		name    string
		prepare func()
		want    string
	}{
		{"world-readable", func() {}, "world-readable"},
		{"missing", func() { os.Remove(path) }, "does not exist"},
	} {
		test.prepare()
		_, _, err := ResolveCredentialVariable(lookup, "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN")
		if err == nil {
			t.Fatalf("%s: the file was accepted", test.name)
		}
		message := err.Error()
		if !strings.Contains(message, "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN_FILE") || !strings.Contains(message, path) || !strings.Contains(message, test.want) {
			t.Fatalf("%s: error %q does not name the variable, the path and the cause", test.name, message)
		}
		if strings.Contains(message, "leaked-content") {
			t.Fatalf("%s: error leaks the content: %q", test.name, message)
		}
		// The checker and the protocol resolver surface the same refusal.
		if err := (EnvironmentCredentialChecker{Lookup: lookup}).Require(context.Background(), []string{"github-token"}); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s: checker error = %v", test.name, err)
		}
		if _, err := (EnvironmentProtocolCredentialResolver{Lookup: lookup}).ResolveProtocol(context.Background(), "github-token"); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s: protocol resolver error = %v", test.name, err)
		}
	}
}

// The protocol resolver reads its JSON bundle from the file form too, which
// is what retires normandy's coordinator wrapper.
func TestProtocolCredentialResolverReadsTheFileForm(t *testing.T) {
	raw, err := json.Marshal(testProtocolCredentials())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "protocol-credential.json")
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	resolver := EnvironmentProtocolCredentialResolver{Lookup: environmentFrom(map[string]string{
		"T3_STEWARD_CREDENTIAL_F02_PROTOCOL_FILE": path,
	})}
	credentials, err := resolver.ResolveProtocol(context.Background(), "f02-protocol")
	if err != nil {
		t.Fatal(err)
	}
	if credentials.CoordinatorPrincipal != "ssh:coordinator" {
		t.Fatalf("credentials = %+v", credentials)
	}
}
