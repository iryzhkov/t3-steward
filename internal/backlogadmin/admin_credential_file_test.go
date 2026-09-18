package backlogadmin

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// F-10: the admin credential resolver accepts the _FILE form like the worker
// resolvers, with the inline variable winning and a world-readable file
// refused by name.
func TestEnvironmentAdminCredentialResolverReadsTheFileForm(t *testing.T) {
	credentials := testAdminCredentials()
	raw, err := json.Marshal(credentials)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "admin-credential.json")
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{"T3_STEWARD_CREDENTIAL_SECRETREF_F03_ADMIN_OMARCHY_PC_FILE": path}
	resolver := EnvironmentAdminCredentialResolver{Lookup: func(name string) (string, bool) {
		value, ok := environment[name]
		return value, ok
	}}
	resolved, err := resolver.ResolveAdmin("secretref:f03-admin/omarchy-pc")
	if err != nil {
		t.Fatalf("the file form was not accepted: %v", err)
	}
	if resolved.ClientPrincipal != credentials.ClientPrincipal || !resolved.Complete() {
		t.Fatalf("resolved = %+v", resolved)
	}

	environment["T3_STEWARD_CREDENTIAL_SECRETREF_F03_ADMIN_OMARCHY_PC"] = "not json"
	if _, err := resolver.ResolveAdmin("secretref:f03-admin/omarchy-pc"); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("the inline variable did not win over the file: %v", err)
	}
	delete(environment, "T3_STEWARD_CREDENTIAL_SECRETREF_F03_ADMIN_OMARCHY_PC")

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = resolver.ResolveAdmin("secretref:f03-admin/omarchy-pc")
	if err == nil || !strings.Contains(err.Error(), "world-readable") || !strings.Contains(err.Error(), path) || strings.Contains(err.Error(), credentials.ClientPrincipal) {
		t.Fatalf("world-readable file error = %v", err)
	}
}
