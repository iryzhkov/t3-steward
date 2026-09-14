package backlogadmin

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeAdminSecret(t *testing.T, home, reference string, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(home, ".config/upkeeper/secrets",
		strings.TrimPrefix(reference, "secretref:"))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(testAdminCredentials())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatal(err)
	}
}

// The store is where UpKeeper's reference actually resolves on these hosts, so
// an admin client must be usable with nothing in its environment.
func TestAdminCredentialsResolveFromTheSecretStore(t *testing.T) {
	home := t.TempDir()
	const reference = AdminCredentialPrefix + "omarchy-pc"
	writeAdminSecret(t, home, reference, 0o600)

	credentials, err := AdminResolver{Home: home}.ResolveAdmin(reference)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !credentials.Complete() {
		t.Fatal("resolved credentials are incomplete")
	}
	if credentials.ClientPrincipal != testAdminCredentials().ClientPrincipal {
		t.Errorf("client principal is %q", credentials.ClientPrincipal)
	}
}

func TestAdminCredentialStoreRefusals(t *testing.T) {
	t.Run("a readable-by-others secret is refused", func(t *testing.T) {
		home := t.TempDir()
		const reference = AdminCredentialPrefix + "omarchy-pc"
		writeAdminSecret(t, home, reference, 0o644)
		_, err := AdminResolver{Home: home}.ResolveAdmin(reference)
		if err == nil || !strings.Contains(err.Error(), "owner-only") {
			t.Fatalf("produced %v", err)
		}
	})

	t.Run("a worker credential is refused by name", func(t *testing.T) {
		_, err := AdminResolver{Home: t.TempDir()}.ResolveAdmin(
			WorkerCredentialPrefix + "omarchy-pc")
		if err == nil || !strings.Contains(err.Error(), "worker protocol credential") {
			t.Fatalf("produced %v", err)
		}
	})

	t.Run("a reference cannot escape the store", func(t *testing.T) {
		home := t.TempDir()
		root := filepath.Join(home, ".config/upkeeper/secrets")
		if err := os.MkdirAll(filepath.Join(root, "f03-admin"), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err := AdminResolver{Home: home}.ResolveAdmin(
			AdminCredentialPrefix + "../../elsewhere")
		if err == nil {
			t.Fatal("a traversing reference resolved")
		}
	})

	t.Run("a missing secret names what is required", func(t *testing.T) {
		home := t.TempDir()
		if err := os.MkdirAll(filepath.Join(home, ".config/upkeeper/secrets/f03-admin"),
			0o700); err != nil {
			t.Fatal(err)
		}
		_, err := AdminResolver{Home: home}.ResolveAdmin(AdminCredentialPrefix + "absent")
		if err == nil || !strings.Contains(err.Error(), "0600 file") {
			t.Fatalf("produced %v", err)
		}
	})
}

// The restricted-environment seam stays available for anything that is not an
// explicit store reference, so an existing deployment keeps working.
func TestAdminResolverKeepsTheEnvironmentSeam(t *testing.T) {
	_, err := AdminResolver{Home: t.TempDir()}.ResolveAdmin("f03-admin/omarchy-pc")
	if err == nil || !strings.Contains(err.Error(), "must start with") {
		t.Fatalf("produced %v", err)
	}
}
