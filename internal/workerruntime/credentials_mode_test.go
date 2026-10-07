package workerruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// R-28: a credential file is owner-only. Any group or other permission bit is
// refused, not just world read, and the refusal is the same through the
// project checker and the protocol resolver. The parent directory is left
// traversable so the mode itself is what protects the file.
func TestCredentialFileRefusesGroupOrOtherAccess(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "accessible")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(testProtocolCredentials())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		mode   os.FileMode
		refuse bool
	}{
		{0o600, false}, {0o400, false},
		{0o640, true}, {0o660, true}, {0o602, true}, {0o620, true}, {0o610, true}, {0o601, true},
	} {
		t.Run(fmt.Sprintf("%04o", test.mode), func(t *testing.T) {
			path := filepath.Join(parent, fmt.Sprintf("credential-%04o.json", test.mode))
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, test.mode); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
			lookup := environmentFrom(map[string]string{"T3_STEWARD_CREDENTIAL_F02_PROTOCOL_FILE": path})
			_, _, resolveErr := ResolveCredentialVariable(lookup, "T3_STEWARD_CREDENTIAL_F02_PROTOCOL")
			checkErr := (EnvironmentCredentialChecker{Lookup: lookup}).Require(context.Background(), []string{"f02-protocol"})
			_, protocolErr := (EnvironmentProtocolCredentialResolver{Lookup: lookup}).ResolveProtocol(context.Background(), "f02-protocol")
			for name, err := range map[string]error{"variable": resolveErr, "checker": checkErr, "protocol": protocolErr} {
				if !test.refuse {
					if err != nil {
						t.Fatalf("%s: owner-only mode %04o was refused: %v", name, test.mode, err)
					}
					continue
				}
				if err == nil {
					t.Fatalf("%s: mode %04o was accepted", name, test.mode)
				}
				message := err.Error()
				if !strings.Contains(message, "T3_STEWARD_CREDENTIAL_F02_PROTOCOL_FILE") || !strings.Contains(message, path) || !strings.Contains(message, "chmod 0600") {
					t.Fatalf("%s: error %q does not name the variable, the path and the remedy", name, message)
				}
				if strings.Contains(message, "coordinator") {
					t.Fatalf("%s: error leaks the content: %q", name, message)
				}
			}
		})
	}
}

// A credential file another user owns is refused even when its mode is
// owner-only, because that owner can replace the worker's signing secrets.
func TestCredentialFileOwnedByAnotherUserIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("owned-content\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := credentialFileOwner
	credentialFileOwner = func() int { return os.Getuid() + 1 }
	t.Cleanup(func() { credentialFileOwner = previous })
	lookup := environmentFrom(map[string]string{"T3_STEWARD_CREDENTIAL_GITHUB_TOKEN_FILE": path})
	_, _, err := ResolveCredentialVariable(lookup, "T3_STEWARD_CREDENTIAL_GITHUB_TOKEN")
	if err == nil || !strings.Contains(err.Error(), "owned by another user") || !strings.Contains(err.Error(), path) {
		t.Fatalf("error = %v, want a refusal of the foreign owner naming the path", err)
	}
	if strings.Contains(err.Error(), "owned-content") {
		t.Fatalf("error leaks the content: %v", err)
	}
}
