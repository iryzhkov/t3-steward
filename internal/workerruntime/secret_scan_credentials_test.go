package workerruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSecretScanCredentialValues(t *testing.T) {
	ctx := context.Background()
	checker := EnvironmentCredentialChecker{Lookup: func(name string) (string, bool) { return "synthetic-resolved-token", true }}
	values, err := checker.SecretValues(ctx, []string{"fixture"})
	if err != nil || len(values) != 1 || values[0] != "synthetic-resolved-token" {
		t.Fatal("credential extraction failed", err)
	}
	home := t.TempDir()
	path := filepath.Join(home, "auth.json")
	if err := os.WriteFile(path, []byte(`{"tokens":{"access_token":"synthetic-access","refresh_token":"synthetic-refresh"},"OPENAI_API_KEY":"synthetic-key","email":"person@example.com"}`), 0600); err != nil {
		t.Fatal(err)
	}
	values, err = modelLoginCanaries([]string{path})
	if err != nil || len(values) != 3 {
		t.Fatal("model login extraction failed", err)
	}
	if _, err := modelLoginCanaries([]string{filepath.Join(home, "absent")}); err != nil {
		t.Fatal(err)
	}
}
