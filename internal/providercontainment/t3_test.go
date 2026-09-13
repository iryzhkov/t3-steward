package providercontainment

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/t3api"
)

func TestScopedTokenRotationAndEscapeRefusal(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	source := t3api.SocketTokenFile{Control: root}
	if err := publishToken(root, []byte("first")); err != nil {
		t.Fatal(err)
	}
	first, err := source.Token(context.Background())
	if err != nil || first != "first" {
		t.Fatalf("first: %q %v", first, err)
	}
	if err := publishToken(root, []byte("bad token")); err == nil {
		t.Fatal("malformed token replaced valid token")
	}
	first, err = source.Token(context.Background())
	if err != nil || first != "first" {
		t.Fatal("failed issuance lost usable credential")
	}
	if err := publishToken(root, []byte("second")); err != nil {
		t.Fatal(err)
	}
	second, err := source.Token(context.Background())
	if err != nil || second != "second" {
		t.Fatal("rotated token not observed")
	}
	outside := filepath.Join(t.TempDir(), "host-secret")
	if err := os.WriteFile(outside, []byte("host-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "token")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "token")); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Token(context.Background()); err == nil {
		t.Fatal("token symlink escaped control root")
	}
	if err := publishToken(root, []byte("third")); err != nil {
		t.Fatal(err)
	}
	retained, err := os.ReadFile(outside)
	if err != nil || string(retained) != "host-secret" {
		t.Fatal("token publication followed symlink")
	}
}
