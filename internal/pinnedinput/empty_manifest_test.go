package pinnedinput

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestEmptyManifestUsesArray(t *testing.T) {
	m, err := NewManifest(nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(m)
	if strings.Contains(string(raw), "null") || m.Digest != fmt.Sprintf("%x", sha256.Sum256([]byte("[]"))) {
		t.Fatalf("noncanonical empty manifest: %s", raw)
	}
}
