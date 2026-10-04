package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetiredEnableFlagsStrictlyDecodeAndRejectTrue(t *testing.T) {
	for _, field := range []string{"backlog:\n  enabled: ", "backlog_v2:\n  coordinator:\n    legacy_file_intake_enabled: "} {
		for _, enabled := range []string{"false", "true"} {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(field+enabled+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadFile(path)
			if enabled == "false" && err != nil {
				t.Fatalf("false rejected: %v", err)
			}
			if enabled == "true" && (err == nil || !strings.Contains(err.Error(), "retired") || !strings.Contains(err.Error(), "task run or campaign submit")) {
				t.Fatalf("true accepted or unclear refusal: %v", err)
			}
		}
	}
}
