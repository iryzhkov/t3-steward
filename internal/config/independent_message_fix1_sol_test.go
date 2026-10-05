package config

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"testing"
)

func TestIndependentMessageFix1MigrationBoundary(t *testing.T) {
	for _, suffix := range []string{"", "\n", "\n\n", " ", "\r\n", "\n custom"} {
		raw, err := yaml.Marshal(map[string]any{"messages": map[string]string{"drain": previousDefaultDrainMessage + suffix}, "resume": map[string]string{"prompt": previousDefaultResumePrompt + suffix}})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		c, err := loadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		wantDrain, wantResume := previousDefaultDrainMessage+suffix, previousDefaultResumePrompt+suffix
		migrated := suffix == "" || suffix == "\n"
		if migrated {
			wantDrain, wantResume = DefaultDrainMessage, DefaultResumePrompt
		}
		if c.Messages.Drain != wantDrain || c.Resume.Prompt != wantResume {
			t.Fatalf("suffix=%q migration/custom mismatch", suffix)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, after) {
			t.Fatal("load rewrote operator bytes", err)
		}
		fmt.Printf("suffix=%q migrated=%v file-identical=true\n", suffix, migrated)
	}
}
