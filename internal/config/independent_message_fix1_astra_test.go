package config

import (
	"bytes"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndependentMessageFix1LoadedMigration(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, suffix string
		migrate              bool
	}{
		{"exact", "", "", true}, {"yaml-newline", "", "\n", true},
		{"leading-space", " ", "", false}, {"double-newline", "", "\n\n", false},
		{"trailing-space", "", " ", false}, {"crlf", "", "\r\n", false}, {"custom", "", " custom", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drain := tc.prefix + previousDefaultDrainMessage + tc.suffix
			resume := tc.prefix + previousDefaultResumePrompt + tc.suffix
			warn := " custom warning {{.UsedPercent}} \n"
			b, err := yaml.Marshal(map[string]any{"messages": map[string]string{"warn": warn, "drain": drain}, "resume": map[string]string{"prompt": resume}})
			if err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, b, 0600); err != nil {
				t.Fatal(err)
			}
			c, err := loadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if tc.migrate {
				drain = DefaultDrainMessage
				resume = DefaultResumePrompt
			}
			if c.Messages.Drain != drain || c.Resume.Prompt != resume || c.Messages.Warn != warn {
				t.Fatal("loaded message bytes changed outside exact migration")
			}
			after, err := os.ReadFile(p)
			if err != nil || !bytes.Equal(after, b) {
				t.Fatal("configuration rewritten")
			}
			t.Logf("exact migration=%v custom/warn/file bytes preserved", tc.migrate)
		})
	}
	p := filepath.Join(t.TempDir(), "sample.yaml")
	if err := os.WriteFile(p, []byte(Sample()), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := loadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSuffix(c.Messages.Drain, "\n") != DefaultDrainMessage || c.Resume.Prompt != DefaultResumePrompt {
		t.Fatal("sample/default mismatch")
	}
}
