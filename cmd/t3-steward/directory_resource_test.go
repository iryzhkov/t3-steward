package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

func TestDirectoryEvidenceRejectsAmbiguousDocuments(t *testing.T) {
	for name, raw := range map[string]string{
		"unknown":   `{"workerId":"w","typo":true}`,
		"trailing":  `{} {}`,
		"oversize":  "{}" + strings.Repeat(" ", 1<<20) + "{}",
		"truncated": `{"workerId":`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "registration.json")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			var r directoryresource.Registration
			if err := readDirectoryJSON(path, &r); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
}
