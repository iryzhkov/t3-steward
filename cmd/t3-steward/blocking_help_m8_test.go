package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBlockingHelpKeepsM8ShortContract(t *testing.T) {
	for _, path := range []string{"task run", "task result", "campaign show", "campaign cancel", "backlog cancel", "backlog command show"} {
		t.Run(path, func(t *testing.T) {
			var short, full bytes.Buffer
			args := append(strings.Fields(path), "--help")
			if _, err := admitHelp(&short, nil, args); err != nil {
				t.Fatal(err)
			}
			if _, err := admitHelp(&full, nil, append(args, "full")); err != nil {
				t.Fatal(err)
			}
			if lines := strings.Count(short.String(), "\n"); lines > 26 {
				t.Fatalf("short help has %d lines", lines)
			}
			if path == "task run" {
				if !strings.Contains(short.String(), "Start one task") {
					t.Error("short purpose lost task submission")
				}
				for _, want := range []string{"--dry-run", "UTF-16", "promptCharacters", "120000"} {
					if !strings.Contains(full.String(), want) {
						t.Errorf("full help lacks %q", want)
					}
				}
				if strings.Contains(short.String(), "promptCharacters") {
					t.Error("short help includes dry-run details")
				}
			} else {
				for _, want := range []string{"--wait", "--timeout", "130"} {
					if !strings.Contains(full.String(), want) {
						t.Errorf("full help lacks %q", want)
					}
				}
			}
			flag := "--wait"
			if path == "task run" {
				flag = "--dry-run"
			}
			if !strings.Contains(short.String(), flag) {
				t.Errorf("short help lacks %s", flag)
			}
			if !strings.Contains(short.String(), "--help full") {
				t.Error("short help lacks full reference")
			}
		})
	}
}
