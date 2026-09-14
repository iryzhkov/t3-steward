package workerproto

import (
	"archive/tar"
	"bytes"
	"strings"
	"testing"
)

// tarArchive builds an archive from headers and bodies exactly as written, so a
// test can reproduce what an ordinary tar command emits.
func tarArchive(t *testing.T, write func(*tar.Writer)) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	write(writer)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// A bundle tarred the obvious way carries its directories as entries with a
// trailing slash. Rejecting those made `tar cf bundle.tar workflow.yaml prompts`
// fail, which is how anyone would build a bundle by hand.
func TestValidateTarArchiveAcceptsDirectoryEntries(t *testing.T) {
	body := []byte("objective\n")
	archive := tarArchive(t, func(writer *tar.Writer) {
		if err := writer.WriteHeader(&tar.Header{Name: "prompts/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteHeader(&tar.Header{Name: "prompts/plan.md", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(body); err != nil {
			t.Fatal(err)
		}
	})

	limits := ArchiveLimits{MaxEntries: 16, MaxBytes: 1 << 20}
	if err := ValidateTarArchive(bytes.NewReader(archive), int64(len(archive)), limits); err != nil {
		t.Fatalf("a bundle with directory entries was rejected: %v", err)
	}
}

// Relaxing the trailing slash must not relax anything else, and a refusal has to
// name the entry so an author can find it in a bundle of a hundred files.
func TestValidateTarArchiveStillRefusesEscapingPaths(t *testing.T) {
	for name, entry := range map[string]string{
		"parent traversal": "../escape.md",
		"absolute path":    "/etc/passwd",
		"traversal inside": "prompts/../../escape.md",
		"directory escape": "../outside/",
	} {
		t.Run(name, func(t *testing.T) {
			flag := byte(tar.TypeReg)
			if strings.HasSuffix(entry, "/") {
				flag = tar.TypeDir
			}
			archive := tarArchive(t, func(writer *tar.Writer) {
				if err := writer.WriteHeader(&tar.Header{Name: entry, Typeflag: flag, Mode: 0o644}); err != nil {
					t.Fatal(err)
				}
			})
			err := ValidateTarArchive(bytes.NewReader(archive), int64(len(archive)), ArchiveLimits{MaxEntries: 16, MaxBytes: 1 << 20})
			if err == nil {
				t.Fatalf("entry %q was accepted", entry)
			}
			if !strings.Contains(err.Error(), entry) {
				t.Fatalf("refusal did not name the entry: %v", err)
			}
		})
	}
}
