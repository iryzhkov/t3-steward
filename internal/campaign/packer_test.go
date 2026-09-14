package campaign

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Two directories with the same content must produce the same bytes. The
// second tree is created in the opposite order and given different modes and
// modification times, because those are exactly the properties a naive packer
// would leak into the archive.
func TestPackIgnoresCreationOrderAndFilesystemMetadata(t *testing.T) {
	files := validTree()
	forward := sortedNames(files)
	backward := make([]string, len(forward))
	for index, name := range forward {
		backward[len(forward)-1-index] = name
	}

	firstRoot := t.TempDir()
	writeTree(t, firstRoot, files, forward)
	secondRoot := t.TempDir()
	writeTree(t, secondRoot, files, backward)
	stamp := time.Date(2019, 3, 4, 5, 6, 7, 0, time.UTC)
	for _, name := range backward {
		path := filepath.Join(secondRoot, filepath.FromSlash(name))
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}

	first, err := Prepare(firstRoot, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Prepare(secondRoot, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Archive, second.Archive) {
		t.Fatalf("archives differ: %d and %d bytes, digests %s and %s",
			len(first.Archive), len(second.Archive), first.ArchiveSHA256, second.ArchiveSHA256)
	}
	if first.ArchiveSHA256 != second.ArchiveSHA256 || first.ContentDigest != second.ContentDigest {
		t.Fatalf("digests differ: %#v and %#v", first, second)
	}
}

// Packing the same tree repeatedly must produce the same digest, which is what
// makes reusing an idempotency key with the same content a replay instead of a
// conflict.
func TestPackIsStableAcrossRepeatedRuns(t *testing.T) {
	root := campaignDir(t)
	first, err := Prepare(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := range 4 {
		loaded, err := Load(root, DefaultLimits)
		if err != nil {
			t.Fatal(err)
		}
		repeated, err := loaded.Pack()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first.Archive, repeated.Archive) ||
			first.ArchiveSHA256 != repeated.ArchiveSHA256 ||
			first.ContentDigest != repeated.ContentDigest {
			t.Fatalf("attempt %d produced a different archive", attempt)
		}
	}
}

func TestPackedArchiveIsCanonical(t *testing.T) {
	root := campaignDir(t)
	bundle, err := Prepare(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(bundle.Archive))
	var names []string
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, header.Name)
		if header.Typeflag != tar.TypeReg {
			t.Fatalf("entry %q has type %q", header.Name, string(header.Typeflag))
		}
		if header.Mode != archiveMode || header.Uid != 0 || header.Gid != 0 ||
			header.Uname != "" || header.Gname != "" {
			t.Fatalf("entry %q carries host metadata: %#v", header.Name, header)
		}
		if !header.ModTime.Equal(archiveModTime) {
			t.Fatalf("entry %q has modification time %s", header.Name, header.ModTime)
		}
	}
	want := []string{"inputs/plan.md", "prompts/implement.md", "prompts/review.md", "workflow.yaml"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("entries = %#v, want %#v", names, want)
	}
}

// Determinism has to hold between runs on different machines, not only
// between two calls in one process, so the fixture's digests are pinned. A
// packer that inherited the clock, the umask or the current user would keep
// every other test in this file passing and still break the idempotency
// guarantee; this is the test that notices. Update these values only together
// with a deliberate change to the fixture or to the archive layout, and say
// which in the commit message.
func TestPackedCampaignDigestsArePinned(t *testing.T) {
	const (
		wantArchive = "030cc3934d26128e87e6e566177edb92e6f1af82e1145c2398cd326afa7f8fdd"
		wantContent = "563f335bef918d0850a625176d2fc92d29f8545dcc9b0a7208c86b280cf170fb"
	)
	bundle, err := Prepare(campaignDir(t), DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ContentDigest != wantContent {
		t.Fatalf("content digest = %s, want %s: the inventory or its contents changed",
			bundle.ContentDigest, wantContent)
	}
	if bundle.ArchiveSHA256 != wantArchive {
		t.Fatalf("archive digest = %s, want %s: either the packer leaked host state into the archive or the tar layout changed",
			bundle.ArchiveSHA256, wantArchive)
	}
}

func TestPackDetectsFilesChangedAfterValidation(t *testing.T) {
	prompt := filepath.Join("prompts", "review.md")
	tests := []struct {
		name      string
		mutate    func(t *testing.T, root string)
		wantError string
	}{
		{
			name: "rewritten to the same length",
			mutate: func(t *testing.T, root string) {
				write(t, filepath.Join(root, prompt), "REVIEW THE PLAN\n")
			},
			wantError: "content changed after validation",
		},
		{
			name: "truncated",
			mutate: func(t *testing.T, root string) {
				write(t, filepath.Join(root, prompt), "short\n")
			},
			wantError: "file changed after validation",
		},
		{
			name: "extended",
			mutate: func(t *testing.T, root string) {
				write(t, filepath.Join(root, prompt), "review the plan\nand more\n")
			},
			wantError: "file grew after validation",
		},
		{
			name: "removed",
			mutate: func(t *testing.T, root string) {
				if err := os.Remove(filepath.Join(root, prompt)); err != nil {
					t.Fatal(err)
				}
			},
			wantError: "prompts/review.md",
		},
		{
			name: "replaced by a symbolic link",
			mutate: func(t *testing.T, root string) {
				path := filepath.Join(root, prompt)
				write(t, filepath.Join(root, "prompts", "other.md"), "review the plan\n")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				symlink(t, "other.md", path)
			},
			wantError: "symbolic link",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := campaignDir(t)
			loaded, err := Load(root, DefaultLimits)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, root)
			bundle, err := loaded.Pack()
			if err == nil {
				t.Fatalf("a mutated campaign was packed into %d bytes", len(bundle.Archive))
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want it to mention %q", err, test.wantError)
			}
			if bundle.Archive != nil {
				t.Fatalf("a refused pack returned %d bytes", len(bundle.Archive))
			}
		})
	}
}

// The archive never becomes a file on disk, so a failed pack has nothing to
// clean up and a successful one leaves nothing behind either.
func TestPackWritesNoTemporaryFiles(t *testing.T) {
	// Take the observation directory first: it fixes this test's temporary
	// base outside the directory the environment is about to point at.
	temporary := t.TempDir()
	root := campaignDir(t)
	before := treeListing(t, root)
	for _, variable := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(variable, temporary)
	}

	if _, err := Prepare(root, DefaultLimits); err != nil {
		t.Fatal(err)
	}
	assertEmpty(t, temporary, "a successful pack")

	loaded, err := Load(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "inputs", "plan.md")); err != nil {
		t.Fatal(err)
	}
	if _, err := loaded.Pack(); err == nil {
		t.Fatal("a campaign missing an input was packed")
	}
	assertEmpty(t, temporary, "a failed pack")

	write(t, filepath.Join(root, "inputs", "plan.md"), "the plan\n")
	if after := treeListing(t, root); !reflect.DeepEqual(before, after) {
		t.Fatalf("campaign directory changed: %#v became %#v", before, after)
	}
}

func TestPackRefusesAnArchiveBeyondTheByteLimit(t *testing.T) {
	root := campaignDir(t)
	loaded, err := Load(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	// The contents fit; the archive framing does not.
	loaded.Limits.MaxBytes = loaded.TotalBytes() + 16
	if _, err := loaded.Pack(); err == nil || !strings.Contains(err.Error(), "exceeds the limit") {
		t.Fatalf("oversized archive error = %v", err)
	}
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertEmpty(t *testing.T, directory, occasion string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("%s left %v in the temporary directory", occasion, names)
	}
}

func treeListing(t *testing.T, root string) []string {
	t.Helper()
	var listing []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		listing = append(listing, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return listing
}
