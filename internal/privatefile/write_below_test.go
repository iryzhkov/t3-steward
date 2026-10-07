//go:build unix

package privatefile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// WriteBelow creates missing directories owner-only, writes the file with
// exactly the requested mode whatever the umask, and leaves no staged file.
func TestWriteBelowCreatesDirectoriesAndAssertsTheMode(t *testing.T) {
	root := t.TempDir()
	for _, perm := range []os.FileMode{0o600, 0o444, 0o400} {
		if err := WriteBelow(root, "one/two/file", []byte("content\n"), perm); err != nil {
			t.Fatalf("perm %#o: %v", perm, err)
		}
		info, err := os.Lstat(filepath.Join(root, "one", "two", "file"))
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != perm {
			t.Fatalf("mode = %v, want regular %#o", info.Mode(), perm)
		}
	}
	for _, directory := range []string{"one", "one/two"} {
		info, err := os.Lstat(filepath.Join(root, filepath.FromSlash(directory)))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("%s mode = %#o, want owner-only", directory, info.Mode().Perm())
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "one", "two"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("directory holds %d entries, want only the file", len(entries))
	}
}

// Every step below the root is refused when it is a link or the wrong kind,
// and the refusal names the step. Nothing outside the root changes.
func TestWriteBelowRefusesLinksAndWrongKinds(t *testing.T) {
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(sentinel, []byte("host"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		prepare func(root string) error
		want    string
	}{
		{"linked directory", func(root string) error { return os.Symlink(outside, filepath.Join(root, "one")) }, "one is a symbolic link"},
		{"file as directory", func(root string) error { return os.WriteFile(filepath.Join(root, "one"), nil, 0o600) }, "one is not a directory"},
		{"linked file", func(root string) error {
			if err := os.MkdirAll(filepath.Join(root, "one", "two"), 0o700); err != nil {
				return err
			}
			return os.Symlink(sentinel, filepath.Join(root, "one", "two", "file"))
		}, "one/two/file is a symbolic link"},
		{"dangling link", func(root string) error {
			if err := os.MkdirAll(filepath.Join(root, "one", "two"), 0o700); err != nil {
				return err
			}
			return os.Symlink(filepath.Join(outside, "absent"), filepath.Join(root, "one", "two", "file"))
		}, "one/two/file is a symbolic link"},
		{"directory as file", func(root string) error { return os.MkdirAll(filepath.Join(root, "one", "two", "file"), 0o700) }, "one/two/file is not a regular file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := test.prepare(root); err != nil {
				t.Fatal(err)
			}
			err := WriteBelow(root, "one/two/file", []byte("content"), 0o600)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "host" {
				t.Fatalf("sentinel = %q, %v", raw, err)
			}
			entries, err := os.ReadDir(outside)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("outside directory holds %d entries, want only the sentinel", len(entries))
			}
		})
	}
}

func TestWriteBelowRefusesAnUncleanPath(t *testing.T) {
	root := t.TempDir()
	for _, relative := range []string{"", "/abs", "a//b", "../escape", "a/./b", "a/"} {
		if err := WriteBelow(root, relative, nil, 0o600); err == nil {
			t.Fatalf("%q was accepted", relative)
		}
	}
}
