package symlinkpath

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/testutil"
)

func TestFirstNamesTheOutermostLinkAndItsTarget(t *testing.T) {
	root := testutil.RealTempDir(t)
	real := filepath.Join(root, "real")
	if err := os.MkdirAll(filepath.Join(real, "inner"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	got, target, ok := First(filepath.Join(link, "inner", "absent"))
	if !ok || got != link || target != real {
		t.Fatalf("First = %q, %q, %v; want %q, %q", got, target, ok, link, real)
	}
	if want := link + " is a symlink to " + real; Describe(filepath.Join(link, "inner")) != want {
		t.Fatalf("Describe = %q, want %q", Describe(filepath.Join(link, "inner")), want)
	}
}

func TestFirstReportsNothingForARealPath(t *testing.T) {
	root := testutil.RealTempDir(t)
	if link, target, ok := First(filepath.Join(root, "absent", "deeper")); ok {
		t.Fatalf("First = %q, %q on a real path", link, target)
	}
	if got := Describe(root); got != "" {
		t.Fatalf("Describe = %q on a real path", got)
	}
}
