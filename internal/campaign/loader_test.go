package campaign

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// validManifest is the smallest campaign that still exercises prompts, inputs,
// a dependency and an artifact binding.
const validManifest = `version: 2
name: example-campaign
environment:
  project: t3-steward
inputs:
  - inputs/plan.md
tasks:
  review:
    prompt_file: prompts/review.md
    outputs: [review.md]
  implement:
    prompt_file: prompts/implement.md
    needs: [review]
    inputs_from:
      review: [review.md]
`

func validTree() map[string]string {
	return map[string]string{
		"workflow.yaml":        validManifest,
		"inputs/plan.md":       "the plan\n",
		"prompts/review.md":    "review the plan\n",
		"prompts/implement.md": "implement the plan\n",
	}
}

// writeTree materializes a campaign directory. The order the files are created
// in is a parameter because the archive must not depend on it.
func writeTree(t *testing.T, root string, files map[string]string, order []string) {
	t.Helper()
	if order == nil {
		order = sortedNames(files)
	}
	if len(order) != len(files) {
		t.Fatalf("creation order lists %d names for %d files", len(order), len(files))
	}
	for _, name := range order {
		content, ok := files[name]
		if !ok {
			t.Fatalf("creation order names %q, which the tree does not contain", name)
		}
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func sortedNames(files map[string]string) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// campaignDir writes a valid campaign and returns its root.
func campaignDir(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeTree(t, root, validTree(), nil)
	return root
}

func TestLoadAcceptsDirectoryOrManifestPath(t *testing.T) {
	root := campaignDir(t)
	fromDirectory, err := Load(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	fromManifest, err := Load(filepath.Join(root, ManifestFileName), DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	if fromDirectory.Root != fromManifest.Root {
		t.Fatalf("roots differ: %q and %q", fromDirectory.Root, fromManifest.Root)
	}
	if !reflect.DeepEqual(fromDirectory.Files, fromManifest.Files) {
		t.Fatalf("inventories differ:\n%#v\n%#v", fromDirectory.Files, fromManifest.Files)
	}
	if fromDirectory.Manifest.Name != "example-campaign" || len(fromDirectory.Manifest.Tasks) != 2 {
		t.Fatalf("manifest = %#v", fromDirectory.Manifest)
	}
	// Defaulting is the ingestion parser's, not a second copy of it.
	if fromDirectory.Manifest.Tasks["review"].MaxTurns != 3 || fromDirectory.Manifest.Environment.Type != "git" {
		t.Fatalf("defaults were not applied: %#v", fromDirectory.Manifest)
	}
}

func TestLoadBuildsCanonicalInventory(t *testing.T) {
	root := campaignDir(t)
	loaded, err := Load(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	want := []File{
		{Path: "inputs/plan.md", Role: RoleInput, Size: 9},
		{Path: "prompts/implement.md", Role: RolePrompt, Size: 19},
		{Path: "prompts/review.md", Role: RolePrompt, Size: 16},
		{Path: "workflow.yaml", Role: RoleManifest, Size: int64(len(validManifest))},
	}
	if len(loaded.Files) != len(want) {
		t.Fatalf("inventory = %#v", loaded.Files)
	}
	for index, file := range loaded.Files {
		if file.Path != want[index].Path || file.Role != want[index].Role || file.Size != want[index].Size {
			t.Fatalf("entry %d = %#v, want %#v", index, file, want[index])
		}
		if len(file.SHA256) != 64 {
			t.Fatalf("entry %d has checksum %q", index, file.SHA256)
		}
	}
	if loaded.TotalBytes() != int64(len(validManifest))+9+19+16 {
		t.Fatalf("total bytes = %d", loaded.TotalBytes())
	}
	if paths := loaded.InputPaths(); !reflect.DeepEqual(paths, []string{"inputs/plan.md"}) {
		t.Fatalf("input paths = %#v", paths)
	}
}

// A file named twice, once literally and once by a pattern, is one archive
// entry. The archive can hold a path once, so the inventory has to resolve the
// overlap rather than leaving it for the tar writer.
func TestLoadSpellsEveryPathOnce(t *testing.T) {
	root := t.TempDir()
	files := validTree()
	files["workflow.yaml"] = strings.Replace(
		validManifest,
		"inputs:\n  - inputs/plan.md\n",
		"inputs:\n  - inputs/plan.md\n  - inputs/*.md\n  - ./inputs/plan.md\n",
		1,
	)
	writeTree(t, root, files, nil)
	loaded, err := Load(root, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int, len(loaded.Files))
	for _, file := range loaded.Files {
		seen[file.Path]++
	}
	if seen["inputs/plan.md"] != 1 || len(loaded.Files) != 4 {
		t.Fatalf("inventory = %#v", loaded.Files)
	}
}

func TestLoadRefusesInvalidCampaigns(t *testing.T) {
	for _, test := range negativeCases() {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			reference := test.build(t, root)
			_, err := Load(reference, DefaultLimits)
			if err == nil {
				t.Fatalf("campaign was accepted")
			}
			if !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want it to mention %q", err, test.wantError)
			}
		})
	}
}

func TestLoadRefusesEmptyReferenceAndUnusableLimits(t *testing.T) {
	root := campaignDir(t)
	if _, err := Load("", DefaultLimits); err == nil || !strings.Contains(err.Error(), "is required") {
		t.Fatalf("empty reference error = %v", err)
	}
	if _, err := Load(root, Limits{}); err == nil || !strings.Contains(err.Error(), "positive file and byte limits") {
		t.Fatalf("zero limits error = %v", err)
	}
	if _, err := Load(root, Limits{MaxFiles: 2, MaxBytes: 1 << 20}); err == nil ||
		!strings.Contains(err.Error(), "exceed the limit of 2") {
		t.Fatalf("file limit error = %v", err)
	}
	if _, err := Load(root, Limits{MaxFiles: 10, MaxBytes: 64}); err == nil ||
		!strings.Contains(err.Error(), "exceed") {
		t.Fatalf("byte limit error = %v", err)
	}
}

// negativeCase is one campaign directory that must be refused before anything
// is submitted. ingestionRefuses records whether the coordinator's own
// submission path refuses the same tree; where it is false the campaign loader
// is deliberately stricter, and the comment on the case says why.
type negativeCase struct {
	name             string
	build            func(t *testing.T, root string) string
	wantError        string
	ingestionRefuses bool
}

func negativeCases() []negativeCase {
	manifestWith := func(old, new string) func(*testing.T, string) string {
		return func(t *testing.T, root string) string {
			t.Helper()
			files := validTree()
			files["workflow.yaml"] = strings.Replace(validManifest, old, new, 1)
			if files["workflow.yaml"] == validManifest {
				t.Fatalf("manifest edit %q changed nothing", old)
			}
			writeTree(t, root, files, nil)
			return root
		}
	}
	return []negativeCase{
		{
			name:             "no manifest",
			build:            func(t *testing.T, root string) string { return root },
			wantError:        ManifestFileName,
			ingestionRefuses: true,
		},
		{
			name: "manifest under another name",
			build: func(t *testing.T, root string) string {
				writeTree(t, root, validTree(), nil)
				return filepath.Join(root, "prompts", "review.md")
			},
			wantError: "must be named " + ManifestFileName,
		},
		{
			name:             "unsupported version",
			build:            manifestWith("version: 2", "version: 1"),
			wantError:        "version must be 2",
			ingestionRefuses: true,
		},
		{
			name:             "unknown field",
			build:            manifestWith("name: example-campaign", "name: example-campaign\nunknown_field: 1"),
			wantError:        "decode workflow manifest",
			ingestionRefuses: true,
		},
		{
			name:             "prompt escapes the campaign",
			build:            manifestWith("prompts/review.md", "../outside.md"),
			wantError:        "escapes the workflow bundle",
			ingestionRefuses: true,
		},
		{
			name:             "absolute prompt path",
			build:            manifestWith("prompts/review.md", "/etc/hosts"),
			wantError:        "absolute paths are not allowed",
			ingestionRefuses: true,
		},
		{
			name:             "duplicate input",
			build:            manifestWith("  - inputs/plan.md\n", "  - inputs/plan.md\n  - inputs/plan.md\n"),
			wantError:        "is duplicated",
			ingestionRefuses: true,
		},
		{
			name:             "missing prompt file",
			build:            manifestWith("prompts/review.md", "prompts/absent.md"),
			wantError:        "prompts/absent.md",
			ingestionRefuses: true,
		},
		{
			name:             "input pattern matches nothing",
			build:            manifestWith("  - inputs/plan.md\n", "  - inputs/absent-*.md\n"),
			wantError:        "matches no files",
			ingestionRefuses: true,
		},
		{
			name: "input pattern matches a directory",
			build: func(t *testing.T, root string) string {
				files := validTree()
				files["workflow.yaml"] = strings.Replace(validManifest, "  - inputs/plan.md\n", "  - inputs/*\n", 1)
				writeTree(t, root, files, nil)
				if err := os.MkdirAll(filepath.Join(root, "inputs", "nested"), 0o755); err != nil {
					t.Fatal(err)
				}
				return root
			},
			wantError: "not a regular file",
			// The coordinator refuses the same tree, but a plain tar of it
			// cannot carry an empty directory the glob would match, so the
			// archive comparison is not meaningful here.
		},
		{
			name: "prompt is a symbolic link inside the campaign",
			build: func(t *testing.T, root string) string {
				files := validTree()
				files["prompts/shared.md"] = "review the plan\n"
				delete(files, "prompts/review.md")
				writeTree(t, root, files, nil)
				symlink(t, "shared.md", filepath.Join(root, "prompts", "review.md"))
				return root
			},
			wantError: "symbolic link",
			// Ingestion accepts a link that stays inside the bundle. A
			// campaign refuses it anyway: the archive carries regular files
			// only, so a link is a second name for a path the archive can
			// spell once, and which name wins would decide the digest.
		},
		{
			name: "input is a symbolic link out of the campaign",
			build: func(t *testing.T, root string) string {
				outside := filepath.Join(t.TempDir(), "outside.md")
				if err := os.WriteFile(outside, []byte("outside\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				files := validTree()
				delete(files, "inputs/plan.md")
				writeTree(t, root, files, nil)
				if err := os.MkdirAll(filepath.Join(root, "inputs"), 0o755); err != nil {
					t.Fatal(err)
				}
				symlink(t, outside, filepath.Join(root, "inputs", "plan.md"))
				return root
			},
			wantError: "escapes the workflow bundle",
			// A tar of this tree would carry the link's target as ordinary
			// content, which is a different bundle, so there is nothing to
			// compare against the coordinator here.
		},
		{
			name: "manifest is a symbolic link",
			build: func(t *testing.T, root string) string {
				files := validTree()
				files["campaign.yaml"] = validManifest
				delete(files, "workflow.yaml")
				writeTree(t, root, files, nil)
				symlink(t, "campaign.yaml", filepath.Join(root, ManifestFileName))
				return root
			},
			wantError: "symbolic link",
		},
		{
			name: "reference is a symbolic link to a file",
			build: func(t *testing.T, root string) string {
				writeTree(t, root, validTree(), nil)
				link := filepath.Join(t.TempDir(), "campaign")
				symlink(t, filepath.Join(root, ManifestFileName), link)
				return link
			},
			wantError: "symbolic link to a file",
		},
	}
}

func symlink(t *testing.T, target, name string) {
	t.Helper()
	if err := os.Symlink(target, name); err != nil {
		t.Skipf("this filesystem cannot create symbolic links: %v", err)
	}
}
