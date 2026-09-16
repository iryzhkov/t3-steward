// Package campaign turns an authoring directory into the exact bytes a version
// 2 workflow submission sends.
//
// It is a facade rather than a second system. The manifest is parsed,
// defaulted and validated by internal/backlog, every referenced file is
// checked by the same path policy ingestion uses, and the finished archive is
// checked by the same internal/workerproto validator the coordinator applies
// on receipt. Nothing here restates those rules: a second copy would drift,
// and a bundle that validates here and then fails to ingest there is worse
// than no validation at all.
//
// Loading and packing are deliberately separate steps. Loading records what a
// campaign contains, including the size and checksum of every file; packing
// re-reads those files and refuses to produce an archive whose contents no
// longer match what was validated.
package campaign

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
)

// ManifestFileName is the only campaign authoring file. A campaign is a
// version 2 workflow bundle, so the manifest keeps the name ingestion already
// expects rather than gaining an alias of its own.
const ManifestFileName = "workflow.yaml"

// Limits bounds one campaign the way the coordinator bounds one submission.
// MaxFiles is the archive entry limit and MaxBytes is the archive byte limit;
// both come from the coordinator's configured message limits so that a
// campaign accepted here cannot be refused for size on arrival.
type Limits struct {
	MaxFiles int
	MaxBytes int64
}

// DefaultLimits repeats the defaults internal/config applies to
// backlog_v2.message_limits. A caller that has the coordinator configuration
// passes those values instead; this exists so that an authoring command can
// check a directory on a machine that is not a coordinator without inventing
// a second policy.
var DefaultLimits = Limits{MaxFiles: 1000, MaxBytes: 4 << 20}

func (l Limits) validate() error {
	if l.MaxFiles < 1 || l.MaxBytes < 1 {
		return errors.New("campaign: positive file and byte limits are required")
	}
	return nil
}

// Role records why a file belongs to the campaign. It is descriptive only:
// every file travels in the same archive and ingestion decides what each one
// becomes.
type Role string

const (
	RoleManifest Role = "manifest"
	RoleInput    Role = "input"
	RolePrompt   Role = "prompt"
)

// File is one entry of the canonical inventory.
type File struct {
	// Path is relative to the campaign root and always spelled with forward
	// slashes, which is the one spelling the archive uses.
	Path   string
	Role   Role
	Size   int64
	SHA256 string
}

// Campaign is a loaded and validated authoring directory.
type Campaign struct {
	// Root is the resolved campaign directory.
	Root string
	// ManifestPath is the resolved workflow.yaml inside Root.
	ManifestPath string
	// Manifest is the normalized manifest: strictly parsed, defaulted and
	// validated by the ingestion parser.
	Manifest backlog.Manifest
	// ManifestBytes are the exact bytes that were parsed and that the archive
	// carries, so that what was validated is what is submitted.
	ManifestBytes []byte
	// Files is the canonical inventory, ordered by path.
	Files  []File
	Limits Limits
}

// TotalBytes is the size of the campaign's content, excluding archive framing.
func (c *Campaign) TotalBytes() int64 {
	var total int64
	for _, file := range c.Files {
		total += file.Size
	}
	return total
}

// InputPaths lists the campaign's workflow-level input files.
func (c *Campaign) InputPaths() []string {
	paths := make([]string, 0, len(c.Files))
	for _, file := range c.Files {
		if file.Role == RoleInput {
			paths = append(paths, file.Path)
		}
	}
	return paths
}

// Load accepts either a campaign directory or the path of its workflow.yaml,
// resolves the campaign root from either, and validates the directory exactly
// as ingestion would: the manifest through the ingestion parser, every
// referenced file through the ingestion path rules. It additionally refuses a
// symbolic link anywhere under the root, because the archive carries regular
// files only and a link is a second spelling of a path the archive can hold
// just once.
func Load(reference string, limits Limits) (*Campaign, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	root, err := campaignRoot(reference)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(root, ManifestFileName)
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("campaign: read %s: %w", ManifestFileName, err)
	}
	if int64(len(raw)) > limits.MaxBytes {
		return nil, fmt.Errorf("campaign: %s exceeds %d bytes", ManifestFileName, limits.MaxBytes)
	}
	// LoadManifest is the ingestion parser and the ingestion path policy. It
	// reads the manifest itself, so the bytes are read again afterwards and
	// compared: a manifest rewritten during validation would otherwise be
	// packed as something other than what was validated.
	manifest, err := backlog.LoadManifest(root)
	if err != nil {
		return nil, fmt.Errorf("campaign: %w", err)
	}
	verify, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("campaign: read %s: %w", ManifestFileName, err)
	}
	if !bytes.Equal(raw, verify) {
		return nil, fmt.Errorf("campaign: %s changed while the campaign was validated", ManifestFileName)
	}
	files, err := inventory(root, manifest, raw, limits)
	if err != nil {
		return nil, err
	}
	return &Campaign{
		Root:          root,
		ManifestPath:  manifestPath,
		Manifest:      manifest,
		ManifestBytes: raw,
		Files:         files,
		Limits:        limits,
	}, nil
}

// campaignRoot accepts a directory or the manifest inside it. A symbolic link
// to a directory is resolved, because an author may keep a stable name for the
// campaign they are working on; a symbolic link standing in for the manifest
// file is refused, because it would silently relocate the campaign root and
// with it every relative path the manifest declares.
func campaignRoot(reference string) (string, error) {
	if strings.TrimSpace(reference) == "" {
		return "", fmt.Errorf("campaign: a campaign directory or %s path is required", ManifestFileName)
	}
	absolute, err := filepath.Abs(reference)
	if err != nil {
		return "", fmt.Errorf("campaign: resolve %q: %w", reference, err)
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return "", fmt.Errorf("campaign: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		resolved, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return "", fmt.Errorf("campaign: resolve %q: %w", reference, err)
		}
		target, err := os.Stat(resolved)
		if err != nil {
			return "", fmt.Errorf("campaign: %w", err)
		}
		if !target.IsDir() {
			return "", fmt.Errorf("campaign: %q is a symbolic link to a file", reference)
		}
		return resolved, nil
	}
	switch {
	case info.IsDir():
		// Resolve the directory the way ingestion does, so that a root behind
		// a symbolic link (a temporary directory on macOS, for one) does not
		// make every file below it look like an escape.
		resolved, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return "", fmt.Errorf("campaign: resolve %q: %w", reference, err)
		}
		return resolved, nil
	case info.Mode().IsRegular():
		if filepath.Base(absolute) != ManifestFileName {
			return "", fmt.Errorf("campaign: a campaign manifest must be named %s", ManifestFileName)
		}
		resolved, err := filepath.EvalSymlinks(filepath.Dir(absolute))
		if err != nil {
			return "", fmt.Errorf("campaign: resolve %q: %w", reference, err)
		}
		return resolved, nil
	default:
		return "", fmt.Errorf("campaign: %q is neither a directory nor a regular %s", reference, ManifestFileName)
	}
}

// inventory lists the manifest and every file it references, exactly once and
// in one spelling, in the order the archive uses. The set is built the way
// ingestion builds it, so the archive carries neither more nor less than the
// coordinator will store.
func inventory(root string, manifest backlog.Manifest, manifestBytes []byte, limits Limits) ([]File, error) {
	roles := map[string]Role{ManifestFileName: RoleManifest}
	for _, task := range manifest.Tasks {
		relative := filepath.ToSlash(filepath.Clean(task.PromptFile))
		if _, exists := roles[relative]; !exists {
			roles[relative] = RolePrompt
		}
	}
	// The overseer prompt is a prompt like a task's, and a gate rubric is review
	// input. Both are referenced by the manifest, so both have to travel in the
	// archive; a referenced file that is not packed is a campaign that validates
	// here and fails on arrival.
	if supervision, ok := manifest.SupervisionConfig(); ok {
		relative := filepath.ToSlash(filepath.Clean(supervision.PromptArtifactID))
		if _, exists := roles[relative]; !exists {
			roles[relative] = RolePrompt
		}
	}
	for _, gate := range manifest.GateDefinitions() {
		if gate.RubricArtifactID == "" {
			continue
		}
		relative := filepath.ToSlash(filepath.Clean(gate.RubricArtifactID))
		if _, exists := roles[relative]; !exists {
			roles[relative] = RoleInput
		}
	}
	for _, pattern := range manifest.Inputs {
		matches, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(pattern)))
		if err != nil {
			return nil, fmt.Errorf("campaign: input pattern %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("campaign: input pattern %q matches no files", pattern)
		}
		for _, match := range matches {
			relative, err := filepath.Rel(root, match)
			if err != nil {
				return nil, fmt.Errorf("campaign: input pattern %q: %w", pattern, err)
			}
			relative = filepath.ToSlash(filepath.Clean(relative))
			if roles[relative] != RoleManifest {
				roles[relative] = RoleInput
			}
		}
	}

	paths := make([]string, 0, len(roles))
	for relative := range roles {
		paths = append(paths, relative)
	}
	sort.Strings(paths)
	if len(paths) > limits.MaxFiles {
		return nil, fmt.Errorf("campaign: %d files exceed the limit of %d", len(paths), limits.MaxFiles)
	}

	files := make([]File, 0, len(paths))
	remaining := limits.MaxBytes
	for _, relative := range paths {
		var size int64
		var sum string
		if relative == ManifestFileName {
			if err := refuseSymbolicLinks(root, relative); err != nil {
				return nil, fmt.Errorf("campaign: file %q: %w", relative, err)
			}
			digest := sha256.Sum256(manifestBytes)
			size, sum = int64(len(manifestBytes)), hex.EncodeToString(digest[:])
		} else {
			var err error
			size, sum, err = measure(root, relative, remaining)
			if err != nil {
				return nil, fmt.Errorf("campaign: file %q: %w", relative, err)
			}
		}
		if size > remaining {
			return nil, fmt.Errorf("campaign: contents exceed %d bytes", limits.MaxBytes)
		}
		remaining -= size
		files = append(files, File{Path: relative, Role: roles[relative], Size: size, SHA256: sum})
	}
	return files, nil
}

// measure records the size and checksum packing will later have to reproduce.
func measure(root, relative string, budget int64) (int64, string, error) {
	if err := refuseSymbolicLinks(root, relative); err != nil {
		return 0, "", err
	}
	handle, err := os.Open(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return 0, "", err
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		return 0, "", err
	}
	if !info.Mode().IsRegular() {
		return 0, "", errors.New("referenced path is not a regular file")
	}
	hash := sha256.New()
	size, err := io.Copy(hash, io.LimitReader(handle, budget+1))
	if err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(hash.Sum(nil)), nil
}

// refuseSymbolicLinks rejects a link at any component of a campaign-relative
// path. The ingestion rules already refuse a link that escapes the bundle; a
// campaign refuses every link, because the archive holds regular files only
// and two names for one file would make the archive depend on which name the
// author happened to write.
func refuseSymbolicLinks(root, relative string) error {
	current := root
	for _, component := range strings.Split(relative, "/") {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("path component is a symbolic link")
		}
	}
	return nil
}
