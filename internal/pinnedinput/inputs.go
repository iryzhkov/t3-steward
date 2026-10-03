// Package pinnedinput snapshots local evidence for generated campaigns.
// The same bytes and canonical manifest are used by task run and review callers.
package pinnedinput

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const MaxFileBytes int64 = 1 << 20
const MaxTotalBytes int64 = 3 << 20
const MaxFiles = 100

type Entry struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type Manifest struct {
	Entries []Entry `json:"entries"`
	Digest  string  `json:"digest"`
}

var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ValidName is a portable, normalized relative path without glob metacharacters.
func ValidName(name string) bool {
	return name != "" && fs.ValidPath(name) && name != "." && !strings.ContainsAny(name, "\\:*?[]\x00") && path.Clean(name) == name
}
func ValidDigest(digest string) bool { return hashPattern.MatchString(digest) }

// NewManifest hashes the compact JSON array of entries sorted by name.
// Source locations are deliberately excluded: only the evidence bytes matter.
func NewManifest(entries []Entry) (Manifest, error) {
	entries = append([]Entry{}, entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	if len(entries) > MaxFiles {
		return Manifest{}, fmt.Errorf("inputs exceed %d files", MaxFiles)
	}
	var total int64
	for i, e := range entries {
		if !ValidName(e.Name) || !ValidDigest(e.SHA256) || e.Size < 0 || e.Size > MaxFileBytes {
			return Manifest{}, fmt.Errorf("invalid input entry %q (limit %d bytes per file)", e.Name, MaxFileBytes)
		}
		if i > 0 && e.Name == entries[i-1].Name {
			return Manifest{}, fmt.Errorf("duplicate input name %q", e.Name)
		}
		total += e.Size
	}
	if total > MaxTotalBytes {
		return Manifest{}, fmt.Errorf("inputs exceed %d total bytes", MaxTotalBytes)
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return Manifest{}, err
	}
	sum := sha256.Sum256(raw)
	return Manifest{Entries: entries, Digest: hex.EncodeToString(sum[:])}, nil
}

type Snapshot struct {
	Manifest Manifest
	files    map[string][]byte
}

// SnapshotFiles uses basenames as workspace names, rejects collisions and every
// symlink component, and reads bounded regular files once. Mutating the originals
// after this call cannot change a submission or its idempotency key.
func SnapshotFiles(files []string) (Snapshot, error) {
	if len(files) > MaxFiles {
		return Snapshot{}, fmt.Errorf("inputs exceed %d files", MaxFiles)
	}
	snapshot := Snapshot{files: make(map[string][]byte)}
	var entries []Entry
	var total int64
	for _, file := range files {
		for _, component := range strings.Split(filepath.ToSlash(file), "/") {
			if component == ".." {
				return Snapshot{}, fmt.Errorf("input %q: parent traversal is refused", file)
			}
		}
		absolute, err := filepath.Abs(file)
		if err != nil {
			return Snapshot{}, err
		}
		name := filepath.Base(absolute)
		if !ValidName(name) {
			return Snapshot{}, fmt.Errorf("input %q: unsafe name", file)
		}
		if _, exists := snapshot.files[name]; exists {
			return Snapshot{}, fmt.Errorf("input %q: duplicate name %q", file, name)
		}
		info, err := refuseLinks(absolute)
		if err != nil {
			return Snapshot{}, fmt.Errorf("input %q: %w", file, err)
		}
		if !info.Mode().IsRegular() {
			return Snapshot{}, fmt.Errorf("input %q: must be a regular file", file)
		}
		if info.Size() > MaxFileBytes {
			return Snapshot{}, fmt.Errorf("input %q exceeds %d bytes", file, MaxFileBytes)
		}
		f, err := os.Open(absolute)
		if err != nil {
			return Snapshot{}, err
		}
		opened, statErr := f.Stat()
		if statErr != nil || !os.SameFile(info, opened) {
			f.Close()
			return Snapshot{}, fmt.Errorf("input %q changed while opening", file)
		}
		data, readErr := io.ReadAll(io.LimitReader(f, MaxFileBytes+1))
		closeErr := f.Close()
		if readErr != nil {
			return Snapshot{}, readErr
		}
		if closeErr != nil {
			return Snapshot{}, closeErr
		}
		if int64(len(data)) > MaxFileBytes {
			return Snapshot{}, fmt.Errorf("input %q exceeds %d bytes", file, MaxFileBytes)
		}
		// Check again so replacing a source component with a link cannot silently
		// change what was recorded, even if the link resolves to the same file.
		if _, err := refuseLinks(absolute); err != nil {
			return Snapshot{}, err
		}
		total += int64(len(data))
		if total > MaxTotalBytes {
			return Snapshot{}, fmt.Errorf("inputs exceed %d total bytes", MaxTotalBytes)
		}
		sum := sha256.Sum256(data)
		entries = append(entries, Entry{Name: name, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])})
		snapshot.files[name] = data
	}
	manifest, err := NewManifest(entries)
	snapshot.Manifest = manifest
	return snapshot, err
}
func refuseLinks(absolute string) (fs.FileInfo, error) {
	current := string(filepath.Separator)
	var info fs.FileInfo
	for _, component := range strings.Split(strings.TrimPrefix(absolute, current), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		var err error
		info, err = os.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("symbolic links are refused")
		}
	}
	if info == nil {
		return nil, errors.New("input is not a file")
	}
	return info, nil
}

// Write publishes the already-snapshotted bytes into a private campaign root.
// os.Root prevents a destination link from redirecting any write outside it.
func (s Snapshot) Write(root string) error {
	dir, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	for _, entry := range s.Manifest.Entries {
		data, ok := s.files[entry.Name]
		sum := sha256.Sum256(data)
		if !ok || int64(len(data)) != entry.Size || hex.EncodeToString(sum[:]) != entry.SHA256 || !ValidName(entry.Name) {
			return errors.New("snapshot does not match its manifest")
		}
		f, err := dir.OpenFile(entry.Name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := f.Write(data)
		closeErr := f.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
