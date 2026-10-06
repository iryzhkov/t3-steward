package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// Hash-only execution evidence preserves known credentials across rotation and
// worker restart. Prefixes are at most four bytes; complete values never persist.
type secretRecorder interface {
	RecordSecretValues(context.Context, workerproto.ExecutionPackage, []string) error
	SnapshotSecrets(context.Context, workerproto.ExecutionPackage) error
}

type canarySignature struct {
	Prefix      []byte `json:"prefix"`
	Length      int    `json:"length"`
	SHA256      string `json:"sha256"`
	Fingerprint string `json:"fingerprint"`
}
type secretSnapshot struct {
	Version    int               `json:"version"`
	Signatures []canarySignature `json:"signatures"`
}

func (s *resultScanner) rawCanaryMatch(data []byte) (int, int, string) {
	start, end, fp := -1, 0, ""
	add := func(at, length int, fingerprint string) {
		if at >= 0 && (start < 0 || at < start) {
			start, end, fp = at, at+length, fingerprint
		}
	}
	for _, c := range s.canaries {
		add(bytes.Index(data, c.value), len(c.value), c.fingerprint)
	}
	for _, c := range s.history {
		search := data
		offset := 0
		for {
			at := bytes.Index(search, c.Prefix)
			if at < 0 {
				break
			}
			at += offset
			if at+c.Length <= len(data) {
				sum := sha256.Sum256(data[at : at+c.Length])
				if hex.EncodeToString(sum[:]) == c.SHA256 {
					add(at, c.Length, c.Fingerprint)
					break
				}
			}
			offset = at + 1
			search = data[offset:]
		}
	}
	return start, end, fp
}
func (s *resultScanner) canaryMatch(data []byte) (int, int, string) {
	start, end, fp := s.rawCanaryMatch(data)
	if !bytes.ContainsAny(data, "%+") {
		return start, end, fp
	}
	for _, query := range []bool{false, true} {
		decoded, locations := decodeScanURL(data, query)
		at, last, fingerprint := s.rawCanaryMatch(decoded)
		if at >= 0 {
			original := locations[at]
			originalEnd := len(data)
			if last < len(locations) {
				originalEnd = locations[last]
			}
			if start < 0 || original < start {
				start, end, fp = original, originalEnd, fingerprint
			}
		}
	}
	return start, end, fp
}
func decodeScanURL(data []byte, query bool) ([]byte, []int) {
	decoded := make([]byte, 0, len(data))
	locations := make([]int, 0, len(data))
	hexByte := func(c byte) (byte, bool) {
		switch {
		case c >= '0' && c <= '9':
			return c - '0', true
		case c >= 'a' && c <= 'f':
			return c - 'a' + 10, true
		case c >= 'A' && c <= 'F':
			return c - 'A' + 10, true
		}
		return 0, false
	}
	for i := 0; i < len(data); i++ {
		locations = append(locations, i)
		c := data[i]
		if c == '%' && i+2 < len(data) {
			high, h := hexByte(data[i+1])
			low, l := hexByte(data[i+2])
			if h && l {
				decoded = append(decoded, high<<4|low)
				i += 2
				continue
			}
		}
		if query && c == '+' {
			c = ' '
		}
		decoded = append(decoded, c)
	}
	return decoded, locations
}
func secretSnapshotKey(pkg workerproto.ExecutionPackage) string {
	raw, _ := json.Marshal(struct {
		Coordinator string
		WorkerEpoch string
		Identity    workerproto.ExecutionIdentity
	}{pkg.CoordinatorID, pkg.WorkerEpoch, pkg.Identity})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// RecordSecretValues captures hashes at credential resolution, before setup.
// SnapshotSecrets captures model/login material before starting a provider turn.
func (s *CustodyStore) RecordSecretValues(_ context.Context, pkg workerproto.ExecutionPackage, values []string) error {
	scanner := newResultScanner(SecretScanConfig{}, values, nil)
	if scanner.overlap > 1<<20 {
		return &SecretScanError{Object: "execution", Detector: "canary-size", Offset: 0}
	}
	snapshot := secretSnapshot{Version: 1}
	seen := map[string]bool{}
	for _, c := range scanner.canaries {
		if len(c.value) <= 4 {
			return &SecretScanError{Object: "execution", Detector: "short-canary", Offset: 0}
		}
		sum := sha256.Sum256(c.value)
		digest := hex.EncodeToString(sum[:])
		if seen[digest] {
			continue
		}
		seen[digest] = true
		snapshot.Signatures = append(snapshot.Signatures, canarySignature{Prefix: append([]byte(nil), c.value[:4]...), Length: len(c.value), SHA256: digest, Fingerprint: c.fingerprint})
	}
	if len(snapshot.Signatures) == 0 {
		return nil
	}
	if len(snapshot.Signatures) > 4096 {
		return &SecretScanError{Object: "execution", Detector: "canary-count", Offset: 0}
	}
	dir := filepath.Join(s.config.Root, "secret-scans")
	if err := ensureRealDirectory(dir); err != nil {
		return &SecretScanError{Object: "execution", Detector: "credential-history", Offset: 0}
	}
	raw, _ := json.Marshal(snapshot)
	sum := sha256.Sum256(raw)
	path := filepath.Join(dir, secretSnapshotKey(pkg)+"-"+hex.EncodeToString(sum[:])+".json")
	if err := writeJSONExclusive(path, snapshot); err != nil && !errors.Is(err, os.ErrExist) {
		return &SecretScanError{Object: "execution", Detector: "credential-history", Offset: 0}
	}
	return nil
}
func (s *CustodyStore) SnapshotSecrets(ctx context.Context, pkg workerproto.ExecutionPackage) error {
	values := append([]string(nil), s.config.SecretScan.StaticCanaries...)
	if s.config.SecretScan.Canaries != nil {
		resolved, err := s.config.SecretScan.Canaries(ctx, pkg)
		if err != nil {
			return &SecretScanError{Object: "execution", Detector: "credential-resolution", Offset: 0}
		}
		values = append(values, resolved...)
	}
	return s.RecordSecretValues(ctx, pkg, values)
}
func (s *CustodyStore) addSecretHistory(pkg workerproto.ExecutionPackage, scanner *resultScanner) error {
	dir := filepath.Join(s.config.Root, "secret-scans")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("secret snapshot read failed")
	}
	prefix := secretSnapshotKey(pkg) + "-"
	seen := map[string]bool{}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		raw, err := readScanBounded(filepath.Join(dir, entry.Name()), 4<<20)
		if err != nil {
			return errors.New("secret snapshot read failed")
		}
		sum := sha256.Sum256(raw)
		if entry.Name() != prefix+hex.EncodeToString(sum[:])+".json" {
			return errors.New("secret snapshot digest failed")
		}
		var snapshot secretSnapshot
		if json.Unmarshal(raw, &snapshot) != nil || snapshot.Version != 1 {
			return errors.New("secret snapshot invalid")
		}
		for _, c := range snapshot.Signatures {
			digest, err := hex.DecodeString(c.SHA256)
			if err != nil || len(digest) != 32 || len(c.Prefix) != 4 || c.Length <= 4 || c.Length > 1<<20 {
				return errors.New("secret snapshot invalid")
			}
			if seen[c.SHA256] {
				continue
			}
			seen[c.SHA256] = true
			scanner.history = append(scanner.history, c)
			if len(scanner.history) > 4096 {
				return errors.New("secret snapshot count exceeded")
			}
			if c.Length*3 > scanner.overlap {
				scanner.overlap = c.Length * 3
			}
		}
	}
	return nil
}
