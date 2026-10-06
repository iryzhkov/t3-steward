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
	RecordScanBaseline(context.Context, workerproto.ExecutionPackage, string) error
}

type canarySignature struct {
	Prefix      []byte `json:"prefix"`
	Length      int    `json:"length"`
	SHA256      string `json:"sha256"`
	Fingerprint string `json:"fingerprint"`
	Base64      bool   `json:"base64,omitempty"`
}
type secretSnapshot struct {
	Version    int               `json:"version"`
	Signatures []canarySignature `json:"signatures"`
}

func (s *resultScanner) canaryMatch(data []byte) (int, int, string) {
	start, end, fp := s.rawCanaryMatch(data, false)
	view := func(decoded []byte, locations []int, base64Only bool) {
		at, last, fingerprint := s.rawCanaryMatch(decoded, base64Only)
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
	if bytes.ContainsAny(data, "%+") {
		for _, query := range []bool{false, true} {
			decoded, locations := decodeScanURL(data, query)
			view(decoded, locations, false)
		}
	}
	if s.hasBase64 || s.historyBase64() {
		if decoded, locations, wrapped := unwrapScanBase64(data); wrapped {
			view(decoded, locations, true)
		}
	}
	return start, end, fp
}

func (s *resultScanner) historyBase64() bool {
	for _, c := range s.history {
		if c.Base64 {
			return true
		}
	}
	return false
}

// unwrapScanBase64 removes the line breaks that wrapped base64 (MIME, PEM)
// inserts between two base64 runs. It reports false when there is no such
// break, so ordinary text costs one pass over its line ends.
func unwrapScanBase64(data []byte) ([]byte, []int, bool) {
	base64Byte := func(c byte) bool {
		return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '+' || c == '/' || c == '-' || c == '_'
	}
	// A break joins two runs when a wrapped line of at least 16 base64 bytes
	// ends at it and base64 continues after it. Prose lines rarely qualify.
	const wrappedLine = 16
	joins := func(i int) (int, bool) {
		j := i
		for j < len(data) && (data[j] == '\r' || data[j] == '\n') {
			j++
		}
		if i < wrappedLine || j >= len(data) || j-i > 2 || !base64Byte(data[j]) {
			return j, false
		}
		for k := i - wrappedLine; k < i; k++ {
			if !base64Byte(data[k]) {
				return j, false
			}
		}
		return j, true
	}
	first := -1
	for i := bytes.IndexAny(data, "\r\n"); i >= 0; {
		j, ok := joins(i)
		if ok {
			first = i
			break
		}
		next := bytes.IndexAny(data[j:], "\r\n")
		if next < 0 {
			break
		}
		i = j + next
	}
	if first < 0 {
		return nil, nil, false
	}
	decoded := make([]byte, 0, len(data))
	locations := make([]int, 0, len(data))
	for i := 0; i < len(data); i++ {
		if i >= first && (data[i] == '\r' || data[i] == '\n') {
			if j, ok := joins(i); ok {
				i = j - 1
				continue
			}
		}
		decoded = append(decoded, data[i])
		locations = append(locations, i)
	}
	return decoded, locations, true
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
		// newResultScanner already skipped values too short to match safely.
		if len(c.value) <= 4 {
			continue
		}
		sum := sha256.Sum256(c.value)
		digest := hex.EncodeToString(sum[:])
		if seen[digest] {
			continue
		}
		seen[digest] = true
		snapshot.Signatures = append(snapshot.Signatures, canarySignature{Prefix: append([]byte(nil), c.value[:4]...), Length: len(c.value), SHA256: digest, Fingerprint: c.fingerprint, Base64: c.base64})
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
	// A signature of a current canary variant is already matched exactly;
	// searching it again would only double the cost of the scan.
	seen := map[string]bool{}
	for _, c := range scanner.canaries {
		sum := sha256.Sum256(c.value)
		seen[hex.EncodeToString(sum[:])] = true
	}
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
