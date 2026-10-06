package workerruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"regexp"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/workerproto"
)

// SecretScanConfig is worker-local policy. Canary values are never serialized.
// PatternPolicy accepts default (commits/bundles block), block, or warn.
type SecretScanConfig struct {
	MaxBytes       int64
	PatternPolicy  string
	StaticCanaries []string
	Canaries       func(context.Context, workerproto.ExecutionPackage) ([]string, error)
	Log            *slog.Logger
}

// SecretScanError contains only redacted evidence, safe for explain and notifications.
type SecretScanError struct {
	Object      string `json:"object"`
	Detector    string `json:"detector"`
	Offset      int64  `json:"byteOffset"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

// retryable reports a worker-local failure to assemble the scan (credential
// resolution, snapshot or baseline reads) rather than a finding in the
// content. A later collection may succeed, so it never fails a result
// permanently.
func (e *SecretScanError) retryable() bool {
	switch e.Detector {
	case "credential-resolution", "credential-history", "scan-baseline":
		return true
	}
	return false
}

func (e *SecretScanError) Error() string {
	return fmt.Sprintf("result secret scan refused object %q: detector=%s byte=%d fingerprint=%s", e.Object, e.Detector, e.Offset, e.Fingerprint)
}

// secretFingerprint names a value in redacted evidence. Its first four bytes
// are shown only for a value long enough that they reveal little of it.
func secretFingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	prefix := "****"
	if len(value) >= 16 {
		prefix = value[:4]
	}
	return fmt.Sprintf("%s:%x", prefix, sum[:6])
}

type secretPattern struct {
	name string
	re   *regexp.Regexp
	// keywords, when set, must occur (ASCII case-insensitively) before the
	// expression runs; a case-insensitive expression has no literal prefix
	// for the regexp engine to skip ahead with.
	keywords []string
}

func (p secretPattern) mayMatch(data []byte) bool {
	if len(p.keywords) == 0 {
		return true
	}
	for _, keyword := range p.keywords {
		if containsFoldASCII(data, keyword) {
			return true
		}
	}
	return false
}

// containsFoldASCII reports whether data contains the lowercase ASCII needle
// in any letter case.
func containsFoldASCII(data []byte, needle string) bool {
	for _, first := range []byte{needle[0], needle[0] - 'a' + 'A'} {
		for rest := data; ; {
			at := bytes.IndexByte(rest, first)
			if at < 0 || len(rest)-at < len(needle) {
				break
			}
			if bytes.EqualFold(rest[at:at+len(needle)], []byte(needle)) {
				return true
			}
			rest = rest[at+1:]
		}
	}
	return false
}

var resultSecretPatterns = []secretPattern{
	{"github", regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{36,255}|github_pat_[A-Za-z0-9_]{60,255})`), nil},
	{"anthropic", regexp.MustCompile(`sk-ant-[A-Za-z0-9_-]{32,255}`), nil},
	{"openai", regexp.MustCompile(`sk-[A-Za-z0-9_-]{32,255}`), nil},
	{"aws", regexp.MustCompile(`AKIA[A-Z0-9]{16}`), nil},
	{"cloudflare", regexp.MustCompile(`(?i)(?:cloudflare[_ -]*(?:api[_ -]*)?token|cf_api_token)[\t "'=:]{1,16}([A-Za-z0-9_-]{40})`), []string{"cloudflare", "cf_api_token"}},
	{"private-key", regexp.MustCompile(`-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----`), nil},
	{"age", regexp.MustCompile(`AGE-SECRET-KEY-1[0-9A-Z]{58}`), nil},
}

// minCanaryBytes is the shortest credential or encoded form matched exactly.
// A value of four bytes or fewer would refuse ordinary text, so it is counted
// and skipped rather than failing the execution. Five bytes and longer, such
// as a short project password, stay protected.
const minCanaryBytes = 5

type canaryVariant struct {
	value       []byte
	fingerprint string
	// base64 variants are also matched across line breaks (MIME, PEM).
	base64 bool
}
type resultScanner struct {
	config    SecretScanConfig
	canaries  []canaryVariant
	allow     map[string]bool
	overlap   int
	history   []canarySignature
	skipped   int
	hasBase64 bool
	// tempRoot holds decoded bundles; empty selects the system default.
	tempRoot string
	index    *canaryIndex
}

func newResultScanner(config SecretScanConfig, canaries []string, allow map[string]bool) *resultScanner {
	if config.MaxBytes <= 0 {
		config.MaxBytes = 64 << 20
	}
	s := &resultScanner{config: config, allow: allow, overlap: 1024}
	seen := map[string]bool{}
	for _, value := range append(append([]string(nil), config.StaticCanaries...), canaries...) {
		if len(value) < minCanaryBytes {
			if value != "" {
				s.skipped++
			}
			continue
		}
		fingerprint := secretFingerprint(value)
		add := func(variant string, base64 bool) {
			if len(variant) < minCanaryBytes || seen[variant] {
				return
			}
			seen[variant] = true
			s.canaries = append(s.canaries, canaryVariant{[]byte(variant), fingerprint, base64})
			width := len(variant)
			if base64 {
				// Room for the line breaks of a wrapped encoding.
				width += len(variant)/16 + 4
				s.hasBase64 = true
			}
			if width > s.overlap {
				s.overlap = width
			}
		}
		for _, variant := range []string{value, url.QueryEscape(value), url.PathEscape(value)} {
			add(variant, false)
		}
		// Also recognize percent-encoding of all bytes, in either hex case.
		var encoded strings.Builder
		for _, c := range []byte(value) {
			fmt.Fprintf(&encoded, "%%%02X", c)
		}
		add(encoded.String(), false)
		add(strings.ToLower(encoded.String()), false)
		for _, variant := range base64CanaryVariants(value) {
			add(variant, true)
		}
	}
	return s
}

// base64CanaryVariants returns, for both alphabets and each of the three byte
// alignments a value can have inside a longer encoded string (a Basic
// credential, a docker auth field), the characters determined by the value
// alone. Characters that also encode neighbouring bytes, and padding, are
// trimmed, so the variant matches whatever precedes or follows the value.
func base64CanaryVariants(value string) []string {
	var variants []string
	for _, encoding := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
		for offset := 0; offset < 3; offset++ {
			encoded := encoding.EncodeToString(append(make([]byte, offset), value...))
			start, end := (offset*8+5)/6, (offset+len(value))*8/6
			if end > start {
				variants = append(variants, encoded[start:end])
			}
		}
	}
	return variants
}
func (s *resultScanner) safeName(name string) string {
	var redacted strings.Builder
	for {
		start, end, _ := s.canaryMatch([]byte(name))
		if start < 0 {
			redacted.WriteString(name)
			break
		}
		redacted.WriteString(name[:start])
		redacted.WriteString("[redacted]")
		name = name[end:]
	}
	name = redacted.String()
	for _, p := range resultSecretPatterns {
		name = p.re.ReplaceAllString(name, "[redacted]")
	}
	return name
}
func (s *resultScanner) scan(object, kind string, r io.Reader) error {
	object = s.safeName(object)
	if s.overlap > 1<<20 {
		return &SecretScanError{Object: object, Detector: "canary-size", Offset: 0}
	}
	if s.config.PatternPolicy != "" && s.config.PatternPolicy != "default" && s.config.PatternPolicy != "block" && s.config.PatternPolicy != "warn" {
		return &SecretScanError{Object: object, Detector: "invalid-policy", Offset: 0}
	}
	buf := make([]byte, 64<<10)
	pending := make([]byte, 0, len(buf)+s.overlap)
	var total, base int64
	for {
		n, err := r.Read(buf)
		total += int64(n)
		if total > s.config.MaxBytes {
			return &SecretScanError{Object: object, Detector: "byte-cap", Offset: s.config.MaxBytes}
		}
		pending = append(pending, buf[:n]...)
		eof := err == io.EOF
		cut := len(pending) - s.overlap
		if eof {
			cut = len(pending)
		}
		if cut > 0 {
			if at, _, fp := s.canaryMatch(pending); at >= 0 && at < cut {
				return &SecretScanError{Object: object, Detector: "canary", Offset: base + int64(at), Fingerprint: fp}
			}
			for _, p := range resultSecretPatterns {
				if !p.mayMatch(pending) {
					continue
				}
				for _, match := range p.re.FindAllSubmatchIndex(pending, -1) {
					if match[0] >= cut {
						continue
					}
					start, end := match[0], match[1]
					if len(match) > 2 {
						start, end = match[2], match[3]
					}
					fp := secretFingerprint(string(pending[start:end]))
					if s.allow[fp] {
						continue
					}
					finding := &SecretScanError{Object: object, Detector: p.name, Offset: base + int64(start), Fingerprint: fp}
					block := s.config.PatternPolicy == "block" || ((s.config.PatternPolicy == "" || s.config.PatternPolicy == "default") && (kind == "commit" || kind == "bundle"))
					if block {
						return finding
					}
					logger := s.config.Log
					if logger == nil {
						logger = slog.Default()
					}
					logger.Warn("result secret scan finding", "object", finding.Object, "detector", finding.Detector, "byte_offset", finding.Offset, "fingerprint", finding.Fingerprint)
				}
			}
			copy(pending, pending[cut:])
			pending = pending[:len(pending)-cut]
			base += int64(cut)
		}
		if eof {
			return nil
		}
		if err != nil {
			return &SecretScanError{Object: object, Detector: "read-error", Offset: total}
		}
	}
}
