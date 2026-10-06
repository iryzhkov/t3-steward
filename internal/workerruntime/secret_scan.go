package workerruntime

import (
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

func (e *SecretScanError) Error() string {
	return fmt.Sprintf("result secret scan refused object %q: detector=%s byte=%d fingerprint=%s", e.Object, e.Detector, e.Offset, e.Fingerprint)
}
func secretFingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	prefix := value
	if len(prefix) > 4 {
		prefix = prefix[:4]
	} else {
		prefix = "****"
	}
	return fmt.Sprintf("%s:%x", prefix, sum[:6])
}

type secretPattern struct {
	name string
	re   *regexp.Regexp
}

var resultSecretPatterns = []secretPattern{
	{"github", regexp.MustCompile(`(?:gh[pousr]_[A-Za-z0-9]{36,255}|github_pat_[A-Za-z0-9_]{60,255})`)},
	{"anthropic", regexp.MustCompile(`sk-ant-[A-Za-z0-9_-]{32,255}`)},
	{"openai", regexp.MustCompile(`sk-[A-Za-z0-9_-]{32,255}`)},
	{"aws", regexp.MustCompile(`AKIA[A-Z0-9]{16}`)},
	{"cloudflare", regexp.MustCompile(`(?i)(?:cloudflare[_ -]*(?:api[_ -]*)?token|cf_api_token)[\t "'=:]{1,16}([A-Za-z0-9_-]{40})`)},
	{"private-key", regexp.MustCompile(`-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----`)},
	{"age", regexp.MustCompile(`AGE-SECRET-KEY-1[0-9A-Z]{58}`)},
}

type canaryVariant struct {
	value       []byte
	fingerprint string
}
type resultScanner struct {
	config   SecretScanConfig
	canaries []canaryVariant
	allow    map[string]bool
	overlap  int
	history  []canarySignature
}

func newResultScanner(config SecretScanConfig, canaries []string, allow map[string]bool) *resultScanner {
	if config.MaxBytes <= 0 {
		config.MaxBytes = 64 << 20
	}
	s := &resultScanner{config: config, allow: allow, overlap: 1024}
	for _, value := range append(append([]string(nil), config.StaticCanaries...), canaries...) {
		if value == "" {
			continue
		}
		variants := []string{value, base64.StdEncoding.EncodeToString([]byte(value)), base64.RawStdEncoding.EncodeToString([]byte(value)), base64.URLEncoding.EncodeToString([]byte(value)), base64.RawURLEncoding.EncodeToString([]byte(value)), url.QueryEscape(value), url.PathEscape(value)}
		// Also recognize percent-encoding of all bytes, in either hex case.
		var encoded strings.Builder
		for _, c := range []byte(value) {
			fmt.Fprintf(&encoded, "%%%02X", c)
		}
		variants = append(variants, encoded.String(), strings.ToLower(encoded.String()))
		for _, v := range variants {
			s.canaries = append(s.canaries, canaryVariant{[]byte(v), secretFingerprint(value)})
			if len(v) > s.overlap {
				s.overlap = len(v)
			}
		}
	}
	return s
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
