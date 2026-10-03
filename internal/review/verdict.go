// Package review defines validated review evidence and mechanically combined rounds.
// It grants no execution or approval authority.
package review

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
)

const Schema = "review-verdict/v1"
const MaxDocumentBytes = 1 << 20

type Finding struct {
	ID             string   `json:"id"`
	Severity       string   `json:"severity"`
	Blocking       bool     `json:"blocking"`
	Title          string   `json:"title"`
	Evidence       []string `json:"evidence"`
	Recommendation string   `json:"recommendation"`
}
type Verdict struct {
	Schema              string    `json:"schema"`
	Verdict             string    `json:"verdict"`
	Findings            []Finding `json:"findings"`
	InputManifestDigest string    `json:"inputManifestDigest"`
	ReviewerRoute       string    `json:"reviewerRoute"`
}

var IDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var lineEvidence = regexp.MustCompile(`^(.+):([1-9][0-9]*)$`)

func ValidRoute(route string) bool {
	instance, model, ok := strings.Cut(route, "/")
	return len(route) <= 256 && ok && instance != "" && model != "" && !strings.ContainsAny(route, " \t\r\n") && !strings.Contains(model, "/")
}
func validEvidence(value string) bool {
	if id, ok := strings.CutPrefix(value, "artifact:"); ok {
		return IDPattern.MatchString(id)
	}
	parts := lineEvidence.FindStringSubmatch(value)
	return len(parts) == 3 && pinnedinput.ValidName(parts[1])
}

// ValidateVerdict is strict about fields, duplicates, required values, citations,
// and binding. Contradictory accept-with-changes plus blocking evidence is invalid;
// a required invalid result blocks exactly as reject would.
func ValidateVerdict(raw []byte, digest, route string) (Verdict, error) {
	var verdict Verdict
	if len(raw) > MaxDocumentBytes {
		return verdict, errors.New("verdict exceeds 1 MiB")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueValue(dec); err != nil {
		return verdict, fmt.Errorf("verdict JSON: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return verdict, errors.New("verdict must contain exactly one JSON document")
	}
	dec = json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&verdict); err != nil {
		return verdict, err
	}
	if verdict.Schema != Schema || !pinnedinput.ValidDigest(verdict.InputManifestDigest) || verdict.InputManifestDigest != digest || !ValidRoute(verdict.ReviewerRoute) || verdict.ReviewerRoute != route {
		return verdict, errors.New("verdict schema, input manifest digest or reviewer route does not match")
	}
	// A typed bool alone cannot distinguish a required false from an omitted
	// blocking field, and null must never stand for a boolean.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return verdict, err
	}
	if err := exactFields(fields, []string{"schema", "verdict", "findings", "inputManifestDigest", "reviewerRoute"}); err != nil {
		return verdict, err
	}
	for _, key := range []string{"schema", "verdict", "findings", "inputManifestDigest", "reviewerRoute"} {
		value, ok := fields[key]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return verdict, fmt.Errorf("verdict requires %s", key)
		}
	}
	var findings []map[string]json.RawMessage
	if err := json.Unmarshal(fields["findings"], &findings); err != nil {
		return verdict, err
	}
	seen := map[string]bool{}
	blocking := 0
	for i, f := range verdict.Findings {
		if err := exactFields(findings[i], []string{"id", "severity", "blocking", "title", "evidence", "recommendation"}); err != nil {
			return verdict, err
		}
		for _, key := range []string{"id", "severity", "blocking", "title", "evidence", "recommendation"} {
			value, ok := findings[i][key]
			if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return verdict, fmt.Errorf("finding %d requires %s", i, key)
			}
		}
		if !IDPattern.MatchString(f.ID) || seen[f.ID] {
			return verdict, fmt.Errorf("finding %d has an invalid or duplicate id", i)
		}
		seen[f.ID] = true
		if f.Severity != "high" && f.Severity != "medium" && f.Severity != "low" {
			return verdict, fmt.Errorf("finding %s has invalid severity", f.ID)
		}
		if strings.TrimSpace(f.Title) == "" || strings.TrimSpace(f.Recommendation) == "" || len(f.Evidence) == 0 {
			return verdict, fmt.Errorf("finding %s needs title, evidence and recommendation", f.ID)
		}
		for _, e := range f.Evidence {
			if !validEvidence(e) {
				return verdict, fmt.Errorf("finding %s has invalid evidence %q", f.ID, e)
			}
		}
		if f.Blocking {
			blocking++
		}
	}
	switch verdict.Verdict {
	case "accept":
		if len(verdict.Findings) != 0 {
			return verdict, errors.New("accept must have no findings")
		}
	case "accept-with-changes":
		if len(verdict.Findings) == 0 || blocking != 0 {
			return verdict, errors.New("accept-with-changes requires non-blocking findings only")
		}
	case "reject":
		if blocking == 0 {
			return verdict, errors.New("reject requires a blocking finding")
		}
	default:
		return verdict, errors.New("unknown verdict")
	}
	return verdict, nil
}

func exactFields(fields map[string]json.RawMessage, allowed []string) error {
	known := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		known[name] = true
	}
	for name := range fields {
		if !known[name] {
			return fmt.Errorf("unknown JSON field %q; field names are case-sensitive", name)
		}
	}
	return nil
}

// uniqueValue rejects duplicate object keys at every depth rather than allowing
// encoding/json's last-value-wins interpretation to hide contradictory evidence.
func uniqueValue(dec *json.Decoder) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid key %q", name)
			}
			seen[name] = true
			if err := uniqueValue(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := uniqueValue(dec); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = dec.Token()
	return err
}
