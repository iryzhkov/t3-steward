package review

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/pinnedinput"
)

const SchemaV2 = "review-verdict/v2"

// ContractRefResolver confirms that a syntactically valid citation names an
// in-scope obligation in retained contract inputs or the reviewed commit.
type ContractRefResolver interface {
	ResolveContractRef(ref string) bool
}

type FindingV2 struct {
	Finding
	Class         string   `json:"class"`
	ContractRef   string   `json:"contract_ref"`
	Detail        string   `json:"detail"`
	AffectedPaths []string `json:"affected_paths"`
	FailingTests  []string `json:"failing_tests"`
	Reproducer    string   `json:"reproducer"`
}

type VerdictV2 struct {
	Schema              string      `json:"schema"`
	Verdict             string      `json:"verdict"`
	InputManifestDigest string      `json:"inputManifestDigest"`
	ReviewerRoute       string      `json:"reviewerRoute"`
	Findings            []FindingV2 `json:"findings"`
	ChecksPassed        bool        `json:"checks_passed"`
	CheckEvidence       []string    `json:"check_evidence"`
}

// NormalizationEvent preserves the original claim when a reference is downgraded.
type NormalizationEvent struct {
	FindingID     string `json:"finding_id"`
	OriginalClass string `json:"original_class"`
	ContractRef   string `json:"contract_ref"`
}

// FormatFailure distinguishes malformed packets from substantive rejection.
type FormatFailure struct{ Reason string }

func (e *FormatFailure) Error() string { return e.Reason }

func validContractRef(ref string) bool {
	if rest, ok := strings.CutPrefix(ref, "test:"); ok {
		file, name, found := strings.Cut(rest, ":")
		return found && pinnedinput.ValidName(file) && strings.TrimSpace(name) != "" &&
			!strings.ContainsAny(name, " \t\r\n:") && safeFindingText(name)
	}
	parts := lineEvidence.FindStringSubmatch(ref)
	return len(parts) == 3 && pinnedinput.ValidName(parts[1])
}

// packetFields checks exact, required, non-null fields and JSON types. The only
// optional ingestion field is contract_ref; normalization materializes it.
func packetFields(raw json.RawMessage, types map[string]string, optional string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("expected object")
	}
	allowed := make([]string, 0, len(types))
	for key := range types {
		allowed = append(allowed, key)
	}
	if err := exactFields(fields, allowed); err != nil {
		return err
	}
	for key, kind := range types {
		value, present := fields[key]
		if !present && key == optional {
			continue
		}
		value = bytes.TrimSpace(value)
		if !present || bytes.Equal(value, []byte("null")) {
			return fmt.Errorf("requires non-null %s", key)
		}
		switch kind {
		case "string":
			var s string
			if err := json.Unmarshal(value, &s); err != nil {
				return fmt.Errorf("%s must be a string", key)
			}
		case "boolean":
			var b bool
			if err := json.Unmarshal(value, &b); err != nil {
				return fmt.Errorf("%s must be a boolean", key)
			}
		case "strings", "findings":
			var items []json.RawMessage
			if err := json.Unmarshal(value, &items); err != nil {
				return fmt.Errorf("%s must be an array", key)
			}
			for _, item := range items {
				if kind == "strings" {
					var s string
					if bytes.Equal(bytes.TrimSpace(item), []byte("null")) || json.Unmarshal(item, &s) != nil {
						return fmt.Errorf("%s must contain strings", key)
					}
				} else if err := packetFields(item, findingV2Fields, "contract_ref"); err != nil {
					return fmt.Errorf("finding: %w", err)
				}
			}
		}
	}
	return nil
}

var verdictV2Fields = map[string]string{
	"schema": "string", "verdict": "string", "inputManifestDigest": "string",
	"reviewerRoute": "string", "findings": "findings",
	"checks_passed": "boolean", "check_evidence": "strings",
}
var findingV2Fields = map[string]string{
	"id": "string", "class": "string", "contract_ref": "string", "title": "string",
	"detail": "string", "affected_paths": "strings", "failing_tests": "strings",
	"reproducer": "string", "severity": "string", "blocking": "boolean",
	"evidence": "strings", "recommendation": "string",
}

// ValidateVerdictV2 returns the normalized verdict, findings and downgrade events.
// A failed packet is a format failure, never an accepting or repair verdict.
// The resolver owns semantic citation resolution; this validator owns syntax.
func ValidateVerdictV2(raw, markdown []byte, digest, route string, resolver ContractRefResolver) (verdict VerdictV2, findings []FindingV2, events []NormalizationEvent, err error) {
	defer func() {
		if err != nil {
			verdict, findings, events = VerdictV2{}, nil, nil
			err = &FormatFailure{Reason: err.Error()}
		}
	}()
	if len(raw) > MaxDocumentBytes || len(markdown) > MaxDocumentBytes || len(markdown) == 0 {
		err = fmt.Errorf("verdict and review.md must be within 1 MiB; review.md must be nonempty")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err = uniqueValue(dec); err != nil {
		return
	}
	if _, tailErr := dec.Token(); tailErr != io.EOF {
		err = fmt.Errorf("verdict must contain exactly one JSON document")
		return
	}
	if err = packetFields(raw, verdictV2Fields, ""); err != nil {
		return
	}
	if err = json.Unmarshal(raw, &verdict); err != nil {
		return
	}
	if verdict.Schema != SchemaV2 {
		err = fmt.Errorf("requires %s; legacy verdicts require fresh v2 review", SchemaV2)
		return
	}
	for i := range verdict.Findings {
		f := &verdict.Findings[i]
		switch f.Class {
		case "defect", "regression", "safety", "note":
		default:
			err = fmt.Errorf("finding %s has invalid class", f.ID)
			return
		}
		if !validContractRef(f.ContractRef) || resolver == nil || !resolver.ResolveContractRef(f.ContractRef) {
			events = append(events, NormalizationEvent{FindingID: f.ID, OriginalClass: f.Class, ContractRef: f.ContractRef})
			f.Class = "note"
		}
		if f.Blocking != (f.Class != "note") {
			err = fmt.Errorf("finding %s blocking contradicts normalized class", f.ID)
			return
		}
		if strings.TrimSpace(f.Detail) == "" {
			err = fmt.Errorf("finding %s requires detail", f.ID)
			return
		}
		for _, path := range f.AffectedPaths {
			if !pinnedinput.ValidName(path) {
				err = fmt.Errorf("finding %s has invalid affected path", f.ID)
				return
			}
		}
		for _, test := range f.FailingTests {
			if strings.TrimSpace(test) == "" {
				err = fmt.Errorf("finding %s has empty failing test", f.ID)
				return
			}
		}
	}
	// Project into v1 solely to reuse its binding, text, citation, ID, severity
	// and verdict consistency checks. Historical v1 records remain unchanged.
	legacy := Verdict{Schema: Schema, Verdict: verdict.Verdict, InputManifestDigest: verdict.InputManifestDigest, ReviewerRoute: verdict.ReviewerRoute, Findings: make([]Finding, 0, len(verdict.Findings))}
	for _, f := range verdict.Findings {
		legacy.Findings = append(legacy.Findings, f.Finding)
	}
	projected, marshalErr := json.Marshal(legacy)
	if marshalErr != nil {
		err = marshalErr
		return
	}
	if _, err = ValidateVerdict(projected, digest, route); err != nil {
		return
	}
	for _, citation := range verdict.CheckEvidence {
		if !validEvidence(citation) {
			err = fmt.Errorf("invalid check evidence %q", citation)
			return
		}
	}
	if !verdict.ChecksPassed && len(verdict.CheckEvidence) == 0 {
		err = fmt.Errorf("failed or unperformed checks require evidence")
		return
	}
	expectedLine := "VERDICT: CHANGES_REQUESTED"
	if verdict.Verdict != "reject" {
		expectedLine = "VERDICT: ACCEPT"
		if !verdict.ChecksPassed {
			err = fmt.Errorf("accepting verdict requires passing checks")
			return
		}
	}
	firstLine, _, _ := strings.Cut(string(markdown), "\n")
	if firstLine != expectedLine {
		err = fmt.Errorf("review.md first line disagrees with JSON verdict")
		return
	}
	findings = verdict.Findings
	return
}
