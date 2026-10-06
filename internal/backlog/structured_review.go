package backlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"io"
	"path"
	"strings"
	"unicode/utf8"
)

const MaxReviewVerdictBytes = 64 * 1024

func validateReviewOutput(r *domain.ReviewOutput, outputs []string) error {
	if r == nil {
		return nil
	}
	if (r.Verdict == "") == (r.VerdictLine == "") {
		return errors.New("declare exactly one of verdict or verdict_line")
	}
	if err := validateRelativePath(r.Verdict+r.VerdictLine, false); err != nil {
		return err
	}
	for _, output := range outputs {
		if path.Clean(output) == r.Path() {
			return nil
		}
	}
	return fmt.Errorf("%q must be a declared output", r.Path())
}

// ParseReviewVerdict accepts a JSON object or exactly the first line of a
// review. It never infers a verdict from body text.
func ParseReviewVerdict(declaration domain.ReviewOutput, raw []byte) (*domain.ReviewVerdict, error) {
	if len(raw) == 0 || len(raw) > MaxReviewVerdictBytes {
		return nil, errors.New("review verdict is empty or exceeds 65536 bytes")
	}
	var out domain.ReviewVerdict
	if declaration.Verdict != "" {
		if err := decodeReviewVerdict(raw, &out); err != nil {
			return nil, fmt.Errorf("review verdict JSON: %w", err)
		}
	} else {
		line, _, _ := strings.Cut(string(raw), "\n")
		out.Verdict = strings.TrimSpace(line)
	}
	normalized := strings.ToUpper(strings.TrimSpace(out.Verdict))
	normalized = strings.NewReplacer("_", " ", "-", " ").Replace(normalized)
	switch normalized {
	case "ACCEPT", "ACCEPTED", "APPROVE":
		out.Verdict = "accept"
	case "CHANGES REQUESTED", "REQUEST CHANGES", "REJECT":
		out.Verdict = "changes-requested"
	default:
		return nil, errors.New("review verdict must be ACCEPT/ACCEPTED/APPROVE or CHANGES_REQUESTED/CHANGES REQUESTED/REQUEST_CHANGES/REJECT")
	}
	if out.BlockingFindings < 0 {
		return nil, errors.New("review blocking_findings must be a nonnegative integer")
	}
	out.FindingTitles = domain.BoundReviewTitles(out.FindingTitles)
	return &out, nil
}

// Decode known fields strictly while allowing review metadata. Duplicate fields
// are ambiguous evidence and must not let a later ACCEPT overwrite a REJECT.
func decodeReviewVerdict(raw []byte, out *domain.ReviewVerdict) error {
	if !utf8.Valid(raw) {
		return errors.New("review JSON must be valid UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("expected an object")
	}
	seen := map[string]bool{}
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		key := strings.ToLower(token.(string))
		if seen[key] {
			return errors.New("duplicate review JSON field")
		}
		seen[key] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
		switch key {
		case "verdict":
			if bytes.Equal(value, []byte("null")) {
				return errors.New("verdict must be a string")
			}
			if err := json.Unmarshal(value, &out.Verdict); err != nil {
				return err
			}
		case "blocking_findings":
			if bytes.Equal(value, []byte("null")) {
				return errors.New("blocking_findings must be an integer")
			}
			if err := json.Unmarshal(value, &out.BlockingFindings); err != nil {
				return err
			}
		case "finding_titles":
			if bytes.Equal(value, []byte("null")) {
				return errors.New("finding_titles must be an array")
			}
			var titles []json.RawMessage
			if err := json.Unmarshal(value, &titles); err != nil {
				return err
			}
			for _, title := range titles {
				if bytes.Equal(title, []byte("null")) {
					return errors.New("finding_titles entries must be strings")
				}
				var text string
				if err := json.Unmarshal(title, &text); err != nil {
					return err
				}
				out.FindingTitles = append(out.FindingTitles, text)
			}
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing review JSON")
	}
	return nil
}

func reviewVerdictFromResult(task domain.Task, artifacts []domain.Artifact, payloads [][]byte) (*domain.ReviewVerdict, error) {
	if task.ReviewOutput == nil {
		return nil, nil
	}
	for index, artifact := range artifacts {
		if artifact.Kind == domain.ArtifactOutput && artifact.Name == task.ReviewOutput.Path() {
			return ParseReviewVerdict(*task.ReviewOutput, bytes.Clone(payloads[index]))
		}
	}
	return nil, fmt.Errorf("missing review verdict output %q", task.ReviewOutput.Path())
}
