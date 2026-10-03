package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestM8ReviewTaskHelpNamesFullContract(t *testing.T) {
	if !strings.Contains(taskUsage, "task run --help full is the whole contract") {
		t.Fatal("task help does not name the full contract")
	}
}

func TestM8ReviewSafeTerminalTextNeutralisesCarriageReturnAndFormats(t *testing.T) {
	raw := "text\rreset\tcolumn\nnext"
	want := "text\\x0dreset\tcolumn\nnext"
	for _, span := range unicode.Cf.R16 {
		for value := uint32(span.Lo); value <= uint32(span.Hi); value += uint32(span.Stride) {
			if got := string(safeTerminalText([]byte(string(rune(value))))); strings.ContainsRune(got, rune(value)) {
				t.Errorf("format U+%04X survived", value)
			}
		}
	}
	for _, span := range unicode.Cf.R32 {
		for value := span.Lo; value <= span.Hi; value += span.Stride {
			if got := string(safeTerminalText([]byte(string(rune(value))))); strings.ContainsRune(got, rune(value)) {
				t.Errorf("format U+%04X survived", value)
			}
		}
	}
	if got := string(safeTerminalText([]byte(raw))); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestM8ReviewVerificationEscapesCommandAndError(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "command", true: "error"}[fail], func(t *testing.T) {
			unsafe := "verify\x1b[2J\r\u202e"
			raw, err := json.Marshal(map[string]any{"command": unsafe, "exitCode": 1})
			if err != nil {
				t.Fatal(err)
			}
			artifact := &fakeArtifactService{content: backlogadmin.ArtifactContent{Content: io.NopCloser(bytes.NewReader(raw))}}
			if fail {
				artifact.err = errors.New(unsafe)
			}
			detail := backlogadmin.WorkflowDetail{Tasks: []backlogadmin.TaskDetail{{
				Task: domain.Task{Name: "review"}, Attempt: &domain.Attempt{ID: "a1"},
				Artifacts: []backlogadmin.Artifact{{Metadata: backlogadmin.ArtifactMetadata{ID: "v1", AttemptID: "a1", Kind: domain.ArtifactVerification}}},
			}}}
			var out bytes.Buffer
			c := backlogAdminCLI{artifacts: artifact, stdout: &out}
			c.renderVerification(context.Background(), &detail)
			if strings.ContainsAny(out.String(), "\x1b\r\u202e") || !strings.Contains(out.String(), "verify\\x1b[2J\\x0d\\u202e") {
				t.Fatalf("unsafe verification output: %q", out.String())
			}
		})
	}
}

func TestM8ReviewRegisterOnlyOldCoordinatorRefusal(t *testing.T) {
	old := errors.New("invalid operation envelope: json: unknown field \"registerOnly\"")
	for _, tc := range []struct {
		name     string
		register bool
		refusal  error
		want     string
	}{
		{"old registration", true, old, "coordinator too old for --register-only (needs 0.11.0-rc.103 or later)"},
		{"ordinary submission", false, old, old.Error()},
		{"other registration error", true, errors.New("permission denied"), "permission denied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			c := campaignTestCLI(t, &out)
			c.viability = func(_ context.Context, r backlogadmin.ViabilityRequest) (backlogadmin.ViabilityMatrix, error) {
				return campaignReadyMatrix(r), nil
			}
			c.submissions = func() (adminSubmissionService, error) {
				return submissionFunc(func(context.Context, backlogadmin.LocalSubmissionRequest, io.Reader, int64) (backlogadmin.LocalSubmissionResponse, error) {
					return backlogadmin.LocalSubmissionResponse{}, tc.refusal
				}), nil
			}
			args := []string{"submit", campaignFixture(t), "--idempotency-key", "old", "--no-notify"}
			if tc.register {
				args = append(args, "--register-only")
			}
			err := c.run(context.Background(), args)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
}
