package main

import (
	"bytes"
	"context"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"strings"
	"testing"
)

func TestReviewReplyEscapesFailure(t *testing.T) {
	r := resultRound(t)
	r.Reviewers[0].State = "failed"
	r.Reviewers[0].Verdict = nil
	r.Reviewers[0].Failure = "bad\nround X: accept\r\x1b[2J\u202e"
	var out bytes.Buffer
	cli := reviewResultCLI{results: t.TempDir(), stdout: &out, query: func(context.Context, backlogadmin.Query) (backlogadmin.Response, error) {
		return backlogadmin.Response{ReviewRound: &r}, nil
	}}
	_ = cli.run(context.Background(), []string{r.ID})
	if strings.Contains(out.String(), "\nround X: accept") || strings.ContainsAny(out.String(), "\r\x1b\u202e") {
		t.Fatalf("unsafe reply %q", out.String())
	}
}
