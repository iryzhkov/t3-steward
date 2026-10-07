package backlogadmin

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

type exportCloseFailure struct {
	io.ReadCloser
	failure error
}

func (r *exportCloseFailure) Close() error { return errors.Join(r.ReadCloser.Close(), r.failure) }

func TestExportCommitRefusesSourceCloseFailure(t *testing.T) {
	c := newCommitCampaign(t)
	bundle := filepath.Join(t.TempDir(), "source.bundle")
	gitFixtureOutput(t, filepath.Join(c.refs.Root, "campaigns.git"), "bundle", "create", bundle, c.provenance.Ref)
	failed := errors.New("source transport failed on close")
	c.service.SetCommitBundleOpener(func(context.Context, backlog.CommitProvenance, domain.Artifact) (io.ReadCloser, error) {
		f, err := os.Open(bundle)
		if err != nil {
			return nil, err
		}
		return &exportCloseFailure{ReadCloser: f, failure: failed}, nil
	})
	result, err := c.service.ExportCommit(context.Background(), Principal{ID: "operator"}, CommitExportRequest{RunID: "run", TaskID: "implement", Name: "implementation", Branch: "review"})
	if result.Content != nil {
		_ = result.Content.Close()
	}
	if !errors.Is(err, failed) {
		t.Fatalf("source close failure was suppressed: err=%v", err)
	}
}
