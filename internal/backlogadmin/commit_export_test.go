package backlogadmin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func (s *remoteFakeService) ExportCommit(_ context.Context, p Principal, r CommitExportRequest) (ArtifactContent, error) {
	s.record(p)
	return ArtifactContent{Metadata: ArtifactMetadata{ID: r.Name, Size: 6}, Provenance: &backlog.CommitProvenance{Commit: "recorded", Base: "base"}, Content: io.NopCloser(bytes.NewReader([]byte("bundle")))}, nil
}
func TestExportCommitSSHStreamsAuthenticatedProvenance(t *testing.T) {
	h := newRemoteHarness(t, true)
	result, err := h.client.ExportCommit(context.Background(), CommitExportRequest{RunID: "run", TaskID: "task", Name: "implementation", Branch: "review"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(result.Content)
	_ = result.Content.Close()
	if err != nil || string(raw) != "bundle" || result.Provenance == nil || result.Provenance.Commit != "recorded" {
		t.Fatalf("result=%+v raw=%q err=%v", result, raw, err)
	}
	if h.service.lastPrincipal().ID != "remote:"+testAdminCredentials().ClientPrincipal {
		t.Fatal("remote principal not authenticated")
	}
}

type exportTransportService struct{ localTransportService }

func (s *exportTransportService) ExportCommit(_ context.Context, p Principal, r CommitExportRequest) (ArtifactContent, error) {
	s.mu.Lock()
	s.artifactPrincipal = p
	s.mu.Unlock()
	return ArtifactContent{Metadata: ArtifactMetadata{ID: r.Name, Size: 6}, Provenance: &backlog.CommitProvenance{Commit: "recorded", Base: "base"}, Content: io.NopCloser(bytes.NewReader([]byte("bundle")))}, nil
}
func TestExportCommitLocalTransportStreamsProvenance(t *testing.T) {
	s := &exportTransportService{}
	c, cancel, done := startLocalTransport(t, uint32(os.Getuid()), s)
	defer stopLocalTransport(t, cancel, done)
	result, err := c.ExportCommit(context.Background(), CommitExportRequest{RunID: "run", TaskID: "task", Name: "implementation", Branch: "review"})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(result.Content)
	_ = result.Content.Close()
	if err != nil || string(raw) != "bundle" || result.Provenance == nil || result.Provenance.Commit != "recorded" {
		t.Fatalf("result=%+v raw=%q err=%v", result, raw, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.artifactPrincipal.ID == "" {
		t.Fatal("missing authenticated peer")
	}
}
func TestExportCommitServiceFallbackAndProvenanceMismatch(t *testing.T) {
	c := newCommitCampaign(t)
	ctx := context.Background()
	before, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "source.bundle")
	gitFixtureOutput(t, filepath.Join(c.refs.Root, "campaigns.git"), "bundle", "create", bundle, c.provenance.Ref)
	called := 0
	c.service.SetCommitBundleOpener(func(_ context.Context, p backlog.CommitProvenance, _ domain.Artifact) (io.ReadCloser, error) {
		called++
		if p.Commit != c.provenance.Commit {
			t.Fatal("wrong provenance")
		}
		return os.Open(bundle)
	})
	result, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, CommitExportRequest{RunID: "run", TaskID: "implement", Name: "implementation", Branch: "review"})
	if err != nil {
		t.Fatal(err)
	}
	exported := filepath.Join(t.TempDir(), "result.bundle")
	raw, err := io.ReadAll(result.Content)
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Content.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(exported, raw, 0600); err != nil {
		t.Fatal(err)
	}
	heads := gitFixtureOutput(t, c.provenance.Repository, "bundle", "list-heads", exported)
	if heads != c.provenance.Commit+" refs/heads/review" || called != 1 {
		t.Fatalf("heads=%q fallback=%d", heads, called)
	}
	after, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("export mutated coordinator records")
	}
	c.service.SetArtifactOpener(func(context.Context, string) (domain.Artifact, io.ReadCloser, error) {
		p := c.provenance
		p.TaskID = "foreign"
		raw, _ := backlog.MarshalCommitProvenance(p)
		return domain.Artifact{}, io.NopCloser(bytes.NewReader(raw)), nil
	})
	_, err = c.service.ExportCommit(ctx, Principal{ID: "operator"}, CommitExportRequest{RunID: "run", TaskID: "implement", Name: "implementation", Branch: "review"})
	if err == nil || called != 1 {
		t.Fatal("foreign provenance reached worker")
	}
}
func TestExportCommitRetainedBundlePreferredAndCorruptionRefused(t *testing.T) {
	c := newCommitCampaign(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.bundle")
	gitFixtureOutput(t, filepath.Join(c.refs.Root, "campaigns.git"), "bundle", "create", path, c.provenance.Ref)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p := c.provenance
	p.Bundle = &backlog.CommitBundleRecord{Artifact: backlog.CommitBundleArtifactName(p.Name), Size: int64(len(raw)), SHA256: fmt.Sprintf("%x", sha256.Sum256(raw))}
	records, err := c.store.LoadCoordinatorRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var origin domain.Artifact
	for _, a := range records.Artifacts {
		if a.Name == p.Name {
			origin = a
		}
	}
	bundle := domain.Artifact{ID: "retained-bundle", WorkflowRunID: p.WorkflowRunID, TaskID: p.TaskID, AttemptID: origin.AttemptID, Name: p.Bundle.Artifact, Kind: domain.ArtifactGitState, Size: p.Bundle.Size, SHA256: p.Bundle.SHA256}
	if err = c.store.SaveCoordinatorRecords(ctx, sqlite.CoordinatorRecords{Artifacts: []domain.Artifact{bundle}}); err != nil {
		t.Fatal(err)
	}
	c.service.SetArtifactOpener(func(_ context.Context, id string) (domain.Artifact, io.ReadCloser, error) {
		if id == bundle.ID {
			return bundle, io.NopCloser(bytes.NewReader(raw)), nil
		}
		record, _ := backlog.MarshalCommitProvenance(p)
		return origin, io.NopCloser(bytes.NewReader(record)), nil
	})
	fallback := false
	c.service.SetCommitBundleOpener(func(context.Context, backlog.CommitProvenance, domain.Artifact) (io.ReadCloser, error) {
		fallback = true
		return nil, errors.New("unexpected fallback")
	})
	req := CommitExportRequest{RunID: "run", TaskID: "implement", Name: "implementation", Branch: "review"}
	result, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, req)
	if err != nil {
		t.Fatal(err)
	}
	_ = result.Content.Close()
	if fallback {
		t.Fatal("retained bundle ignored")
	}
	raw[len(raw)-1] ^= 1
	if _, err = c.service.ExportCommit(ctx, Principal{ID: "operator"}, req); err == nil {
		t.Fatal("corrupt bundle accepted")
	}
	if fallback {
		t.Fatal("corruption silently fell back")
	}
}
func TestExportCommitRefusesUnknownTargetsAndMissingProvenance(t *testing.T) {
	c := newCommitCampaign(t)
	ctx := context.Background()
	reachedWorker := false
	c.service.SetCommitBundleOpener(func(context.Context, backlog.CommitProvenance, domain.Artifact) (io.ReadCloser, error) {
		reachedWorker = true
		return nil, errors.New("unexpected worker access")
	})
	for _, test := range []struct {
		name     string
		request  CommitExportRequest
		notFound bool
	}{
		{"run", CommitExportRequest{RunID: "missing", TaskID: "implement", Name: "implementation", Branch: "review"}, true},
		{"task", CommitExportRequest{RunID: "run", TaskID: "missing", Name: "implementation", Branch: "review"}, true},
		{"name", CommitExportRequest{RunID: "run", TaskID: "implement", Name: "missing", Branch: "review"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, test.request)
			if err == nil || (test.notFound && !errors.Is(err, ErrNotFound)) {
				t.Fatalf("error=%v", err)
			}
			if reachedWorker {
				t.Fatal("unknown target reached worker")
			}
		})
	}
	c.service.SetArtifactOpener(func(context.Context, string) (domain.Artifact, io.ReadCloser, error) {
		return domain.Artifact{}, nil, os.ErrNotExist
	})
	_, err := c.service.ExportCommit(ctx, Principal{ID: "operator"}, CommitExportRequest{RunID: "run", TaskID: "implement", Name: "implementation", Branch: "review"})
	if !errors.Is(err, os.ErrNotExist) || reachedWorker {
		t.Fatalf("missing provenance error=%v worker=%v", err, reachedWorker)
	}
}
func TestExportCommitCarriersRefuseUnauthenticatedRequests(t *testing.T) {
	t.Run("local peer", func(t *testing.T) {
		s := &exportTransportService{}
		c, cancel, done := startLocalTransport(t, uint32(os.Getuid()+1), s)
		defer stopLocalTransport(t, cancel, done)
		_, err := c.ExportCommit(context.Background(), CommitExportRequest{RunID: "run", TaskID: "task", Name: "implementation", Branch: "review"})
		if err == nil {
			t.Fatal("unauthenticated peer accepted")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.artifactPrincipal.ID != "" {
			t.Fatal("unauthenticated peer reached export")
		}
	})
	t.Run("SSH signature", func(t *testing.T) {
		h := newRemoteHarness(t, false)
		config := h.client.config
		config.Credentials.ClientSecret = []byte("wrong-secret-at-least-sixteen-bytes")
		c, err := NewSSHClient(config)
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.ExportCommit(context.Background(), CommitExportRequest{RunID: "run", TaskID: "task", Name: "implementation", Branch: "review"})
		if ClassOf(err) != ClassAuthentication {
			t.Fatalf("error=%v", err)
		}
		if h.service.lastPrincipal().ID != "" {
			t.Fatal("unauthenticated signature reached export")
		}
	})
}
func TestExportCommitAuthorizesBeforeLoading(t *testing.T) {
	denied := errors.New("denied")
	reader := &countingReader{}
	service, err := New(reader, &allowAuthorizer{err: denied})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ExportCommit(context.Background(), Principal{ID: "operator"}, CommitExportRequest{RunID: "run", TaskID: "task", Name: "implementation", Branch: "review"})
	if !errors.Is(err, denied) {
		t.Fatalf("authorization error = %v", err)
	}
	if reader.reads != 0 {
		t.Fatal("loaded records before authorization")
	}
}

func TestExportCommitRefusesMalformedAndMixedRequest(t *testing.T) {
	service, err := New(&countingReader{}, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	for _, branch := range []string{"", "--bad", "../bad", "review\nother"} {
		_, err = service.ExportCommit(context.Background(), Principal{}, CommitExportRequest{RunID: "run", TaskID: "task", Name: "implementation", Branch: branch})
		if err == nil {
			t.Fatalf("accepted branch %q", branch)
		}
	}
	d := adminDispatch{service: &localTransportService{}, maxArtifactBytes: 1024}
	req := localRequest{Operation: localOperationQuery, Query: &Query{}, CommitExport: &CommitExportRequest{RunID: "run"}}
	response, content := d.handle(context.Background(), Principal{}, req, nil)
	if response.Error == "" || content != nil {
		t.Fatal("mixed request accepted")
	}
	if mutatingOperation(localOperationCommitExport) {
		t.Fatal("commit export requires mutation authority")
	}
}
