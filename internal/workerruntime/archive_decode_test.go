package workerruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// Field defect 2026-10-08: an archive the completion check could not decode
// deferred the collection on every pass, for hours, and the attempt kept
// reading as running. The same decode error is now retried a few passes and
// then the declared outputs and commits are collected anyway, with the
// undecodable archive published beside them for the coordinator to fail the
// attempt as thread-archive-invalid.
func TestAnUndecodableArchiveStopsDeferringAndTheResultsAreStillCollected(t *testing.T) {
	repository, commit := makeGitRepository(t)
	pkg := testPackage()
	pkg.Environment.Repository = "https://example.com/steward.git"
	pkg.Environment.Ref = commit
	pkg.Outputs = []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}
	t.Cleanup(func() { forgetArchiveDecode(pkg) })
	catalog, err := backlog.NewProjectCatalog(
		[]backlog.ProjectDefinition{{Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
		[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
	)
	if err != nil {
		t.Fatal(err)
	}
	fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
	fake.set(fakeTurnThread{messages: []int{0}, turn: &fakeTurn{id: "turn-1", state: "completed", requested: 0}, session: &fakeSession{status: "ready", updated: 1}})
	// A field the completion check decodes, in a shape it cannot: the T3
	// client reads the thread detail without it, so only the archive judgement
	// fails, and it fails the same way on every export.
	fake.editDetail = func(detail map[string]any) { detail["hasPendingApprovals"] = "no" }
	root := t.TempDir()
	publisher := &recordingPublisher{}
	driver, err := NewLocalDriver(LocalDriver{
		Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
		Catalog: catalog,
		Workspace: backlog.WorkspacePreparer{
			Cache:     staticRepositoryCache{path: repository},
			Processes: successfulProcessRunner{},
		},
		Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification-1" }},
		Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt")},
		Publisher: publisher, T3: control, Now: func() time.Time { return runtimeTestNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := driver.Prepare(context.Background(), pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "answer.txt"), []byte("answer\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for pass := 1; pass < maxArchiveDecodePasses; pass++ {
		err := driver.Collect(context.Background(), pkg, workspace)
		var invalid *backlog.ThreadArchiveInvalidError
		if !errors.As(err, &invalid) || !strings.Contains(err.Error(), "could not be decoded") {
			t.Fatalf("pass %d: error = %v, want a deferral naming the undecodable archive", pass, err)
		}
		if len(publisher.results) != 0 {
			t.Fatalf("pass %d published before the bound was reached", pass)
		}
	}
	if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
		t.Fatalf("the pass at the bound still deferred: %v", err)
	}
	if len(publisher.results) != 1 {
		t.Fatalf("published=%d, want one result", len(publisher.results))
	}
	result := publisher.results[0]
	var output bool
	for _, artifact := range result.Finalized.Artifacts {
		output = output || (artifact.Kind == domain.ArtifactOutput && artifact.Name == "answer.txt")
	}
	if !output || !result.Finalized.Completion.ExplicitSuccess {
		t.Fatalf("the declared output was not collected: completion=%+v artifacts=%+v", result.Finalized.Completion, result.Finalized.Artifacts)
	}
	// The archive travels as it was read, and the coordinator reaches the
	// typed failure from those bytes.
	_, err = backlog.ResultCompletionFailure(result.ThreadArchive, pkg.Identity.ThreadID, result.FinalMessage)
	var invalid *backlog.ThreadArchiveInvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("the published archive decodes: %v", err)
	}
	if failure := backlog.ThreadArchiveInvalidFailure(invalid); !backlog.IsThreadArchiveInvalidFailure(failure) ||
		!strings.Contains(failure, "infrastructure failure "+backlog.ThreadArchiveInvalidReason) {
		t.Fatalf("failure %q is not the typed thread-archive-invalid failure", failure)
	}
	if err := driver.Cleanup(context.Background(), pkg, workspace); err != nil {
		t.Fatal(err)
	}
}

// Review finding (repair round 2): a resumed or woken request that no turn
// adopted is read from the archive before the completion judgement, to see
// whether T3 refused it, and an undecodable archive there deferred the
// collection on every pass. The same bound now applies to that read. Once it
// is reached, a session that failed after the request ends the request and
// the results are collected; without that evidence the request may still
// start a turn, so the collection keeps deferring and publishes nothing.
func TestAnUndecodableArchiveOfAnUnadoptedRequestIsBounded(t *testing.T) {
	for _, test := range []struct {
		name    string
		session fakeSession
		collect bool
	}{
		{name: "session failed after the request", session: fakeSession{status: "error", updated: 3}, collect: true},
		{name: "session failed before the request", session: fakeSession{status: "error", updated: 1}},
		{name: "session ready", session: fakeSession{status: "ready", updated: 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, commit := makeGitRepository(t)
			pkg := testPackage()
			pkg.Environment.Repository = "https://example.com/steward.git"
			pkg.Environment.Ref = commit
			pkg.Outputs = []domain.ArtifactDeclaration{{Name: "answer.txt", MediaType: "text/plain"}}
			t.Cleanup(func() { forgetArchiveDecode(pkg) })
			catalog, err := backlog.NewProjectCatalog(
				[]backlog.ProjectDefinition{{Name: "steward", Repository: "https://example.com/steward.git", DefaultRef: commit, T3ProjectTemplate: "development", SetupProfile: "go"}},
				[]backlog.SetupProfile{{Name: "go", Commands: []string{"true"}, Timeout: time.Minute}},
			)
			if err != nil {
				t.Fatal(err)
			}
			fake, control := newFakeTurnT3(t, pkg.Identity.ThreadID)
			// An earlier turn completed; the user message at 2 is a newer
			// request no turn has adopted.
			session := test.session
			fake.set(fakeTurnThread{messages: []int{0, 2}, turn: &fakeTurn{id: "turn-1", state: "completed", requested: 0}, session: &session})
			fake.editDetail = func(detail map[string]any) { detail["hasPendingApprovals"] = "no" }
			root := t.TempDir()
			publisher := &recordingPublisher{}
			driver, err := NewLocalDriver(LocalDriver{
				Config:  LocalDriverConfig{CatalogRevision: "catalog-1", ArtifactRoot: filepath.Join(root, "artifacts"), RunsRoot: filepath.Join(root, "runs")},
				Catalog: catalog,
				Workspace: backlog.WorkspacePreparer{
					Cache:     staticRepositoryCache{path: repository},
					Processes: successfulProcessRunner{},
				},
				Finalizer: backlog.AttemptFinalizer{Processes: successfulProcessRunner{}, Now: func() time.Time { return runtimeTestNow }, NewID: func(string) string { return "verification-1" }},
				Source:    mapArtifactSource{pkg.Prompt.ID: []byte("prompt")},
				Publisher: publisher, T3: control, Now: func() time.Time { return runtimeTestNow },
			})
			if err != nil {
				t.Fatal(err)
			}
			workspace, err := driver.Prepare(context.Background(), pkg)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workspace, "answer.txt"), []byte("answer\n"), 0o600); err != nil {
				t.Fatal(err)
			}

			for pass := 1; pass < maxArchiveDecodePasses; pass++ {
				err := driver.Collect(context.Background(), pkg, workspace)
				if !isThreadArchiveInvalid(err) || !strings.Contains(err.Error(), "could not be decoded") {
					t.Fatalf("pass %d: error = %v, want a deferral naming the undecodable archive", pass, err)
				}
				if len(publisher.results) != 0 {
					t.Fatalf("pass %d published before the bound was reached", pass)
				}
			}
			if !test.collect {
				for pass := maxArchiveDecodePasses; pass <= maxArchiveDecodePasses+2; pass++ {
					err := driver.Collect(context.Background(), pkg, workspace)
					if !isThreadArchiveInvalid(err) || !strings.Contains(err.Error(), "request is unresolved") {
						t.Fatalf("pass %d: error = %v, want a deferral naming the unresolved request", pass, err)
					}
				}
				if len(publisher.results) != 0 {
					t.Fatal("a request that may still start a turn was collected")
				}
				return
			}
			if err := driver.Collect(context.Background(), pkg, workspace); err != nil {
				t.Fatalf("the pass at the bound still deferred: %v", err)
			}
			if len(publisher.results) != 1 {
				t.Fatalf("published=%d, want one result", len(publisher.results))
			}
			result := publisher.results[0]
			var output bool
			for _, artifact := range result.Finalized.Artifacts {
				output = output || (artifact.Kind == domain.ArtifactOutput && artifact.Name == "answer.txt")
			}
			if !output || !result.Finalized.Completion.ExplicitSuccess {
				t.Fatalf("the declared output was not collected: completion=%+v artifacts=%+v", result.Finalized.Completion, result.Finalized.Artifacts)
			}
			_, err = backlog.ResultCompletionFailure(result.ThreadArchive, pkg.Identity.ThreadID, result.FinalMessage)
			if !isThreadArchiveInvalid(err) {
				t.Fatalf("the published archive decodes: %v", err)
			}
			if err := driver.Cleanup(context.Background(), pkg, workspace); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The bound counts the same decoding failure whichever reader met it: the
// current-request read and the completion judgement name themselves
// differently.
func TestArchiveDecodeBoundCountsTheSameCauseAcrossReaders(t *testing.T) {
	pkg := testPackage()
	t.Cleanup(func() { forgetArchiveDecode(pkg) })
	cause := errors.New("json: cannot unmarshal string into Go struct field .thread.hasPendingApprovals of type bool")
	archiveDecodeExhausted(pkg, &backlog.ThreadArchiveInvalidError{Err: cause})
	archiveDecodeExhausted(pkg, &backlog.ThreadArchiveInvalidError{Err: cause})
	if done, passes := archiveDecodeExhausted(pkg, &backlog.ThreadArchiveInvalidError{Context: "result import", Err: cause}); !done || passes != maxArchiveDecodePasses {
		t.Fatalf("the judgement's read restarted the count: done=%v passes=%d", done, passes)
	}
}

// The bound counts the same error on consecutive passes: a different error is
// a different read, and starts the count again.
func TestArchiveDecodeBoundCountsTheSameErrorOnly(t *testing.T) {
	pkg := testPackage()
	t.Cleanup(func() { forgetArchiveDecode(pkg) })
	first, second := errors.New("first"), errors.New("second")
	if done, passes := archiveDecodeExhausted(pkg, first); done || passes != 1 {
		t.Fatalf("first pass: done=%v passes=%d", done, passes)
	}
	if done, passes := archiveDecodeExhausted(pkg, second); done || passes != 1 {
		t.Fatalf("a different error kept the count: done=%v passes=%d", done, passes)
	}
	for pass := 2; pass <= maxArchiveDecodePasses; pass++ {
		done, passes := archiveDecodeExhausted(pkg, second)
		if passes != pass || done != (pass == maxArchiveDecodePasses) {
			t.Fatalf("pass %d: done=%v passes=%d", pass, done, passes)
		}
	}
}
