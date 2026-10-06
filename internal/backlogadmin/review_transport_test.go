package backlogadmin

import (
	"context"
	"github.com/iryzhkov/t3-steward/internal/review"
	"github.com/iryzhkov/t3-steward/internal/store/sqlite/sqlitetest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type reviewTransportService struct {
	*localTransportService
	reader *Service
}

func (s reviewTransportService) Query(ctx context.Context, q Query) (Response, error) {
	return s.reader.Query(ctx, q)
}

func TestLargeReviewRoundUsesBoundedDocumentQueries(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitetest.OpenMigrated(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r, err := store.CreateReviewRound(ctx, review.Round{ID: "round-large", InputManifestDigest: strings.Repeat("a", 64), Reviewers: []review.Reviewer{{ID: "r", Role: "review", Route: "codex/sol", Required: true}}})
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema":"review-verdict/v1","verdict":"accept","findings":[],"inputManifestDigest":"` + r.InputManifestDigest + `","reviewerRoute":"codex/sol"}`)
	// Each byte escapes to six JSON bytes in an inline string: a legal 1 MiB
	// markdown file cannot be carried in a 4 MiB admin response.
	markdown := strings.Repeat("\x01", review.MaxDocumentBytes)
	if _, err := store.RecordReviewResult(ctx, r.ID, "r", r.Revision, review.Result{State: "succeeded", ReviewMD: markdown, VerdictJSON: raw}); err != nil {
		t.Fatal(err)
	}
	service, err := New(store, &allowAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	// Allow race instrumentation and CPU contention while checking the full-size
	// payload and byte caps; this test does not assert request latency.
	client, cancel, done := startLocalTransportWithTimeout(t, uint32(os.Getuid()), reviewTransportService{localTransportService: &localTransportService{}, reader: service}, 15*time.Second)
	defer func() { cancel(); <-done }()
	client.MaxResponseBytes = 4 << 20
	response, err := client.Query(ctx, Query{Version: Version, Kind: QueryReviewRound, RoundID: r.ID})
	if err != nil {
		t.Fatal(err)
	}
	if response.ReviewRound == nil || response.ReviewRound.Reviewers[0].ReviewMD != "" || len(response.ReviewRound.Reviewers[0].VerdictJSON) != 0 {
		t.Fatal("round response contains bulk documents")
	}
	doc, err := client.Query(ctx, Query{Version: Version, Kind: QueryReviewDocument, RoundID: r.ID, ReviewerID: "r", ReviewDocument: "review.md"})
	if err != nil || doc.ReviewDocument == nil || string(doc.ReviewDocument.Content) != markdown {
		t.Fatalf("document query failed: %v", err)
	}
	verdict, err := client.Query(ctx, Query{Version: Version, Kind: QueryReviewDocument, RoundID: r.ID, ReviewerID: "r", ReviewDocument: "verdict.json"})
	if err != nil || verdict.ReviewDocument == nil {
		t.Fatalf("verdict query failed: %v", err)
	}
	if _, err := review.ValidateVerdict(verdict.ReviewDocument.Content, r.InputManifestDigest, "codex/sol"); err != nil {
		t.Fatal(err)
	}
}
