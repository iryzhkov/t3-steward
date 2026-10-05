package sqlite

import (
	"context"
	"reflect"
	"testing"
)

func TestRepair1AstraReturnedProfileIsolationReopened(t *testing.T) {
	s, f, cp, p, _ := executionChildFixture(t)
	ctx := context.Background()
	first, err := s.MaterializeReviewChild(ctx, f, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	original, err := f.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	before := executionSQLSnapshot(t, s)
	first.Authority.Requirements.Members[0].Execution.QuotaPoolID = "returned-only"
	first.Authority.Requirements.Members[0].Execution.Resources.MemoryMB++
	first.Graph.Tasks[0].Routes[0].Options["effort"] = "high"
	first.Graph.Tasks[0].Routes[0].QuotaPoolID = "returned-only"
	first.Graph.Tasks[0].MaxTurns++
	first.Graph.Tasks[0].ResourceDemand.MemoryMB++
	if before != executionSQLSnapshot(t, s) {
		t.Fatal("return-value mutation changed durable records")
	}
	path := s.path
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay, err := reopened.MaterializeReviewChild(ctx, original, cp, p)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := p.Build(original, cp, replay.Graph.Run.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replay.Graph, expected) {
		t.Fatal("reopened graph changed after returned profile mutation")
	}
	if before != executionSQLSnapshot(t, reopened) {
		t.Fatal("read-only replay changed logical SQL/native audit")
	}
	t.Log("mutated returned profile/options/pool/turns/resources detached; reopened original Build replay exact; all logical tables unchanged")
}
