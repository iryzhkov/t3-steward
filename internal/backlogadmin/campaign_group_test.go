package backlogadmin

import (
	"context"
	"testing"
)

func TestCampaignGroupRerunOverrideAndList(t *testing.T) {
	ctx := context.Background()
	s, _, _ := rerunFixture(t)
	r := rerunRequest("group-rerun", "implement")
	r.Group = "wave-2"
	result, err := s.AmendGraph(ctx, Principal{ID: "operator"}, r)
	if err != nil || result.Run.Group != "wave-2" {
		t.Fatalf("override: %+v %v", result, err)
	}
	replay, err := s.AmendGraph(ctx, Principal{ID: "operator"}, r)
	if err != nil || !replay.Replay || replay.Run.Group != "wave-2" {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	r.Group = "wave-3"
	if _, err = s.AmendGraph(ctx, Principal{ID: "operator"}, r); err == nil {
		t.Fatal("changed rerun receipt accepted")
	}
	response, err := s.Query(ctx, Query{Version: CurrentReadVersion, Kind: QueryWorkflows, Principal: Principal{ID: "operator"}, Filter: Filter{Group: "wave-2"}})
	if err != nil || len(response.Workflows) != 1 || response.Workflows[0].Run.ID != result.Run.ID {
		t.Fatalf("list: %+v %v", response, err)
	}
}
