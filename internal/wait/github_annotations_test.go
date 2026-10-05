package wait

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

const annotationHead = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const annotationRun = `{"databaseId":123,"attempt":2,"headSha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","status":"completed","conclusion":"success","url":"https://github.com/o/r/actions/runs/123"}`

type annotationAPI struct {
	t         *testing.T
	status    string
	calls     [][]string
	responses map[string]string
	errors    map[string]error
}

func annotationFixture(t *testing.T) *annotationAPI {
	return &annotationAPI{t: t, status: annotationRun, errors: map[string]error{}, responses: map[string]string{
		"repos/o/r/actions/runs/123/attempts/2/jobs?per_page=50&page=1": `{"total_count":1,"jobs":[{"id":9,"run_id":123,"run_attempt":2,"head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"lint","check_run_url":"https://api.github.com/repos/o/r/check-runs/7"}]}`,
		"repos/o/r/check-runs/7":                                `{"id":7,"head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"lint","status":"completed","conclusion":"success","details_url":"https://github.com/o/r/actions/runs/123/job/9","output":{"annotations_count":1}}`,
		"repos/o/r/check-runs/7/annotations?per_page=50&page=1": `[{"annotation_level":"warning","path":"main.go","start_line":2,"end_line":4,"title":"lint warning","message":"avoid stale value"}]`,
	}}
}

// dispatch is deliberately strict: there are no URL-follow, pagination or shell arguments.
func (a *annotationAPI) dispatch(ctx context.Context, dir string, args []string) (string, error) {
	if dir != "/registered/in" {
		a.t.Fatalf("wrong directory %q", dir)
	}
	a.calls = append(a.calls, append([]string(nil), args...))
	if args[0] == "run" || args[0] == "pr" {
		return a.status, nil
	}
	if len(args) < 6 {
		a.t.Fatalf("unsafe args %v", args)
	}
	if err := a.errors[args[5]]; err != nil {
		return "", err
	}
	if v, ok := a.responses[args[5]]; ok {
		if len(args) != 6 || !reflect.DeepEqual(args[:5], []string{"api", "--method", "GET", "--hostname", "github.com"}) {
			a.t.Fatalf("unsafe API args %v", args)
		}
		return v, nil
	}
	var s annotationSnapshot
	if err := json.Unmarshal([]byte(a.status), &s); err != nil {
		a.t.Fatal(err)
	}
	if args[5] == "repos/o/r/actions/runs/123?exclude_pull_requests=true" {
		if !reflect.DeepEqual(args[:5], []string{"api", "--method", "GET", "--hostname", "github.com"}) || len(args) != 6 {
			a.t.Fatal(args)
		}
		b, _ := json.Marshal(map[string]any{"id": s.ID, "run_attempt": s.Attempt, "head_sha": s.Head, "status": s.Status, "conclusion": s.Conclusion, "html_url": s.URL})
		return string(b), nil
	}
	if args[5] == "graphql" {
		expected := []string{"api", "--method", "POST", "--hostname", "github.com", "graphql", "-f", "query=" + annotationRecheckQuery, "-F", "owner=o", "-F", "name=r", "-F", "number=4"}
		if !reflect.DeepEqual(args, expected) || !strings.Contains(args[7], "contexts(first:100)") || strings.Contains(args[7], "after:") {
			a.t.Fatalf("unbounded/unpinned query %v", args)
		}
		contexts := map[string]any{"nodes": s.Checks, "pageInfo": map[string]any{"hasNextPage": false}}
		commit := map[string]any{"commit": map[string]any{"statusCheckRollup": map[string]any{"contexts": contexts}}}
		pr := map[string]any{"headRefOid": s.PRHead, "state": s.State, "commits": map[string]any{"nodes": []any{commit}}}
		b, _ := json.Marshal(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": pr}}})
		return string(b), nil
	}
	a.t.Fatalf("unexpected endpoint %v", args)
	return "", nil
}
func TestGitHubAnnotationSuccessfulWarningPersistsAndForwards(t *testing.T) {
	a := annotationFixture(t)
	runner, store, control, _ := gitHubRunner(t, a.dispatch)
	runner.Tick(context.Background(), nil, nil)
	w := store.waits["w1"]
	if w.Outcome != "met" || w.LastExit != 0 {
		t.Fatalf("changed outcome: %+v", w)
	}
	if !strings.Contains(w.LastOutput, "warning=1") || !strings.Contains(w.LastOutput, "avoid stale value") {
		t.Fatalf("successful warning lost: %q", w.LastOutput)
	}
	if len(control.texts) != 1 || !strings.Contains(control.texts[0], "avoid stale value") {
		t.Fatalf("interactive wake lost warning %v", control.texts)
	}
	fields, ok := ParseWakeTrailer(control.texts[0])
	if !ok || fields["conclusion"] != "success" || fields["outcome"] != "met" {
		t.Fatal(fields)
	}
	// Persist/reopen both wait and task result with the production JSON shapes.
	b, err := json.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	var reopened Wait
	if err = json.Unmarshal(b, &reopened); err != nil {
		t.Fatal(err)
	}
	result := taskWaitResult(reopened, time.Now())
	b, err = json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var restored domain.TaskWaitResult
	if err = json.Unmarshal(b, &restored); err != nil {
		t.Fatal(err)
	}
	prompt := (domain.TaskWaitWakeContext{Waits: []domain.TaskWait{{ID: "tw", Result: &restored}}}).Prompt()
	if !strings.Contains(prompt, "avoid stale value") || restored.Output != w.LastOutput || restored.Outcome != domain.TaskWaitMet {
		t.Fatal(prompt)
	}
	calls := len(a.calls)
	runner.Tick(context.Background(), nil, nil)
	if len(a.calls) != calls {
		t.Fatal("settled replay fetched again")
	}
	if len(w.LastOutput) > 4000 || !utf8.ValidString(w.LastOutput) {
		t.Fatal("invalid bounded output")
	}
}
