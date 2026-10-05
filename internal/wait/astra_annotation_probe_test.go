package wait

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iryzhkov/t3-steward/internal/domain"
)

func TestAstraAnnotationSecondPageEvidence(t *testing.T) {
	for _, mode := range []string{"complete", "denied", "short"} {
		t.Run(mode, func(t *testing.T) {
			a := annotationFixture(t)
			a.responses[annotationMetadata] = strings.ReplaceAll(a.responses[annotationMetadata], "\"annotations_count\":1", "\"annotations_count\":51")
			records := []map[string]any{}
			for i := 0; i < 50; i++ {
				records = append(records, map[string]any{"annotation_level": "warning", "path": "x.go", "start_line": i + 1, "end_line": i + 1, "message": fmt.Sprintf("warning %d", i)})
			}
			b, _ := json.Marshal(records)
			a.responses[annotationPage] = string(b)
			page2 := "repos/o/r/check-runs/7/annotations?per_page=50&page=2"
			a.responses[page2] = `[{"annotation_level":"notice","path":"y.go","start_line":1,"end_line":1,"message":"last notice"}]`
			if mode == "denied" {
				a.errors[page2] = errors.New("HTTP 403 PRIVATE")
			}
			if mode == "short" {
				a.responses[page2] = "[]"
			}
			w := annotationEvaluate(t, a, GitHubTarget{Kind: "run", ID: "123", State: "completed"})
			if w.Outcome != "met" || !strings.Contains(w.LastOutput, "warning=50") {
				t.Fatal(w.LastOutput)
			}
			if mode == "complete" {
				if !strings.Contains(w.LastOutput, "Annotations complete") || !strings.Contains(w.LastOutput, "notice=1") || !strings.Contains(w.LastOutput, "records=51") {
					t.Fatal(w.LastOutput)
				}
			} else if !strings.Contains(w.LastOutput, "Annotations partial") || !strings.Contains(w.LastOutput, "notice=unknown") || !strings.Contains(w.LastOutput, "failure=unknown") || strings.Contains(w.LastOutput, "PRIVATE") {
				t.Fatal(w.LastOutput)
			}
			n := 0
			for _, args := range a.calls {
				if len(args) > 5 && args[5] == page2 {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("page 2 fetched %d times", n)
			}
			t.Logf("mode=%s gate=%s calls=%d; exact observed warning=50 and honest completeness", mode, w.Outcome, len(a.calls)-1)
		})
	}
}

func TestAstraAnnotationMixedContextAndNotice(t *testing.T) {
	a := annotationPRFixture(t)
	a.status = annotationMutate(t, a.status, func(m map[string]any) {
		m["statusCheckRollup"] = append(m["statusCheckRollup"].([]any), map[string]any{"__typename": "StatusContext", "context": "legacy", "state": "SUCCESS"})
	})
	a.responses[annotationPage] = strings.ReplaceAll(a.responses[annotationPage], "warning", "notice")
	w := annotationEvaluate(t, a, GitHubTarget{Kind: "pr", ID: "4", State: "checks-passed"})
	if w.Outcome != "met" || !strings.Contains(w.LastOutput, "Annotations partial") || !strings.Contains(w.LastOutput, "notice=1") || !strings.Contains(w.LastOutput, "warning=unknown") || !strings.Contains(w.LastOutput, "status-context-no-annotations") {
		t.Fatal(w.LastOutput)
	}
	t.Log("mixed PR keeps notice and marks inaccessible status-context counts unknown")
}

type astraAnnotationLostControl struct {
	*memControl
	lost bool
}

func (c *astraAnnotationLostControl) ResumeThread(ctx context.Context, th domain.Thread, text string) error {
	if c.lost {
		return errors.New("lost delivery")
	}
	return c.memControl.ResumeThread(ctx, th, text)
}

func TestAstraAnnotationInteractiveJSONReopen(t *testing.T) {
	a := annotationFixture(t)
	runner, store, control, now := gitHubRunner(t, a.dispatch)
	runner.control = &astraAnnotationLostControl{memControl: control, lost: true}
	runner.Tick(context.Background(), nil, nil)
	original := store.waits["w1"]
	if original.Status != StatusMet || !strings.Contains(original.LastOutput, "avoid stale value") {
		t.Fatal(original)
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var reopened Wait
	if err = json.Unmarshal(raw, &reopened); err != nil {
		t.Fatal(err)
	}
	store.waits["w1"] = reopened
	fresh := New(store, control, nil)
	fresh.SetClock(func() time.Time { return now.Add(time.Minute) })
	fresh.GitHub = func(context.Context, string, []string) (string, error) {
		t.Fatal("settled reopen fetched GitHub")
		return "", nil
	}
	fresh.Tick(context.Background(), nil, nil)
	if store.waits["w1"].Status != StatusWoken || store.waits["w1"].LastOutput != original.LastOutput || len(control.texts) != 1 || !strings.Contains(control.texts[0], "avoid stale value") {
		t.Fatal(store.waits["w1"], control.texts)
	}
	t.Log("failed interactive delivery persisted; new Runner + JSON reopen delivers exact saved diagnostics without fetch")
}

func TestAstraAnnotationTaskJSONReopen(t *testing.T) {
	a := annotationFixture(t)
	runner, store, control, now := taskWaitRunner(t, domain.ProgressWaitingExternal)
	w := store.waits["w1"]
	w.Kind = domain.WaitKindGitHub
	w.Dir = "/registered/in"
	w.GitHub = &GitHubTarget{Kind: "run", ID: "123", State: "completed"}
	store.waits["w1"] = w
	runner.GitHub = a.dispatch
	control.sendErr = errors.New("lost delivery")
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	original := store.taskWaits["tw-1"]
	if original.Result == nil || !strings.Contains(original.Result.Output, "avoid stale value") {
		t.Fatal(original)
	}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var reopened domain.TaskWait
	if err = json.Unmarshal(raw, &reopened); err != nil {
		t.Fatal(err)
	}
	store.taskWaits["tw-1"] = reopened
	raw, err = json.Marshal(store.waits["w1"])
	if err != nil {
		t.Fatal(err)
	}
	var check Wait
	if err = json.Unmarshal(raw, &check); err != nil {
		t.Fatal(err)
	}
	store.waits["w1"] = check
	runner.GitHub = func(context.Context, string, []string) (string, error) {
		t.Fatal("task JSON reopen fetched GitHub")
		return "", nil
	}
	control.sendErr = nil
	control.observed = map[string]bool{"task-wake:tw-1": true}
	*now = now.Add(time.Minute)
	runner.Tick(context.Background(), nil, healthyBuckets())
	if store.taskWaits["tw-1"].Result.Output != original.Result.Output || len(control.sends) != 1 {
		t.Fatal("task evidence/delivery changed")
	}
	t.Log("task wait and check JSON reopen preserve exact diagnostics and do not fetch or duplicate delivery")
}
