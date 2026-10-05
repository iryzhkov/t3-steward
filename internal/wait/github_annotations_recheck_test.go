package wait

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestGitHubAnnotationBoundedSingleRequestRecheckRefusals(t *testing.T) {
	for _, mode := range []string{"page-cap", "malformed", "access"} {
		t.Run(mode, func(t *testing.T) {
			a := annotationPRFixture(t)
			gh := func(ctx context.Context, dir string, args []string) (string, error) {
				raw, err := a.dispatch(ctx, dir, args)
				if len(args) > 5 && args[5] == "graphql" {
					switch mode {
					case "page-cap":
						raw = strings.ReplaceAll(raw, `"hasNextPage":false`, `"hasNextPage":true`)
					case "malformed":
						raw = "{}"
					case "access":
						return "", errors.New("HTTP 403 secret")
					}
				}
				return raw, err
			}
			runner, store, _, _ := gitHubRunner(t, gh)
			w := store.waits["w1"]
			w.GitHub = &GitHubTarget{Kind: "pr", ID: "4", State: "checks-passed"}
			store.waits["w1"] = w
			runner.Tick(context.Background(), nil, nil)
			w = store.waits["w1"]
			if w.Outcome != "met" || !strings.Contains(w.LastOutput, "unavailable") || strings.Contains(w.LastOutput, "warning=0") || strings.Contains(w.LastOutput, "No annotations.") || strings.Contains(w.LastOutput, "secret") {
				t.Fatal(w.LastOutput)
			}
			rechecks := 0
			for _, args := range a.calls {
				if args[0] == "pr" {
					continue
				}
				if args[0] != "api" {
					t.Fatal("hidden CLI lookup", args)
				}
				if args[5] == "graphql" {
					rechecks++
				}
			}
			if rechecks != 1 || len(a.calls)-1 > 20 {
				t.Fatal("recheck paginated or repeated", a.calls)
			}
		})
	}
}
