package backlog

import (
	"strings"
	"testing"
)

func TestGeneratedRoundCannotBeSecondLoopTemplate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, implement, review, want string
	}{
		{"both generated", "implement-round-2", "review-round-2", "implement task is missing"},
		{"generated implement", "implement-round-2", "review", "implement task is missing"},
		{"generated review", "implement", "review-round-2", "review must name a distinct existing task"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := strings.Replace(fixLoopManifest, "tasks:\n",
				"  zrepair:\n    implement: "+tc.implement+"\n    review: "+tc.review+"\n    max_rounds: 1\ntasks:\n", 1)
			manifest, err := ParseManifest([]byte(raw))
			if err == nil {
				t.Fatalf("accepted overlapping loops: implement=%+v review=%+v",
					manifest.Tasks["implement-round-2"].FixLoop, manifest.Tasks["review-round-2"].FixLoop)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %s", err, tc.want)
			}
		})
	}
}
