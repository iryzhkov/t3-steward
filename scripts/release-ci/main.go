// release-ci gates publication using either recorded gh api JSON or a bounded
// live wait. The asset mode verifies the downloaded release without GitHub.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

func api(ctx context.Context, endpoint string, paginate bool) ([]byte, error) {
	args := []string{"api", endpoint}
	if paginate {
		args = append(args, "--paginate", "--slurp")
	}
	cmd := exec.CommandContext(ctx, "gh", args...)
	cmd.Stderr = os.Stderr
	return cmd.Output()
}
func waitCI(ctx context.Context, repo, sha string) error {
	last := run{}
	for {
		runs, err := api(ctx, fmt.Sprintf("repos/%s/actions/workflows/ci.yml/runs?head_sha=%s&branch=main&event=push&per_page=100", repo, sha), false)
		if err != nil {
			return fmt.Errorf("SHA %s run %d: query CI: %w", sha, last.ID, err)
		}
		r, err := selectRun(runs, sha)
		if err != nil {
			return fmt.Errorf("SHA %s run %d: decode CI: %w", sha, last.ID, err)
		}
		last = r
		var jobs []byte
		if r.ID != 0 && r.Status == "completed" && r.Conclusion == "success" {
			jobs, err = api(ctx, fmt.Sprintf("repos/%s/actions/runs/%d/attempts/%d/jobs?per_page=100", repo, r.ID, r.Attempt), true)
			if err != nil {
				return fmt.Errorf("SHA %s run %d: query jobs: %w", sha, r.ID, err)
			}
		}
		ok, err := decision(r, jobs, false)
		if err != nil {
			return fmt.Errorf("SHA %s: %w", sha, err)
		}
		if ok {
			fmt.Printf("SHA %s: run %d attempt %d and every job successful\n", sha, r.ID, r.Attempt)
			return nil
		}
		fmt.Printf("SHA %s: waiting for run %d attempt %d\n", sha, r.ID, r.Attempt)
		timer := time.NewTimer(15 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("SHA %s run %d attempt %d: timeout: %w", sha, r.ID, r.Attempt, ctx.Err())
		case <-timer.C:
		}
	}
}
func execute() error {
	sha := flag.String("sha", "", "full commit SHA")
	repo := flag.String("repo", "", "owner/repository for live wait")
	runsPath := flag.String("runs", "", "recorded workflow-runs JSON")
	jobsPath := flag.String("jobs", "", "recorded paginated jobs JSON (gh api --paginate --slurp)")
	timedOut := flag.Bool("timed-out", false, "offline deadline has expired")
	timeout := flag.Duration("timeout", 30*time.Minute, "live wait bound")
	assets := flag.String("assets-dir", "", "verify all downloaded release assets")
	flag.Parse()
	if *assets != "" {
		return verifyAssets(*assets)
	}
	if !regexp.MustCompile("^[0-9a-fA-F]{40}$").MatchString(*sha) {
		return fmt.Errorf("full 40-character commit SHA required")
	}
	*sha = strings.ToLower(*sha)
	if *runsPath != "" {
		data, err := os.ReadFile(*runsPath)
		if err != nil {
			return err
		}
		r, err := selectRun(data, *sha)
		if err != nil {
			return fmt.Errorf("SHA %s: %w", *sha, err)
		}
		var jobs []byte
		if *jobsPath != "" {
			jobs, err = os.ReadFile(*jobsPath)
			if err != nil {
				return err
			}
		}
		ok, err := decision(r, jobs, *timedOut)
		if err != nil {
			return fmt.Errorf("SHA %s: %w", *sha, err)
		}
		if !ok {
			return fmt.Errorf("SHA %s run %d: pending", *sha, r.ID)
		}
		return nil
	}
	if !regexp.MustCompile("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$").MatchString(*repo) || *timeout <= 0 {
		return fmt.Errorf("valid repo and positive timeout required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return waitCI(ctx, *repo, *sha)
}
func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
