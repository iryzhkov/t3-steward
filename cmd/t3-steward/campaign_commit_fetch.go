package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func slicesContainInto(args []string) bool {
	for _, arg := range args {
		if arg == "--into" {
			return true
		}
	}
	return false
}

// Fetch is an export followed by a local bundle verification and import.
// It uses exactly the same authenticated producer/provenance path as --bundle.
func (c campaignCLI) fetchCommitExport(ctx context.Context, args []string) error {
	temp, err := os.MkdirTemp("", "t3-commit-fetch-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	bundle := filepath.Join(temp, "commit.bundle")
	var repository string
	translated := append([]string(nil), args...)
	for i := 0; i < len(translated); i++ {
		if translated[i] != "--into" {
			continue
		}
		if repository != "" {
			return fmt.Errorf("duplicate --into")
		}
		if i+1 >= len(translated) || translated[i+1] == "" {
			return fmt.Errorf("--into requires a value")
		}
		repository = translated[i+1]
		translated[i], translated[i+1] = "--bundle", bundle
		i++
	}
	request, _, err := parseCampaignCommitExportArgs(translated)
	if err != nil {
		return err
	}
	runGit := func(arguments ...string) error {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", repository}, arguments...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("commit fetch: git %s: %w: %s", arguments[0], err, out)
		}
		return nil
	}
	if err := runGit("rev-parse", "--git-dir"); err != nil {
		return err
	}
	// Only create a new local branch; never update an existing one.
	check := exec.CommandContext(ctx, "git", "-C", repository, "show-ref", "--verify", "--quiet", "refs/heads/"+request.Branch)
	if err := check.Run(); err == nil {
		return fmt.Errorf("commit fetch: local branch %s already exists", request.Branch)
	} else if status, ok := err.(*exec.ExitError); !ok || status.ExitCode() != 1 {
		return fmt.Errorf("commit fetch: inspect destination: %w", err)
	}
	var receipt bytes.Buffer
	exporter := c
	exporter.stdout = &receipt
	if err := exporter.runCommit(ctx, append([]string{"export"}, translated...)); err != nil {
		return err
	}
	if err := runGit("bundle", "verify", bundle); err != nil {
		return err
	}
	listing, err := exec.CommandContext(ctx, "git", "-C", repository, "bundle", "list-heads", bundle).Output()
	if err != nil {
		return fmt.Errorf("commit fetch: list bundle heads: %w", err)
	}
	fields := strings.Fields(string(listing))
	if len(fields) != 2 || fields[1] != "refs/heads/"+request.Branch {
		return fmt.Errorf("commit fetch: unexpected bundle heads")
	}
	head := fields[0]
	if _, err := hex.DecodeString(head); err != nil || (len(head) != 40 && len(head) != 64) || !strings.HasPrefix(receipt.String(), "commit "+head+"\n") {
		return fmt.Errorf("commit fetch: bundle head disagrees with provenance")
	}
	if err := runGit("fetch", "--no-tags", "--no-write-fetch-head", "--", bundle, "refs/heads/"+request.Branch); err != nil {
		return err
	}
	// The empty old value is an atomic creation guard, including concurrent
	// imports that create the branch after the early existence check.
	if err := runGit("update-ref", "refs/heads/"+request.Branch, head, ""); err != nil {
		return err
	}
	for _, line := range strings.Split(receipt.String(), "\n") {
		if line != "" && !strings.HasPrefix(line, "bundle ") {
			if _, err := fmt.Fprintln(c.stdout, line); err != nil {
				return err
			}
		}
	}
	_, err = fmt.Fprintf(c.stdout, "fetched refs/heads/%s into %s\n", request.Branch, repository)
	return err
}
