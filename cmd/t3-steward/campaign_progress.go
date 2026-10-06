package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func parseProgressArgs(args []string) (backlog.ProgressFilter, bool, error) {
	filter := backlog.ProgressFilter{}
	asJSON := false
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "--json":
			if seen[arg] {
				return filter, false, fmt.Errorf("duplicate %s", arg)
			}
			seen[arg] = true
			asJSON = true
		case "--owner", "--since":
			if seen[arg] {
				return filter, false, fmt.Errorf("duplicate %s", arg)
			}
			seen[arg] = true
			if i+1 >= len(args) || args[i+1] == "" || strings.HasPrefix(args[i+1], "-") {
				return filter, false, fmt.Errorf("%s needs a value", arg)
			}
			i++
			if arg == "--owner" {
				filter.Owner = args[i]
			} else {
				since, err := time.Parse(time.RFC3339, args[i])
				if err != nil {
					return filter, false, errors.New("--since needs an RFC3339 timestamp")
				}
				filter.Since = since.UTC()
			}
		default:
			if arg == "" || strings.HasPrefix(arg, "-") || strings.Contains(arg, "/") {
				return filter, false, fmt.Errorf("invalid campaign progress argument %q", arg)
			}
			filter.RunIDs = append(filter.RunIDs, arg)
		}
	}
	return filter, asJSON, nil
}

func (c campaignCLI) runProgress(ctx context.Context, args []string) error {
	filter, asJSON, err := parseProgressArgs(args)
	if err != nil {
		return err
	}
	if c.query == nil {
		return errors.New("coordinator admin transport is unavailable")
	}
	response, err := c.query(ctx, backlogadmin.Query{Kind: backlogadmin.QueryWorkflows, ProgressMirror: &filter})
	if err != nil {
		return err
	}
	if response.ProgressMirror == nil {
		return errors.New("progress mirror unavailable; upgrade the coordinator")
	}
	if asJSON {
		encoder := json.NewEncoder(c.stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(response.ProgressMirror)
	}
	return backlog.RenderProgress(c.stdout, *response.ProgressMirror)
}
