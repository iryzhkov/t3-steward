package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

func (c backlogAdminCLI) runGraphDOT(ctx context.Context, args []string) error {
	clean := []string{}
	seen := false
	for _, arg := range args {
		if arg == "--dot" {
			if seen {
				return errors.New("--dot may only be specified once")
			}
			seen = true
			continue
		}
		clean = append(clean, arg)
	}
	query, asJSON, err := parseBacklogAdminQuery(clean)
	if err != nil {
		return err
	}
	if asJSON || query.Kind != backlogadmin.QueryGraph {
		return errors.New("--dot requires graph and cannot be combined with --json")
	}
	query.Version, query.Principal = backlogadmin.Version, c.principal
	response, err := c.service.Query(ctx, query)
	if err != nil {
		return err
	}
	if response.Graph == nil {
		return errors.New("coordinator returned no graph")
	}
	return renderGraphDOT(c.stdout, *response.Graph)
}

func renderGraphDOT(out io.Writer, graph backlogadmin.Graph) error {
	if _, err := fmt.Fprintf(out, "digraph %s {\n  label=%s;\n", strconv.Quote(graph.WorkflowRunID),
		strconv.Quote(fmt.Sprintf("%s · graph revision %d", graph.WorkflowRunID, graph.GraphRevision))); err != nil {
		return err
	}
	for _, node := range graph.Nodes {
		shape := "box"
		if node.Sink != nil {
			shape = "doublecircle"
		}
		if _, err := fmt.Fprintf(out, "  %s [label=%s, shape=%s];\n", strconv.Quote(node.TaskID),
			strconv.Quote(node.Name+"\n"+string(node.Progress)), shape); err != nil {
			return err
		}
	}
	for _, edge := range graph.Edges {
		if _, err := fmt.Fprintf(out, "  %s -> %s;\n", strconv.Quote(edge.FromTaskID), strconv.Quote(edge.ToTaskID)); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(out, "}")
	return err
}
