package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
)

// projectsQueryRelease is the first release whose coordinator answers the
// projects query. It names when the coordinator gained the query, which is not
// what this binary's own version says, so it is written here rather than taken
// from the linker.
const projectsQueryRelease = "v0.11.0-rc.70"

// coordinatorQuery is one question to the coordinator. Every verb that asks
// for the catalog holds its transport differently, so the shared explanation
// takes the question rather than the client.
type coordinatorQuery func(context.Context, backlogadmin.Query) (backlogadmin.Response, error)

// The three verbs that ask for the catalog, and what each of them can offer a
// caller whose coordinator has no projects query. Every one of them is a
// command the rewritten skills tell an agent to run, so none of them may end
// at the coordinator's bare "invalid query", which reads like a client bug.
const (
	taskRunWithoutTheCatalog = "Pass --project NAME and --model INSTANCE/MODEL to start a task without " +
		"the catalog, or upgrade the coordinator"
	projectsWithoutTheCatalog = `Run "t3-steward backlog workers", which lists the projects and the ` +
		"instance/model routes every worker offers, or upgrade the coordinator"
	modelsWithoutTheCatalog = `Run "t3-steward models" without --project, which lists every route the ` +
		"fleet advertises rather than one project's, or upgrade the coordinator"
)

// explainRefusedProjectsQuery turns the refusal of a coordinator that has no
// projects query into an answer the caller can act on. The coordinator answers
// it as "invalid query", which reads like a client bug; during a mixed-release
// window it is nothing of the kind, and the way forward is either the release
// that has the query or whatever this verb can do without it, which is what
// instead says. Any other failure is returned unchanged.
func explainRefusedProjectsQuery(ctx context.Context, ask coordinatorQuery, err error, instead string) error {
	if !refusedAsAnUnknownQuery(err, backlogadmin.QueryProjects) {
		return err
	}
	release := coordinatorRelease(ctx, ask)
	if release == "" {
		release = "an unreported release"
	}
	return fmt.Errorf("this coordinator runs %s and has no %q query, which needs %s or newer: %w\n%s",
		release, backlogadmin.QueryProjects, projectsQueryRelease, err, instead)
}

// refusedAsAnUnknownQuery reports the coordinator's own refusal of a query
// kind it does not have. The refusal crosses the transport as its text, not as
// a sentinel, so the text is what there is to read.
func refusedAsAnUnknownQuery(err error, kind backlogadmin.QueryKind) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.Contains(message, "invalid query") && strings.Contains(message, strconv.Quote(string(kind)))
}

// coordinatorRelease asks what release the coordinator runs, for a message
// about version skew. Every release answers the status query, and an answer
// that does not arrive only costs the message its most useful clause.
func coordinatorRelease(ctx context.Context, ask coordinatorQuery) string {
	if ask == nil {
		return ""
	}
	response, err := ask(ctx, backlogadmin.Query{Kind: backlogadmin.QueryStatus})
	if err != nil || response.Status == nil {
		return ""
	}
	return response.Status.Runtime.Release
}
