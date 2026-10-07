package main

import (
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/campaign"
	"github.com/iryzhkov/t3-steward/internal/compat"
)

// A dry run is a projection, not a submission receipt or a readiness promise.
type taskRunDryDocument struct {
	Selection        *policySelection `json:"selection,omitempty"`
	SchemaVersion    int              `json:"schemaVersion"`
	DryRun           bool             `json:"dryRun"`
	Project          string           `json:"project"`
	Ref              string           `json:"ref"`
	Fresh            bool             `json:"fresh"`
	Route            taskRunRoute     `json:"route"`
	IdempotencyKey   string           `json:"idempotencyKey"`
	NotifyThread     string           `json:"notifyThread"`
	PromptCharacters int              `json:"promptCharacters"`
}

func (c taskRunCLI) renderDryRun(parsed taskRunArgs, project, ref string, route taskRunRoute, key, thread string, prompts []taskRunPrompt, bundle campaign.Bundle) error {
	size := 0
	for _, prompt := range prompts {
		body := prompt.body
		if !strings.HasSuffix(body, "\n") {
			body += "\n"
		}
		task := bundle.Campaign.Manifest.Tasks[prompt.name]
		size += compat.TurnInputLength(backlog.FirstTurnPrompt(body, task.OutputDeclarations()))
	}
	doc := taskRunDryDocument{Selection: parsed.selection, SchemaVersion: 1, DryRun: true, Project: project, Ref: ref, Fresh: parsed.fresh, Route: printedRoute(route), IdempotencyKey: key, NotifyThread: thread, PromptCharacters: size}
	if !parsed.asJSON && parsed.selection != nil {
		renderPolicySelection(c.stdout, *parsed.selection)
	}
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, doc)
	}
	notify := thread
	if notify == "" {
		notify = "none (--no-notify)"
	}
	_, err := fmt.Fprintf(c.stdout, "dry run\nproject %s\nref %s\nfresh %t\nroute %s/%s (worker %s, pool %s)\nkey %s\nnotify %s\ncomposed prompt %d characters (limit %d) (total across %d task(s), limit per task, including completion contract)\n",
		project, ref, parsed.fresh, doc.Route.Instance, doc.Route.Model, doc.Route.Worker, doc.Route.QuotaPool, key, notify, size, compat.MaxTurnInputLength, len(prompts))
	return err
}
