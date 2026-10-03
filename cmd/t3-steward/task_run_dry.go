package main

import (
	"fmt"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/domain"
	"github.com/iryzhkov/t3-steward/internal/workerruntime"
)

// A dry run is a projection, not a submission receipt or a readiness promise.
type taskRunDryDocument struct {
	SchemaVersion  int          `json:"schemaVersion"`
	DryRun         bool         `json:"dryRun"`
	Project        string       `json:"project"`
	Ref            string       `json:"ref"`
	Fresh          bool         `json:"fresh"`
	Route          taskRunRoute `json:"route"`
	IdempotencyKey string       `json:"idempotencyKey"`
	NotifyThread   string       `json:"notifyThread"`
	PromptBytes    int          `json:"promptBytes"`
}

func (c taskRunCLI) renderDryRun(parsed taskRunArgs, project, ref string, route taskRunRoute, key, thread string, prompts []taskRunPrompt) error {
	var outputs []domain.ArtifactDeclaration
	for _, name := range parsed.outputs {
		outputs = append(outputs, domain.ArtifactDeclaration{Name: name})
	}
	supplement := workerruntime.TaskCompletionSupplement(outputs)
	size := 0
	for _, prompt := range prompts {
		size += len(prompt.body) + len(supplement)
		if !strings.HasSuffix(prompt.body, "\n") {
			size++
		}
	}
	doc := taskRunDryDocument{SchemaVersion: 1, DryRun: true, Project: project, Ref: ref, Fresh: parsed.fresh, Route: printedRoute(route), IdempotencyKey: key, NotifyThread: thread, PromptBytes: size}
	if parsed.asJSON {
		return encodeCampaignJSON(c.stdout, doc)
	}
	notify := thread
	if notify == "" {
		notify = "none (--no-notify)"
	}
	_, err := fmt.Fprintf(c.stdout, "dry run\nproject %s\nref %s\nfresh %t\nroute %s/%s (worker %s, pool %s)\nkey %s\nnotify %s\ncomposed prompt %d bytes (total across %d task(s), including completion contract)\n",
		project, ref, parsed.fresh, doc.Route.Instance, doc.Route.Model, doc.Route.Worker, doc.Route.QuotaPool, key, notify, size, len(prompts))
	return err
}
