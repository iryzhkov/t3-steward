package main

import (
	"fmt"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// triageProviderErrors lists every attempt a worker still holds whose provider
// session ended its turn with a provider-side error, from the journal excerpts
// in the workers' snapshots. Before in-session resumes existed such an attempt
// was reported active and running and nothing listed it; now the worker resumes
// it, and the item says when, or what the resume waits for. An attempt that
// recovered, or failed once its resumes were spent, is not held any more and is
// not listed: its task shows the event, and a failure is the task's own item.
func triageProviderErrors(report *triageReport, workers []backlogadmin.Worker) {
	for _, worker := range workers {
		for _, assignment := range worker.Snapshot.Assignments {
			if assignment.Journal == nil || assignment.Journal.ProviderError == nil {
				continue
			}
			providerError := *assignment.Journal.ProviderError
			if !providerError.Active() {
				continue
			}
			since := providerError.ObservedAt
			severity := "warn"
			if providerError.State == domain.ProviderResumeQuotaWait {
				// Nothing resumes it until the quota reopens.
				severity = "action"
			}
			subject := assignment.AssignmentID
			summary := fmt.Sprintf("on worker %s, thread %s: the provider session ended its turn with an error, %s",
				worker.Snapshot.WorkerID, assignment.ThreadID, providerError.Summary())
			report.add(triageItem{
				Kind: "provider-error", Severity: severity, Subject: subject, Since: &since,
				Summary: summary,
				Commands: []triageCommand{
					{Host: worker.Snapshot.WorkerID, Run: "journalctl --user -u t3-steward-worker --grep " + shellQuote(assignment.AssignmentID) + " -n 50"},
					{Run: "t3-steward models"},
				},
			})
		}
	}
}
