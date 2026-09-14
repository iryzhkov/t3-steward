package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/iryzhkov/t3-steward/internal/backlog"
	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/campaign"
)

// coordinatorPermanentValidator repeats the permanent part of the readiness
// check during acceptance.
//
// It asks the same viability query the client asks, through the same composer,
// so a campaign the client was told is impossible is refused here for the same
// stated reasons. Only permanent findings refuse: a temporary obstruction is
// precisely what asynchronous execution exists for, and refusing on one would
// turn a queue into a busy wait.
type coordinatorPermanentValidator struct {
	admin *backlogadmin.Service
}

// ValidatePermanent refuses a campaign that can never run, and refuses a
// submission it could not judge at all.
//
// Every path that cannot produce a verdict returns ErrValidationUnavailable
// and names what failed. None of them accepts. An acceptance gate that fails
// open is not a gate: it is quiet on exactly the occasions it matters, and with
// --allow-unverified it was the only remaining check.
func (v coordinatorPermanentValidator) ValidatePermanent(ctx context.Context, manifest backlog.Manifest) error {
	if v.admin == nil {
		return fmt.Errorf("%w: this coordinator has no readiness service to validate against",
			backlog.ErrValidationUnavailable)
	}
	plan, err := campaign.Project(manifest, campaign.Options{})
	if err != nil {
		// The manifest passed the ingestion parser and the projection cannot
		// read it, so the two disagree. That is a fault in this coordinator,
		// not a verdict about the campaign, and it is still a refusal.
		return fmt.Errorf("%w: the manifest could not be projected for validation: %v",
			backlog.ErrValidationUnavailable, err)
	}
	// Acceptance checks the manifest, not the packed bundle: the archive has
	// already been accepted by the message limits that guard the transport.
	request, err := campaignViabilityRequest(plan, 0, 0, "")
	if err != nil {
		return fmt.Errorf("%w: the readiness request could not be built: %v",
			backlog.ErrValidationUnavailable, err)
	}
	response, err := v.admin.Query(ctx, backlogadmin.Query{
		Version: backlogadmin.Version,
		Kind:    backlogadmin.QueryViability,
		// Acceptance is the coordinator checking its own decision, so the
		// principal is the coordinator itself rather than the submitter.
		Principal: backlogadmin.Principal{
			ID: "coordinator", Roles: []string{backlogadmin.LocalAdminRole},
		},
		Viability: &request,
	})
	if err != nil {
		// The coordinator could not answer its own question: no configured
		// project, a store read that failed, an authorization refusal. Retrying
		// with the same idempotency key is safe once it is repaired.
		return fmt.Errorf("%w: the readiness query failed: %v",
			backlog.ErrValidationUnavailable, err)
	}
	if response.Viability == nil {
		return fmt.Errorf("%w: the readiness query returned no matrix",
			backlog.ErrValidationUnavailable)
	}
	if response.Viability.Outcome != backlogadmin.ViabilityImpossible {
		return nil
	}
	lines := make([]string, 0)
	seen := make(map[string]bool)
	for _, reason := range response.Viability.PermanentReasons() {
		line := reason.Code + ": " + reason.Detail
		if seen[line] {
			continue
		}
		seen[line] = true
		lines = append(lines, line)
	}
	sort.Strings(lines)
	return fmt.Errorf("this campaign can never run as written:\n  %s", strings.Join(lines, "\n  "))
}
