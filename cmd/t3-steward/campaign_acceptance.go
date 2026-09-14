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

func (v coordinatorPermanentValidator) ValidatePermanent(ctx context.Context, manifest backlog.Manifest) error {
	if v.admin == nil {
		return nil
	}
	plan, err := campaign.Project(manifest, campaign.Options{})
	if err != nil {
		// A manifest the projection cannot read has already passed the
		// ingestion parser, so this is a fault in the readiness check rather
		// than in the submission, and it must not refuse the submission.
		return nil
	}
	// Acceptance checks the manifest, not the packed bundle: the archive has
	// already been accepted by the message limits that guard the transport.
	request, err := campaignViabilityRequest(plan, 0, 0, "")
	if err != nil {
		return nil
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
		// The coordinator could not answer its own question. Refusing here
		// would turn an internal fault into a permanent verdict about the
		// submission, which is the one thing a permanent verdict must never be.
		return nil
	}
	if response.Viability == nil || response.Viability.Outcome != backlogadmin.ViabilityImpossible {
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
