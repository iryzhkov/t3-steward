package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignNotifyTimeout is how long a submission's notification wait stays
// pending. It matches the default of "wait add", because a campaign is not a
// different kind of thing to wait on.
const campaignNotifyTimeout = 24 * time.Hour

// campaignNotification is what submit reports about the wait it registered.
type campaignNotification struct {
	WaitID   string `json:"waitId"`
	ThreadID string `json:"threadId"`
	Target   string `json:"target"`
	Delivery string `json:"delivery,omitempty"`
}

// campaignNotifyThread resolves the thread a submission should wake.
//
// It runs before anything is submitted. A caller that asked for "current" from
// a session the steward cannot identify gets an error and no run, which is the
// right way round: a campaign nobody will hear about is worse than a campaign
// that was not submitted.
func (c campaignCLI) campaignNotifyThread(requested string) (string, error) {
	if requested == "" {
		return "", nil
	}
	if c.resolveThread == nil {
		return "", errors.New("thread identity resolution is unavailable")
	}
	explicit := requested
	if requested == "current" {
		// "current" is resolved, never assumed. A provider session id is an
		// input to that resolution and never a thread id of its own.
		explicit = ""
	}
	thread, err := c.resolveThread(explicit)
	if err != nil {
		return "", fmt.Errorf(
			"--notify-thread current could not be resolved to a T3 thread, so nothing was submitted: %w. "+
				"Pass --notify-thread <id> with the thread to wake", err)
	}
	if thread == "" {
		return "", errors.New("--notify-thread resolved to an empty T3 thread id")
	}
	return thread, nil
}

// registerCampaignNotification parks the calling agent on the new run's sink.
//
// It reuses the node-wait machinery unchanged: the wait is an ordinary durable
// registration on a run sink, and it creates and alters no workflow state of
// its own. The registration ID is derived from the submission idempotency key,
// so re-running the same submit command registers the same wait rather than a
// second one.
func (c campaignCLI) registerCampaignNotification(ctx context.Context, key, runID, threadID string) (campaignNotification, error) {
	if c.notify == nil {
		return campaignNotification{}, errors.New("coordinator node-wait transport is unavailable")
	}
	target := domain.NodeRef{RunID: runID, TaskID: domain.SinkTaskName}
	response, err := c.notify(ctx, backlogadmin.NodeWaitOperation{
		Action: "register",
		Request: domain.NodeWaitRequest{
			ID:       "nw-campaign-" + key,
			ThreadID: threadID,
			Name:     target.String(),
			Target:   target,
			Timeout:  campaignNotifyTimeout,
		},
	})
	if err != nil {
		return campaignNotification{}, fmt.Errorf(
			"the campaign was submitted as run %s, but the notification wait was not registered: %w. "+
				"Register it with \"t3-steward wait add --run %s --thread %s\"",
			runID, err, runID, threadID)
	}
	notification := campaignNotification{ThreadID: threadID, Target: target.String()}
	if len(response.Waits) == 1 {
		notification.WaitID = response.Waits[0].Request.ID
		notification.Delivery = response.Waits[0].Delivery
	}
	return notification, nil
}
