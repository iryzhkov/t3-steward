package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/iryzhkov/t3-steward/internal/backlogadmin"
	"github.com/iryzhkov/t3-steward/internal/domain"
)

// campaignNotifyTimeout is how long a submission's notification wait stays
// pending. It matches the default of "wait add", because a campaign is not a
// different kind of thing to wait on.
const campaignNotifyTimeout = 24 * time.Hour

// nodeWakeDeliveryHostRelease is the first release whose coordinator records
// the calling host as a wait's delivery host. An older one decodes the
// registration with unknown fields disallowed and refuses the whole operation,
// which would leave a client one release ahead unable to start any task at all,
// so the field is sent only to a coordinator known to be at least this release.
const nodeWakeDeliveryHostRelease = "v0.11.0-rc.71"

// campaignNotification is what submit reports about the wait it registered.
type campaignNotification struct {
	WaitID   string `json:"waitId"`
	ThreadID string `json:"threadId"`
	Target   string `json:"target"`
	Delivery string `json:"delivery,omitempty"`
	// Host is the host the coordinator recorded as this wait's delivery host,
	// read back from its answer rather than assumed from what was asked for.
	Host string `json:"host,omitempty"`
	// Undeliverable says why nothing on this host will be woken, and is empty
	// when the wake will arrive here. A caller that reads it must not end its
	// turn expecting a wake.
	Undeliverable string `json:"undeliverable,omitempty"`
}

// undeliverableWake explains a wake that cannot reach the calling host.
//
// It compares the delivery host the coordinator recorded with this host,
// stating what the coordinator answered rather than inferring anything from
// what the client asked for: an older coordinator ignores the calling host, and
// so does one that already held this registration under another host. A wake is
// sent by the wait runner whose host matches the wait's, into the T3 of that
// host, and a thread exists only on the host that opened it.
//
// An answer that carries no host at all is no evidence either way and is not
// reported as a failure.
func undeliverableWake(recorded, local, release string) string {
	if recorded == "" || local == "" || recorded == local {
		return ""
	}
	reason := fmt.Sprintf("the coordinator records %s as this wait's delivery host and this host is %s, "+
		"so the wake is sent into the T3 of %s, where this thread does not exist", recorded, local, recorded)
	newEnough, known := releaseAtLeast(release, nodeWakeDeliveryHostRelease)
	switch {
	case !known:
		return reason + "; this coordinator reports no release this client can read (" +
			strconv.Quote(release) + "), so the calling host was not stated"
	case !newEnough:
		return reason + "; recording the calling host needs a coordinator of " +
			nodeWakeDeliveryHostRelease + " or newer, and this one runs " + release
	default:
		// The coordinator does record the calling host, so this registration ID
		// already existed under another one. Say that rather than blame a
		// release that is not the cause.
		return reason + "; this coordinator runs " + release + " and does record the calling host, so " +
			"this wait was registered earlier from " + recorded
	}
}

// wakeDeliveryHost is the host whose steward would deliver a wake to this
// process's threads. It is the same name the wait runner matches on, which is
// why both derive it from the operating system rather than from configuration.
func (c campaignCLI) wakeDeliveryHost() string { return localWakeHost(c.wakeHost) }

// localWakeHost reads this host's name, or the empty string when it cannot be
// read. A host this process cannot name is not evidence that a wake will go
// somewhere else, so nothing is claimed about it and nothing new is sent.
func localWakeHost(hostname func() (string, error)) string {
	if hostname == nil {
		hostname = os.Hostname
	}
	host, err := hostname()
	if err != nil {
		return ""
	}
	return host
}

// coordinatorRelease is the release the coordinator reports, or the empty
// string when there is no seam to ask or the question fails. A release that
// cannot be read is treated as one that does not record the calling host.
func (c campaignCLI) coordinatorRelease(ctx context.Context) string {
	if c.release == nil {
		return ""
	}
	release, err := c.release(ctx)
	if err != nil {
		return ""
	}
	return release
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
	operation := backlogadmin.NodeWaitOperation{
		Action: "register",
		Request: domain.NodeWaitRequest{
			ID:       "nw-campaign-" + key,
			ThreadID: threadID,
			Name:     target.String(),
			Target:   target,
			Timeout:  campaignNotifyTimeout,
		},
	}
	local := c.wakeDeliveryHost()
	release := c.coordinatorRelease(ctx)
	if newEnough, known := releaseAtLeast(release, nodeWakeDeliveryHostRelease); known && newEnough {
		// Only a coordinator known to be new enough is told which host to wake.
		// An older one decodes this operation strictly and would refuse the whole
		// registration, and a release this client cannot read is not evidence
		// that it can be read by the coordinator either; both keep the previous
		// wire shape and are reported honestly below.
		operation.Host = local
	}
	response, err := c.notify(ctx, operation)
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
		notification.Host = response.Waits[0].Host
	}
	notification.Undeliverable = undeliverableWake(notification.Host, local, release)
	return notification, nil
}
