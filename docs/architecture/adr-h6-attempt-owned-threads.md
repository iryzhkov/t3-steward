# H6 ADR: a thread belongs to its attempt, and a quota stop is a pause

Status: accepted. Implemented in the fix/w1-attempt-quota workstream.
Date: 2026-09-17
Authority: issues S-2, S-3, S-4, S-5, S-16, S-17 and S-18 from the Home Assistant rebuild
campaign (`ha-rebuild-prep`, run `run-0f9ae89d246e5940686a1fe6f1831d37`).

## What went wrong

Two components held contradictory beliefs about the same T3 sessions. The quota watchdog on
homelab stopped every running thread that matched `claudeAgent/claude/seven_day` at 97 %,
including the threads of four campaign tasks, and recorded a resume intent for each. The
coordinator saw those turns end without a completed turn and refused the results ("provider
session is not ready without an active turn or error"), so the attempts failed and the campaign
was cancelled. After the reset the watchdog resumed all of the stopped threads. Nothing owned them
any more; they ran for an hour, spent quota, and had to be stopped by hand through the T3 API.

The watchdog had no notion of a thread that belongs to a worker attempt, the worker had no way to
tell a quota stop from a finished turn, and the coordinator's admission read only its own host's
bucket observations, so an idle coordinator host reported the pool as `snapshot-stale` while the
worker host was at 95 %.

## State and ownership

**A T3 thread created by the worker runtime for an attempt is owned by that attempt** from
dispatch until the attempt is terminal on the worker (completed, failed, unknown) or the
assignment is released by a confirmed coordinator stop. The authority is the worker runtime's
durable journal on that host. Nothing else decides the fate of an owned thread: not the watchdog,
not an operator's resume intent, not the reset.

The watchdog consults ownership through `daemon.ThreadOwnership`, implemented by
`workerruntime.JournalThreadOwnership`, which reads the journal file the persistent worker
writes. The two run in different processes on the same host (`t3-steward run` and
`t3-steward worker serve`), so the journal on disk, not memory, is the seam. A host without a
worker bootstrap owns nothing.

Ownership read from the journal is bounded by freshness. Only a live worker advances the
journal, and the assignment lease in each record is renewed only through that worker by the
coordinator, so a record owns its thread only while its lease has not expired
(`ownershipStale` in `internal/workerruntime/ownership.go`). A record without a lease expiry is
bounded by `OwnershipMaxAge` (one hour) since its last update. A worker that crashes or is
stopped with running attempts therefore hands its threads back to the watchdog once the leases
lapse (two minutes by default, `backlog_v2.leases.duration`), instead of leaving them immune
until the provider's 100 %. The watchdog logs once when it ignores stale records and once when
none remain.

**The host watchdog's bucket table stays the authority on quota phase.** The worker does not
reconstruct policy; it reads the same bucket states from the same state database and applies the
same functions the daemon applies (`daemon.GoverningPause`, `daemon.BucketsRecovered`,
`daemon.ProbeWindowOpen`, `daemon.ApplicableHealthy`), which were extracted from the daemon for
that purpose. There is one copy of each rule.

**The local pause is first-class.** `LocalThrottleRequest` in the attempt record is the worker's
own throttle: kind (drain or hard stop), bucket, phase, percent, reset time, the request time, the
time the thread was observed stopped, and the checkpoint. It is written before the effect, so a
worker that restarts inside the driver call comes back knowing the stop was its own. While it is
in force the attempt reports `ControlPaused` with the pause summary (`claudeAgent/claude/seven_day
at 97%`) in its journal excerpt; when it ends it moves to `LastLocalThrottle` so a later
collection can still name it.

## Contract

Must:

1. The watchdog excludes owned threads from warn, drain, stop and resume. On every tick any
   pending or eligible resume intent for an owned thread is cancelled with the reason
   `thread owned by steward attempt <attempt id>`; this also clears intents recorded before the
   rule existed. An intent for a thread whose attempt the journal shows as settled (completed,
   failed, unknown or released) is cancelled with `thread belonged to a settled steward attempt
   <attempt id>`: such an intent can only predate this rule, and nothing would use the resumed
   turn. If ownership cannot be read, the watchdog logs once and treats every thread as
   unowned, never the reverse.
2. The worker observes the thread before deciding on a pause, and pauses only a thread that is
   still working: a thread that already ended its turn is finished work, spends no quota to
   collect, and takes the collection path as before. When the bucket governing a working
   attempt's route is draining, the worker sends the drain notice through the driver's
   checkpoint path once and then observes. When it is stopped, the worker still sends the
   drain notice first (a session that checkpoints on request loses nothing; an interrupted one
   loses its subagents) and escalates to the driver's stop path only when the thread is still
   working after the daemon's `policy.stop_verify_timeout` (`Config.PauseEscalation`); the
   escalated request keeps the notice's request time, so the throttle command id is stable.
   Either way the attempt is paused, not failed:
   nothing is collected, a commanded collection is deferred, and the coordinator sees
   `ControlPaused`. A collection that later meets a session that is not ready reports
   `paused by quota watchdog: <bucket> at <percent>; provider session is not ready ...`.
3. The worker resumes a paused thread only when the bucket has recovered under the watchdog's
   eligibility rules **and** the attempt is still live per the assignment state the worker holds:
   claimed, under a lease that has not expired, with no coordinator stop command. A cancelled
   attempt is never resumed; its stop command settles it through the ordinary stop path. After a
   restart the decision is rebuilt from the journal and the next coordinator reconcile.
4. A worker build advertises `quota-observations-v1`. The coordinator asks such a worker, on the
   snapshot exchange, for its host's bucket observations, and the quota bridge merges them with
   the coordinator's own by bucket key, keeping the freshest. A pool closes at its stop threshold
   whichever host observed it, and `snapshot-stale` clears while any worker has a fresh reading.
   The wire version is unchanged: an older worker is never asked and never sends, an older
   coordinator never asks, so neither meets the field. The pause reason and thread state in the
   journal excerpt of each assignment observation are gated by the same ask: both sides decode
   snapshots strictly, so any field added to the snapshot is sent only to a coordinator that
   asked for `quota-observations-v1` on that exchange.
5. `t3-steward thread stop <thread-id> [--session]` dispatches `thread.turn.interrupt` and, with
   `--session`, `thread.session.stop` through the local T3 control client and prints what it
   sent. `t3-steward backlog rewake <run>/<task> --reason TEXT` resumes an attempt that is
   `waiting-external` with no live task-bound wait, applying the transition a settled wait's wake
   applies; it is refused while a wait is live, naming the wait. Every rejection of an attempt
   command names the commands the current state admits.
6. A mutation submitted without `--expected-revision` and rejected with `stale revision` is
   resubmitted once by the CLI under a new command id after re-reading the target; the response
   says which revision was used. With `--expected-revision`, or with an explicit `--command-id`,
   the rejection is returned as it is.
7. Task detail carries the worker's last report on the attempt (thread id, worker, observed
   control, journal phase, thread state, pause reason) for the renderers.
8. (S-18) The burn-rate projection asks for a drain at most while usage is below `stop_percent`;
   a hard stop needs the percentage threshold or an exhaustion under the hard-stop floor (two
   minutes). An exhaustion projected after the reset is no reason to act (runway margin 1). A
   turn whose latest user message is newer than the bucket's stop is not held while the bucket
   is stopped. The drain notice says how long the session has at the current rate and that the
   provider ends it at 100 %.
9. (F-1, S-18 remainder; agent-experience campaign stage 2) A bucket's phase for one window epoch
   has exactly three writers, each leaving an action record: the engine from a reading or a grace
   timer (the only one that raises a phase); load-time re-derivation, which at daemon start lowers
   a stored phase of a current epoch that the loaded thresholds would not produce from the stored
   percentage, clearing `StoppedAt` and the drain deadline, setting `RecoveredAt` when the result
   is normal, and recording a rearm that names both threshold sets (the engine stores the ladder it
   evaluated under as `AppliedThresholds`); and `t3-steward bucket rearm <key> --reason TEXT
   [--force]`, which sets normal with `RecoveredAt` now and records the actor, the reason and the
   phase before and after, refusing a stored percentage at or above `stop_percent` without
   `--force`. No phase changes without its record; a save failure keeps the stored phase and logs
   at ERROR. The worker's resume rule needs no change: a rearm is a recovery newer than the pause.
   The worker's probe rule resumes one paused attempt per bucket epoch when the bucket is stopped
   below the current `stop_percent`, the stored reading is older than `resume.probe_after_reset`,
   the host's thread list is known and no running thread on it matches the bucket; `ProbedAt` on
   the bucket and a resume action record it, the probing thread is not paused again while its
   reading is outstanding, and the reading rearms or re-stops the bucket. The interactive override
   is a sub-rule of the same owner: a thread whose latest user message is newer than the bucket's
   stop beyond the ten-second tolerance is recorded in the bucket's thread notices as
   `user-resumed` with that time; for the rest of the epoch no path stops or drains it (poll,
   execute on a reading, grace timer, `stop_new_sessions`), it is warned at most once, and it gets
   no resume intent. The record is evidence, not a phase reset; a rearm clears it with the epoch.
   Owned threads are excluded before the rule.

May: reuse the throttle command kinds drain, hard-stop and resume for the local pause.

Does not guarantee: coordination across hosts for a pool no worker observes; resumption while
the coordinator is unreachable (the pause stays, the lease expires, and lease expiry handles the
rest as today); ownership of a thread whose assignment lease has expired (a coordinator outage
longer than the lease duration makes a running attempt's thread the watchdog's again until the
next renewal, which is the coordinator's own view of that lease); that the coordinator counts a locally paused attempt's remaining cost as a
reservation (it has no coordinator-side throttle record, and quota planning skips it with a
warning as it does today for any paused attempt without one).

## Invariants

- Local: an owned thread is never the target of a watchdog warn, drain, stop or resume while
  ownership is readable.
- Local: while `LocalThrottle` is set, the attempt is not collected and reports `ControlPaused`.
- Cross-component: the coordinator's admission for a pool is closed whenever any host with a
  fresh reading of the pool's bucket reports it at or above the stop threshold. Eventual within
  one snapshot exchange.
- Eventual: a cancelled attempt's thread is stopped by the worker's stop path on the next
  reconcile after the stop command; the watchdog never resumes it in between because ownership
  lasts until the stop is confirmed.

## Evidence

Regression tests, each failing on the base commit `e1a0a90`: `TestOwnedThreadIntentIsCancelledNeverResumed`,
`TestOwnedThreadsAreExcludedFromWatchdogActions`, `TestOwnershipReadFailureDegradesToUnowned`
(daemon); `TestLocalQuotaStopReportsPausedAndDefersCollection`, `TestLocalQuotaDrainThenHardStop`,
`TestLocalQuotaPauseNeverResumesACancelledAttempt`, `TestLocalQuotaResumeIsGatedOnRecoveryAndLiveness`,
`TestSnapshotReportsQuotaObservationsWhenAsked`, `TestJournalThreadOwnershipListsLiveAttemptThreads`
(workerruntime); `TestQuotaBridgeWorkerObservationClosesPoolWhenLocalReadingIsStale`,
`TestResultCompletionFailureNamesRecordedPause` (backlog); `TestPlanAdminRewakeResumesParkedAttemptWithoutLiveWait`,
`TestExecutePendingRewakeAppliesThroughTheStore`, `TestPlanAdminRejectionsNameAllowedCommands`,
`TestTaskDetailCarriesAttemptEvidence` (backlogadmin); `TestBacklogMutationRetriesOnceAfterStaleRevision`,
`TestBacklogMutationHonoursExplicitRevisionAndCommandID`, `TestThreadStopDispatchesInterruptAndSession`
(cmd); `TestBurnRateProjectionDrainsButNeverStopsBelowStopPercent`, `TestRunwayMarginOneIgnoresExhaustionAfterReset`,
`TestStoppedAtFollowsTheStopAndTheReset`, `TestGraceTimerStops` (policy; the S-18 test ticks through the
drain grace and the grace tests use a projection at the hard-stop floor); `TestUserStartedTurnIsNotReStoppedWhileBucketStopped`,
`TestSettledAttemptThreadIntentIsCancelled`, `TestGraceExpiryStopsWithoutNewEvent` (daemon);
`TestSnapshotOmitsPauseFieldsUnlessQuotaObservationsWereAsked` (an rc.66-shaped strict decoder accepts
the unasked snapshot), `TestFinishedTurnIsCollectedNotPausedWhileBucketStopped`,
`TestJournalThreadOwnershipExpiresWithTheLease`, `TestLocalQuotaStopPrefersDrainAndEscalatesAfterTheWindow`,
`TestJournalToleratesUnknownFieldsWithinTheVersion` (workerruntime);
stage 2 of the agent-experience campaign, each failing on `9c79972`: `TestLoadRederivesStoredPhaseUnderRelaxedThresholds`,
`TestLoadKeepsAPhaseTheThresholdsStillProduce`, `TestLoadNeverRaisesAStoredPhase`, `TestUserResumedThreadIsNotStoppedByThePoll`,
`TestUserResumedThreadIsNotStoppedByExecute`, `TestUserResumedThreadIsNotStoppedByTheGraceTimer`,
`TestUserResumedThreadIsWarnedAtMostOnce`, `TestUserResumedExemptionEndsWithTheEpoch` (daemon);
`TestBucketRearmRearmsAStoppedBucket`, `TestBucketRearmRefusesAtOrAboveStopPercentWithoutForce`,
`TestBucketRearmUnknownKeyListsKnownKeys`, `TestBucketRearmJSONPrintsOneDocument`, `TestBucketListShowsEveryBucket` (cmd);
`TestRearmResumesAPausedAttemptWithinOneReconcile`, `TestProbeResumesOnePausedAttemptPerBucketEpoch`,
`TestProbeResumesThePausedAttemptThroughTheRuntime` (workerruntime), with `TestU3PausedAttemptLeaseIsRenewedAcrossOneLeaseDuration`
and `TestOwnedThreadWithNewerUserMessageIsLeftToTheWorker` as guards that pass on the base;
`TestOlderWorkerSnapshotReconcilesWithoutGatedFields` (backlog);
`TestDrainNoticeNamesTimeToExhaustion` (daemon).

Live validation still owed: a supervised campaign with a forced quota stop on the worker host
(thresholds lowered with approval) pausing and resuming without a failed attempt; a cancel during
the pause leaving no live session; a `wait cancel` from the thread followed by `backlog rewake`.
