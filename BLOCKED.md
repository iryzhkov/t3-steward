# M17-4a blocked: code-map mismatch

Observed HEAD: d2e827681336e76413fdab34392f029a3e131cf4.

The unit's verified code map says BundleIngester.buildRecords creates ProgressReady attempts when a task has no local needs. At internal/backlog/ingest.go:413–415 the actual condition is len(taskManifest.Needs) != 0, so external-only dependencies also create ProgressBlocked attempts. Lines 373–381 separately distinguish localNeeds and externalNeeds, confirming they are different sets.

Nearest verified replacement: admission can select actual built ProgressReady attempts, which excludes external-only dependent tasks at creation. The brief must reconcile that behavior with its roots definition before implementation proceeds. Per rules.md's Mismatch rule, no implementation or tests were changed.
