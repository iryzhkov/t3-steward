# rc.114 models JSON consumer check

Repository searches covered models command invocations, observedAt,
oldestObservedAt, availability, authorization and statusline references.

- `cmd/t3-steward/models*_test.go`: existing tuple, route-availability and
  missing-authorization tests already use M17-1 semantics. The older-coordinator
  test intentionally retains unknown authorization when no worker reports any.
- `docs/backlog-v2-operations.md`: M17-1 already updates the operational JSON
  description. `docs/model-discovery.md` now gives field migration guidance.
- `README.md`, CLI help, route-policy, coordinator-fleet-ownership and admission
  provenance docs: descriptions and command syntax do not consume the changed
  timestamp fields; their authorization/observation distinction remains valid.
- `scripts/qualification/case5_runtime.py`, `fleet.sh`, `fleet_limits.py` and
  `fleet_limits.sh`: model lists belong to provider caches, inventory or UpKeeper
  fleet-plan documents, not models JSON. Other script observedAt fields belong
  to worker snapshots, quota records or qualification evidence.
- No repository statusline helper or shell/JSON selector consuming models JSON
  was found. Historical audit/review documents describe the pre-fix defect and
  remain historical evidence. The compressed historical verification log and
  external task-input symlinks were excluded from source-text search.

Validation: focused race tests in `cmd/t3-steward` exercise model JSON tuple,
route availability and worker authorization, together with legacy scope and
reason consumers. See the integration handoff's verification log.
