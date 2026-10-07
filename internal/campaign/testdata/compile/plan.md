---
compile: v1
project: t3-steward-github
ref: d2e827681336e76413fdab34392f029a3e131cf4
class: required
template: implement-review
routes:
  execute: {instance: claudeAgent, model: claude-opus-5-5, quota_pool: claude-main, effort: medium}
  review: {instance: codex, model: gpt-6.1-sol, quota_pool: codex-main, effort: high}
verify: [go test ./..., git diff --check]
units:
  - id: c1
    title: Campaign compile skeleton
    section: "C-1"
  - id: b1
    title: Thread filter
    section: B-1
---
# Remaining units

Shared context for every unit.

## C-1: campaign compile skeleton

Turn a plan into campaign directories.

```markdown
## B-1 is not a heading inside a fence
```

### Details

Nested detail stays in the C-1 section.

## B-1

List the runs a thread owns.

## C-10 later

Not part of C-1.
