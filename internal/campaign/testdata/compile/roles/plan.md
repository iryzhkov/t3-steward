---
compile: v1
project: t3-steward-github
ref: d2e827681336e76413fdab34392f029a3e131cf4
class: required
roles:
  execute: {effort: medium}
  review: {effort: high}
ledger:
  jocasta_project: t3-steward
  plan: jocasta:31a0e0feedfe0d77f39ef05be31085e5@4
  risk: medium
  acceptance:
    - Golden files pin the compiled workflow
placement:
  requires: [go]
resources:
  implement: {preset: build}
  review: {preset: light}
max_turns: 5
inputs: [design/notes.md]
verify: [make test]
units:
  - {id: r1, title: Role routing, section: R-1}
---
# Role-routed units

## R-1

Route both tasks by role.
