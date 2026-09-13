# Repository-free workspaces

A version 2 capsule can select `environment.type: fresh`. The logical project
must also declare `type: fresh` in the coordinator catalog. No repository,
Git ref, pre-created task directory, or pre-created T3 project is needed.
Eligible workers and provider routes still come from the catalog; omitting
capsule host constraints permits placement on any eligible worker.

Example coordinator project (omitting setup_profile selects an empty setup
only on this project's workers, without changing unrelated worker catalogs):

```yaml
backlog_v2:
  projects:
    scratch:
      type: fresh
      workers: [homelab, omarchy-pc]
```

Example `workflow.yaml` (supply provider routes supported by your workers):

```yaml
version: 2
name: transform-input
environment:
  project: scratch
  type: fresh
inputs: [input.txt]
tasks:
  transform:
    prompt_file: transform.md
    outputs: [result.txt]
    verify: ['test -s result.txt']
```

The worker creates a new owned attempt directory and an empty workspace,
materializes checksum-verified inputs under `.t3/inputs`, then runs the setup
profile. Output collection and verification use the same custody path as Git
tasks. The execution package signs the workspace type and catalog revision.
A fresh capsule cannot select a Git catalog project, supply a ref, or use
workflow-scoped workspaces. Failed preparation retains its log and removes
unpublished staging state. Reconciliation inspects the same attempt directory;
a repeated preparation call cannot overwrite a published attempt.

If `t3_project` is omitted, Steward provisions stable per-worker T3 project
metadata automatically. The project root and individual attempt workspace are
separate. Retention cleanup removes the owned attempt tree.

Existing version 2 capsules and catalog projects default to Git when type is
omitted. Omitted type fields preserve existing signed package/catalog encodings.
Fresh support must be deployed to coordinator and workers before enabling a
fresh catalog; older workers reject its new mode.

This mode does not attach an existing host directory or provide a provider
sandbox. Input file permissions preserve the existing artifact materialization
contract; they do not isolate an unrestricted same-user provider. Enforced
read-only host datasets and writable-resource fencing require the separate
existing-directory implementation.
