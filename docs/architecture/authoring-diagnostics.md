# Campaign authoring diagnostics

`campaign validate` and `campaign compile` report a `diagnostics/v1` document on refusal. The strict manifest and compile parsers still decide acceptance. Loading, inventory limits, path checks, graph checks and generated-workflow validation retain their existing authority.

## Document and owner state

The document has `schemaVersion: "diagnostics/v1"`, `ok` and `entries`. Each entry has `code`, `severity`, `file`, `line`, `column`, `path`, `message` and `hint`. Refusals have `ok: false` and error severity. Entries sort by source line and column, then field path and code. The file is the authored document; the path is its YAML field path. Empty strings and zero coordinates mean unknown. Text displays unknown coordinates as `?`. There is no automatic fix.

The owner parses the original bytes once with its strict decoder. Its YAML nodes and original Markdown header offset supply proven locations and paths. Diagnostics retain all independent errors the decoder reports, including unknown fields, duplicate keys and type errors. A syntax failure can produce one entry. Failed decoding does not establish exhaustive semantic validation.

A typed carrier attaches the document to the original error. It unwraps the cause, preserving `errors.As` access to `UnknownManifestFieldError` and `CompilePlanError`, their human messages and transport classifications. Loaders add the actual authored filename while keeping parser evidence. Nested wrappers retain the carrier. Legacy semantic, graph, path and IO failures remain refusals.

## Stable codes and hints

| Code | Meaning |
| --- | --- |
| `yaml-syntax` | YAML syntax could not be parsed |
| `yaml-duplicate-key` | Decoder reported a repeated mapping key |
| `yaml-unknown-field` | Strict owner decoder refused a field |
| `yaml-type` | Decoder reported an incompatible value type |
| `yaml-multiple-documents` | Owner refused additional YAML documents |
| `front-matter-missing` | Compile plan lacks its opening fence |
| `front-matter-unclosed` | Compile plan lacks its closing fence |
| `legacy` | Existing refusal without a dedicated diagnostic conversion |

Unknown-field hints reuse owner suggestions and release context. Syntax hints cover flow titles with unquoted commas, indentation tabs, colon-space plain titles and missing fences when source context supports them. Ambiguous failures get a generic YAML or block-style hint. Hints do not interpret fenced Markdown, quoted text, shell commands or block scalars as mappings. They never execute or resolve values, repair bytes or relax parser refusal.

## CLI lifecycle and compatibility

Validate and pre-report compile failures locally compose the existing transport error envelope with an additive `diagnostics` document. Envelope keys, class, operation, message and exit code retain their meanings. Validate preserves its existing schema version. Compile preserves its existing pre-report envelope version behavior. The coordinator wire protocol and other command families are unchanged.

A compile report already printed contains diagnostics only on failed unit objects, alongside their original `errors`. `afterDocument` still fences the global error reporter, so stdout contains exactly one JSON document. Valid summaries and valid unit shapes omit diagnostics. Text retains the original refusal and appends diagnostic locations and hints.

Malformed compile front matter refuses before directory creation, extra input reads or readiness. Bounded reads and offline behavior remain in force. Diagnostics provide evidence for the author; they confer no schema, authorization, acceptance or readiness authority.

## Deferred creation boundary

`campaign new`, schema generation and proposal import remain unavailable. Future creation must separate preparation from frozen replay, bind the full digest to archive, envelope and notify, and resolve `current` to a concrete thread at preparation. It must not change replay flags or acknowledgements. Arbitrary operational plan values require authenticated digest-bound acknowledgement.

Proposal receipts must bind run, task, attempt, artifact and digest to accepted independent planning review. Data-only proposals may carry unit text, dependencies, risks, questions and context labels, never grants, routes, commands or executable follow-ups. Default verification, final exact-head independent acceptance and bounded fixes remain required. Unsupported inter-unit, after, key and group sequencing must refuse before submission. Missing capabilities cannot be downgraded, dependencies cannot be flattened, and future fields cannot bypass strict compile or raw-schema unknown-field refusal. Proposal parsing, import and family-independent provenance remain with their future owners.
