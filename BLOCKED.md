# B1 blocked: code-map mismatch

At base HEAD `618d0119edcfdd4839605fffe240447c8ab167d4`, `cmd/t3-steward/backlog_admin.go:1742` already renders `Explanation.Details` in `renderExplanation`. The unit brief's code map says Details are not printed and requests adding them. Per the supplied Mismatch rule, implementation stopped before source edits.

Nearest verified replacement: retain the existing Details loop; add the worker/pool suffix to blocker rendering and render waiting explanations in `renderDiagnosis` once the brief is corrected. No B1 implementation or acceptance is claimed. Fix round independently confirmed the mismatch at received HEAD `68aadc5f55c07ebf5c09bcfffeb428e73030dce1`; review1 requests correction and reissue, and the supplied brief remains unchanged.
