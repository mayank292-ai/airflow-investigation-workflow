# Verification matrix

Updated October 12, 2026. Pass means only the stated procedure/scope. The current
local workflow has passed its execution gates; report-quality limitations and
clean-install verification remain documented. Fixture/model-double checks are
explicitly distinguished from real service execution.

| Requirement | Procedure / expected | Actual | Status | Evidence |
|---|---|---|---|---|
| Local Python dependencies | Installed versions and pip check compatible | Environment installed; pip check passed earlier | pass | Lockfile and doctor versions |
| Clean lock install | New venv installs hashes successfully | Not executed | pending | requirements.lock |
| Docker Linux engine | Regular shell client reaches Linux server | User output confirms 29.8.2 / Desktop 4.95.0 | pass, user shell only | User-supplied version output |
| Executor Docker access | Launch installed CLI | AppData executable denied | blocked | Actual command error, engineering journal |
| RAM admission | Host >=2 GB free; engine approx.4 GB | User startup measured 2.02 GB free / 3.74 GB engine RAM | pass at startup | User startup output |
| Airflow services | Start script; API and service health checks pass | Corrected API, scheduler, processor, PostgreSQL healthy; direct executor REST check passed | pass | User RestartAirflow output; demo check-environment |
| Real DAG loading | DAG processor loads three DAGs with no imports errors | Actual processor stats show 3 DAGs / 0 errors | pass, user logs | pipelines.py.log and user container output |
| Genuine scheduled runs | PrepareHistory creates terminal scheduled runs, then pauses | 10 scheduled runs: 4 success / 6 failure; all paused | pass | state/history.json |
| Log/task/attempt/map adapter | Contract tests + real API checks | Real attempts 1/2/logs/dependencies pass; mapped identity/pagination remain contract tests | partial | tests/test_live_airflow.py + test_airflow_adapter.py |
| Source navigation/boundaries | Hash/identity/path checks; real helper retrieval | Real bound helper search/read and unavailable/mismatch cases pass | pass, tool scope | tests/test_live_airflow.py |
| Watcher detection/dedup | Actual failed task; duplicate scan no new inv; new attempt distinct | Six real failures detected; repeated scan/reopened store dedup passed; isolated test records | pass, detection scope | tests/test_live_airflow.py |
| OpenAI API access | Bounded doctor call | Earlier live call: 11 input, 4 output tokens | pass, smoke only | Engineering journal |
| Agent evidence loop/checkpoints | Independent live tools, valid reports and resumed follow-up | Responses initial report and two follow-ups saved; v4 reads deployed helper/DAG and corrects availability/contract; recommendation scope error remains | partial, live verified with quality limitations | Investigation 90ca084f; operations 2aa1842c, d7d7f8f9, a9e551c3 |
| Bounded continuation | Tool exhaustion preserves final schema, no excess retrieval; context bounded | Looping doubles pass partial-batch/finalization; live v3/v4 complete before exhaustion; v4 source retained and 120 KB admission | partial | tests/test_context.py, test_agent_harness.py; live runtime metadata |
| Retry behavior | Actual writes, duplicate/completeness/rollback checks | Primary attempt1 8 rows/8 keys/checkpoint0; attempt2 16 rows/8 keys/checkpoint0; rollback 0 rows; success 16 unique keys/checkpoint4 | pass, real execution | tests/test_live_airflow.py; frozen outputs |
| Enrichment semantics | Python/SQL null/reference equivalence | Local equivalence and real successful output166 rows/100 keys/0 exact duplicates pass; BigQuery unverified | partial | tests/test_pipelines.py; test_live_airflow.py |
| Patch generation | Explicit live request, exact source revision, isolated diff | Live v5 patch saved: only append_pages changed, exact revision matched, other helpers/deployment unchanged | pass, generation/static scope | Patch e565fab8; review520c2a8f |
| Patch validation | Prepared image runs bounded trusted tests | All five checks pass: retry completeness, checkpoint/partial-insert rollback, helper equivalence and DAG loading;18.281s,120s/1 CPU/384 MB/no network. Earlier failures preserved | pass, local validation only | validation843aea7e-223f-40ce-9726-a7196c34c951; patch e565fab8 |
| Snapshot collections/access checks | Reject manual, distinct attempts, detect mutation; real hashes/logs | Ten collections/logs/outputs accessible before and after real Stop/Start; nine bound source snapshots verified, one unavailable binding preserved | pass, access scope | state/verifications/; tests/test_history.py |
| Real restart + fresh investigation | Stop/Start, VerifyHistory, new agent UUID/thread/report | Full Stop/Start passed; ten runs/logs/outputs persisted and paused. New05dff001 thread independently retrieved evidence and saved v7 reportd6be33f2 after two retained incomplete operations; quality defects recorded | pass, persistence/access/live execution scope; report quality partial | 80c8ca3a before /88ea2eb4 after /1e61cbb2 latest restoration; post-restart-d6be33f2-review.json |
| Report integrity correction | Strict schema plus same-investigation IDs; max one correction in total caps | Doubles pass success/second-rejection stop; v7 live missing-evidence correction rejected again; v10 explicit phase parsing worked live for primary/rollback | pass, boundary behavior; successful live correction not established | test_agent_harness.py/test_output.py; suite03e0ca76 |
| Three live repeats/counterexamples | Fresh results in approved budget; retain failures | v7 all19 attempted:4 accepted/15 incomplete; v10 budget-stopped after3 accepted; v11 all19:17 accepted/two rejected. v12 focused12/12 accepted, three repeats per key condition and baseline. Primary exception now preserved; evidence coverage and recovery directions vary | pass, execution/integrity scope; semantic limitations retained | suites9cce2f73 /03e0ca76 /36d051d3 /e1e95c9e; accounting and development review notes |
| Watcher live model launch | Real failure discovery launches investigator; second process does not duplicate | Six real failed attempts produced six complete v12 reports; second process preserved all six events and operation counts without launching more investigations | pass, live launch/persisted dedup scope | watcher-67c603a7-e620-45ad-831d-7fe8bba0c407.json; workflow.sqlite events |
| Cross-investigation leakage | New IDs/threads, tool allowlists, reference rejection | Deterministic isolation tests pass | partial | tests/test_boundaries.py |
| No frontend/secrets output | Inspect project and commands | CLI/docs only; credentials and generated runtime data excluded from Git; user subsequently requested repository publication | pass in performed work | Project files / .gitignore |

Latest complete offline result is under
`state/evaluations/f02489d7-0c70-4d4f-8966-5e64e71ee616`:64 passed /nine skipped
in24.32s, including current log retention, final-schema, framework/boundary checks
and explicitly scoped evaluation planning. Earlier suites remain retained.
Update this matrix with actual output paths as each live gate executes. A startup
success alone must not change the investigation/validation/restart rows to pass.

Real integration evidence: `state/integration-6d5cd05f-64f5-406b-b715-47029583b970`
(9 passed in 13.90 seconds). A prior test invocation failed at CLI argument parsing
before collecting tests; its separate log is retained. Corrected variable-based
PowerShell argument passing produced the successful invocation.

Executed private runtime code is also archived under
`state/infrastructure/a9136474645a32fbda5e0e0ee251e1aa22eb5d22a17efacd3ed5953bdc2353e5`.
Its content hash matches all ten execution-recorded bindings; archival occurred
after execution and is labeled accordingly. It remains outside investigator tools.

Post-restart source check8a9683b6 verified nine execution-bound snapshots. One
run's execution revision remains unavailable as recorded; current source was not
substituted. This is operator-only access/hash verification, not a model diagnosis.
