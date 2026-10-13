# Interview guide

For the complete step-by-step teaching sequence, command explanations, code trace,
transactions, controls and debugging history, read
[Airflow Investigation Workflow Demo and Code Handbook](interview-handbook.md).
This shorter guide is the presentation outline.

The local execution gates have passed: scheduled history/restart, fresh live
investigations and repetitions, watcher launch/dedup and isolated patch validation.
Show the verification matrix and report-quality limitations honestly. A clean
lock install and independent human semantic review are not established.
The conservative ledger records$4.182984; the user has subsequently approved and
configured$15 total. Fresh bounded demo investigations can now be admitted. Keep
the existing ledger, check admission before the demo, and label saved reports as
fallback.

## Why build it

Supporting an unfamiliar DAG means locating the real failure, exact attempt,
executed source and helper behavior, then understanding writes and retries.
This project turns those navigation steps into bounded tools for one agent,
with evidence and inspectable results. There is no evidence of workplace
adoption, measured time savings or production access.

## Eight-to-ten-minute walkthrough

1. Show healthy local services, paused DAGs and persisted scheduled history in
   Airflow. Explain these were real scheduler executions prepared earlier.
2. Select the primary failed ingestion run and its actual failed attempt.
3. After confirming budget admission, run `investigate ... --fresh --bounded`.
   Point to the new investigation/thread IDs and
   actual model/tool activity. Do not rerun the pipeline.
4. Show JSON/Markdown and source/attempt references. Follow the evidence through
   the helper's commit/checkpoint order and destination duplicate counts.
5. Ask specifically about retry safety and unknown business semantics.
6. If verified beforehand, request a targeted diff and show separate container
   validation results. Explain local validation is not BigQuery verification.
7. Briefly contrast rollback/already-idempotent behavior or unavailable evidence.
   Explain why the answer should change, rather than receive a canned fix.

If the provider is unavailable, show a clearly labeled saved fallback and say
the model did not run live. The `show` command prints that distinction.

## Implementation-specific questions

**What is agentic?** In `agent.py`, the model chooses calls from eight typed tools
and responds to retrieved results. Deterministic code performs retrieval/stats,
enforces limits and validates structures. It is not a scenario classifier.

**Why LangGraph?** `create_agent` already uses its graph runtime. SqliteSaver
persists thread state for follow-ups; no decorative custom graph was added.

**How do you avoid source mismatch?** `demo.py:setup` creates a hash manifest;
runtime records the run binding and verifies deployed content. `evidence.py`
rejects foreign bindings/tampering and reads only the snapshot. Current API
definitions are labeled current, and unknown binding stays unknown.

**Does SIGKILL prove OOM?** No. The demo infrastructure deliberately kills a
resource-bounded child. Logs establish termination, not its memory cause. No
worker/container metrics currently support an OOM conclusion.

**Why not recommend MERGE for every retry?** `batch_ingest.py` shows transaction
and checkpoint alternatives. Immutable synthetic event identity may support
deduplication; real update semantics and stable keys still need evidence.

**Can mapping solve memory and writes?** It may distribute independent work, but
does not remove parent materialization, bound an oversized group, guarantee
idempotency or reduce aggregate concurrent memory by itself.

**Are references proof?** `validate_references` proves IDs exist in the correct
investigation and supported references are available. It cannot prove semantic
correctness. Human review and behavior tests remain necessary.

**What failed during development?** The doctor smoke call passed, but tools plus
reasoning failed on Chat Completions with HTTP400. Responses fixed the actual
compatibility error. Follow-ups then hit context admission; after handoff, coarse
context editing erased useful source from the model request. A completed v3 report
incorrectly called that evidence unavailable. v4 retained source and corrected
commit/checkpoint order, but still mislabeled the alternative transaction scope.
These reports remain saved. The journal distinguishes implementation defects from
injected DAG failures; schema-valid completion never served as the quality gate.

**What does the proposed fix guarantee?** The live v5 patch e565fab8 puts each
page's writes and checkpoint update in the same SQLite transaction, with rollback
on a write/checkpoint error. Static review confirms other helpers/deployed source
unchanged. It assumes stable pagination, one writer and consistent initial state;
it does not impose event uniqueness or clean the captured historical duplicates.
Validation843aea7e passed all five prepared checks in18.281s, including repeated
failure/completeness, checkpoint and partial-insert rollback, helper equivalence
and three-DAG loading. Describe it as locally validated; deployment and BigQuery
verification remain unperformed.

**Why cap requests differently in evaluation?** Long follow-ups use120 KB/eight
calls here, while fresh benchmarks use up to100 KB/five calls/3000 output tokens
in both evidence conditions. Fresh threads have no prior diagnosis. process.py
passes the admitted limits to runner.py. Five-call evaluations reserve one final
output slot; eight-call operations also reserve one optional integrity correction.
The approved total is now$15 after explicit user authorization; the ledger retains
past reservations and reserves conservative allowance before dispatch.
it may stop the suite, and is not an exact provider billing cap. Compare recorded
settings, not only the model name.

**What survives restart?** PostgreSQL volume, native logs, content snapshots,
per-run/attempt evidence, workflow records and checkpoints. `verify_history`
checks persistence; a fresh investigation separately checks runtime access.

**What did Codex contribute?** Development, adapter/tool design, implementation,
test/debugging assistance and documentation. The deployed runtime calls OpenAI
through LangChain; it does not call Codex as the investigator. Explain only
execution and results recorded in the journal/matrix.

## Deeper discussion and future work

Use `code-walkthrough.md` and `decisions.md` for the technical portion of the
45-minute discussion. Discuss actual permission/temp-path defects, the held-out
control boundary, resource admission and evidence freshness. Prepared tests
versus real service verification is a key tradeoff, not a detail to conceal.
Future priorities are recorded failures, evaluation bounds/review, BigQuery
job evidence, worker metrics, source integrations and human-approved recovery.
