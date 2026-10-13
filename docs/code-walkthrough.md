# Code reading order

Start with a manual investigation rather than the scheduler.

1. **`src/afi/schema.py`** defines the evidence/report contract. `Claim.support`
   is categorical, not a fabricated probability. Every supported claim needs
   available evidence; `validate_references` rejects unknown/foreign IDs. The
   wrapper supplies run identity and runtime metadata deterministically.
2. **`config.py` and `store.py`** separate settings, credentials and records.
   dotenv loads the local key without printing it. Store uses parameterized
   statements, UUIDs, WAL and transactional event deduplication. Reports live in
   operation folders so follow-ups do not overwrite earlier reports.
3. **`cli.py` and `runner.py`** show process ownership. `investigate` always
   creates a fresh thread. `ask` resumes an existing thread; `show` reads a saved
   artifact. Parent timeout terminates the child and records interruption. This
   is distinct from the Airflow LocalExecutor that runs DAG tasks.
4. **`airflow.py`** implements authentication, bounded HTTP requests, projected
   task metadata, attempt endpoints and paginated logs. Read `logs` alongside
   `tests/test_airflow_adapter.py`. Those tests verify contract handling with an
   explicit HTTP double; real Airflow verification is still a separate gate.
5. **`evidence.py`** explains why the agent cannot read the whole project.
   `capture` assigns an evidence ID, provenance, collection time, availability
   and truncation. `safe_file`, `source_root` and binding identity enforce source
   capability boundaries in code. Search is literal and bounded; AST reports
   imports without executing them. `destination` computes actual statistics
   from an immutable attempt snapshot using fixed SQL.
6. **`agent.py`** is the framework learning core. `create_agent` constructs the
   supported agent graph; tools are ordinary typed Python callables decorated
   through LangChain. The model sees their schemas and chooses calls.
   `ProviderStrategy(Diagnosis, strict=True)` requests schema-constrained output.
   `SqliteSaver` persists the thread at graph boundaries. Middleware and Usage
   implement limits; application checks reject unsupported source-alignment and
   bad reference claims after structured output. Native parse failures remain
   incomplete. A `ReportValidationError` permits one correction using saved
   evidence IDs with retrieval closed, within the original total call/time cap.
   The rejected candidate is saved as `report_repair`; a second invalid result
   stops. No ID is silently rewritten and the validator is never weakened.
   Read `FollowupContext`, `EvidenceContext` and `FinalizeWithinBudget` next:
   they bound only the next model request, retain recent code bodies, and reserve
   finalization; operations with six or more calls also reserve a correction slot.
   Follow-up state may contain prior model inference, explicitly
   labeled for rechecking; fresh investigations receive none of it. Review
   `tests/test_context.py` and the deliberate retrieval-loop double in
   `tests/test_agent_harness.py` for the boundaries and schema preservation.
7. **`demo/source/batch_ingest.py`** is a useful business-code walkthrough.
   Compare transaction scope, commit timing, checkpoint writes and retries.
   `reconcile` reports checkpoint inconsistencies; it does not silently repair
   them. Technical keys support only the documented immutable synthetic contract.
   Follow with enrichment Python/SQL null and reference multiplicity semantics,
   then profiling materialization versus streaming.
8. **`demo/dags/pipelines.py`** supplies realistic dependencies. Processing feeds
   publish and all-done reconciliation. Publish remains an upstream-failed leaf
   after a processing failure, so successful reconciliation cannot make the run
   look successful. Retry metadata belongs to the actual Airflow attempt.
9. **`demo/runtime/runtime.py` and `demo.py`** are operator-only preparation
   infrastructure. They are outside model source capabilities. Controlled child
   termination is SIGKILL, not demonstrated OOM. Attempt snapshots and external
   collection have different timestamps and availability guarantees.
10. **`patching.py` and `validation/checks/test_contracts.py`** separate proposed
    code from executed validation. Edits require one exact context match and an
    exact source revision. Tests are mounted from trusted validation code, never
    generated/modified by the patch. Docker launch errors are blocked validation.

Read tests for boundaries and history before attempting a live run. The explicit
DeterministicModel tests exercise the real framework tool/checkpoint loop, but
their synthetic responses are not evidence of live model reasoning.

`output.py:ResponsePhaseOutput` handles observed protocol concatenation of
commentary and final text. It parses only completed explicit final_answer text;
multiple finals must be byte-identical. Unmarked, conflicting and incomplete
responses remain rejected. Original messages stay in checkpoints; normalization
has a separate operation record and still requires evidence validation.

Source reads also return execution binding provenance already checked internally:
run identity, revision and alignment. Hash-valid source with unknown deployment
remains unknown; the report validator checks the embedded run/revision match.

`retention.py` selects two successful distinct attempt logs from the current
request within16 KB. `formatting.py` derives final schema enums from this
investigation's evidence IDs/availability and checked source provenance. It does
not supply findings. JSON schema output is revalidated as Diagnosis; evidence
integrity checks and semantic review remain necessary.

The agent prompt is versioned in `agent.py` as `investigator-v12`. A model/framework
or prompt change needs reevaluation. Same model ID does not guarantee identical
responses. Do not infer correctness from a well-formatted report.
