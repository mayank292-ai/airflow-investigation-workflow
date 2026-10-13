# Evaluation report — actual results

Ten genuine scheduled Airflow executions completed: 4 success and 6 failure.
All three DAGs are paused. Pipeline execution facts and model quality are separate
gates. No pipeline rerun is needed to investigate this history.

Earlier full offline command: `.\.venv\Scripts\python.exe -m afi.cli evaluate`.
`state/evaluations/949f1985-0561-4cca-a06c-45acacfadd65/pytest.log`: 42 passed /
9 skipped in 13.61s, including context/limits, transport retry and preservation
of incomplete verification and repeated validation logs.
Latest direct pytest regression7d218906 passed46 tests/9 skipped in13.67s,
including strict schema and bounded reference-correction cases. Its terminal
result is recorded explicitly, rather than presented as a captured pytest log.
Explicit deterministic model doubles exercise the actual framework but do not
count as live reasoning.

Nine real Airflow integration tests passed in 13.90s under
`state/integration-6d5cd05f-64f5-406b-b715-47029583b970`. They retrieve existing
run/attempt/log/source/output evidence and test watcher dedup in isolated storage.
Primary ingestion: attempt1 has 8 rows/8 keys/checkpoint0; attempt2 has 16 rows,
8 keys, 8 repeated complete tuples/checkpoint0. Rollback has zero rows. Successful
keyed ingestion has 16 unique keys/checkpoint4. Successful enrichment has 166
rows/100 business keys/66 key excess but zero repeated complete stored tuples:
reference multiplicity is not observed retry duplication. These verify execution
and retrieval, not model interpretation.

## Live development investigations (all retained)

Provider: GPT-5.4 mini, Responses API, low reasoning. LangChain 1.4.4,
langchain-openai 1.7.1, LangGraph 1.2.14, SQLite checkpointer 3.1.1.
Doctor smoke passed with 11 input / 4 output tokens in regular PowerShell.
This Codex executor's OpenAI connection is blocked by Windows socket10013.
Live provider commands run in the user's regular shell against the local project.

Investigation `3c3d3ab4-c677-4544-ab69-a6854256a551`: initial attempt and
continuation rejected at call1/HTTP400. Reasoning plus function tools was
unsupported on Chat Completions. Responses fixed that wire compatibility defect;
a regression checks route, reasoning, tools, schema and store=False.

Primary investigation `90ca084f-7961-41b7-863d-80f120758840`, ingest attempt2:
`local_pipeline_03 / scheduled__2026-10-12T20:11:06.529124+00:00`.

| Operation | Prompt/outcome | Calls/usage | Review |
|---|---|---|---|
| 2aa1842c-06f8-4bd7-a75e-86fc9b63cc42 | v1, saved initial report | 6 model/16 tools; 46279 input/2474 output; estimated $0.04584225; 20.688s | Correct page2/checkpoint/duplicate evidence, but source-path failures and inaccurate missing-contract/truncation claims |
| Source follow-up and first continuation | v2, incomplete context admission | Returned usage/evidence retained | Counting and handoff defects fixed; not completed reports |
| 237f4e70-70b4-469b-a058-f9489cd336fe | v2, tool limit failure | 6 model/21 executed tools; 63730 input/852 output; estimated $0.0516315 | Repeated source requests exceeded next batch bound |
| d7d7f8f9-470d-4565-880d-2b3c06052dc6 | v3, saved continuation | 6 model/16 tools; 63786 input/2669 output; estimated $0.05985; 26.109s | Correct failure/data facts, but source/contract incorrectly called unavailable; quality gate failed |
| a9e551c3-9840-4c2d-a421-95cb124e6cda | v4, saved requested follow-up | 4 model/10 tools; 30148 input/2602 output; estimated $0.03432; 21.375s | Correct source alignment, append commit/checkpoint order, immutable-event contract and resolved availability. Residual error: remediation calls atomic_pages page-scoped, though its transaction spans the entire loop |

The v4 follow-up confirms that findings change with retrieved source. It is not
three fresh evaluations or a fully correct report. Its observed_failure field
describes the follow-up rather than restating the original exception, and line
citations use artifact provenance for full source ranges rather than precise
claim-level lines. Preserve these limitations for review.

v3's coarse context editing removed source bodies before its answer. v4 retains
up to four recent source ranges/24 KB. Request replay kept helper1–61, schema1–7,
DAG1–54 at98652 bytes including buffer. The user authorized context120 KB.
Settings: eight model calls,24 tools,4000 output tokens,180s; conservative admission
reservation $1.728, cumulative authorization still $3. Local records include a
retry allowance and do not constitute provider invoice verification.

Deliberate retrieval-loop doubles verify excess tools cannot execute and the
final allowed model call retains the structured schema. The live completed
reports ended before exhaustion, so they do not prove live limit finalization.

## Planned fresh suite and remaining gates

```powershell
.\.venv\Scripts\python.exe -m afi.cli evaluate --live --repeat 3
```

Plan: 19 operations, ten-case coverage once, primary/rollback/insufficient cases
three times, same-model primary logs-only baseline per iteration. Every operation
creates a new investigation/thread and retrieves evidence independently. The
suite has a900s deadline and checks admission before each operation. stopped_budget
preserves completed and failed cases; planned repeats are not executed repeats.
Prompt-development variants and held-out variants need separate final review.

Rubric: exact failure/attempt location, supported diagnosis, unsupported claims,
uncertainty, duplicates versus risk/multiplicity, specific appropriate remediation,
patch validation, calls,timing,usage. Lexical task-name checks are review aids,
not semantic grading. Explicit human review is required. Valid evidence references
alone do not prove correctness. Logs-only has less evidence, so differences cannot
be attributed solely to reasoning.

Prompt v5 adds generic transaction-scope tracing, precise source lines and
preservation of the original observed failure in follow-ups. It requires fresh
evaluation; the v4 result above is retained without relabeling.

Remaining: fresh v10 evaluations and semantic review of repeated/counterexample
reports. The v7 suite executed all19 operations with substantial failures below.
The NEW post-restart report is saved; prepared container validation
has passed all five checks.
This synthetic suite establishes neither universal reliability
nor workplace adoption or time savings. Saved reports are explicitly fallback
artifacts and are never presented as live runs.

## Requested live patch

Operation e565fab8-3faf-44e0-b39a-2083cd3aa205, same primary investigation:
v5, three model calls/five retrievals,22885 input/1229 output tokens,
estimated $0.02269425,10.25s. Runtime selected exact executed revision and
proposed per-page BEGIN/write/checkpoint/COMMIT with rollback on exceptions.
Only append_pages changed; syntax passed and other helpers/deployed source
matched original content. Static review520c2a8f records limits: stable pages,
single writer, consistent starting state, no uniqueness enforcement, and no
cleanup/reconciliation of the historical duplicates. This patch is now locally
validated by the prepared checks below; it remains undeployed.

Prepared validation now includes repeated upstream failure/completeness,
checkpoint-update failure rollback, partial-page insert rollback, DAG loading
and local relational/profiling equivalence. Every validation attempt retains
separate logs/results. Full Stop/Start now passed with ten runs still paused
and accessible (before80c8ca3a, after88ea2eb4, final restorationecf03e21).
The image built, but validation14f9671f timed out40.657s with no log output.
Execution of tests/generated code is unconfirmed; the prior executed=true flag
is interpreted as unknown. Diagnostic retry adds unbuffered phases and disables
unused pytest plugin auto-loading. Second validationb6d2ebdf collected five
tests in2.79s and stalled at DAG loading; State running/OOMKilled=false. Corrected
the deprecated DagBag import/unsupported include_examples argument against
Airflow3.3.1 source. Next check uses120s/one CPU/384 MB, behavior tests first and
import/parse/stack markers; all assertions unchanged. Earlier pre-restart
ReadTimeout remains recorded separately. Neither stall cause is confirmed.

Fresh-suite settings are now five model calls, context<=100 KB, output<=3000
tokens, keeping any lower configured limits. Same settings apply to both
conditions. In v7, final output and one optional integrity correction occupy the
last two slots within the same five-call limit. These benchmark bounds are recorded and explicitly propagated to
the child process; they require fresh evaluation. Admission $0.885 improves room
within the unchanged $3 cumulative authorization but does not guarantee all19
operations complete. Long follow-up total settings are unchanged.

## Actual validation and post-restart investigation

Validation9d8f2c5c passed four behavior checks, then exited139 during Airflow
import inside pytest. Parsing was isolated to a bare Python child inside the
same container. Validation843aea7e-223f-40ce-9726-a7196c34c951 passed all five
checks in18.281s (pytest15.20s), including the three DAGs and zero import errors.
Log SHA256: baeb1b1d2bc80ab7d518253fa822ccc0ba30654f7d31897f5d98638f3cd6a74d.
Limits remained120s/one CPU/384 MB/48 PIDs/no network/read-only. The exact native
crash cause is unconfirmed; earlier failures have not been erased.

Fresh post-restart investigation05dff001-d1f9-4627-bbe4-7379bdcfeb68:

| Operation | Settings / actual usage | Outcome |
|---|---|---|
| 047a38b8-ed91-4b5b-bfd3-9b99715d1f72 | v5;8 model/15 tools;58384 input/4151 output; estimated$0.0624675;39.25s | Native structured parsing failed; no report accepted |
| bc0c808c-dca8-4154-b3b5-7997a0ba5a82 | v6 strict schema;4 model/17 tools;40025 input/2688 output; estimated$0.04211475;22.891s | Schema passed, application rejected a source evidence ID with an extra trailing character; no report accepted |
| d6be33f2-68bc-4558-9b4f-e2b2f44dd0f0 | v7;5 model/17 tools;55244 input/2508 output; estimated$0.052719;20.75s | Report saved without repair; correct failure/write/checkpoint/source facts, but overstates unresolved event semantics and loosely conflates skipped/upstream_failed |

The v6 thread retrieved executed source, both attempt logs, checkpoints and
destination snapshots independently. The v7 continuation completes that separate
post-restart thread. Development review is in post-restart-d6be33f2-review.json;
it is not an independent human judgment or three fresh repetitions. Local
reservations were reconciled to recorded token usage plus retry allowance where
all responses had usage; this is not a provider invoice or refund.

Prompt v7 allows at most one application-integrity correction, with retrieval
closed and the same overall limits. Both rejected candidates and a second
rejection remain inspectable.18 focused tests and the full46-pass/9-skip offline
suite passed. No live correction was necessary in d6be33f2. The v7 suite exercised
one live correction, but its second candidate still cited unavailable evidence.

## Actual fresh suite: v7

Command: `python -m afi.cli evaluate --live --repeat 3`.
Directory9cce2f73-ca60-4ef1-aaec-95d4abbb89dd. All19 operations executed in528.484s,
with distinct investigation/thread IDs.4 reports were accepted,15 operations
incomplete (14 native parsing errors,1 application-reference rejection).
352833 input/51981 output tokens; estimated$0.49853925. Deterministic accounting
and report identity/reference verification are saved in accounting-fb7d60b8.
Original results and rejected outputs remain unchanged.

| Condition/case | Actual attempts | Accepted reports |
|---|---:|---:|
| Primary, tool access | 3 | 0 |
| Rollback contrast, tool access | 3 | 1 |
| Insufficient evidence, tool access | 3 | 0 |
| Primary, logs-only | 3 | 3 |
| Other seven cases, tool access | 7 | 0 |

The rollback report correctly traces whole-loop rollback, observes0 rows/checkpoint0
and preserves atomic semantics. Logs-only reports identify page2 ConnectionError,
logged partial commits and potential replay risk without inventing measured
duplicates. Some wording overstates inferred downstream facts or truncation.
Development review-notes.json records those defects; independent human review is
pending. The third baseline's lexical task-name aid is false even though its
identity/task-purpose locate the task; this aid is not an accuracy score.

Tool access did not demonstrate a quality advantage. It often reached forced
finalization; output JSON messages were concatenated and rejected, sometimes
after exhausting the output budget. Logs-only usually finished in one call.
Differences involve protocol behavior, retrieval rounds and evidence access.

Public-text phase metadata diagnostics were added during the suite without changing
model requests/settings. Completed0a54fa84 contained two identical final_answer
blocks. v8 normalized only that narrow case and passed56 offline tests/9 skipped,
but fresh20455e20 failed: completed commentary JSON3800 characters followed by
final_answer3604 characters were concatenated.4 calls/8 tools,28384 input/2177
output, estimated$0.0310845,20.688s. Failure retained.

v9 uses explicit completed final_answer labels and preserves original messages.
Multiple finals must be identical; incomplete/conflicting/unmarked output fails.
Compact report instructions reduce output pressure; five-call evaluations reserve
one final output slot, allowing four retrieval rounds. One correction may use an
unused call but cannot exceed five. Manual eight-call operations still reserve
output plus correction.57 offline tests/9 skipped passed in15.31s (7a64d781).

Fresh v9 case42b6e076 parsed successfully but application alignment validation
rejected its report because configuration_schema was not called.5 calls/11 tools,
34277 input/1846 output, estimated$0.03401475,26.531s. No accepted report was saved.
Source reads already verify the run binding and snapshot hashes internally; v10
now returns that checked provenance explicitly and validates it without requiring
an extra binding-tool call. Unknown deployment, foreign run and mismatched revision
still fail. This is a capability-wide provenance change, not a scenario answer.
26 focused tests plus11 boundary checks passed; fresh v10 results remain required.
Ledger after this failure:$1.9572975 including retry allowance; authorization$3.

## Actual v10 and v11 results

The v10 suite03e0ca76 stopped at the authorized budget after three accepted
reports (primary, its logs-only comparison, rollback). All three retained valid
references, but tool-access reports omitted checkpoint/destination retrieval.
It used84874 input/5645 output tokens, estimated$0.089058, in74.14s.
The ledger recorded$2.1354135 including retry allowance and refused the next
$0.885 reservation. The user then explicitly approved$5 total, an additional$2.
Previous ledger entries remain unchanged; approval is saved in budget-approval-5.json.

v11 suite36d051d3-9bbf-420d-8a47-ec70d0b8e120 executed all19 operations in403.031s:
17 accepted reports and two application-integrity rejections. No native parsing
failures occurred.491124 input/29257 output tokens cost an estimated$0.4999995;
accounting-78acc2bd verifies identities/references separately from correctness.
The conservative cumulative ledger afterward is$3.1354125 against$5 approved.

| Condition/case | Actual attempts | Accepted reports |
|---|---:|---:|
| Primary, tool access | 3 | 3 |
| Rollback contrast, tool access | 3 | 3 |
| Insufficient evidence, tool access | 3 | 2 |
| Primary, logs-only | 3 | 3 |
| Other seven cases, tool access | 7 | 6 |

The source-mismatch report54b900e9 claimed verified alignment without supporting
provenance. Insufficient-evidence reportfee35845 cited a missing checkpoint as
support for a fact. Both were rejected on the fifth/final call; rejected candidates
and failure records remain saved. There was no spare correction call.

Development review found material defects among accepted reports. Primary
iterations0/2 falsely describe successfully retrieved logs as unavailable and
omit the immediate exception; context editing had removed their bodies. Iteration1
identifies the exception but never retrieves checkpoint/destination state.
Rollback reports correctly distinguish whole-loop rollback and zero committed
output from the append/retry case. Accepted insufficient-evidence reports leave
alignment and writes unresolved. The termination case does not equate SIGKILL
with OOM, but recommends streaming without establishing the external kill cause.
The enrichment-success case observes166 rows yet still treats documented reference
multiplicity as an unresolved contract and proposes conditional deduplication.
The already-streaming success case retrieves destination state for publish rather
than the transforming task, so the missing-artifact result does not establish
whole-run destination unavailability. Baseline iteration2 recommends checkpoint
persistence before writes without establishing a safe atomic boundary.

These are development review findings, not independent human approval or an
accuracy score. Valid references and completion do not establish correctness.
The full results, structural accounting and separate review notes are retained.

## v12 changes and pending live checks

Request context now retains up to two distinct successful attempt-log bodies,
bounded to16 KB, alongside four source ranges/24 KB. Complete stored messages
remain untouched. Final forced calls use a schema whose evidence-ID enums come
only from this investigation: supported claims require available records, while
uncertain claims may cite known unavailable records. Verified alignment is allowed
only when retrieved provenance proves it. The schema supplies no diagnosis,
remediation text or scenario labels; application validation still runs afterward.
Original provider commentary/final messages remain preserved.

64 offline tests passed and nine live-service tests were skipped in24.32s
(f02489d7-0c70-4d4f-8966-5e64e71ee616). A focused live plan now supports
`evaluate --live --repeat 3 --key-cases-only`:12 new operations covering the
three key cases and primary logs-only comparisons. This scope is recorded and
does not replace the earlier ten-case suite. Current live semantic results and
the real watcher model-launch/dedup check remain pending.

The focused v12 suite subsequently executed all12 operations in269.812s, with
12 accepted reports and zero parsing/integrity failures. Each condition had three
new investigations/threads.314070 input/16775 output tokens cost an estimated
$0.31104; accounting-9f89d69f verifies identity/reference integrity. The cumulative
retry-adjusted ledger is$3.7574925. Directorye1e95c9e-5f4d-421f-bc27-f0a8e328e9ee
retains all reports and qualitative development review-notes.json.

All primary repetitions identify ConnectionError/page2; the false unavailable-log
claim did not recur. Rollback repetitions identify the whole-loop transaction
and observe zero committed rows/checkpoint0. Missing-evidence repetitions retain
unknown alignment and unresolved writes. Primary iteration0 does not read source
bodies or destination/checkpoint; iteration1 inspects those but overstates duplicate
uncertainty; iteration2 connects exception, write order,16 rows/8 keys and stale
checkpoint0/watermark2. It is an illustrative result, not an accuracy estimate.
One missing-evidence repetition never requests checkpoint/destination. Several
source claims omit precise file/line citations, and recovery directions still
require review before action. The live watcher check is requested separately.

## Actual watcher launch and persistence check

Verification watcher-67c603a7-e620-45ad-831d-7fe8bba0c407 passed in175.844s.
The main event table started empty. The first real scan discovered six failed
task/attempt identities and launched six fresh v12 investigations; all completed.
A second CLI process preserved the same event IDs and operation counts, with no
new investigations or model operations. All six report identities/references
were independently checked by deterministic operator code. Estimated returned-token
cost:$0.21274575 (222965 input/10116 output tokens).

The current source-mismatch report retains unknown alignment; the SIGKILL report
does not declare OOM and observes zero destination writes. Enrichment reports
measure332 rows/166 exact duplicate tuples, but still conflate exact stored
duplicates with legitimate distinct reference-match multiplicity. One says the
task committed checkpoint0, although unchanged checkpoint state does not prove
a checkpoint write. The primary connects ConnectionError, partial commits,
checkpoint0 and16 rows/8 keys; prior attempt logs remain uninspected, and its
recovery direction needs a verified atomic boundary and historical reconciliation.
The insufficient-evidence report never requests checkpoint/destination. These
qualitative limitations are kept alongside the successful operational check.

The final conservative ledger is$4.182984 of the approved$5 total. It refuses
the next$0.885 bounded reservation. No additional model work is needed to prove
the executed gates, and no authorization or ledger entry was reset. Ledger
amounts include retry allowance and are not provider invoices.

The user subsequently approved raising the total to$15 (with$20/$25 only if
required). Configuration is now$15, recorded in budget-approval-15.json; no
provider request or ledger reset occurred for this change. Both the$1.728 standard
and$0.885 bounded demo reservations now pass admission against the preserved
$4.182984 ledger. The historical$5 refusal above remains an actual result.
