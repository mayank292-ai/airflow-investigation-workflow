# Actual engineering journal

This records observed implementation/environment problems. Controlled DAG faults
are a separate preparation design; none has yet been executed through real
Airflow in the verified project environment.

## Local executor and Docker before firmware repair

Symptom: Docker engine pipes were absent and the service was stopped. Attempts
to launch Docker Desktop from the restricted executor produced UI processes,
but no reachable engine. Windows denied service startup. WSL status queries
were also denied; that did not prove WSL was absent.

Evidence: command outputs reported missing engine pipes/access denied. The user
then supplied Docker's “virtualization disabled in firmware” screenshot.
Read-only registry identified HP Pavilion x360 15-cr0xxx / Intel i7-8550U.
Official Intel specifications confirmed VT-x/EPT, and HP documentation supplied
the BIOS Configuration -> Virtualization Technology procedure.

Change: the user enabled firmware virtualization and restarted. No agent changed
firmware or rebooted the computer.

Verification: the user's regular PowerShell output on October 12, 2026 showed
Docker client/server 29.8.2 and a Linux/amd64 engine under Desktop 4.95.0.
This proves engine reachability from that user shell, not from the restricted
executor and not Airflow execution.

## Docker CLI relocation and restricted access

Symptom: `docker` was absent from this executor PATH after restart. The old
system-wide executable path no longer existed. Process paths identified the new
per-user installation under AppData/Local/Programs/DockerDesktop.

Reproduction: launching `resources/bin/docker.exe version` at that absolute path
returned Windows “Access is denied”. Directory enumeration was also denied.

Change: added `scripts/airflow.ps1` to discover either installation path and make
the CLI available to its child processes. It is intended for a regular user
PowerShell window. It checks engine/resources, migrates metadata and checks DAG
loading before requesting scheduled preparation.

Verification: PowerShell syntax parsed without errors; the Windows resource
probe executed successfully. Actual script startup/container checks remain
blocked until the regular user shell runs it. This is not a solved executor ACL.

## Restricted old pytest temporary/cache directories

Symptom: first regression invocation after restart produced 15 fixture setup
errors and 3 passing tests. Errors were Windows permission failures on old temp
and cache directories, before affected test bodies executed.

Reproduction: `.venv/Scripts/python.exe -m pytest -q` in the restricted executor.

Change: reran with a new workspace `--basetemp` and `cache_dir`; changed offline
evaluation to allocate both under each new evaluation UUID. No tests or
assertions were weakened and old evidence was retained.

Verification: 21 tests passed after the history tests were added. The subsequent
evaluation with run-binding checks also passed; exact count is in its pytest log.

## Earlier bounded live provider check

Symptom: a live doctor attempt returned an OpenAI connection error before network
permission was available. It was not treated as proof of bad key/billing.

Change: used the official secure key provisioning/local destination flow and
scoped network access. Secrets were written locally, not printed.

Verification: a bounded GPT-5.4-mini doctor response used 11 input / 4 output
tokens. This proves one API call; no live diagnostic or patch quality claim follows.

## Preservation work awaiting real integration

Added post-execution API collection with distinct collection UUIDs and per-attempt
log records. Added artifact hashes and restart verification. API doubles prove
manual runs are rejected, collections remain distinct, attempts remain separate,
and mutation/insufficient counts fail verification. These are unit checks; real
scheduled execution, abrupt failure and restart remain pending.

## First real Airflow startup: authentication mount defect

Symptom: the user-run Start script pulled both pinned images, initialized the
PostgreSQL metadata tables, then API container exited with code 1. Scheduler and
DAG processor continued running.

Reproduction/evidence: the user retrieved actual Compose logs. The API traceback
ended at `SimpleAuthManager.init`, `open(password_file, "a+")`, with
`OSError: [Errno 30] Read-only file system` for the deployment password JSON.
The Airflow 3.3.1 official implementation also opens that file with write modes.
Actual DAG processor stats recorded 3 DAGs and 0 errors. Scheduler logged
LocalExecutor loading and worker startup. This was an implementation defect,
not an injected demonstration pipeline failure.

Cause: Compose placed the authentication file inside a read-only deployment
mount. The read-only source boundary was applied too broadly to a mutable
authentication file.

Exact change: point `SIMPLE_AUTH_MANAGER_PASSWORDS_FILE` at
`/opt/airflow/simple-auth-passwords.json` and mount only the existing host JSON
there with `rw`. Retain read-only deployment/DAG mounts. Add RestartAirflow to
recreate services without repeating downloads/migration or clearing data. Add
bounded container logs after future startup failures.

Verification: YAML loaded successfully; password destination/writable mount and
read-only source mount were checked. PowerShell script syntax passed. The actual
corrected API container startup passed after the user-shell RestartAirflow
command. All four services became healthy, and this executor subsequently
authenticated and checked Airflow directly through its REST API.

## Genuine scheduled preparation and real adapter integration

Ran `demo prepare-history` through the now-reachable local REST API. Airflow's
scheduler produced 10 real completed runs: 4 success and 6 failure. Each DAG was
paused when its count reached 3/3/4. History, logs, attempt snapshots, output DBs
and post-execution collections were preserved. `demo verify-history` passed.

Eight opted-in real integration tests passed in 6.16 seconds. Primary ingestion
had 8 rows/8 event keys/checkpoint0 at attempt1 exit and 16 rows/8 keys/checkpoint0
at attempt2 exit. Rollback retained zero rows. Successful keyed ingestion had 16
unique keys and checkpoint4. SIGKILL logs retained uncertainty. Source mismatch
and withheld evidence were observed through actual tools. A watcher scan in
isolated test storage found six actual failures and persisted deduplication.

The first integration-test CLI command passed an expression directly after
`--basetemp=`, which PowerShell rendered incorrectly. Pytest rejected the path
before test collection. Corrected to explicit path variables; the retained second
invocation passed without weakening assertions.

## Model wire compatibility defect

The user-shell doctor smoke call passed (11 input/4 output tokens), but the first
full investigation failed before tools at `OpenAIInvalidRequestError`. Initial
failure logging recorded only exception type, hiding the actionable parameter.
Added bounded provider diagnostic fields and secret redaction, then explicitly
continued the saved investigation. The returned HTTP400 named reasoning_effort
and said function tools with reasoning were unsupported on Chat Completions for
this model. The smoke test had no tools and therefore missed the incompatibility.

Changed the investigator to LangChain's supported Responses API flag, retaining
GPT-5.4 mini and low reasoning. Added a wire regression test covering route,
reasoning, function tools, structured schema and disabled provider state storage.
Three focused provider/framework tests passed. A fresh live investigation is
required before calling the defect fully verified.

First-call definitive HTTP400 reservations are now released while the failed
operations remain. A one-off reconciliation of the older reservation initially
called `Store.operation` while holding another SQLite write transaction, causing
`database is locked`. Moved the audit insertion after committing the ledger
update; the retry recorded the audit successfully. The first intent folder is
retained, and no provider invoice or higher spend approval was inferred.

## Live context admission and retrieval exhaustion

The Responses route produced a real initial report in investigation 90ca084f,
operation 2aa1842c. Review found useful failure/checkpoint/duplicate evidence but
absolute source paths failed and business-contract limitations were inaccurate.
Known deployed aliases now resolve into the same manifest; arbitrary paths remain
rejected. Invalid ranges return start/count bounds and allowlisted file hints.

A live follow-up hit context admission. The counter included local response
metadata and parsed-object duplicates. Count request-directed content/arguments/IDs
instead. Context editing alone still hit the hard limit on continuation; a
same-investigation handoff now bounds older turns in a request copy, retaining
complete checkpoints. Explicit serializer types removed the checkpoint warning.

The next continuation passed admission but requested more than 24 tools. After
six model responses /21 retrievals, ToolCallLimitExceededError rejected the next
batch. Checkpoint usage matched the record (63730 input/852 output). Reconciled
the reservation to twice known token cost for retry allowance, audit
f5867e94-dcca-4647-ab6b-f7c8f69edbe0; cumulative authorization remains $3.

Changed native tool limits to continue with rejected-call errors, and added
FinalizeWithinBudget to close tools when exhausted or before the final model
call while retaining ProviderStrategy. Looping-model doubles prove partial-batch
enforcement and final schema output. The first implementation used self.tools for
the retriever, colliding with AgentMiddleware's tool list. Tests caught TypeError
at construction; renamed it self.retriever. Test instrumentation also initially
ignored response_format at bind_tools; corrected its recording. Eight focused
tests passed. Neither local defect reached a live provider call.

## Completion did not imply source-aware quality

Live v3 operation d7d7f8f9 completed in six model calls/16 tools. Failure and output
facts were correct, but it called source/immutable-event semantics unavailable
despite successful reads/contract and contradicted its partial-commit assessment.
Saved report unchanged; quality gate failed.

Cause: generic ClearToolUsesEdit removed source bodies after later log reads.
EvidenceContext retains up to four recent successful source ranges/24 KB around
generic clearing, without new retrieval or checkpoint mutation. Tests cover late
log displacement, bounded retention and original-message preservation. Request
replay kept helper1–61, schema1–7, DAG1–54 at98652 bytes including buffer. Applied
the user's cap increase: context120000, per-operation admission1.8 ($1.728 worst
reservation), total approval still$3. Thirteen focused tests passed.

Live v4 follow-up a9e551c3 then completed in four model calls/10 tools. Correctly
verified source alignment, exact append commit/checkpoint ordering, immutable
event identity and resolved prior source-availability claims. Review still found
an incorrect description of atomic_pages as page-scoped; it wraps the entire loop.
The report's observed_failure field also answered the follow-up instead of
restating the original exception. Preserve these defects in evaluation rather
than implying a well-formed report is fully correct.

Nine real integration tests passed in13.90s, integration-6d5cd05f. A prior command
again used an expression directly after --basetemp= and failed before collection
(integration-13493716); explicit path variable corrected it. Tests distinguish
enrichment's66 key excess/zero repeated full tuples from eight exact ingestion
duplicates. A historical binding input_rows=100 counts auxiliary records.json,
while paginated ingestion has16 events. Keep that binding unchanged; the tool now
cautions that this field alone cannot establish paginated-source completeness.

Archived the unchanged private runtime by its SHA256 after verifying it equals
all ten execution-recorded infrastructure hashes. Archive metadata explicitly
labels collection after execution. No historical binding/source was rewritten,
and this archive is outside the investigator's tools. Prompt v5 adds generic
transaction-scope tracing, claim-level source lines and preservation of the
original observed failure during follow-ups; reevaluation remains required.

## Requested patch generation and verification orchestration

Live v5 propose-fix saved e565fab8 in3 calls/5 retrievals (22885 input/1229 output,
estimated$0.02269425). It changes only append_pages to per-page writes/checkpoint
in one transaction. Static AST/hash/syntax review proved other helpers and deployed
source unchanged. Its claim that no existing cleanup is required must be scoped:
this prevents the tested future retry pattern from consistent state; historical
duplicates/checkpoint divergence remain. Review520c2a8f records stable pagination,
single writer and reconciliation preconditions. No generated code ran on the host.

The inline wording 'followed by' was pasted into one PowerShell command, causing
duplicate Action binding before any Docker action. Added verify-demo.ps1 to
sequence restart/build/validate/fresh investigation as one invocation. First
wrapper execution timed out during pre-restart overview retrieval, so neither
Stop/Start nor validation ran. Subsequent direct health and history checks passed;
Windows freeRAM was1.03 GB. A causal link to memory was not established.

Added one bounded retry for GET transport errors (no mutation retries), structured
incomplete verification artifacts and a120s verification request budget. A failed
precheck no longer prevents service recovery; post-restart verification must pass.
Wrapper stops project services during build/validation to free RAM, restores them
in finally, and checks history afterward. Sixteen focused tests and script syntax
checks passed; real current-history verification d71dc121 passed in3.703s.

Validation now preserves each invocation's UUID/log/result instead of overwriting
previous logs. Trusted checks strengthened checkpoint-update and partial-page
insert rollback without weakening existing requirements. Process-double tests
prove archival only, not real container execution.

Fresh evaluations use5 calls/100 KB/3000 output tokens to bound spending across
19 planned operations (same bounds for logs-only/tool-access). Inspection caught
that execute_bounded initially reloaded .env in the child rather than forwarding
the admitted settings. Explicit AFI settings now propagate via child environment;
regression checks only non-secret selectors. No live benchmark ran with the
incorrect bound. Cumulative authorization remains$3; model quality is unverified
under these new fresh-suite settings until actual evaluation.

## Real restart succeeded; validation stalled without output

The corrected wrapper stopped all four project containers, restarted PostgreSQL
and Airflow, and passed history before80c8ca3a and after88ea2eb4. It stopped
services for RAM and built afi-validation:3.3.1 successfully (247.6s, base
extraction215.3s, fixed pytest install24s).

Validation14f9671f reached40.657s and was killed. Its log is zero bytes, so neither
collected tests nor generated-code execution was established. The original
executed=true flag reflected Docker CLI launch and was too strong. Preserve it
unchanged; interpret execution as unknown. Services restored healthy and history
ecf03e21 passed. The new live investigation was skipped after validation failure.
No DAG was unpaused/triggered. Post-restart source check8a9683b6 verified nine
known revisions; one unavailable execution binding stayed unavailable.

Added a trusted unbuffered runner with timestamped Python/pytest/collection/test
markers and prepared package version assertions. Disable unused pytest plugin
auto-loading. Retain all five checks and40s/384 MB/0.5 CPU/48 PID/network-none/
read-only bounds. On timeout collect only bounded Docker State fields, mark
execution unknown and preserve the log. Failed logs print a bounded tail.
Five focused tests and script/runner syntax passed. First-stall cause unconfirmed.
Retry can skip the completed restart/image-build gates; explicit trusted runner
entry point uses the built image. Services still stop for RAM and restore in finally.

## Second timeout narrowed to DAG loading

Validationb6d2ebdf started Python3.12.13, imported pytest and collected five
tests in2.79s, then stalled at test_dag_loading until40s. Container State was
running, OOMKilled=false. No behavior test completion was established. Services
restored healthy and history2191b233 passed. Both failures remain separate.

Official tagged Airflow3.3.1 source exposed an independent validator API defect:
airflow.models.dagbag is a deprecated shim; parsing uses
airflow.dag_processing.dagbag. Its constructor no longer accepts include_examples.
Corrected both against source. This was not a generated-patch defect and would
fail after import. The exact stall cause still awaits diagnostic traces.

Added import/parse markers and15s repeated faulthandler traces. Behavior checks
run before DAG loading; assertions remain intact. Increase the identified heavy
check's bounded allowance to120s/one CPU, retaining384 MB/48 PID/no network and
read-only mounts. Each result records its limits. Two focused tests passed;
full regression949f1985:42 passed/9 skipped in13.61s. New live investigation now
runs before optional patch validation, separating those acceptance gates.

## Bare Python DAG validation passed; report integrity still rejected

Validation9d8f2c5c passed all four behavior tests, then Airflow import inside
pytest exited139 after a15s faulthandler trace in typing.py. The partial stack
does not establish a root cause or OOM. Isolated DAG parsing in a bare Python
subprocess inside the same restricted container, retaining all assertions and
limits. Validation843aea7e passed all five checks in18.281s; its log records the
three expected DAGs and zero import errors. No patch/deployed source changed.
Final service restoration/history1e61cbb2 passed; ten runs remain paused.

NEW post-restart thread05dff001 initially ended with StructuredOutputValidationError
(operation047a38b8,8 model calls/15 tools). Its earlier error record lacked the
parser details. Added bounded redacted schema diagnostics and strict provider
output. v6 continuationbc0c808c passed native schema in4 calls/17 tools but
application integrity rejected one fabricated/typo evidence ID: it added an
extra trailing character to a real source ID. The raw candidate remains in the
checkpoint; no accepted report was claimed. All responses carried usage, so
the local reservation was reconciled with retry allowance, not treated as a
provider billing refund.

v7 saves invalid candidates as rejected report_repair operations and permits
one model correction from existing evidence IDs. Tools close for correction;
total call/time/spend caps do not reset. Reserve two final slots when at least
three calls are allowed. A second invalid candidate fails the same validator;
there is no automatic ID substitution. Explicit model doubles prove success
after one correction and failure after a second invalid candidate.18 focused
tests passed in9.26s; full regression7d218906 passed46/9 skipped in13.67s.
Live continuation and repeated evaluations remain required.

v7 continuationd6be33f2 completed the separate post-restart thread05dff001 in
5 calls/17 tools,55244 input/2508 output, estimated$0.052719,20.75s. No repair
was needed. Failure/write/checkpoint/source facts are supported; development
review retains overstatement of unknown ingestion semantics, loose skipped/
upstream_failed wording and ambiguous cumulative-versus-attempt row wording.
Recorded the review outside investigator access. This passes post-restart live
execution/access, not the full quality/repeated-evaluation gate. Ledger shows
$0.8300205 including retry allowance, within unchanged$3 authorization.

## Fresh suite exposed phase concatenation and constrained retrieval

v7 suite9cce2f73 executed19 operations/ten cases and three key repetitions in
528.484s.4 accepted reports/15 incomplete; estimated$0.49853925. All failures
remain, including14 native parse errors and one unavailable-reference correction
that failed again. Some attempts used all three retrieval rounds on source before
retrieving logs/destination. No tool-access superiority is established.

Added public text-block IDs/phases/lengths to diagnostics, excluding reasoning
and credentials, without changing the running suite's model requests. Completed
0a54fa84 revealed two identical final_answer messages; incomplete responses also
showed commentary and truncated finals. Current ProviderStrategy concatenates all
text. Responses documentation distinguishes commentary from final_answer and
requires preserving original phase labels for replay.

v8 identical-final normalization passed28 focused and56 full offline tests/9
skipped. Fresh20455e20 still failed with completed commentary3800 chars and
final_answer3604 chars.4 calls/8 tools, estimated$0.0310845. The narrow handler
refused it; no report was accepted. v9 follows explicit completed final phases via
supported wrap_model_call/ModelResponse, retains original messages and a separate
normalization operation, and refuses differing finals, incomplete or unmarked
output. The origin of multiple provider messages remains unconfirmed.

Compact instructions and log prioritization address output/retrieval pressure
without scenario labels. Five-call evaluations allow four retrieval rounds plus
final output; correction only uses an unused call. Eight-call operations retain
two reserved slots.29 focused tests passed11.24s; full57/9 skipped in15.31s.

Fresh v9 case42b6e076 parsed successfully, then failed source-alignment validation:
the agent never called configuration_schema.5 calls/11 tools, estimated$0.03401475.
Candidate/failure preserved. Inspection showed source_root already checks binding
identity/revision and all source hashes, but source_read omitted that provenance.
v10 returns the checked binding/alignment with source reads and validates matching
run/revision before accepting alignment. Unknown deployment remains unknown.
Added foreign-run/revision/unknown-deployment rejection tests;26 focused checks
passed, then11 boundary tests passed after adding the stricter assertions.
Also clarify that destination multiplicity wording is a generic statistical caveat,
not the selected run's business contract. Fresh evaluation remains required.

One diagnostic pytest command omitted parent temp-directory creation, causing
fixture WinError3. Creating the directory first corrected the invocation;4 tests
passed. This was test orchestration, not an injected DAG/model failure.

The user approved an additional$2 ($5 total). Configuration and a separate
approval audit record changed; previous spending entries were preserved. v11
retrieval hints and source-range schema constraints passed32 focused checks.
The actual v11 suite36d051d3 completed19 operations:17 accepted/two rejected,
403.031s, estimated$0.4999995. Accounting verifies references/identity only.
Primary iterations0/2 falsely called retrieved logs missing after later source
navigation cleared their bodies. Other defects remain in development review:
documented enrichment multiplicity treated as uncertain, missing evidence from
a publish selector generalized too broadly, and some weak recovery directions.

v12 preserves two distinct successful attempt-log bodies (16 KB) in request
context and constrains final report IDs/availability/alignment using retrieved
provenance. It never supplies the diagnosis or edits stored messages.31 focused
checks passed; full regression f02489d7 passed64 tests/nine skipped in24.32s.
Added an explicitly scoped12-operation key-case plan to conserve the approved
budget for live watcher execution. The watcher verification wrapper records a
first real model-launch scan and a second scan in a new process, compares persisted
events/operation counts, and never clears events or reruns DAGs. Live checks pending.

Focused v12 suitee1e95c9e executed12/12 operations in269.812s: all accepted,
314070 input/16775 output, estimated$0.31104. Cumulative conservative ledger
$3.7574925/$5. All primary repetitions now name ConnectionError/page2; no false
unavailable-log claim recurred. Rollback and insufficient-evidence contrasts are
preserved. Coverage is variable: primary0 omitted source bodies/state; primary1
overstates uncertainty; primary2 links measured state and write order. Record
all12 qualitative reviews instead of equating completion with correctness.
Watcher verification is requested in the regular shell; executor API access remains
blocked. No dependency wheels are present in pip's local wheel cache, so a clean
offline lock install is not established.

The real watcher wrapper passed in175.844s (watcher-67c603a7). Starting from an
empty main event table, it discovered six failures and completed six v12 reports.
The second CLI process created no new events or operations. Operator report
identity/reference checks passed. Estimated$0.21274575,222965 input/10116 output.
Unknown source alignment and SIGKILL uncertainty remain preserved. Enrichment
reports still overstate uncertainty about exact duplicate tuples versus distinct
reference matches, and recovery directions need manual review. No diagnosis is
claimed universally reliable. Ledger$4.182984/$5 cannot admit the next$0.885
operation; preserve it and require explicit additional authorization for future
model work. All requested local execution gates have now run successfully.

The user subsequently authorized$15 total, with$20 or$25 only if required. Set
AFI_APPROVED_TOTAL_USD=15 and recorded budget-approval-15.json. Expanded Settings'
configuration bound to25 to support that authorization; default authorization
remains3 and no automatic increase occurs. Existing ledger/spending entries stay
unchanged. No provider request was made merely to change the authorization.
Actual configuration/admission verification passes for standard ($1.728) and
bounded ($0.885) runs against the unchanged$4.182984 ledger. Six budget/limit
tests passed in5.71s. An initial test invocation hit denied default temp/cache
paths; the corrected invocation used a new workspace temp/cache directory.
