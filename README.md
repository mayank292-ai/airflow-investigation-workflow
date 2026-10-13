# airflow-investigation-workflow

A local CLI investigator for unfamiliar Airflow failures. The runtime uses one
LangChain `create_agent` agent, an OpenAI model, bounded evidence tools, and
LangGraph SQLite checkpoints. Codex helps develop the code; it is not the runtime
investigator. No frontend or hosting is required.

## Source repository and local demo data

The repository contains application source, demonstration pipeline templates,
trusted validation checks, tests, scripts and documentation. Credentials, installed
dependencies, deployed copies, native logs, databases and generated investigation
artifacts remain local and are excluded by .gitignore.

The recorded run IDs, report UUIDs and validation results below describe the
verified installation on this computer. They are not bundled runtime data in a
fresh clone. On a new machine, install the locked environment, configure .env,
start the services and run PrepareHistory once to generate genuine scheduled
executions. Use the actual new IDs in that installation's state/history.json.
Saved-artifact examples in the handbook apply only where those artifacts exist.
Do not reprepare history on the existing verified installation.

**Current verification:** Airflow is healthy, three DAGs loaded with zero import
errors, and 10 real scheduled runs completed (4 success / 6 failed). All DAGs are
paused. Nine real Airflow integration tests passed, including observed retry
duplicates, rollback, missing evidence, attempts/logs and watcher deduplication.
History/access/hash checks pass. The OpenAI smoke call passed from regular
PowerShell. Three live Responses API reports are saved. The v4 follow-up retrieves
the executed helper and resolves earlier inaccurate source/contract limitations;
review still found a transaction-scope recommendation error. Bounded finalization
and source retention have regression coverage. The latest
offline suite passed64 tests (nine real-integration tests skipped in that invocation).
Full Stop/Start preserved the ten runs and evidence; validation image built.
The isolated patch passed all five prepared container checks in validation
843aea7e, including DAG loading and retry/rollback behavior. Earlier timeouts and
an import crash remain recorded. NEW post-restart investigation05dff001 saved
reportd6be33f2 after independent retrieval; prior parsing/unknown-ID failures remain
recorded. Review confirms the failure/write facts but retains semantic uncertainty
and task-state wording defects. The v7 bounded reference correction passes
deterministic tests. The v7 fresh suite executed all19 operations:4 accepted
reports/15 incomplete outcomes, recorded without backfilling diagnoses. Current
v11 suite executed all19 operations:17 accepted and two integrity rejections.
Development review retains false log-unavailability claims, incomplete write
inspection and weak recovery directions. v12 retains bounded attempt-log bodies
and constrains final evidence IDs/alignment labels. The fresh focused suite passes
integrity checks for12/12 reports over three repetitions of each key condition.
All primary reports locate ConnectionError/page2; evidence coverage and recovery
directions still vary, as recorded in qualitative review. The real watcher launched
six new investigations, all completed; a second process created no new events or
operations. Accepted reports require semantic review. All earlier candidates
and outcomes remain saved.

The conservative ledger records$4.182984. The user subsequently approved a$15
total, now configured and sufficient to admit fresh demo investigations. The
previous$5 approval and all spending entries remain preserved. Do not reset the
ledger to replay a demo. This ledger includes retry allowance and is not a
provider invoice.

The user confirmed Docker Desktop 4.95.0 / Linux engine 29.8.2. This Codex executor
can reach local Airflow REST, but cannot launch the relocated Docker CLI or
connect to OpenAI (Windows socket error 10013). Run provider/container commands
in regular PowerShell. The recorded authentication mount startup defect is fixed
and real corrected service startup passed.

A live v5 isolated ingestion patch is generated and locally validated. It has
not been deployed and does not clean historical duplicates.

## Start on this Windows computer

For Bash on native Linux, see [the Linux terminal runbook](docs/linux-terminal.md).
The Python commands and Compose services have Linux equivalents; the `.ps1`
helpers and Windows memory probe are specific to Windows. The existing recorded
end-to-end verification used Windows with Docker Linux containers, not a native
Linux host.

Virtualization has been enabled. Save work and close unused apps first: the last
probe showed less than 1 GB of available Windows memory on an 8 GB machine.
The startup script requires at least 2 GB available on Windows and approximately
4 GB available to the Docker Linux engine. It does not close apps or change BIOS,
Windows features, Docker settings, or credentials.

Open a **regular Windows PowerShell window outside Codex**, then run:

```powershell
cd 'C:\Users\mayank\Documents\Codex\2026-10-12\github-plugin-github-openai-curated-remote\outputs\airflow-investigation-workflow'
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\airflow.ps1 -Action Start
```

The policy setting applies only to that PowerShell process. The script discovers
both per-user and system Docker CLI locations, adds the CLI to its process PATH,
checks resources, validates Compose, pulls pinned images, migrates PostgreSQL,
starts health-checked services, and checks DAG import errors through the REST API.
First image downloads can take several minutes. A failed check stops the script;
inspect the error before retrying. Startup preserves existing data and never
unpauses or triggers DAGs.

For the observed read-only authentication-file startup failure, the corrected
Compose configuration needs container recreation. Run this from regular
PowerShell; it preserves PostgreSQL and data and avoids repeating initialization:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\airflow.ps1 -Action RestartAirflow
```

The authentication JSON has its own writable file mount because SimpleAuthManager
opens it with `a+`/`r+`. Deployed DAGs and business source remain read-only.
Startup failures now include bounded container status/log diagnostics.

Airflow UI: <http://localhost:8080>. Synthetic local login: `airflow` / `airflow`.
The API binds only to loopback. These demo credentials are not production secrets.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\airflow.ps1 -Action Status
```

## Prepare genuine scheduled history once

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\airflow.ps1 -Action PrepareHistory
```

This runs **before the interview**, takes approximately 5–8 minutes, and requires
healthy services. It arms a future, six-minute schedule window and lets Airflow's
scheduler create and execute runs. It requests at least 3, 3, and 4 terminal runs
for the three opaque DAGs; individual DAGs pause once their target is reached.
The fixed end date bounds creation if the preparation process is interrupted.
Normal completion and errors attempt to pause all demonstration DAGs. If the
process is forcibly killed, pause them in Airflow and inspect partial history.

It never inserts scheduled-run records, never relabels manual runs, and never
uses historical catchup to claim past execution. Partial completed-run inventory
is saved as it is collected. Existing history cannot be silently rearmed.
`demo trigger` exists only for explicitly labeled manual development runs.

`state/history.json` records actual IDs, states, times, failed task identities,
and collection paths. Native logs and PostgreSQL metadata persist separately.
Every run owns its input/output directory. Attempt-specific database and
checkpoint copies are captured at task exit. An external preparation collector
also saves bounded API logs and metadata after terminal execution, with collection
times and artifact hashes. These post-execution collections are not described as
execution-time measurements. Abrupt worker death can still prevent task-exit
snapshot capture; absence must remain explicit.

## Restart and investigate existing runs

The completed demonstration inventory already exists. **Do not run preparation
again.** Run IDs are Airflow's genuine scheduler-generated identifiers; their
timestamps do not encode the private fault scenario.

| Demonstration role | DAG / task | Existing run ID |
|---|---|---|
| Primary partial-commit/retry case | local_pipeline_03 / ingest, attempt 2 | scheduled__2026-10-12T20:11:06.529124+00:00 |
| Contrasting rollback case | local_pipeline_03 / ingest, attempt 2 | scheduled__2026-10-12T20:12:06.529124+00:00 |
| Insufficient source/destination evidence | local_pipeline_03 / ingest, attempt 2 | scheduled__2026-10-12T20:13:06.529124+00:00 |
| Successful ingestion | local_pipeline_03 / ingest, attempt 1 | scheduled__2026-10-12T20:14:06.529124+00:00 |

Alternatively, `-Action VerifyRestart` performs Stop/Start and checks before and
after, preserving all data. `-Action BuildValidation` prepares the fixed image.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\airflow.ps1 -Action Stop
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\airflow.ps1 -Action Start
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\airflow.ps1 -Action VerifyHistory
$history = Get-Content .\state\history.json -Raw | ConvertFrom-Json
$history | Format-Table dag_id, run_id, state
```

Do not delete Docker volumes or run `docker compose down -v`. The verification
command checks existing scheduled metadata, DAG pause states, log access and
preserved artifact hashes; it does not itself make a model call or prove a fresh
investigation works.

Choose the primary failed ingestion run in the Airflow UI and use its actual ID:

```powershell
.\.venv\Scripts\python.exe -m afi.cli doctor --model
.\.venv\Scripts\python.exe -m afi.cli investigate local_pipeline_03 '<actual-run-id>' --task ingest --attempt 2 --fresh
```

Every `investigate` invocation creates a new investigation UUID and checkpoint
thread, even for the same historical run. It does not delete previous reports,
return cached diagnoses, reset watcher deduplication, or rerun a pipeline.
The terminal prints actual model calls and tool evidence IDs. Use IDs shown by
Airflow rather than assuming the number of attempts.
Add `--bounded` for the same five-call/100 KB/3000-token limits as fresh evaluation,
without changing `.env`. Short operations reserve one final output call;
one correction is possible only if a call remains within the total limit.

```powershell
.\.venv\Scripts\python.exe -m afi.cli ask '<investigation-id>' 'Explain how the commit and checkpoint order affects retry safety. Separate observed duplicates from risks.'
.\.venv\Scripts\python.exe -m afi.cli propose-fix '<investigation-id>' 'Propose the smallest retry-safe change for these immutable synthetic events. Retrieve the exact source and preserve the output contract.'
.\.venv\Scripts\python.exe -m afi.cli validate-fix '<investigation-id>' '<patch-operation-id>'
.\.venv\Scripts\python.exe -m afi.cli show '<investigation-id>'
```

`show` is explicitly a **saved report**, not a live model run. An interrupted
investigation requires `continue <investigation-id>`; a fresh evaluation uses
`investigate --fresh`. Follow-ups append operations to the existing thread.

## Runtime and storage

Containers: PostgreSQL 16.10, Airflow 3.3.1 API/UI, scheduler with LocalExecutor,
and DAG processor. The host runs Python 3.12, the investigator CLI and model API
calls. Host -> Airflow uses `127.0.0.1:8080`; task execution -> API uses Docker
service DNS `api:8080`; PostgreSQL uses `postgres:5432`. Application storage is
SQLite, never the Airflow metadata database.

- Docker named volume `metadata`: Airflow execution metadata.
- `logs/`: native Airflow logs.
- `deployment/`: deployed code, manifest and bounded schedule.
- `state/snapshots/`: content-addressed source snapshots.
- `state/runs/`: isolated run/attempt artifacts and external collections.
- `state/workflow.sqlite`, `state/checkpoints.sqlite`: watcher, investigations,
  evidence, budget reservations and agent checkpoints.
- `state/investigations/`: append-only operation folders, JSON/Markdown reports,
  proposed diffs and validation results.
- `private/`: fault controls, accessible to demo infrastructure, excluded from
  all investigator tool capabilities.

## Dependencies and API key

The existing `.venv` and securely configured `.env` can be reused. Never paste a
key into a chat or print `.env`. A key identifies the provider project/account;
`AFI_MODEL` selects the model separately. See `.env.example` for nonsecret settings.

For a new machine with Python 3.12:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --require-hashes -r requirements.lock
.\.venv\Scripts\python.exe -m pip install --no-deps --no-build-isolation -e .
```

The hash lock was generated and the current environment passed `pip check`; a
clean lock-only installation still needs verification. The selected provider is
OpenAI, model `gpt-5.4-mini`, documented standard pricing $0.75/M input tokens and
$4.50/M output tokens. Model access was smoke tested; it does not prove agent
quality. See [official model documentation](https://developers.openai.com/api/docs/models/gpt-5.4-mini).
Keep billing and key management in the OpenAI Platform secure flow.

The approved local total is now $15, following explicit user authorization.
Default `AFI_APPROVED_TOTAL_USD=3`; this computer's local setting is15.
Existing ledger entries and the earlier budget-stopped suite remain intact.
The agent reserves a conservative
per-operation upper estimate before calls; it limits calls, output, context,
provider retries and wall time. Reservations remain on failed/interrupted calls
when usage may be missing. Cost accounting is approximate and not a provider
billing cap. Doctor smoke calls are small and currently outside the agent ledger.

`AFI_CONTEXT_BYTES` bounds request-directed bytes plus a 32 KB schema/system
buffer. Default 100000 with `AFI_MAX_USD=1.5`; this computer now uses the approved
120000 with `AFI_MAX_USD=1.8` (a $1.728 conservative admission reservation).
The per-operation bounds are separate from the approved cumulative total. Actual token usage is
recorded separately. With at least six allowed calls, the last two slots are
reserved for structured finalization and one optional report-integrity correction;
both remain inside the total call limit. Tools close after 24 calls or at
finalization. Recent source reads remain available
in a bounded request view, while complete messages/evidence remain on disk.

## Tests, watcher and isolated validation

```powershell
.\.venv\Scripts\python.exe -m afi.cli evaluate
.\.venv\Scripts\python.exe -m afi.cli watch --once
.\.venv\Scripts\python.exe -m afi.cli watch
```

Real integration checks, requiring the already-prepared history:

```powershell
$env:AFI_INTEGRATION = '1'
.\.venv\Scripts\python.exe -m pytest -q tests/test_live_airflow.py
Remove-Item Env:\AFI_INTEGRATION
```

These tests only read existing Airflow executions and isolate watcher/evidence
test records. They do not make provider calls or rerun pipelines. Their trusted
expectations are outside the investigator's source capability.

Watcher polling is 10 seconds by default with error backoff up to 60 seconds.
Use `watch --once --bounded` for the evaluation call/context/output limits without
changing `.env`. A second scan uses persisted deduplication and should launch no
new investigation for unchanged attempts. Incomplete investigations need explicit
`continue`; deduplication never implies a successful diagnosis.
It runs outside failing tasks and persists event identity by DAG/run/task/map/
attempt. Duplicate notifications do not create another investigation. New
attempts are distinct. A missed intermediate retry transition is a known polling
limitation; no exactly-once claim is made.

`python scripts/verify-watcher.py` runs two real watcher scans in separate
processes, retaining a UUID verification record with event/operation counts and
new investigation statuses. It does not clear deduplication state. If events
already exist, no new launch is expected and the wrapper records that limitation.
For focused regression within the approved budget, use
`evaluate --live --repeat 3 --key-cases-only` (12 operations). Full evaluation
remains19 operations and covers all ten scheduled cases.

Build the prepared validation image once in regular PowerShell:

```powershell
& "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin\docker.exe" build -t afi-validation:3.3.1 -f validation/Dockerfile validation
```

Validation uses a separate source copy, no network, no production credentials,
a read-only root filesystem, a bounded temporary filesystem, CPU/memory/PID
limits, and fixed trusted tests. Generated code cannot install dependencies.
Passing syntax alone is not container validation. Container tests are local;
BigQuery semantics and performance remain unverified without service tests and
measurement. Generated, locally validated and service-verified remain distinct.

For the recorded proposed patch, run this single command in regular PowerShell:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\verify-demo.ps1 -Investigation 90ca084f-7961-41b7-863d-80f120758840 -Patch e565fab8-3faf-44e0-b39a-2083cd3aa205 -FreshRun 'scheduled__2026-10-12T20:11:06.529124+00:00'
```

It verifies Stop/Start, temporarily stops this project's services to free RAM,
builds the prepared image, validates the isolated copy, restores services and
verifies history again. The fresh live investigation runs before optional patch
validation so those gates are independent. It stops on a
failed validation; each validation UUID retains its own log/result. The patch
does not reconcile the historical failed destination: existing duplicates and
its stale checkpoint remain preserved. The fix assumes consistent starting
state, stable pagination and one writer; it does not enforce event uniqueness.
Current validation uses120 seconds/one CPU/384 MB/48 PIDs/no network. All five
checks passed; DAG parsing runs in bare Python inside the same restricted container.
Import/parse diagnostics and unsuccessful attempts are preserved. Append
`-SkipRestartVerification -SkipBuild` to reuse already completed restart/build
stages; service restoration and current-history checks still run.

For the live fresh evaluation suite, both tool-access and logs-only use five
model calls, up to100 KB context and3000 output tokens (or lower configured
limits), reserving one final output slot. A correction may use remaining calls
but never exceed five. The conservative admission
reservation is $0.885 per operation with one provider retry allowance. These
settings are passed to the child process and recorded; long follow-up settings
remain eight calls/120 KB on this computer. Budget exhaustion stops the suite
honestly; neither setting increases the separately approved cumulative total.

## Pre-interview checklist

- `Status` passes; API/UI, scheduler, DAG processor and PostgreSQL are healthy.
- `doctor --model` passes with the configured economical model.
- All three demonstration DAGs are paused.
- `VerifyHistory` passes after Stop/Start; selected actual run IDs and evidence
  are available, including a contrasting run and an insufficient-evidence run.
- A NEW `investigate --fresh` succeeds, displaying real tool calls and saving
  inspectable JSON/Markdown. Follow-up and requested patch paths are checked.
- No pipeline rerun is required. Any saved report is labeled fallback.

## Scope and limitations

This is a bounded local engineering workflow, not production recovery. It does
not execute arbitrary diagnostic SQL/shell commands, import source during
inspection, automatically patch deployed DAGs, or claim OOM from SIGKILL alone.
Technical keys do not establish business update semantics. Source mismatch,
missing evidence, provider failure and malformed outputs remain honest incomplete
outcomes. Valid evidence references do not prove semantic correctness.

The main demo spans logs, helper code, commit/checkpoint ordering and retries.
Controlled faults and counterexamples are held outside tool access. The
investigator cannot read prior reports in a fresh thread. Docker is required for
this prepared route. Airflow is not supported natively on Windows; a Docker-free
WSL2 environment would still require virtualization and separate validation.
