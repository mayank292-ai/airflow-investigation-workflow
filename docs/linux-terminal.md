# Linux terminal runbook

The Python investigator CLI and Docker Compose services can be operated from Bash.
The two PowerShell helpers (`scripts/airflow.ps1` and `scripts/verify-demo.ps1`)
locate Windows executables and a Windows virtual environment; the startup helper
also calls the Windows-only memory probe in `src/afi/system.py`. Use the commands
below on Linux instead of invoking those helpers.

The existing end-to-end evidence was collected on Windows with Linux containers.
These Bash equivalents were checked against the source, but a clean Linux install,
live provider call, container startup and complete demo have not been exercised
on a native Linux host. Rehearse that installation before presenting from it.
Nothing needs to be started while studying the guide.

## 1 Choose the environment

- **Native Linux:** Python and Docker run on the Linux host. Use this runbook.
- **WSL:** it is a separate Linux environment. Install a Linux Python environment
  and make Docker available in that distribution. Do not reuse the Windows
  `.venv/Scripts/python.exe` as a Linux virtual environment. A new clone also needs
  its own history; Windows Docker metadata and ignored artifacts are not in Git.
- **Git Bash on Windows:** Bash syntax does not make Windows a Linux host. You can
  call the existing Windows Python executable with `.venv/Scripts/python.exe`,
  but the already verified PowerShell startup path remains the demo-day route.

For the upcoming interview on the existing computer, continuing with the verified
Windows installation preserves the existing history, reports and validation.

## 2 Install in a fresh Linux clone

Prerequisites: Python **3.12**, Git, a running Docker engine with Docker Compose v2
and sufficient memory. The custom services have container limits totalling about
3.8 GiB; allow at least 4 GiB for the engine and leave headroom for the host Python
process. Do not replace this project's Compose file with Airflow's generic example.

From the cloned project directory:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r requirements.lock
python -m pip install --no-deps --no-build-isolation -e .
cp .env.example .env
chmod 600 .env
```

The first command creates an isolated Linux dependency environment. `source`
selects its Python for this shell. The lock pins packages and accepted hashes;
the editable install makes `src/afi` importable. Configure `OPENAI_API_KEY` privately
in `.env` and the approved cumulative budget; do not print that file or commit it.
A Linux virtual environment is not included in the repository.

Before any Compose command in each new shell:

```bash
export AIRFLOW_UID="$(id -u)"
docker version
docker compose version
free -h
```

Compose uses `${AIRFLOW_UID:-50000}:0` for Airflow services. On Linux this selects
your host user's ID, with group 0, so generated files on bind mounts remain usable
by the host. On Windows the unset variable retains user 50000.
This follows [Airflow's container user guidance](https://airflow.apache.org/docs/apache-airflow/3.3.1/howto/docker-compose/index.html#setting-the-right-airflow-user).
Setting this variable alone cannot transfer an existing Windows installation.

## 3 First startup

Only in a **fresh clone without `deployment/`**:

```bash
python -m afi.cli demo setup
```

This creates deployed source, its manifest and snapshot, private synthetic fault
controls, the writable authentication file and local evidence/log directories.
It refuses an existing deployment to preserve history. It does not call the model.

Start services, checking each command's exit status before continuing:

```bash
docker compose config --quiet
docker compose pull postgres api scheduler dag-processor
docker compose up -d --wait --wait-timeout 120 postgres
docker compose --profile setup run --rm init
docker compose up -d --wait --wait-timeout 240 api scheduler dag-processor
python -m afi.cli demo check-environment
```

These commands validate configuration, download pinned images, start PostgreSQL,
migrate Airflow's metadata database, start health-checked Airflow services and
check service health/DAG import errors. DAGs start paused. Open
<http://localhost:8080> and sign in with the synthetic local credentials
`airflow` / `airflow`.

If startup fails, inspect:

```bash
docker compose ps --all
docker compose logs --no-color --tail 80 api scheduler dag-processor
```

The direct Bash route does not implement the PowerShell helper's automatic memory
admission or automatic error diagnostics; the checks above are explicit steps.

## 4 Generate history once, before the interview

Only for a new installation whose scheduled history has not yet been prepared:

```bash
python -m afi.cli demo prepare-history
python -m afi.cli demo verify-history
python -m json.tool state/history.json
```

The preparation arms a bounded future schedule and lets Airflow actually execute
the cases, then pauses the DAGs. It takes several minutes. It does not insert fake
historical runs. The history file contains this installation's actual run IDs.
Do not prepare again on the existing verified installation or substitute its
Windows run IDs for newly generated Linux IDs.

## 5 Resume an existing Linux installation

After activating its virtual environment and exporting `AIRFLOW_UID`:

```bash
docker compose up -d --wait --wait-timeout 120 postgres
docker compose up -d --wait --wait-timeout 240 api scheduler dag-processor
python -m afi.cli demo check-environment
python -m afi.cli demo verify-history
```

This preserves the existing volume, logs, source snapshots and investigations.
Ordinary restart does not require `demo setup`, history preparation or a new run.

## 6 Run the investigator

Substitute values from the current installation; text in angle brackets is a
placeholder, not a literal ID to submit:

```bash
python -m afi.cli doctor
python -m afi.cli investigate local_pipeline_03 '<actual-run-id>' \
  --task ingest --attempt 2 --fresh --bounded
python -m afi.cli show '<investigation-id>'
python -m afi.cli ask '<investigation-id>' \
  'Explain the commit/checkpoint order and distinguish observed duplicates from risk.'
```

`doctor` checks connections without a model request; `doctor --model` adds a small
provider smoke call. `investigate` creates a new thread, retrieves evidence and
calls the model; it does not rerun a DAG. `--bounded` selects the evaluation-sized
limits. `show` reads saved output. `ask` makes a follow-up model operation in the
same investigation. Budget accounting and validation rules are the same in Bash.

For tests, evaluations and watcher scans:

```bash
python -m afi.cli evaluate
python -m afi.cli evaluate --live --repeat 3 --key-cases-only
python -m afi.cli watch --once --bounded
python scripts/verify-watcher.py
```

The first is the offline suite. `--live` makes paid provider calls on existing
runs. Watcher commands may launch model investigations for new failed attempts;
already recorded attempts are deduplicated. A continuous `python -m afi.cli watch`
runs until stopped with Ctrl+C. Do not clear persisted events to manufacture
repeat launches.

## 7 Inspect reports and evidence

Terminal progress describes model/tool activity. Task logs record Airflow task
execution. A report is the model's structured interpretation of retrieved
evidence. They are different artifacts, connected by evidence IDs.

```bash
find state/investigations -name report.md
less 'state/investigations/<investigation-id>/<operation-id>/report.md'
python -m json.tool 'state/investigations/<investigation-id>/<operation-id>/result.json'
python -m json.tool 'state/investigations/<investigation-id>/<operation-id>/evidence.json'
docker compose logs --tail 80 scheduler
```

`find` locates saved Markdown reports; `less` opens one (press `q` to exit).
The JSON commands display the result and its evidence records. Match a claim's
`evidence_ids` to those records to see its source/log/state support. Docker logs
are service diagnostics, not automatically the selected task-attempt log.
Use the Airflow UI's task-instance **Logs** view for that attempt, or inspect
saved `attempt_logs` records. See the main handbook's log/report tracing section.

## 8 Validate a proposed patch separately

```bash
docker build -t afi-validation:3.3.1 -f validation/Dockerfile validation
python -m afi.cli propose-fix '<investigation-id>' \
  'Propose the smallest retry-safe change while preserving the output contract.'
python -m afi.cli validate-fix '<investigation-id>' '<patch-operation-id>'
```

Building the prepared image may download dependencies. Proposal generation calls
the model; validation runs trusted checks in a restricted local container using
an isolated source copy. These commands do not deploy the patch. The Windows
`verify-demo.ps1` additionally coordinates service stops/restoration for low RAM;
the direct Linux commands do not provide that orchestration. Retain its resource
constraints and verify available memory before attempting validation.

## 9 Stop while preserving the demo

Stop any continuous watcher with Ctrl+C in its terminal, then:

```bash
docker compose stop
deactivate
```

Stopping containers preserves named volumes and bind-mounted evidence. Deactivating
the virtual environment only changes the shell's PATH. Avoid volume deletion or
history reset when you need to preserve the recorded demo.
