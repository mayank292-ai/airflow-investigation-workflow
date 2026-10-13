import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
import typer
from afi.config import Settings
from afi.store import Store
from afi.schema import Identity
from afi.airflow import Airflow, q

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)
demo_app = typer.Typer(no_args_is_help=True)
app.add_typer(demo_app, name='demo')


def environment():
    settings = Settings.load()
    if not (settings.root / 'pyproject.toml').exists():
        raise typer.BadParameter('Run commands from the standalone project directory')
    return settings, Store(settings.state)


def launch(settings, store, inv, question=None, patch=False):
    from afi.process import execute_bounded
    code = execute_bounded(settings, store, inv, question, mode='patch' if patch else 'report')
    if code:
        raise typer.Exit(code)


@app.command()
def doctor(model: bool = False):
    """Check local dependencies and connections. --model makes a bounded live API call."""
    settings, store = environment()
    result = {'python': sys.version.split()[0], 'model': settings.model,
              'key_configured': bool(os.environ.get('OPENAI_API_KEY')),
              'frameworks': {p: importlib.metadata.version(p) for p in ('langchain', 'langchain-openai', 'langgraph', 'langgraph-checkpoint-sqlite')}}
    try:
        completed = subprocess.run(['docker', 'version', '--format', '{{.Server.Version}}'], capture_output=True, timeout=10)
        result['docker'] = 'healthy' if completed.returncode == 0 else 'blocked: engine/config unavailable'
    except (OSError, subprocess.TimeoutExpired):
        result['docker'] = 'blocked'
    api = Airflow(settings)
    try:
        result['airflow_health'] = api.request('GET', '/api/v2/monitor/health')
    except Exception as exc:
        result['airflow_health'] = {'status': 'blocked', 'error_type': type(exc).__name__}
    finally:
        api.close()
    if model:
        if not result['key_configured']:
            result['model_access'] = 'blocked: OPENAI_API_KEY missing'
        else:
            from langchain_openai import ChatOpenAI
            try:
                reply = ChatOpenAI(model=settings.model, use_responses_api=True, store=False,
                                   max_completion_tokens=128, reasoning_effort='none', timeout=15,
                                   max_retries=0).invoke('Reply with OK only.')
                result['model_access'] = {'status': 'pass', 'usage': reply.usage_metadata}
            except Exception as exc:
                result['model_access'] = {'status': 'blocked', 'error_type': type(exc).__name__}
    settings.state.mkdir(exist_ok=True)
    (settings.state / 'doctor.json').write_text(json.dumps(result, indent=2))
    typer.echo(json.dumps(result, indent=2))


@app.command()
def investigate(dag: str, run: str, task: str | None = None, attempt: int | None = None, map_index: int = -1, fresh: bool = True, bounded: bool = False):
    """Always creates a new investigation/checkpoint. Never returns a cached diagnosis."""
    settings, store = environment()
    if bounded:
        from afi.evaluation import bounded_evaluation_settings
        settings = bounded_evaluation_settings(settings)
    inv, _ = store.create(Identity(dag_id=dag, run_id=run, task_id=task, attempt=attempt, map_index=map_index))
    typer.echo(f'New investigation {inv}')
    launch(settings, store, inv)


@app.command(name='continue')
def continue_investigation(investigation: str):
    """Explicit continuation after provider/tool failures; preserves checkpoint history."""
    settings, store = environment()
    launch(settings, store, investigation, 'Continue the incomplete investigation. Verify unresolved evidence; do not assume missing results.')


@app.command()
def ask(investigation: str, question: str):
    settings, store = environment()
    launch(settings, store, investigation, question)


@app.command()
def show(investigation: str):
    settings, store = environment()
    op, _ = store.latest(investigation)
    typer.echo('SAVED REPORT — this command does not call the model.')
    typer.echo((settings.state / 'investigations' / investigation / op / 'report.md').read_text(encoding='utf-8'))


@app.command()
def propose_fix(investigation: str, request: str):
    settings, store = environment()
    launch(settings, store, investigation, request, patch=True)


@app.command()
def validate_fix(investigation: str, patch: str):
    from afi.patching import validate_patch
    settings, store = environment()
    with store.lock(investigation):
        typer.echo(json.dumps(validate_patch(settings, store, investigation, patch), indent=2))


@app.command()
def watch(once: bool = False, bounded: bool = False):
    from afi.watcher import watch as run_watch
    settings, store = environment()
    if bounded:
        from afi.evaluation import bounded_evaluation_settings
        settings = bounded_evaluation_settings(settings)
    api = Airflow(settings)
    try:
        run_watch(settings, store, api, lambda inv: launch(settings, store, inv), once=once)
    finally:
        api.close()


@demo_app.command('setup')
def demo_setup():
    from afi.demo import setup
    settings, _ = environment()
    typer.echo(json.dumps(setup(settings), indent=2))


@demo_app.command('prepare-history')
def demo_history():
    from afi.demo import prepare_history
    settings, _ = environment()
    api = Airflow(settings)
    try:
        typer.echo(json.dumps(prepare_history(settings, api), indent=2))
    finally:
        api.close()


@demo_app.command('check-environment')
def demo_check_environment():
    from afi.demo import check_environment
    settings, _ = environment()
    api = Airflow(settings)
    try:
        result = check_environment(settings, api)
        typer.echo(json.dumps(result, indent=2))
        if result['status'] != 'pass':
            raise typer.Exit(1)
    finally:
        api.close()


@demo_app.command('verify-history')
def demo_verify_history():
    from afi.demo import verify_history
    settings, _ = environment()
    api = Airflow(settings)
    try:
        result = verify_history(settings, api)
        summary = {key: value for key, value in result.items() if key not in ('runs', 'environment')}
        summary['incomplete_runs'] = [r for r in result['runs'] if r['status'] != 'pass']
        typer.echo(json.dumps(summary, indent=2))
        if result['status'] != 'pass':
            raise typer.Exit(1)
    finally:
        api.close()


@demo_app.command('trigger')
def demo_trigger(dag: str):
    """Optional manual development run. Never used as interview scheduled history."""
    settings, _ = environment()
    if dag not in settings.dags:
        raise typer.BadParameter('Only configured demo DAGs may be triggered')
    api = Airflow(settings)
    try:
        row = api.request('POST', f'/api/v2/dags/{q(dag)}/dagRuns', json={'dag_run_id': str(uuid.uuid4()), 'logical_date': None, 'conf': {}})
        typer.echo(json.dumps({'run_id': row['dag_run_id'], 'run_type': row['run_type']}))
    finally:
        api.close()


@demo_app.command('reset')
def demo_reset():
    """Archive only project data after stopping services; keep old investigations."""
    from afi.demo import reset
    settings, _ = environment()
    # Require containers stopped to avoid moving files still in use.
    running = subprocess.run(['docker', 'compose', 'ps', '--status', 'running', '-q'], capture_output=True, timeout=10)
    if running.returncode or running.stdout.strip():
        raise typer.BadParameter('Stop project containers and ensure Docker is reachable before reset')
    typer.echo(str(reset(settings)))
    typer.echo('Metadata volume retained. For a fresh demo use a different Compose project name/volume; never delete history implicitly.')


@app.command()
def evaluate(live: bool = False, repeat: int = 3, key_cases_only: bool = False):
    from afi.evaluation import evaluate as run_evaluation
    settings, _ = environment()
    result = run_evaluation(settings, live, repeat, key_cases_only)
    if live:
        # Full structured records are in results.json. Keep terminal progress short.
        result = {key: result.get(key) for key in ('status', 'output_directory', 'planned_operations', 'elapsed_seconds', 'budget')} | {
            'executed_operations': len(result['cases']),
            'completed_reports': sum(c['status'] == 'complete' for c in result['cases']),
            'incomplete_operations': sum(c['status'] == 'incomplete' for c in result['cases']),
            'semantic_review': 'required; completion is not correctness'}
    typer.echo(json.dumps(result, indent=2))


if __name__ == '__main__':
    app()
