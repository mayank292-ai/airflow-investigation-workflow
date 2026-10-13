"""Administrator CLI helpers. Not registered as investigator tools."""
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import time
import uuid
from afi.airflow import q
from afi.schema import Identity, now
from afi.evidence import run_key, safe_file


def check_environment(settings, airflow):
    health = airflow.request('GET', '/api/v2/monitor/health')
    errors, truncated = airflow.collection('/api/v2/importErrors', 'import_errors')
    dags = []
    for dag in settings.dags:
        row = airflow.request('GET', f'/api/v2/dags/{q(dag)}')
        dags.append({'dag_id': dag, 'is_paused': row['is_paused'],
                     'is_stale': row.get('is_stale'), 'last_parsed_time': row.get('last_parsed_time')})
    checks = {name: (health.get(name) or {}).get('status') == 'healthy'
              for name in ('metadatabase', 'scheduler', 'dag_processor')}
    checks['dag_imports'] = not errors and not truncated and all(not d['is_stale'] for d in dags)
    return {'status': 'pass' if all(checks.values()) else 'fail', 'collected_at': now(),
            'checks': checks, 'dags': dags, 'health': health, 'import_error_count': len(errors)}


def capture_completed_run(settings, airflow, dag, run):
    """Collect outside DAG tasks, only after genuine execution reaches terminal state.

    API metadata/logs were collected AFTER execution, not at task execution time.
    Attempt DB/checkpoint snapshots retain their own execution-time timestamps.
    No private control file, user conf, or previous diagnosis enters this bundle.
    """
    identity = Identity(dag_id=dag, run_id=run['dag_run_id'])
    overview, truncated = airflow.overview(identity)
    actual = overview['run']
    if actual['run_type'] != 'scheduled' or actual['state'] not in ('success', 'failed'):
        raise ValueError('Only completed real scheduled runs may enter demonstration history')
    folder = settings.state / 'runs' / run_key(dag, identity.run_id)
    collection = folder / 'collections' / str(uuid.uuid4())
    collection.mkdir(parents=True)
    logs, failures = [], []
    for task in overview['tasks']:
        for attempt in range(1, min(task.get('try_number') or 0, 8) + 1):
            selector = {'task_id': task['task_id'], 'map_index': task['map_index'], 'attempt': attempt}
            try:
                data, cut = airflow.logs(identity, **selector, max_chars=40000)
                record = selector | {'collected_at': now(), 'truncated': cut, 'availability': 'available', 'data': data}
            except Exception as exc:
                record = selector | {'collected_at': now(), 'availability': 'error', 'error_type': type(exc).__name__}
            name = 'log-' + hashlib.sha256(json.dumps(selector, sort_keys=True).encode()).hexdigest()[:16] + '.json'
            (collection / name).write_text(json.dumps(record, indent=2), encoding='utf-8')
            logs.append(name)
        if task['state'] == 'failed':
            failures.append({k: task[k] for k in ('task_id', 'map_index', 'try_number')})
    # Each run owns its output; immutable attempt copies are hashed after terminal state.
    files = {str(path.relative_to(settings.state)).replace('\\', '/'): hashlib.sha256(path.read_bytes()).hexdigest()
             for path in folder.rglob('*') if path.is_file() and 'collections' not in path.relative_to(folder).parts}
    record = {'identity': identity.model_dump(), 'collected_at': now(), 'basis': 'post-execution collection',
              'overview': overview, 'overview_truncated': truncated, 'logs': logs, 'artifact_hashes': files}
    (collection / 'capture.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
    return {'dag_id': dag, 'run_id': identity.run_id, 'state': actual['state'], 'run_type': actual['run_type'],
            'start_date': actual['start_date'], 'end_date': actual['end_date'], 'failed_tasks': failures,
            'capture_path': str((collection / 'capture.json').relative_to(settings.state)).replace('\\', '/')}


def verify_history(settings, airflow):
    """Verify existing evidence against live metadata without creating or clearing runs."""
    started = time.monotonic()
    deadline = started + 120
    history = json.loads((settings.state / 'history.json').read_text(encoding='utf-8'))
    if len(history) > 30:
        raise ValueError('History verification exceeds bounded demonstration inventory')
    try:
        environment = check_environment(settings, airflow)
    except Exception as exc:
        environment = {'status': 'fail', 'dags': [], 'error_type': type(exc).__name__,
                       'reason': 'Environment health could not be retrieved; not claimed healthy'}
    rows = []
    for entry in history:
        identity = Identity(dag_id=entry['dag_id'], run_id=entry['run_id'])
        checks, errors = {}, []
        try:
            if time.monotonic() > deadline:
                raise TimeoutError('Verification deadline; no further API requests')
            overview, truncated = airflow.overview(identity)
            current = overview['run']
            checks.update(terminal_scheduled=current['run_type'] == 'scheduled' and current['state'] == entry['state'] and bool(current['end_date']),
                          overview_complete=not truncated)
        except Exception as exc:
            checks.update(terminal_scheduled=False, overview_complete=False)
            errors.append({'scope': 'current API overview', 'error_type': type(exc).__name__})
        try:
            capture_path = safe_file(settings.state, entry['capture_path'])
            capture = json.loads(capture_path.read_text(encoding='utf-8'))
            checks.update(identity=capture['identity']['dag_id'] == identity.dag_id and capture['identity']['run_id'] == identity.run_id,
                          artifact_hashes=bool(capture['artifact_hashes']) and all(safe_file(settings.state, name).is_file() and
                              hashlib.sha256(safe_file(settings.state, name).read_bytes()).hexdigest() == sha
                              for name, sha in capture['artifact_hashes'].items()),
                          capture_logs_present=all(safe_file(capture_path.parent, name).is_file() for name in capture['logs']))
        except Exception as exc:
            checks.update(identity=False, artifact_hashes=False, capture_logs_present=False)
            errors.append({'scope': 'saved execution artifacts', 'error_type': type(exc).__name__})
        for task in entry['failed_tasks']:
            try:
                if time.monotonic() > deadline:
                    raise TimeoutError('Verification deadline; no further API requests')
                log, cut = airflow.logs(identity, task['task_id'], task['try_number'], task['map_index'])
                checks['failed_attempt_log_' + task['task_id']] = bool(log['text']) and not cut
            except Exception as exc:
                checks['failed_attempt_log_' + task['task_id']] = False
                errors.append({'scope': 'current attempt log', 'task_id': task['task_id'], 'error_type': type(exc).__name__})
        rows.append({'identity': identity.model_dump(), 'status': 'pass' if all(checks.values()) else 'fail', 'checks': checks, 'errors': errors})
    paused = bool(environment['dags']) and all(d['is_paused'] for d in environment['dags'])
    counts = {dag: sum(entry['dag_id'] == dag for entry in history) for dag in settings.dags}
    complete = all(counts[dag] >= (4 if dag.endswith('03') else 3) for dag in settings.dags)
    unique = len({(entry['dag_id'], entry['run_id']) for entry in history}) == len(history)
    result = {'checked_at': now(), 'status': 'pass' if environment['status'] == 'pass' and paused and complete and unique and all(r['status'] == 'pass' for r in rows) else 'fail',
              'required_run_counts': complete, 'run_counts': counts,
              'unique_run_identities': unique, 'elapsed_seconds': time.monotonic() - started,
              'deadline_seconds': 120,
              'dags_paused': paused, 'environment': environment, 'runs': rows,
              'live_investigation_verified': False,
              'limitation': 'This checks persistence/access. A NEW model investigation is a separate required gate.'}
    target = settings.state / 'verifications'
    target.mkdir(exist_ok=True)
    path = target / (str(uuid.uuid4()) + '.json')
    result['output_path'] = str(path)
    path.write_text(json.dumps(result, indent=2), encoding='utf-8')
    return result


def setup(settings):
    deployment = settings.root / 'deployment'
    if deployment.exists():
        raise ValueError('Deployment already exists. Preserve history; use a fresh project copy for a new window')
    source = deployment / 'source'
    shutil.copytree(settings.root / 'demo/source', source, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    dags = deployment / 'dags'
    shutil.copytree(settings.root / 'demo/dags', dags, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    (source / 'dags').mkdir()
    shutil.copyfile(dags / 'pipelines.py', source / 'dags/pipelines.py')
    files = {str(p.relative_to(source)).replace('\\', '/'): hashlib.sha256(p.read_bytes()).hexdigest()
             for p in source.rglob('*') if p.is_file() and '__pycache__' not in p.parts}
    revision = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    manifest = {'revision': revision, 'files': files, 'deployed_at': now()}
    (deployment / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    snapshot = settings.state / 'snapshots' / revision
    snapshot.mkdir(parents=True)
    shutil.copytree(source, snapshot / 'source')
    (snapshot / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    start = datetime.now(timezone.utc) + timedelta(seconds=30)
    (deployment / 'schedule.json').write_text(json.dumps({'start': start.isoformat(), 'end': (start + timedelta(minutes=6)).isoformat()}))
    (deployment / 'passwords.json').write_text(json.dumps({'airflow': 'airflow'}))
    private = settings.root / 'private'
    private.mkdir(exist_ok=True)
    plan = {'01': [{'callable': 'materialized', 'rows': 100}, {'callable': 'materialized', 'rows': 15000, 'terminate': True}, {'callable': 'streaming', 'rows': 15000}],
            '02': [{'fail_after_write': True}, {'source_mismatch': True, 'fail_after_write': True}, {}],
            '03': [{'callable': 'append_pages', 'fail_page': True, 'misleading_log': True},
                   {'callable': 'atomic_pages', 'fail_page': True},
                   {'callable': 'keyed_pages', 'fail_page': True, 'withhold_binding': True},
                   {'callable': 'keyed_pages'}]}
    (private / 'plan.json').write_text(json.dumps(plan, indent=2))
    (settings.root / 'logs').mkdir(exist_ok=True)
    return manifest


def prepare_history(settings, airflow, timeout=480):
    schedule = settings.root / 'deployment/schedule.json'
    # Only change the future bounded window before any runs exist. Never create or
    # edit metadata records, simulate old execution, or trigger manual runs here.
    for dag in settings.dags:
        rows, _ = airflow.runs(dag)
        if rows:
            raise ValueError('History already exists; do not rearm schedules for the interview')
    start = datetime.now(timezone.utc) + timedelta(seconds=30)
    schedule.write_text(json.dumps({'start': start.isoformat(), 'end': (start + timedelta(minutes=6)).isoformat()}))
    time.sleep(15)  # Bounded parse interval, outside failing tasks.
    for dag in settings.dags:
        airflow.request('PATCH', f'/api/v2/dags/{q(dag)}', json={'is_paused': False})
    deadline = time.monotonic() + timeout
    captured, paused = {}, set()
    try:
        while time.monotonic() < deadline:
            completed = {}
            ready = True
            for dag in settings.dags:
                runs, _ = airflow.runs(dag)
                completed[dag] = [r for r in runs if r['run_type'] == 'scheduled' and r['state'] in ('success', 'failed')]
                print(f'actual completed scheduled runs {dag}: {len(completed[dag])}', flush=True)
                required = 4 if dag.endswith('03') else 3
                for run in completed[dag]:
                    selector = (dag, run['dag_run_id'])
                    if selector not in captured:
                        captured[selector] = capture_completed_run(settings, airflow, dag, run)
                        # Preserve partial progress too, so timeout/interruption never erases real runs.
                        (settings.state / 'history.json').write_text(json.dumps(list(captured.values()), indent=2), encoding='utf-8')
                if len(completed[dag]) >= required and dag not in paused:
                    airflow.request('PATCH', f'/api/v2/dags/{q(dag)}', json={'is_paused': True})
                    paused.add(dag)
                ready &= len(completed[dag]) >= required
            if ready:
                inventory = list(captured.values())
                return inventory
            time.sleep(10)
        raise TimeoutError('Scheduled history preparation timed out; no success claimed')
    finally:
        for dag in settings.dags:
            airflow.request('PATCH', f'/api/v2/dags/{q(dag)}', json={'is_paused': True})


def reset(settings):
    # Rename this project's data rather than deleting prior investigations.
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
    archive = settings.root / 'work' / ('reset-' + stamp)
    archive.mkdir(parents=True)
    for name in ('state', 'deployment', 'private', 'logs'):
        target = (settings.root / name).resolve()
        if target.parent != settings.root.resolve():
            raise ValueError('Reset path escaped project')
        if target.exists():
            shutil.move(str(target), archive / name)
    return archive
