import difflib
import hashlib
import json
import os
import shutil
import subprocess
import time
import uuid
from afi.evidence import EvidenceTools, safe_file
from afi.schema import now


def create_patch(settings, store, inv, proposal, runtime=None):
    root, manifest = EvidenceTools(settings, store, inv, None).source_root()
    if proposal.source_revision != manifest['revision']:
        raise ValueError('Patch revision does not match executed snapshot')
    op, folder = store.operation(inv, 'patch', 'generated', proposal.model_dump() | {'runtime': runtime or {}})
    workspace = folder / 'source'
    shutil.copytree(root, workspace)
    diffs = []
    seen = set()
    try:
        for edit in proposal.edits:
            if edit.path not in manifest['files'] or edit.path in seen:
                raise ValueError('Patch touches disallowed or repeated file')
            if not edit.path.endswith(('.py', '.sql')):
                raise ValueError('Only business Python/SQL changes are supported')
            seen.add(edit.path)
            path = safe_file(workspace, edit.path)
            before = path.read_text(encoding='utf-8')
            if before.count(edit.old) != 1:
                raise ValueError('Patch context must match exactly once')
            after = before.replace(edit.old, edit.new, 1)
            if len(after.encode()) > 50000:
                raise ValueError('Patched file exceeds size bound')
            path.write_text(after, encoding='utf-8')
            diffs.extend(difflib.unified_diff(before.splitlines(True), after.splitlines(True),
                                             fromfile='a/' + edit.path, tofile='b/' + edit.path))
        (folder / 'proposed.diff').write_text(''.join(diffs), encoding='utf-8')
        (folder / 'validation.json').write_text(json.dumps({'status': 'generated', 'executed': False,
                                                           'source_revision': manifest['revision']}))
    except BaseException:
        (folder / 'validation.json').write_text(json.dumps({'status': 'generation_failed', 'executed': False}))
        raise
    return op, folder


def validate_patch(settings, store, inv, patch_id):
    store.get(inv)
    uuid.UUID(patch_id)
    folder = settings.state / 'investigations' / inv / patch_id
    proposal = json.loads((folder / 'result.json').read_text())
    if 'edits' not in proposal:
        raise ValueError('This operation is not a patch')
    validation_id = str(uuid.uuid4())
    validation_folder = folder / 'validations' / validation_id
    validation_folder.mkdir(parents=True)
    workspace = folder / 'source'
    syntax = []
    for file in workspace.rglob('*.py'):
        try:
            compile(file.read_text(encoding='utf-8'), file.name, 'exec')
            syntax.append({'path': str(file.relative_to(workspace)), 'pass': True})
        except SyntaxError:
            syntax.append({'path': str(file.relative_to(workspace)), 'pass': False})
    if not all(row['pass'] for row in syntax):
        result = {'status': 'failed', 'syntax': syntax, 'executed': False}
    else:
        name = 'afi-validation-' + uuid.uuid4().hex[:12]
        args = ['docker', 'run', '--name', name, '--rm', '--network', 'none', '--read-only',
                '--memory', '384m', '--cpus', '1', '--pids-limit', '48',
                '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                '--tmpfs', '/tmp:rw,noexec,nosuid,size=64m', '--user', '50000:0',
                '-e', 'PYTHONDONTWRITEBYTECODE=1', '-e', 'PYTHONUNBUFFERED=1',
                '-e', 'PYTEST_DISABLE_PLUGIN_AUTOLOAD=1', '-e', 'PYTHONPATH=/patch:/checks',
                '-e', 'AIRFLOW_HOME=/tmp/airflow', '-e', 'AIRFLOW__LOGGING__BASE_LOG_FOLDER=/tmp/airflow/logs',
                '-v', str(workspace.resolve()) + ':/patch:ro',
                '-v', str((settings.root / 'validation/checks').resolve()) + ':/checks:ro',
                '-v', str((settings.root / 'validation/fixtures').resolve()) + ':/opt/airflow/deployment:ro',
                '--entrypoint', 'python', 'afi-validation:3.3.1', '-u', '/checks/run_validation.py']
        if not any(edit['path'] == 'batch_ingest.py' for edit in proposal['edits']):
            args += ['-k', 'not requested_append_remediation']
        log = validation_folder / 'validation.log'
        started = time.monotonic()
        with log.open('wb') as stream:
            try:
                process = subprocess.Popen(args, stdout=stream, stderr=stream)
                while process.poll() is None:
                    if time.monotonic() - started > 120 or log.stat().st_size > 200000:
                        # Inspect only container state, never environment/config.
                        state = None
                        try:
                            inspection = subprocess.run(['docker', 'inspect', '--format', '{{json .State}}', name],
                                                        capture_output=True, text=True, timeout=5)
                            if inspection.returncode == 0 and len(inspection.stdout) <= 16000:
                                raw = json.loads(inspection.stdout)
                                if isinstance(raw, dict):
                                    state = {key: raw.get(key) for key in ('Status', 'Running', 'OOMKilled', 'ExitCode', 'StartedAt', 'FinishedAt')}
                        except (OSError, subprocess.TimeoutExpired, ValueError):
                            pass
                        process.kill()
                        process.wait(timeout=5)
                        subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=10)
                        result = {'status': 'failed', 'reason': 'timeout/output bound', 'executed': None,
                                  'execution_status': 'unknown: inspect phase log and container state', 'container_state': state}
                        break
                    time.sleep(0.1)
                else:
                    code = process.returncode
                    result = {'status': 'locally_validated' if code == 0 else 'failed', 'returncode': code,
                              'executed': code == 0 or code != 125, 'syntax': syntax,
                              'scope': 'Prepared Airflow image, local semantics; BigQuery unverified'}
                    if code == 125:
                        result['status'] = 'blocked'
            except (OSError, subprocess.TimeoutExpired):
                result = {'status': 'blocked', 'reason': 'Docker unavailable', 'syntax': syntax, 'executed': False}
        result['elapsed_seconds'] = time.monotonic() - started
        result['log_sha256'] = hashlib.sha256(log.read_bytes()).hexdigest()
        result['log_path'] = str(log.relative_to(settings.state))
        print(f'validation log: {log}', flush=True)
        if result['status'] != 'locally_validated':
            print(log.read_bytes()[-12000:].decode('utf-8', errors='replace'), flush=True)
    result.update(validation_id=validation_id, collected_at=now(),
                  artifact_directory=str(validation_folder.relative_to(settings.state)),
                  patch_source_revision=proposal.get('source_revision'),
                  validation_limits={'wall_seconds': 120, 'output_bytes': 200000,
                                     'memory_mb': 384, 'cpus': 1, 'pids': 48, 'network': 'none',
                                     'read_only': True, 'pytest_plugin_autoload': False})
    (validation_folder / 'result.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    # Latest summary is convenient; each complete validation/log remains separate.
    (folder / 'validation.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    store.operation(inv, 'validation', result['status'], result | {'patch_id': patch_id})
    return result
