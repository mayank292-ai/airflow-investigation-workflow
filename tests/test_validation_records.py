"""Process doubles prove artifact preservation; no generated code executes here."""
import json
import subprocess
from afi.config import Settings
from afi.schema import Identity
from afi.store import Store
from afi.patching import validate_patch


def test_repeated_validation_preserves_each_log_and_outcome(tmp_path, monkeypatch):
    settings = Settings(root=tmp_path)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    patch, folder = store.operation(inv, 'patch', 'generated',
        {'source_revision': 'test-only', 'edits': [{'path': 'batch_ingest.py'}]})
    (folder / 'source').mkdir()
    source = folder / 'source' / 'batch_ingest.py'
    source.write_text('value = 1\n')  # Trusted test fixture, syntax check only.
    calls = []

    class ProcessDouble:
        def __init__(self, args, stdout, stderr):
            calls.append(args)
            self.returncode = 1 if len(calls) == 1 else 0
            stdout.write(f'EXPLICIT PROCESS DOUBLE attempt={len(calls)}'.encode())

        def poll(self):
            return self.returncode

    monkeypatch.setattr('afi.patching.subprocess.Popen', ProcessDouble)
    first = validate_patch(settings, store, inv, patch)
    second = validate_patch(settings, store, inv, patch)
    assert first['status'] == 'failed' and second['status'] == 'locally_validated'
    assert first['validation_id'] != second['validation_id']
    for n, result in enumerate((first, second), 1):
        path = settings.state / result['artifact_directory']
        assert f'attempt={n}' in (path / 'validation.log').read_text()
        assert json.loads((path / 'result.json').read_text()) == result
    assert json.loads((folder / 'validation.json').read_text()) == second
    assert source.read_text() == 'value = 1\n'
    assert all(args[args.index('--network') + 1] == 'none' and '--read-only' in args for args in calls)
    assert all('PYTEST_DISABLE_PLUGIN_AUTOLOAD=1' in args and 'PYTHONUNBUFFERED=1' in args
               and args[args.index('--entrypoint') + 1] == 'python' and '/checks/run_validation.py' in args for args in calls)


def test_timeout_preserves_phase_output_and_collects_only_container_state(tmp_path, monkeypatch):
    settings = Settings(root=tmp_path)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    patch, folder = store.operation(inv, 'patch', 'generated',
        {'source_revision': 'test-only', 'edits': [{'path': 'batch_ingest.py'}]})
    (folder / 'source').mkdir()
    (folder / 'source' / 'batch_ingest.py').write_text('value = 1\n')
    clock = iter([0, 121, 122])
    monkeypatch.setattr('afi.patching.time.monotonic', lambda: next(clock))
    commands = []

    class ProcessDouble:
        killed = False
        def __init__(self, args, stdout, stderr):
            stdout.write(b'EXPLICIT PROCESS DOUBLE collection_started\n')

        def poll(self):
            return None

        def kill(self):
            self.killed = True

        def wait(self, timeout):
            return 0

    def command(args, **kwargs):
        commands.append(args)
        return subprocess.CompletedProcess(args, 0,
            stdout=json.dumps({'Status': 'running', 'Running': True, 'OOMKilled': False, 'ExitCode': 0}), stderr='')

    monkeypatch.setattr('afi.patching.subprocess.Popen', ProcessDouble)
    monkeypatch.setattr('afi.patching.subprocess.run', command)
    result = validate_patch(settings, store, inv, patch)
    assert result['status'] == 'failed' and result['executed'] is None
    assert result['container_state']['Running'] and not result['container_state']['OOMKilled']
    assert commands[0][1:4] == ['inspect', '--format', '{{json .State}}']
    assert commands[1][1:3] == ['rm', '-f']
    assert 'collection_started' in (settings.state / result['log_path']).read_text()
    assert result['validation_limits']['wall_seconds'] == 120
    assert result['validation_limits']['cpus'] == 1
