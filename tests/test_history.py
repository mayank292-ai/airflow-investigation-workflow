"""Explicit API doubles: persistence checks, NOT live scheduled-run verification."""
import json
from pathlib import Path
import pytest
from afi.config import Settings
from afi.demo import capture_completed_run, verify_history
from afi.evidence import run_key


class FakeCompletedAirflow:
    run_type = 'scheduled'

    def request(self, method, path, **kwargs):
        if path.endswith('/health'):
            return {name: {'status': 'healthy'} for name in ('metadatabase', 'scheduler', 'dag_processor')}
        return {'is_paused': True, 'is_stale': False, 'last_parsed_time': '2026-01-01T00:00:00Z'}

    def collection(self, path, key):
        return [], False

    def overview(self, identity):
        return {'run': {'dag_id': identity.dag_id, 'dag_run_id': identity.run_id,
                        'run_type': self.run_type, 'state': 'failed',
                        'start_date': '2026-01-01T00:01:00Z', 'end_date': '2026-01-01T00:02:00Z'},
                'tasks': [{'task_id': 'ingest', 'map_index': -1, 'try_number': 2, 'state': 'failed'}]}, False

    def logs(self, identity, task_id, attempt, map_index, max_chars=12000):
        return {'text': f'EXPLICIT TEST DOUBLE attempt {attempt}', 'attempt': attempt}, False


def test_capture_refuses_manual_run(tmp_path):
    api = FakeCompletedAirflow()
    api.run_type = 'manual'
    with pytest.raises(ValueError, match='scheduled'):
        capture_completed_run(Settings(root=tmp_path), api, 'local_pipeline_03', {'dag_run_id': 'opaque'})
    assert not (tmp_path / 'state').exists()


def test_collections_are_distinct_and_attempts_never_mixed(tmp_path):
    settings = Settings(root=tmp_path)
    api = FakeCompletedAirflow()
    first = capture_completed_run(settings, api, 'local_pipeline_03', {'dag_run_id': 'opaque'})
    second = capture_completed_run(settings, api, 'local_pipeline_03', {'dag_run_id': 'opaque'})
    assert first['capture_path'] != second['capture_path']
    path = settings.state / first['capture_path']
    record = json.loads(path.read_text())
    assert record['basis'] == 'post-execution collection'
    logs = [json.loads((path.parent / name).read_text()) for name in record['logs']]
    assert [log['attempt'] for log in logs] == [1, 2]
    assert logs[0]['data']['text'] != logs[1]['data']['text']


def test_history_verification_detects_mutation_and_incomplete_inventory(tmp_path):
    settings = Settings(root=tmp_path)
    api = FakeCompletedAirflow()
    history = []
    for dag in settings.dags:
        for n in range(4 if dag.endswith('03') else 3):
            run = f'opaque-{n}'
            output = settings.state / 'runs' / run_key(dag, run) / 'attempts/ingest.-1.2/output.sqlite'
            output.parent.mkdir(parents=True)
            output.write_bytes(b'explicit synthetic artifact')
            history.append(capture_completed_run(settings, api, dag, {'dag_run_id': run}))
    inventory = settings.state / 'history.json'
    inventory.write_text(json.dumps(history))
    assert verify_history(settings, api)['status'] == 'pass'
    inventory.write_text(json.dumps(history[:1]))
    assert not verify_history(settings, api)['required_run_counts']
    inventory.write_text(json.dumps(history))
    output.write_bytes(b'changed')
    result = verify_history(settings, api)
    assert result['status'] == 'fail'
    assert not result['live_investigation_verified']


def test_api_failure_records_incomplete_verification_and_keeps_artifacts(tmp_path):
    from afi.airflow import AirflowError
    settings = Settings(root=tmp_path, dags=['local_pipeline_03'])
    api = FakeCompletedAirflow()
    run = 'opaque'
    output = settings.state / 'runs' / run_key('local_pipeline_03', run) / 'output.sqlite'
    output.parent.mkdir(parents=True)
    output.write_bytes(b'explicit test artifact')
    entry = capture_completed_run(settings, api, 'local_pipeline_03', {'dag_run_id': run})
    (settings.state / 'history.json').write_text(json.dumps([entry]))
    class UnavailableAPI(FakeCompletedAirflow):
        def overview(self, identity):
            raise AirflowError('explicit timeout double')
    result = verify_history(settings, UnavailableAPI())
    assert result['status'] == 'fail'
    assert result['runs'][0]['checks']['artifact_hashes']
    assert not result['runs'][0]['checks']['terminal_scheduled']
    assert result['runs'][0]['errors'][0]['error_type'] == 'AirflowError'
    assert output.read_bytes() == b'explicit test artifact'
    assert json.loads(Path(result['output_path']).read_text()) == result
