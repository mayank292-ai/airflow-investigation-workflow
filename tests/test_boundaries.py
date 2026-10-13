import hashlib
import json
from pathlib import Path
import pytest
from afi.config import Settings
from afi.demo import setup
from afi.evidence import EvidenceTools, safe_file, run_key
from afi.schema import Identity, Evidence, Claim, Diagnosis, validate_references, now
from afi.store import Store
from afi.watcher import discover


@pytest.mark.parametrize('name', ['../other', '/etc/passwd', 'C:/Users/key', 'a\\b', '../../private/plan.json'])
def test_path_escape_blocked(tmp_path, name):
    with pytest.raises(ValueError):
        safe_file(tmp_path, name)


def test_watcher_dedup_survives_restart_and_attempts_distinct(tmp_path):
    settings = Settings(root=tmp_path, dags=['local_pipeline_03'])
    class API:
        attempt = 1
        def runs(self, dag):
            return [{'dag_run_id': 'opaque', 'state': 'failed'}], False
        def overview(self, identity):
            return {'tasks': [{'task_id': 'ingest', 'map_index': 2, 'state': 'failed', 'try_number': self.attempt},
                              {'task_id': 'publish', 'map_index': -1, 'state': 'upstream_failed', 'try_number': 0}]}, False
    api = API()
    store = Store(settings.state)
    first = discover(settings, store, api)
    assert len(first) == 1
    reopened = Store(settings.state)
    assert discover(settings, reopened, api) == []
    api.attempt = 2
    assert len(discover(settings, reopened, api)) == 1
    fresh, created = reopened.create(store.get(first[0])['identity'])
    assert created and fresh != first[0]
    assert discover(settings, reopened, api) == []  # manual freshness never clears watcher events


def test_source_manifest_is_verified_and_private_data_excluded(tmp_path):
    import shutil
    project = Path(__file__).resolve().parents[1]
    shutil.copytree(project / 'demo', tmp_path / 'demo')
    settings = Settings(root=tmp_path)
    manifest = setup(settings)
    store = Store(settings.state)
    identity = Identity(dag_id='local_pipeline_03', run_id='opaque')
    inv, _ = store.create(identity)
    run = settings.state / 'runs' / run_key(identity.dag_id, identity.run_id)
    run.mkdir(parents=True)
    (run / 'binding.json').write_text(json.dumps({'dag_id': identity.dag_id, 'run_id': identity.run_id,
                                                'revision': manifest['revision'], 'deployment_verified': True}))
    tools = EvidenceTools(settings, store, inv, None)
    assert tools.source('batch_ingest.py', 1, 60)['availability'] == 'available'
    alias = tools.source('/opt/airflow/deployment/source/batch_ingest.py', 1, 60)
    assert alias['availability'] == 'available' and alias['data']['path'] == 'batch_ingest.py'
    assert tools.source('/opt/airflow/dags/pipelines.py', 1, 60)['availability'] == 'available'
    range_error = tools.source('batch_ingest.py', 1, 160)
    assert range_error['availability'] == 'error' and range_error['data']['max_count'] == 150
    assert tools.source('/opt/airflow/deployment/source/../../private/plan.json', 1, 20)['availability'] == 'error'
    denied = tools.source('runtime.py', 1, 20)
    assert denied['availability'] == 'error' and 'runtime.py' not in denied['data']['allowed_paths']
    assert tools.source('../../private/plan.json', 1, 20)['availability'] == 'error'
    source = settings.state / 'snapshots' / manifest['revision'] / 'source/batch_ingest.py'
    source.write_text('modified')
    assert tools.source('batch_ingest.py', 1, 20)['availability'] == 'error'


def test_source_read_reports_checked_binding_and_unknown_deployment_stays_unknown(tmp_path):
    import shutil
    project = Path(__file__).resolve().parents[1]
    shutil.copytree(project / 'demo', tmp_path / 'demo')
    settings = Settings(root=tmp_path)
    manifest = setup(settings)
    store = Store(settings.state)
    identity = Identity(dag_id='d', run_id='r')
    inv, _ = store.create(identity)
    folder = settings.state / 'runs' / run_key('d', 'r')
    folder.mkdir(parents=True)
    binding = {'dag_id': 'd', 'run_id': 'r', 'revision': manifest['revision'], 'deployment_verified': True}
    (folder / 'binding.json').write_text(json.dumps(binding))
    tools = EvidenceTools(settings, store, inv, None)
    record = tools.source('batch_ingest.py', 1, 61)
    assert record['data']['source_binding']['alignment'] == 'verified'
    assert record['data']['source_binding']['revision'] == manifest['revision']
    from afi.agent import validate_diagnosis
    claim = Claim(text='Explicit test of checked source provenance', support='supported', evidence_ids=[record['id']])
    diagnosis = Diagnosis(source_alignment='verified', task_purpose=claim, observed_failure=claim,
                          confirmed_findings=[], hypotheses=[], missing_or_conflicting_evidence=[],
                          downstream_effects=[], data_write_retry_assessment=[], remediation_options=[], limitations=[])
    validate_diagnosis(diagnosis, store.evidence(inv))
    checked = Evidence.model_validate(record)
    foreign = checked.model_copy(deep=True)
    foreign.data['source_binding']['run_id'] = 'other'
    with pytest.raises(ValueError, match='Source alignment'):
        validate_diagnosis(diagnosis, {foreign.id: foreign})
    changed = checked.model_copy(deep=True)
    changed.data['source_binding']['revision'] = 'b' * 64
    with pytest.raises(ValueError, match='Source alignment'):
        validate_diagnosis(diagnosis, {changed.id: changed})
    binding['deployment_verified'] = False
    (folder / 'binding.json').write_text(json.dumps(binding))
    unknown = tools.source('batch_ingest.py', 1, 61)
    assert unknown['data']['source_binding']['alignment'] == 'unknown'
    claim = claim.model_copy(update={'evidence_ids': [unknown['id']]})
    with pytest.raises(ValueError, match='Source alignment'):
        validate_diagnosis(diagnosis.model_copy(update={'task_purpose': claim, 'observed_failure': claim}),
                           {unknown['id']: Evidence.model_validate(unknown)})


def test_evidence_scoped_to_one_investigation(tmp_path):
    store = Store(tmp_path)
    one, _ = store.create(Identity(dag_id='d', run_id='r'))
    two, _ = store.create(Identity(dag_id='d', run_id='r'))
    e = Evidence(id='e1', investigation_id=one, kind='logs', provenance={}, collected_at=now(), availability='available', truncated=False, data={})
    store.save_evidence(e)
    assert store.evidence(two) == {}
    claim = Claim(text='Observed failure', support='supported', evidence_ids=['e1'])
    diagnosis = Diagnosis(source_alignment='unknown', task_purpose=claim, observed_failure=claim,
                          confirmed_findings=[claim], hypotheses=[], missing_or_conflicting_evidence=[],
                          downstream_effects=[], data_write_retry_assessment=[], remediation_options=[], limitations=[])
    validate_references(diagnosis, store.evidence(one))
    with pytest.raises(ValueError):
        validate_references(diagnosis, store.evidence(two))


def test_missing_destination_not_created(tmp_path):
    settings = Settings(root=tmp_path)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    tools = EvidenceTools(settings, store, inv, None)
    assert tools.destination('t', 1, -1)['availability'] == 'missing'
    assert not tools.run_dir.exists()


def test_foreign_binding_rejected_and_missing_revision_remains_unknown(tmp_path):
    settings = Settings(root=tmp_path)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    tools = EvidenceTools(settings, store, inv, None)
    tools.run_dir.mkdir(parents=True)
    binding = tools.run_dir / 'binding.json'
    binding.write_text(json.dumps({'dag_id': 'other', 'run_id': 'r', 'revision': None, 'deployment_verified': False}))
    assert tools.configuration()['availability'] == 'error'
    binding.write_text(json.dumps({'dag_id': 'd', 'run_id': 'r', 'revision': None, 'deployment_verified': False}))
    evidence = tools.configuration()
    assert evidence['availability'] == 'available'
    assert evidence['data']['alignment'] == 'unknown'
    assert not evidence['data']['schema_available']
    assert tools.source('batch_ingest.py', 1, 20)['availability'] == 'missing'
