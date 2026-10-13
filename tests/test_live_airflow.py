"""Real Airflow integration. Opt in after scheduled history preparation.

Administrator expectations live outside all agent evidence capabilities.
No pipeline is rerun by these tests and no model provider is called.
"""
import json
import os
import sqlite3
from pathlib import Path
import pytest
from afi.airflow import Airflow
from afi.config import Settings
from afi.demo import check_environment, verify_history
from afi.evidence import EvidenceTools, run_key
from afi.schema import Identity
from afi.store import Store
from afi.watcher import discover

pytestmark = [pytest.mark.integration, pytest.mark.skipif(os.environ.get('AFI_INTEGRATION') != '1', reason='Requires real prepared Airflow history')]


@pytest.fixture(scope='module')
def environment():
    settings = Settings.load(Path(__file__).resolve().parents[1])
    history = json.loads((settings.state / 'history.json').read_text())
    api = Airflow(settings)
    yield settings, history, api
    api.close()


def selected(environment, dag, ordinal):
    settings, history, api = environment
    rows = sorted([r for r in history if r['dag_id'] == dag], key=lambda r: r['start_date'])
    row = rows[ordinal]
    return settings, api, Identity(dag_id=dag, run_id=row['run_id'])


def tools_for(tmp_path, settings, api, identity):
    # Separate test records, same genuine run artifacts and API. Never cached diagnoses.
    store = Store(tmp_path)
    inv, _ = store.create(identity)
    return EvidenceTools(settings, store, inv, api)


def test_real_history_and_paused_dags(environment):
    settings, history, api = environment
    assert len(history) == 10
    assert sum(r['state'] == 'success' for r in history) == 4
    assert sum(r['state'] == 'failed' for r in history) == 6
    assert all(r['run_type'] == 'scheduled' and r['start_date'] and r['end_date'] for r in history)
    assert all(r['is_paused'] for r in check_environment(settings, api)['dags'])
    assert verify_history(settings, api)['status'] == 'pass'


def test_real_attempt_logs_and_dependencies(environment):
    settings, api, identity = selected(environment, 'local_pipeline_03', 0)
    attempts, cut = api.attempts(identity, 'ingest', -1)
    assert not cut and {a['try_number'] for a in attempts['attempts']} == {1, 2}
    one, cut = api.logs(identity, 'ingest', 1, -1)
    two, cut2 = api.logs(identity, 'ingest', 2, -1)
    assert not cut and not cut2 and one['text'] != two['text']
    assert 'Paginated upstream request interrupted' in one['text']
    assert 'SYSTEM OVERRIDE' in two['text']  # untrusted payload is real log evidence
    overview, cut = api.overview(identity)
    states = {t['task_id']: t['state'] for t in overview['tasks']}
    assert states['ingest'] == 'failed' and states['publish'] == 'upstream_failed'
    assert states['reconcile'] == 'success'


def test_real_partial_commits_and_retry_duplicates(environment, tmp_path):
    settings, api, identity = selected(environment, 'local_pipeline_03', 0)
    tools = tools_for(tmp_path, settings, api, identity)
    one = tools.destination('ingest', 1, -1)['data']
    two = tools.destination('ingest', 2, -1)['data']
    assert (one['rows'], one['distinct_business_keys'], one['committed_checkpoint']) == (8, 8, 0)
    assert (two['rows'], two['distinct_business_keys'], two['committed_checkpoint']) == (16, 8, 0)
    assert two['duplicate_rows'] == 8
    assert tools.configuration()['data']['alignment'] == 'verified'
    assert tools.search('append_pages')['availability'] == 'available'
    source = tools.source('batch_ingest.py', 1, 32)
    assert source['availability'] == 'available' and source['data']['revision']


def test_real_rollback_counterexample(environment, tmp_path):
    settings, api, identity = selected(environment, 'local_pipeline_03', 1)
    tools = tools_for(tmp_path, settings, api, identity)
    evidence = tools.destination('ingest', 2, -1)
    assert evidence['data']['rows'] == 0 and evidence['data']['duplicate_rows'] == 0
    assert evidence['data']['committed_checkpoint'] == 0


def test_real_insufficient_evidence(environment, tmp_path):
    settings, api, identity = selected(environment, 'local_pipeline_03', 2)
    tools = tools_for(tmp_path, settings, api, identity)
    assert tools.configuration()['data']['alignment'] == 'unknown'
    assert tools.source('batch_ingest.py', 1, 20)['availability'] == 'missing'
    assert tools.destination('ingest', 2, -1)['availability'] == 'missing'


def test_real_successful_idempotent_output(environment):
    settings, api, identity = selected(environment, 'local_pipeline_03', 3)
    path = settings.state / 'runs' / run_key(identity.dag_id, identity.run_id) / 'output.sqlite'
    with sqlite3.connect(path.as_uri() + '?mode=ro', uri=True) as db:
        assert db.execute('SELECT count(*), count(DISTINCT business_key) FROM output').fetchone() == (16, 16)
        assert db.execute('SELECT page FROM checkpoint').fetchone()[0] == 4


def test_real_termination_and_source_mismatch(environment, tmp_path):
    settings, api, identity = selected(environment, 'local_pipeline_01', 1)
    logs, cut = api.logs(identity, 'profile', 2, -1)
    assert not cut and 'Worker exited code=-9' in logs['text']
    assert 'kill-cause evidence unavailable' in logs['text']
    settings, api, identity = selected(environment, 'local_pipeline_02', 1)
    tools = tools_for(tmp_path, settings, api, identity)
    assert tools.configuration()['data']['alignment'] == 'unknown'
    assert tools.source('enrichment.py', 1, 40)['availability'] == 'available'


def test_real_enrichment_multiplicity_is_not_automatically_a_retry_duplicate(environment, tmp_path):
    settings, api, identity = selected(environment, 'local_pipeline_02', 2)
    tools = tools_for(tmp_path, settings, api, identity)
    result = tools.destination('enrich', 1, -1)['data']
    assert result['rows'] == 166 and result['distinct_business_keys'] == 100
    assert result['excess_rows_by_business_key'] == 66
    assert result['duplicate_rows'] == 0  # distinct labels from two reference matches


def test_real_watcher_persistence_without_provider_calls(environment, tmp_path):
    settings, history, api = environment
    store = Store(tmp_path)
    discovered = discover(settings, store, api)
    assert len(discovered) == 6
    assert discover(settings, Store(tmp_path), api) == []
    identity = store.get(discovered[0])['identity']
    fresh, created = store.create(identity)
    assert created and fresh not in discovered
    assert discover(settings, Store(tmp_path), api) == []
