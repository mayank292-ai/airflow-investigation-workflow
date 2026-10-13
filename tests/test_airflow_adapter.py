import httpx
import pytest
from afi.airflow import Airflow, AirflowError
from afi.config import Settings
from afi.schema import Identity


def test_attempt_specific_mapped_log_pagination(tmp_path):
    calls = []
    def handler(request):
        if request.url.path == '/auth/token':
            return httpx.Response(201, json={'access_token': 'synthetic-token'})
        calls.append(request)
        page = 2 if request.url.params.get('token') else 1
        return httpx.Response(200, json={'content': [f'attempt 2 page {page}'], 'continuation_token': 'next' if page == 1 else None})
    api = Airflow(Settings(root=tmp_path), httpx.MockTransport(handler))
    data, truncated = api.logs(Identity(dag_id='d', run_id='scheduled__date'), 'mapped', 2, 7)
    assert len(calls) == 2
    assert all(r.url.path.endswith('/logs/2') and r.url.params['map_index'] == '7' for r in calls)
    assert 'page 1' in data['text'] and 'page 2' in data['text'] and not truncated


def test_auth_refresh_bounded(tmp_path):
    api = Airflow(Settings(root=tmp_path), httpx.MockTransport(lambda r: httpx.Response(201, json={'access_token': 'x'}) if r.url.path == '/auth/token' else httpx.Response(401)))
    with pytest.raises(AirflowError):
        api.request('GET', '/api/v2/dags')


def test_log_truncation_and_repeated_token(tmp_path):
    api = Airflow(Settings(root=tmp_path), httpx.MockTransport(lambda r: httpx.Response(201, json={'access_token': 'x'}) if r.url.path == '/auth/token' else httpx.Response(200, json={'content': ['x' * 20000], 'continuation_token': 'same'})))
    data, truncated = api.logs(Identity(dag_id='d', run_id='r'), 't', 1, -1)
    assert truncated and len(data['text']) <= 12000


def test_read_timeout_retries_once_but_mutations_are_not_retried(tmp_path, monkeypatch):
    monkeypatch.setattr('afi.airflow.time.sleep', lambda seconds: None)
    calls = []
    def handler(request):
        calls.append(request.method)
        if len(calls) == 1 or request.method == 'POST':
            raise httpx.ReadTimeout('explicit transport double', request=request)
        return httpx.Response(200, json={'state': 'failed'})
    api = Airflow(Settings(root=tmp_path), httpx.MockTransport(handler))
    api.token = 'synthetic-token'
    assert api.request('GET', '/api/v2/example')['state'] == 'failed'
    assert calls == ['GET', 'GET']
    with pytest.raises(AirflowError, match='1 attempt'):
        api.request('POST', '/api/v2/example', json={})
    assert calls == ['GET', 'GET', 'POST']


def test_repeated_get_timeout_is_bounded(tmp_path, monkeypatch):
    monkeypatch.setattr('afi.airflow.time.sleep', lambda seconds: None)
    calls = []
    def handler(request):
        calls.append(request.method)
        raise httpx.ReadTimeout('explicit transport double', request=request)
    api = Airflow(Settings(root=tmp_path), httpx.MockTransport(handler))
    api.token = 'synthetic-token'
    with pytest.raises(AirflowError, match='2 attempt'):
        api.request('GET', '/api/v2/example')
    assert calls == ['GET', 'GET']
