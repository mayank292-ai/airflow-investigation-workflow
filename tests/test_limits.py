"""Bounded admission and interruption tests; no provider calls or Docker execution."""
import subprocess
import pytest
from afi.agent import BudgetExceeded, budget_room, reserve_budget
from afi.config import Settings
from afi.process import execute_bounded
from afi.schema import Identity
from afi.store import Store


def test_global_budget_survives_restart_and_denies_before_provider(tmp_path):
    settings = Settings(root=tmp_path)
    store = Store(settings.state)
    reserve_budget(store, settings)
    reserve_budget(store, settings)
    reopened = Store(settings.state)
    assert not budget_room(reopened, settings)['can_admit']
    with pytest.raises(BudgetExceeded):
        reserve_budget(reopened, settings)
    with reopened.connect() as db:
        assert db.execute('SELECT count(*) FROM budget').fetchone()[0] == 2


def test_larger_context_reserves_more_without_raising_approved_total(tmp_path):
    settings = Settings(root=tmp_path, context_bytes=120000, max_usd=1.8)
    store = Store(settings.state)
    reservation, amount = reserve_budget(store, settings)
    assert amount == pytest.approx(1.728)
    assert budget_room(store, settings)['approved_total'] == 3.0
    with pytest.raises(BudgetExceeded):
        reserve_budget(store, settings)


def test_explicit_budget_setting_preserves_existing_ledger_and_still_stops(tmp_path):
    default = Settings(root=tmp_path)
    store = Store(default.state)
    reserve_budget(store, default)
    reserve_budget(store, default)
    assert not budget_room(store, default)['can_admit']
    authorized = default.model_copy(update={'approved_total_usd': 5.0})
    reserve_budget(store, authorized)
    assert budget_room(store, default)['recorded_reserved_or_spent'] == pytest.approx(4.464)
    assert not budget_room(store, authorized)['can_admit']
    with pytest.raises(BudgetExceeded):
        reserve_budget(store, authorized)


def test_subprocess_timeout_preserves_interrupted_record(tmp_path, monkeypatch):
    settings = Settings(root=tmp_path, investigation_seconds=10)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    class FakeProcess:
        waits = 0
        killed = False

        def wait(self, timeout):
            self.waits += 1
            if self.waits == 1:
                raise subprocess.TimeoutExpired('explicit-test-double', timeout)
            return 0

        def kill(self):
            self.killed = True
    process = FakeProcess()
    monkeypatch.setattr('afi.process.subprocess.Popen', lambda *args, **kwargs: process)
    assert execute_bounded(settings, store, inv, mode='logs-only') == 1
    assert process.killed
    assert Store(settings.state).get(inv)['status'] == 'interrupted'
    _, failure = store.latest(inv, 'failure')
    assert failure['reason'] == 'Parent deadline terminated operation'


def test_subprocess_uses_admitted_settings_instead_of_parent_env(tmp_path, monkeypatch):
    settings = Settings(root=tmp_path, max_model_calls=5, context_bytes=100000, max_output_tokens=3000)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    monkeypatch.setenv('AFI_MAX_MODEL_CALLS', '8')
    captured = {}

    class ProcessDouble:
        def wait(self, timeout):
            return 0

    def launch(args, **kwargs):
        # Capture only non-secret selectors; never print or assert the whole env.
        captured.update({name: kwargs['env'][name] for name in
                         ('AFI_MAX_MODEL_CALLS', 'AFI_CONTEXT_BYTES', 'AFI_MAX_OUTPUT_TOKENS')})
        return ProcessDouble()

    monkeypatch.setattr('afi.process.subprocess.Popen', launch)
    assert execute_bounded(settings, store, inv) == 0
    assert captured == {'AFI_MAX_MODEL_CALLS': '5', 'AFI_CONTEXT_BYTES': '100000', 'AFI_MAX_OUTPUT_TOKENS': '3000'}


def test_usage_completion_requires_returned_response_metadata(tmp_path):
    from afi.agent import Usage
    from langchain_core.messages import AIMessage
    from langchain_core.outputs import ChatGeneration, LLMResult
    usage = Usage(Settings(root=tmp_path))
    usage.on_chat_model_start({}, [[AIMessage(content='request')]])
    assert usage.completed_calls == 0
    usage.on_llm_end(LLMResult(generations=[[ChatGeneration(message=AIMessage(content='response',
        usage_metadata={'input_tokens': 10, 'output_tokens': 5, 'total_tokens': 15}))]]))
    assert usage.completed_calls == usage.calls == 1
    usage.on_chat_model_start({}, [[AIMessage(content='second request')]])
    assert usage.completed_calls != usage.calls  # unknown in-flight usage retains reservation
