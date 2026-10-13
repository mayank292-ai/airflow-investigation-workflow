"""Explicit deterministic model double. This is not live model or Airflow evidence."""
import json
import uuid
import pytest
from typing import Any
from pydantic import Field
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, ToolMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from afi.config import Settings
from afi.store import Store
from afi.schema import Identity, Diagnosis, Claim
from afi.agent import run_agent
from afi.schema import ReportValidationError


class DeterministicModel(BaseChatModel):
    @property
    def _llm_type(self):
        return 'explicit-test-double'

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        tool_results = [json.loads(m.content) for m in messages if isinstance(m, ToolMessage)]
        if not tool_results:
            answer = AIMessage(content='', tool_calls=[{'name': 'run_overview', 'args': {}, 'id': 'test-tool-call', 'type': 'tool_call'}])
        else:
            ref = tool_results[0]['id']
            claim = Claim(text='Synthetic overview records a failed task; cause unresolved.', support='supported', evidence_ids=[ref])
            diagnosis = Diagnosis(source_alignment='unknown', task_purpose=claim, observed_failure=claim,
                                  confirmed_findings=[], hypotheses=[], missing_or_conflicting_evidence=['Logs/source not inspected by this test double'],
                                  downstream_effects=[], data_write_retry_assessment=[], remediation_options=[], limitations=['Test double only'])
            answer = AIMessage(content=diagnosis.model_dump_json())
        return ChatResult(generations=[ChatGeneration(message=answer)])


class FakeAirflow:
    def overview(self, identity):
        return {'tasks': [{'task_id': 't', 'state': 'failed', 'try_number': 1}]}, False


def test_real_framework_tool_loop_checkpoint_and_fresh_thread(tmp_path):
    settings = Settings(root=tmp_path)
    store = Store(settings.state)
    identity = Identity(dag_id='d', run_id='r')
    inv, _ = store.create(identity)
    run_agent(settings, store, inv, FakeAirflow(), model_override=DeterministicModel())
    first_thread = store.get(inv)['thread_id']
    assert (settings.state / 'checkpoints.sqlite').exists()
    reopened = Store(settings.state)
    run_agent(settings, reopened, inv, FakeAirflow(), question='Explain missing evidence', model_override=DeterministicModel())
    assert store.get(inv)['thread_id'] == first_thread
    second, _ = reopened.create(identity)
    run_agent(settings, reopened, second, FakeAirflow(), model_override=DeterministicModel())
    assert store.get(second)['thread_id'] != first_thread
    assert set(store.evidence(second)).isdisjoint(store.evidence(inv))
    assert store.latest(second)[1]['runtime']['evaluation_mode'] == 'test double'


class RetrievalLoopModel(BaseChatModel):
    """Deliberately over-requests retrieval until the application removes tools."""
    enabled: bool = True
    has_schema: bool = False
    trace: list = Field(default_factory=list)

    @property
    def _llm_type(self):
        return 'retrieval-loop-test-double'

    def bind_tools(self, tools, **kwargs):
        return self.model_copy(update={'enabled': bool(tools), 'has_schema': 'response_format' in kwargs})

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.trace.append({'tools_enabled': self.enabled, 'has_schema': self.has_schema})
        if self.enabled:
            answer = AIMessage(content='', tool_calls=[{'name': 'run_overview', 'args': {},
                                'id': str(uuid.uuid4()), 'type': 'tool_call'} for _ in range(3)])
        else:
            evidence = [json.loads(m.content) for m in messages if isinstance(m, ToolMessage) and m.status != 'error']
            claim = Claim(text='Failure observed; further diagnosis unresolved at retrieval limit.',
                          support='supported', evidence_ids=[evidence[0]['id']])
            answer = AIMessage(content=Diagnosis(source_alignment='unknown', task_purpose=claim,
                observed_failure=claim, confirmed_findings=[], hypotheses=[],
                missing_or_conflicting_evidence=['Source not retrieved'], downstream_effects=[],
                data_write_retry_assessment=[], remediation_options=[],
                limitations=['Explicit test double; retrieval stopped at application limit']).model_dump_json())
        return ChatResult(generations=[ChatGeneration(message=answer)])


def test_partial_tool_batch_finishes_with_schema_without_exceeding_retrieval(tmp_path):
    settings = Settings(root=tmp_path, max_model_calls=5, max_tool_calls=4)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    model = RetrievalLoopModel()
    run_agent(settings, store, inv, FakeAirflow(), model_override=model)
    report = store.latest(inv)[1]
    assert report['runtime']['tool_calls'] == 4
    assert report['runtime']['model_calls'] == 3
    assert len(store.evidence(inv)) == 4
    assert model.trace == [{'tools_enabled': True, 'has_schema': True},
                           {'tools_enabled': True, 'has_schema': True},
                           {'tools_enabled': False, 'has_schema': True}]
    assert store.get(inv)['status'] == 'complete'


def test_last_model_call_is_reserved_for_structured_finalization(tmp_path):
    settings = Settings(root=tmp_path, max_model_calls=2, max_tool_calls=24)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    model = RetrievalLoopModel()
    run_agent(settings, store, inv, FakeAirflow(), model_override=model)
    report = store.latest(inv)[1]
    assert report['runtime']['model_calls'] == 2 and report['runtime']['tool_calls'] == 3
    assert model.trace[-1] == {'tools_enabled': False, 'has_schema': True}


class BadReferenceModel(DeterministicModel):
    always_invalid: bool = False

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        results = [json.loads(m.content) for m in messages if isinstance(m, ToolMessage)]
        if not results:
            return super()._generate(messages, stop, run_manager, **kwargs)
        correction = any(isinstance(m, HumanMessage) and 'Application report validation failed:' in str(m.content)
                         for m in messages)
        ref = results[0]['id'] if correction and not self.always_invalid else 'ev_not_in_this_investigation'
        claim = Claim(text='Explicit synthetic test failure; cause unresolved.', support='supported', evidence_ids=[ref])
        diagnosis = Diagnosis(source_alignment='unknown', task_purpose=claim, observed_failure=claim,
            confirmed_findings=[], hypotheses=[], missing_or_conflicting_evidence=[], downstream_effects=[],
            data_write_retry_assessment=[], remediation_options=[], limitations=['Explicit test double'])
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=diagnosis.model_dump_json()))])


def test_one_reference_correction_preserves_rejected_candidate(tmp_path):
    settings = Settings(root=tmp_path, max_model_calls=4)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    run_agent(settings, store, inv, FakeAirflow(), model_override=BadReferenceModel())
    _, rejected = store.latest(inv, 'report_repair')
    assert not rejected['validated'] and 'Unknown' in rejected['error']
    report = store.latest(inv)[1]
    assert report['runtime']['report_repairs'] == 1
    assert report['runtime']['model_calls'] == 3 and report['runtime']['tool_calls'] == 1
    assert report['diagnosis']['observed_failure']['evidence_ids'][0] in store.evidence(inv)


def test_second_invalid_candidate_stops_without_weakening_reference_check(tmp_path):
    settings = Settings(root=tmp_path, max_model_calls=8)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    with pytest.raises(ReportValidationError, match='Unknown'):
        run_agent(settings, store, inv, FakeAirflow(), model_override=BadReferenceModel(always_invalid=True))
    assert store.get(inv)['status'] == 'incomplete'
    with store.connect() as db:
        assert db.execute("SELECT count(*) FROM operations WHERE investigation_id=? AND kind='report_repair'", (inv,)).fetchone()[0] == 2
    assert store.latest(inv, 'failure')[1]['runtime']['report_repairs'] == 1


class RepeatedFinalModel(DeterministicModel):
    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        result = super()._generate(messages, stop, run_manager, **kwargs)
        message = result.generations[0].message
        if not message.tool_calls:
            message = message.model_copy(update={'content': [
                {'type': 'text', 'text': message.content, 'phase': 'final_answer', 'id': 'one'},
                {'type': 'text', 'text': message.content, 'phase': 'final_answer', 'id': 'two'}],
                'response_metadata': {'status': 'completed'}})
        return ChatResult(generations=[ChatGeneration(message=message)])


def test_protocol_normalization_in_real_framework_keeps_report_validation(tmp_path):
    settings = Settings(root=tmp_path)
    store = Store(settings.state)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    run_agent(settings, store, inv, FakeAirflow(), model_override=RepeatedFinalModel())
    report = store.latest(inv)[1]
    assert report['runtime']['normalized_provider_outputs'] == 1
    assert report['runtime']['model_calls'] == 2 and report['runtime']['report_repairs'] == 0
    assert report['diagnosis']['observed_failure']['evidence_ids'][0] in store.evidence(inv)
    assert store.latest(inv, 'provider_output')[1]['application_validated'] is False
