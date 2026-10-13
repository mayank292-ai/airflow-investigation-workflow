"""Protocol doubles, not provider reliability evidence."""
import json
from types import SimpleNamespace
import pytest
from langchain.agents.structured_output import StructuredOutputValidationError
from langchain_core.messages import AIMessage
from pydantic import BaseModel
from afi.output import ResponsePhaseOutput
from afi.schema import Identity
from afi.store import Store


class Example(BaseModel):
    value: str


def exercise(tmp_path, content, status='completed'):
    store = Store(tmp_path)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    usage = SimpleNamespace(normalized_outputs=0)
    message = AIMessage(content=content, response_metadata={'status': status})
    def handler(request):
        raise StructuredOutputValidationError('Example', ValueError('Extra data'), message)
    result = ResponsePhaseOutput(Example, store, inv, usage).wrap_model_call(None, handler)
    return result, message, store, inv, usage


def blocks(second='{"value":"test"}', phase='final_answer'):
    return [{'type': 'text', 'text': '{"value":"test"}', 'id': 'one', 'phase': phase},
            {'type': 'text', 'text': second, 'id': 'two', 'phase': phase}]


def test_identical_completed_finals_preserve_original_and_separate_integrity(tmp_path):
    result, original, store, inv, usage = exercise(tmp_path, blocks())
    assert result.structured_response == Example(value='test')
    assert result.result == [original] and len(result.result[0].content) == 2
    assert usage.normalized_outputs == 1
    _, record = store.latest(inv, 'provider_output')
    assert len(record['blocks']) == 2 and not record['application_validated']


def test_explicit_commentary_and_final_use_final_without_dropping_original(tmp_path):
    content = blocks('{"value":"final"}')
    content[0]['phase'] = 'commentary'
    result, original, store, inv, usage = exercise(tmp_path, content)
    assert result.structured_response == Example(value='final')
    assert result.result[0] == original and original.content[0]['text'] == '{"value":"test"}'
    assert [b['phase'] for b in store.latest(inv, 'provider_output')[1]['blocks']] == ['commentary', 'final_answer']


@pytest.mark.parametrize('content,status', [
    (blocks(), 'incomplete'),
    (blocks('{"value":"different"}'), 'completed'),
    (blocks('{"value":'), 'completed'),
    (blocks(phase='commentary'), 'completed'),
    (blocks(phase=None), 'completed'),
    ([{'type': 'text', 'text': '{}'}], 'completed'),
    ([{'type': 'text', 'text': '{'}, {'type': 'text', 'text': '{'}], 'completed')])
def test_ambiguous_partial_and_unmarked_results_remain_rejected(tmp_path, content, status):
    with pytest.raises(StructuredOutputValidationError):
        exercise(tmp_path, content, status)
