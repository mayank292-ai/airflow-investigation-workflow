"""Explicit HTTP double verifies wire routing; this is not live provider evaluation."""
import json
import httpx
import pytest
from langchain_openai import ChatOpenAI
from langchain.tools import tool
from afi.agent import investigator_model
from afi.config import Settings
from afi.errors import provider_error
from afi.schema import Diagnosis


def test_reasoning_tools_use_responses_wire_contract(tmp_path, monkeypatch):
    requests = []
    def response(request):
        requests.append(request)
        return httpx.Response(400, json={'error': {'type': 'invalid_request_error', 'message': 'Explicit test rejection'}})
    transport = httpx.MockTransport(response)
    def model_factory(**kwargs):
        return ChatOpenAI(**kwargs, api_key='test-not-real', http_client=httpx.Client(transport=transport))
    monkeypatch.setattr('afi.agent.ChatOpenAI', model_factory)

    @tool
    def sample_tool(value: str):
        """Explicit test tool; never executed."""
        raise AssertionError('No tool may execute in this wire test')

    model = investigator_model(Settings(root=tmp_path))
    with pytest.raises(Exception) as captured:
        model.bind_tools([sample_tool], response_format={'type': 'json_schema', 'json_schema': {
            'name': 'Diagnosis', 'schema': Diagnosis.model_json_schema(), 'strict': True}}).invoke('Wire test only')
    assert provider_error(captured.value)['http_status'] == 400
    assert len(requests) == 1
    request = requests[0]
    assert request.url.path == '/v1/responses'
    payload = json.loads(request.content)
    assert payload['reasoning']['effort'] == 'low'
    assert payload['store'] is False
    assert payload['tools'][0]['type'] == 'function'
    assert payload['text']['format']['type'] == 'json_schema'
    assert payload['text']['format']['strict'] is True


def test_provider_diagnostics_redact_secret(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test-credential')
    class ExampleError(Exception):
        status_code = 400
        body = {'message': 'Invalid sk-test-credential and sk-another-credential', 'param': 'test'}
    details = provider_error(ExampleError())
    assert 'sk-' not in details['message']
    assert '[REDACTED]' in details['message']


def test_structured_failure_preserves_bounded_redacted_output(monkeypatch):
    from langchain.agents.structured_output import StructuredOutputValidationError
    from langchain_core.messages import AIMessage
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test-credential')
    error = StructuredOutputValidationError('Diagnosis', ValueError('unexpected field sk-test-credential'),
        AIMessage(content='sk-test-credential ' + 'x' * 20000, response_metadata={'status': 'completed'}))
    details = provider_error(error)
    assert details['schema_name'] == 'Diagnosis' and details['response_status'] == 'completed'
    assert 'sk-' not in details['schema_error'] and 'sk-' not in details['unvalidated_output']
    assert len(details['unvalidated_output']) <= 16000 and details['unvalidated_output_truncated']


def test_parser_diagnostics_show_public_message_phases_without_reasoning():
    from langchain.agents.structured_output import StructuredOutputValidationError
    from langchain_core.messages import AIMessage
    message = AIMessage(content=[{'type': 'text', 'text': '{}', 'id': 'a', 'phase': 'commentary'},
                                {'type': 'text', 'text': '{}', 'id': 'b', 'phase': 'final_answer'},
                                {'type': 'reasoning', 'summary': [{'text': 'private-test'}]}])
    details = provider_error(StructuredOutputValidationError('Diagnosis', ValueError('Extra data'), message))
    assert details['output_text_blocks'] == [{'id': 'a', 'phase': 'commentary', 'characters': 2},
                                            {'id': 'b', 'phase': 'final_answer', 'characters': 2}]
    assert 'private-test' not in json.dumps(details)
