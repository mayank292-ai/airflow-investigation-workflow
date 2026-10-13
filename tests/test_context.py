"""Context editing must preserve original evidence and tool call pairing."""
from copy import deepcopy
from langchain_core.messages import AIMessage, ToolMessage, HumanMessage
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain.agents.middleware.types import ModelRequest
from afi.agent import context_editor, request_size, Usage, AdmissionRejected, FollowupContext, EvidenceContext
from afi.config import Settings
from afi.store import Store
from afi.schema import Identity
import pytest
import json


def test_older_tool_output_is_omitted_only_from_request():
    messages = []
    names = ['attempt_logs', 'destination_summary'] + ['source_read'] * 5
    for n, name in enumerate(names):
        call = str(n)
        messages += [AIMessage(content='', tool_calls=[{'id': call, 'name': name, 'args': {}, 'type': 'tool_call'}]),
                     ToolMessage(content='x' * 12000, tool_call_id=call, name=name)]
    before = deepcopy(messages)
    request = ModelRequest(model=FakeListChatModel(responses=['unused']), messages=messages)
    seen = []
    context_editor().wrap_model_call(request, lambda edited: seen.append(edited) or AIMessage(content='test only'))
    edited_tools = [m for m in seen[0].messages if isinstance(m, ToolMessage)]
    assert len(edited_tools[0].content) < 200
    assert len(edited_tools[1].content) == 12000  # destination is explicitly preserved
    assert all(len(m.content) == 12000 for m in edited_tools[-4:])
    assert messages == before  # original checkpoint/evidence content is untouched
    assert [m.tool_call_id for m in edited_tools] == [str(n) for n in range(7)]


def test_local_metadata_not_counted_but_request_content_and_arguments_are(tmp_path):
    message = AIMessage(content='small request', response_metadata={'local_only': 'x' * 120000})
    assert request_size([message]) < 1000
    usage = Usage(Settings(root=tmp_path))
    usage.on_chat_model_start({}, [[message]])
    assert usage.calls == 1
    large = AIMessage(content='x' * 70000)
    with pytest.raises(AdmissionRejected):
        usage.on_chat_model_start({}, [[large]])
    assert usage.calls == 1  # rejected before dispatch, not another billable request


def test_handoff_is_same_investigation_and_preserves_history(tmp_path):
    store = Store(tmp_path)
    inv, _ = store.create(Identity(dag_id='d', run_id='r'))
    messages = [HumanMessage(content='old question'), AIMessage(content='x' * 60000), HumanMessage(content='new question')]
    seen = []
    request = ModelRequest(model=FakeListChatModel(responses=['unused']), messages=messages)
    FollowupContext(store, inv).wrap_model_call(request, lambda edited: seen.append(edited.messages) or AIMessage(content='test'))
    assert request_size(seen[0]) < 2000 and seen[0][-1].content == 'new question'
    assert len(messages[1].content) == 60000
    fresh, _ = store.create(Identity(dag_id='d', run_id='r'))
    fresh_messages = [HumanMessage(content='fresh question'), AIMessage(content='x' * 60000)]
    seen.clear()
    FollowupContext(store, fresh).wrap_model_call(ModelRequest(model=request.model, messages=fresh_messages), lambda edited: seen.append(edited.messages) or AIMessage(content='test'))
    assert seen[0] == fresh_messages  # no report handoff into a fresh thread


def test_recent_source_body_survives_later_log_reads_without_changing_checkpoint():
    source = {'id': 'ev_source', 'availability': 'available', 'data': {'revision': 'a' * 64,
              'path': 'helper.py', 'start': 1, 'end': 2, 'text': '1: write()\n2: commit()'}}
    messages = [HumanMessage(content='Inspect helper'),
                AIMessage(content='', tool_calls=[{'id': 's', 'name': 'source_read', 'args': {}, 'type': 'tool_call'}]),
                ToolMessage(content=json.dumps(source), tool_call_id='s', name='source_read')]
    for n in range(5):
        messages += [AIMessage(content='', tool_calls=[{'id': str(n), 'name': 'attempt_logs', 'args': {}, 'type': 'tool_call'}]),
                     ToolMessage(content='x' * 12000, tool_call_id=str(n), name='attempt_logs')]
    before = deepcopy(messages)
    seen = []
    request = ModelRequest(model=FakeListChatModel(responses=['unused']), messages=messages)
    EvidenceContext().wrap_model_call(request, lambda edited: seen.append(edited) or AIMessage(content='test'))
    result = [m for m in seen[0].messages if isinstance(m, ToolMessage)]
    assert json.loads(result[0].content) == source
    assert len(result[1].content) < 200  # older log is still cleared
    assert messages == before


def test_source_retention_is_bounded_and_does_not_promote_failed_reads():
    messages = []
    for n in range(8):
        evidence = {'id': f'ev_{n}', 'availability': 'available' if n < 7 else 'error',
                    'data': {'revision': 'a' * 64, 'path': f'{n}.py', 'start': 1, 'end': 1, 'text': 'x' * 11000}}
        messages += [AIMessage(content='', tool_calls=[{'id': str(n), 'name': 'source_read', 'args': {}, 'type': 'tool_call'}]),
                     ToolMessage(content=json.dumps(evidence), tool_call_id=str(n), name='source_read')]
    seen = []
    request = ModelRequest(model=FakeListChatModel(responses=['unused']), messages=messages)
    EvidenceContext().wrap_model_call(request, lambda edited: seen.append(edited) or AIMessage(content='test'))
    by_id = {m.tool_call_id: m.content for m in seen[0].messages if isinstance(m, ToolMessage)}
    assert len(by_id['0']) < 200 and len(by_id['6']) > 11000
    assert len(by_id['3']) < 200  # outside bounded recent source selection and keep=4


def test_attempt_log_survives_later_source_reads_without_checkpoint_mutation():
    from tests.test_log_retention import log
    messages = [HumanMessage(content='Inspect failure'),
                AIMessage(content='', tool_calls=[{'id': 'l', 'name': 'attempt_logs', 'args': {}, 'type': 'tool_call'}]),
                log('l', 2, 'ConnectionError at page 2')]
    for n in range(5):
        evidence = {'id': f'ev_{n}', 'availability': 'available', 'data': {'revision': 'a' * 64,
                    'path': f'{n}.py', 'start': 1, 'end': 1, 'text': 'x' * 11000}}
        messages += [AIMessage(content='', tool_calls=[{'id': str(n), 'name': 'source_read', 'args': {}, 'type': 'tool_call'}]),
                     ToolMessage(content=json.dumps(evidence), tool_call_id=str(n), name='source_read')]
    before = deepcopy(messages)
    seen = []
    EvidenceContext().wrap_model_call(ModelRequest(model=FakeListChatModel(responses=['unused']), messages=messages),
                                      lambda edited: seen.append(edited.messages) or AIMessage(content='test'))
    assert 'ConnectionError at page 2' in next(m.content for m in seen[0] if isinstance(m, ToolMessage) and m.tool_call_id == 'l')
    assert messages == before
