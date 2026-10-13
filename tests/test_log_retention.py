import json
from copy import deepcopy
from langchain_core.messages import ToolMessage
from afi.retention import recent_attempt_logs


def log(call, attempt, body, availability='available'):
    value = {'id': 'ev_' + call, 'availability': availability,
             'provenance': {'selector': {'task_id': 't', 'map_index': -1, 'attempt': attempt}},
             'data': {'text': body}}
    return ToolMessage(content=json.dumps(value), tool_call_id=call, name='attempt_logs')


def test_distinct_attempts_bounded_and_empty_or_failed_reads_do_not_displace_text():
    messages = [log('one', 1, 'first failure'), log('two', 2, 'second failure'),
                log('empty', 2, ''), log('error', 1, 'not a real log', 'error')]
    before = deepcopy(messages)
    assert set(recent_attempt_logs(messages)) == {'one', 'two'}
    assert messages == before
    assert len(recent_attempt_logs([log(str(n), n, 'x' * 9000) for n in range(5)])) == 1
    assert len(recent_attempt_logs([log(str(n), n, 'small') for n in range(5)])) == 2


def test_malformed_or_nonlog_messages_are_not_promoted():
    assert recent_attempt_logs([ToolMessage(content='not JSON', tool_call_id='x', name='attempt_logs'),
                                ToolMessage(content='{}', tool_call_id='y', name='source_read')]) == {}
