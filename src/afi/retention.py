"""Select bounded evidence bodies from one request; never retrieve or edit storage."""
import json
from langchain_core.messages import ToolMessage


def recent_attempt_logs(messages, max_attempts=2, max_bytes=16000):
    retained, selectors, size = {}, set(), 0
    for message in reversed(messages):
        if not isinstance(message, ToolMessage) or message.name != 'attempt_logs':
            continue
        try:
            evidence = json.loads(message.content)
            args = evidence['provenance']['selector']
            selector = (args['task_id'], args['map_index'], args['attempt'])
            text = evidence['data']['text']
            length = len(message.content.encode('utf-8'))
            if evidence['availability'] != 'available' or not text.strip() or selector in selectors:
                continue
        except (ValueError, KeyError, TypeError, AttributeError):
            continue
        if len(retained) >= max_attempts or size + length > max_bytes:
            continue
        retained[message.tool_call_id] = message
        selectors.add(selector)
        size += length
    return retained
