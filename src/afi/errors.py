"""Bounded provider diagnostics without request headers, keys or raw tracebacks."""
import os
import re


def redact(value, limit=2000):
    value = str(value)
    key = os.environ.get('OPENAI_API_KEY')
    if key:
        value = value.replace(key, '[REDACTED]')
    return re.sub(r'sk-[A-Za-z0-9_-]+', '[REDACTED]', value)[:limit]


def provider_error(exc):
    seen, result = set(), {'error_type': type(exc).__name__}
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if type(exc).__name__ == 'ReportValidationError':
            result['report_validation_error'] = redact(exc, 1200)
        if type(exc).__name__ == 'StructuredOutputValidationError':
            result['schema_name'] = getattr(exc, 'tool_name', 'unknown')
            result['schema_error'] = redact(getattr(exc, 'source', 'unavailable'))
            message = getattr(exc, 'ai_message', None)
            if message is not None:
                metadata = message.response_metadata
                result['response_status'] = metadata.get('status')
                result['finish_reason'] = metadata.get('finish_reason')
                result['incomplete_details'] = metadata.get('incomplete_details')
                # Preserve bounded text for debugging; not a validated diagnosis.
                text = message.text
                result['unvalidated_output'] = redact(text, 16000)
                result['unvalidated_output_truncated'] = len(text) > 16000
                # Public text metadata only; never log reasoning blocks/headers.
                if isinstance(message.content, list):
                    result['output_text_blocks'] = [
                        {'id': block.get('id'), 'phase': block.get('phase'),
                         'characters': len(str(block.get('text', '')))}
                        for block in message.content if isinstance(block, dict) and block.get('type') == 'text'][:16]
        status = getattr(exc, 'status_code', None)
        if isinstance(status, int):
            result['http_status'] = status
        if getattr(exc, 'winerror', None):
            result['windows_socket_error'] = exc.winerror
        body = getattr(exc, 'body', None)
        if isinstance(body, dict):
            fields = body.get('error', body)
            if isinstance(fields, dict):
                for name in ('code', 'type', 'param', 'message'):
                    value = fields.get(name)
                    if isinstance(value, str):
                        result[name] = redact(value, 1200)
        exc = exc.__cause__ or exc.__context__
    return result
