"""Narrow protocol normalization; never select a diagnosis from conflicting output."""
from langchain.agents.middleware.types import AgentMiddleware, ModelResponse
from langchain.agents.structured_output import StructuredOutputValidationError
from afi.errors import redact


class ResponsePhaseOutput(AgentMiddleware):
    """Parse explicitly labeled completed final messages, retaining raw output.

    Observed with this pinned Responses/LangChain combination: two distinct
    final_answer output messages carry the same complete JSON; other responses
    include commentary followed by final_answer. ProviderStrategy concatenates
    their text and rejects Extra data. Respect explicit phase labels, never infer
    a final message from its position or content. No first-JSON heuristics,
    truncation recovery, competing-answer selection or evidence-ID rewriting.
    """
    def __init__(self, schema, store, inv, usage):
        self.schema, self.store, self.inv, self.usage = schema, store, inv, usage

    def wrap_model_call(self, request, handler):
        try:
            return handler(request)
        except StructuredOutputValidationError as exc:
            message = exc.ai_message
            if message.response_metadata.get('status') != 'completed' or message.tool_calls or message.invalid_tool_calls:
                raise
            content = message.content
            if not isinstance(content, list):
                raise
            texts = [b for b in content if isinstance(b, dict) and b.get('type') == 'text']
            if not 2 <= len(texts) <= 16 or any(b.get('phase') not in ('commentary', 'final_answer') for b in texts):
                raise
            finals = [b for b in texts if b['phase'] == 'final_answer']
            if not finals:
                raise
            first = finals[0].get('text')
            if not isinstance(first, str) or len(first.encode('utf-8')) > 32000:
                raise
            if any(b.get('text') != first for b in finals[1:]):
                raise
            if any(not isinstance(b, dict) or b.get('type') not in ('text', 'reasoning') for b in content):
                raise
            try:
                candidate = self.schema.model_validate_json(first)
            except ValueError:
                raise exc
            self.store.operation(self.inv, 'provider_output', 'normalized', {
                'reason': 'Completed explicit final_answer; multiple finals must be byte-identical',
                'schema': self.schema.__name__, 'response_status': 'completed',
                'blocks': [{'id': b.get('id'), 'phase': b['phase'], 'characters': len(str(b.get('text', '')))} for b in texts],
                'public_text': redact(first, 32000), 'application_validated': False,
                'limitation': 'Schema parsing only; report evidence integrity and semantic review remain separate'})
            self.usage.normalized_outputs += 1
            print('provider output: explicit completed final phase parsed; original messages retained', flush=True)
            # Complete original content/metadata stays in LangGraph checkpoints.
            return ModelResponse(result=[message], structured_response=candidate)
