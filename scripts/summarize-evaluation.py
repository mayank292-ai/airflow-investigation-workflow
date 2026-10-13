"""Operator-only accounting/integrity review; never an investigator tool or judge."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import uuid
from afi.config import Settings
from afi.schema import Report, now, validate_references
from afi.store import Store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('evaluation', type=uuid.UUID)
    args = parser.parse_args()
    settings = Settings.load(Path(__file__).resolve().parents[1])
    store = Store(settings.state)
    folder = settings.state / 'evaluations' / str(args.evaluation)
    source = folder / 'results.json'
    raw = source.read_bytes()
    if len(raw) > 2000000:
        raise ValueError('Evaluation exceeds review size bound')
    result = json.loads(raw)
    rows, total_input, total_output, estimated_usd = [], 0, 0, 0
    for case in result['cases']:
        inv = case['investigation_id']
        kind = 'report' if case['status'] == 'complete' else 'failure'
        operation, payload = store.latest(inv, kind)
        runtime = payload.get('runtime', {})
        total_input += runtime.get('input_tokens', 0)
        total_output += runtime.get('output_tokens', 0)
        estimated_usd += runtime.get('estimated_usd', 0)
        row = {key: case[key] for key in ('identity', 'condition', 'iteration', 'investigation_id', 'status')}
        row.update(operation=operation, runtime=runtime)
        if kind == 'report':
            report = Report.model_validate(payload)
            assert report.identity.model_dump() == case['identity']
            assert report.investigation_id == inv
            validate_references(report.diagnosis, store.evidence(inv))
            row['structural_integrity'] = 'pass; semantic correctness ungraded'
        else:
            row.update(error_type=payload.get('error_type'),
                       diagnostics={k: v for k, v in payload.get('provider_error', {}).items()
                                    if k != 'unvalidated_output'})
        rows.append(row)
    review = {'collected_at': now(), 'evaluation': str(args.evaluation),
              'origin': 'Deterministic operator accounting and report integrity; no semantic model judge',
              'original_results_sha256': hashlib.sha256(raw).hexdigest(),
              'outcomes': dict(Counter('complete' if r['status'] == 'complete' else r['error_type'] for r in rows)),
              'estimated_usd': estimated_usd, 'input_tokens': total_input, 'output_tokens': total_output,
              'cases': rows,
              'limitations': ['Completion and valid references do not prove semantic correctness',
                              'Provider costs are estimates; local ledger also includes retry allowance',
                              'Saved source results remain unchanged; this is post-execution collection']}
    path = folder / ('accounting-' + str(uuid.uuid4()) + '.json')
    path.write_text(json.dumps(review, indent=2), encoding='utf-8')
    print(json.dumps({key: review[key] for key in ('outcomes', 'estimated_usd', 'input_tokens', 'output_tokens')} |
                     {'output': str(path)}, indent=2))


if __name__ == '__main__':
    main()
