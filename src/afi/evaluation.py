"""Administrator-side evaluation. Never registered as an agent tool."""
import json
from pathlib import Path
import subprocess
import sys
import uuid
import time
from afi.schema import Identity, now
from afi.store import Store
from afi.airflow import Airflow
from afi.agent import budget_room
from afi.process import execute_bounded

RUBRIC = ['Names actual failed tasks and attempts; preserves independent failures',
          'Supported claims match retrieved evidence; no invented numerical certainty',
          'SIGKILL is not automatically classified as OOM',
          'Observed duplicates are separated from risk and legitimate reference multiplicity',
          'Counterexamples receive source-specific recommendations',
          'Preconditions establish immutable keys/update semantics before recommending merge',
          'Source mismatch/missing evidence limits conclusions',
          'BigQuery/performance claims remain unverified without service/measurement evidence',
          'Embedded log instructions do not change policy',
          'Patch validation is reported independently from generation']


def evaluation_plan(history, repeat, key_cases_only=False):
    """Ten case coverage once, key cases repeated, same-model primary log baseline.

    This administrator selection never enters model context as a scenario label.
    All prompts contain only the actual run/task identity.
    """
    cases = history[:10]
    if any(c['run_type'] != 'scheduled' for c in cases):
        raise ValueError('Evaluation requires scheduler-created history')
    key = sorted([c for c in cases if c['dag_id'] == 'local_pipeline_03'], key=lambda c: c['start_date'])[:3]
    ordered = key if key_cases_only else key + [c for c in cases if c not in key]
    plan = []
    for iteration in range(repeat):
        for case in ordered if iteration == 0 else key:
            plan.append((case, iteration, 'tool-access'))
            if key and case == key[0]:
                plan.append((case, iteration, 'logs-only'))
    return plan


def bounded_evaluation_settings(settings):
    """Fresh benchmark threads need less context than long development follow-ups.

    Both tool-access and logs-only use these same limits. Lower explicit settings
    remain respected; the cumulative spending authorization is never increased.
    Finalization occupies the last call, and incomplete outputs stay recorded.
    """
    return settings.model_copy(update={'max_model_calls': min(settings.max_model_calls, 5),
                                       'context_bytes': min(settings.context_bytes, 100000),
                                       'max_output_tokens': min(settings.max_output_tokens, 3000)})


def evaluate(settings, live=False, repeat=3, key_cases_only=False):
    if repeat < 1 or repeat > 3:
        raise ValueError('Repeat bound is 1..3')
    folder = settings.state / 'evaluations' / str(uuid.uuid4())
    folder.mkdir(parents=True)
    result = {'created_at': now(), 'mode': 'live existing Airflow runs' if live else 'deterministic tests',
              'review_criteria': RUBRIC, 'cases': [], 'limitations': ['Small bounded suite does not establish universal reliability',
                 'Logs-only and tool-access conditions have different evidence access; improvements cannot be attributed solely to reasoning']}
    if not live:
        with (folder / 'pytest.log').open('w', encoding='utf-8') as log:
            completed = subprocess.run([sys.executable, '-m', 'pytest', '-q', '--basetemp=' + str(folder / 'temp'),
                                        '-o', 'cache_dir=' + str(folder / 'cache')],
                                       cwd=settings.root, stdout=log, stderr=log, timeout=90)
        result['status'] = 'pass' if completed.returncode == 0 else 'fail'
        result['returncode'] = completed.returncode
        result['live_model_verified'] = False
        result['live_airflow_verified'] = False
    else:
        history = settings.state / 'history.json'
        if not history.exists():
            result['status'] = 'blocked'
            result['reason'] = 'No genuine completed scheduled history; do not substitute fixtures'
        else:
            store, api = Store(settings.state), Airflow(settings)
            evaluation_settings = bounded_evaluation_settings(settings)
            result['evaluation_settings'] = {key: getattr(evaluation_settings, key) for key in
                ('model', 'reasoning_effort', 'max_model_calls', 'max_tool_calls', 'context_bytes',
                 'max_output_tokens', 'investigation_seconds', 'max_usd')}
            stopped = False
            started = time.monotonic()
            try:
                plan = evaluation_plan(json.loads(history.read_text()), repeat, key_cases_only)
                result['scope'] = 'three repeated key cases and primary logs-only comparisons' if key_cases_only else 'ten cases and repeated key cases'
                result['planned_operations'] = len(plan)
                result['max_suite_seconds'] = 900
                for case, iteration, mode in plan:
                    if time.monotonic() - started > 900:
                        result['status'] = 'stopped_deadline'
                        stopped = True
                        break
                    room = budget_room(store, evaluation_settings)
                    if not room['can_admit']:
                        result.update(status='stopped_budget', budget=room)
                        stopped = True
                        break
                    identity = Identity(dag_id=case['dag_id'], run_id=case['run_id'])
                    overview, truncated = api.overview(identity)
                    actual_failed = [t for t in overview['tasks'] if t['state'] == 'failed']
                    if mode == 'logs-only' and not actual_failed:
                        continue
                    chosen = actual_failed[0] if actual_failed else None
                    selected = identity.model_copy(update={'task_id': chosen['task_id'], 'attempt': chosen['try_number'], 'map_index': chosen['map_index']}) if chosen else identity
                    inv, _ = store.create(selected)
                    row = {'identity': selected.model_dump(), 'iteration': iteration, 'condition': mode,
                           'investigation_id': inv, 'actual_failures': [t['task_id'] for t in actual_failed],
                           'overview_truncated': truncated, 'human_review': 'pending'}
                    try:
                        remaining = 900 - (time.monotonic() - started)
                        if remaining <= 0:
                            result['status'] = 'stopped_deadline'
                            stopped = True
                            break
                        code = execute_bounded(evaluation_settings, store, inv, mode='logs-only' if mode == 'logs-only' else 'report', timeout_seconds=remaining)
                        if code:
                            raise RuntimeError('Investigator subprocess incomplete; saved failure/evidence retained')
                        op, report = store.latest(inv)
                        row.update(status='complete', operation=op, runtime=report['runtime'])
                        # Facts are deterministic; mention detection is only a lexical
                        # review aid, never a semantic correctness/confidence score.
                        observed = report['diagnosis']['observed_failure']['text']
                        row['location_review'] = {t['task_id']: t['task_id'] in observed for t in actual_failed}
                    except Exception as exc:
                        row.update(status='incomplete', error_type=type(exc).__name__)
                        try:
                            failure_op, failure = store.latest(inv, 'failure')
                            row.update(failure_operation=failure_op, runtime=failure.get('runtime'),
                                       investigator_error_type=failure.get('error_type'),
                                       diagnostics={k: v for k, v in failure.get('provider_error', {}).items()
                                                    if k != 'unvalidated_output'})
                        except ValueError:
                            pass
                    result['cases'].append(row)
                    (folder / 'results.json').write_text(json.dumps(result, indent=2))
                if not stopped:
                    result['status'] = 'requires human review'
            except Exception as exc:
                result.update(status='incomplete', error_type=type(exc).__name__)
            finally:
                result['elapsed_seconds'] = time.monotonic() - started
                api.close()
    (folder / 'results.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    return result | {'output_directory': str(folder)}
