from afi.evaluation import evaluation_plan, bounded_evaluation_settings
from afi.config import Settings
from afi.agent import admission_amount
import pytest


def test_ten_cases_covered_and_three_key_cases_repeated_without_model_calls():
    history = [{'dag_id': f'local_pipeline_{family:02}', 'run_id': str(n), 'start_date': str(n), 'run_type': 'scheduled'}
               for family, count in [(1, 3), (2, 3), (3, 4)] for n in range(count)]
    plan = evaluation_plan(history, 3)
    assert len(plan) == 19
    for case in history:
        occurrences = [entry for entry in plan if entry[0] == case and entry[2] == 'tool-access']
        assert len(occurrences) == (3 if case['dag_id'].endswith('03') and case['run_id'] in ('0', '1', '2') else 1)
    baselines = [entry for entry in plan if entry[2] == 'logs-only']
    assert len(baselines) == 3 and {entry[1] for entry in baselines} == {0, 1, 2}
    focused = evaluation_plan(history, 3, key_cases_only=True)
    assert len(focused) == 12
    assert all(case['dag_id'] == 'local_pipeline_03' for case, _, _ in focused)
    assert {case['run_id'] for case, _, _ in focused} == {'0', '1', '2'}


def test_fresh_evaluation_admission_is_smaller_and_respects_lower_limits(tmp_path):
    followup = Settings(root=tmp_path, context_bytes=120000, max_usd=1.8)
    benchmark = bounded_evaluation_settings(followup)
    assert admission_amount(benchmark) == pytest.approx(0.885)
    assert admission_amount(benchmark) < admission_amount(followup)
    assert followup.context_bytes == 120000 and followup.max_model_calls == 8
    lower = Settings(root=tmp_path, max_model_calls=3, max_tool_calls=10,
                     context_bytes=60000, max_output_tokens=1000)
    assert bounded_evaluation_settings(lower) == lower
