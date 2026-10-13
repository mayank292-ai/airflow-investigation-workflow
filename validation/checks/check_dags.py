"""Trusted bare-Python DAG loading check, separate from pytest's process state."""
import json
import faulthandler

print(json.dumps({'validation_phase': 'bare_airflow_import_started'}), flush=True)
faulthandler.enable()
from airflow.dag_processing.dagbag import DagBag
print(json.dumps({'validation_phase': 'bare_airflow_import_complete'}), flush=True)
bag = DagBag(dag_folder='/patch/dags', safe_mode=False)
print(json.dumps({'validation_phase': 'bare_dag_parse_complete', 'dags': sorted(bag.dags),
                  'import_errors': bag.import_errors}), flush=True)
assert not bag.import_errors
assert {'local_pipeline_01', 'local_pipeline_02', 'local_pipeline_03'} <= set(bag.dags)
