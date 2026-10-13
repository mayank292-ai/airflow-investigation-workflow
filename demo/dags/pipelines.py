"""Business helpers are versioned with this DAG definition.

Runtime services provide inputs and capture execution artifacts. They are deployed
separately; static inspection of this file cannot resolve their dynamic behavior.
"""
import json
from datetime import timedelta
import pendulum
from airflow.sdk import DAG, task, get_current_context
import runtime


with open('/opt/airflow/deployment/schedule.json', encoding='utf-8') as handle:
    window = json.load(handle)


def pipeline(dag_id, task_name, family):
    with DAG(dag_id, start_date=pendulum.parse(window['start']), end_date=pendulum.parse(window['end']),
             schedule=timedelta(seconds=60), catchup=False, max_active_runs=1,
             default_args={'retries': 1, 'retry_delay': timedelta(seconds=5)},
             is_paused_upon_creation=True, tags=['local-investigation-demo']) as dag:

        @task(task_id='prepare', retries=0)
        def prepare():
            context = get_current_context()
            runtime.prepare(context['dag'].dag_id, context['run_id'], family)

        @task(task_id=task_name)
        def execute():
            context = get_current_context()
            ti = context['ti']
            runtime.execute(context['dag'].dag_id, context['run_id'], family,
                            ti.task_id, ti.try_number, ti.map_index)

        @task(task_id='reconcile', trigger_rule='all_done', retries=0)
        def reconcile():
            context = get_current_context()
            runtime.reconcile(context['dag'].dag_id, context['run_id'])

        @task(task_id='publish', retries=0)
        def publish():
            context = get_current_context()
            runtime.publish(context['dag'].dag_id, context['run_id'])

        ready, processing = prepare(), execute()
        review, published = reconcile(), publish()
        ready >> processing >> published
        processing >> review
    return dag


local_pipeline_01 = pipeline('local_pipeline_01', 'profile', '01')
local_pipeline_02 = pipeline('local_pipeline_02', 'enrich', '02')
local_pipeline_03 = pipeline('local_pipeline_03', 'ingest', '03')
