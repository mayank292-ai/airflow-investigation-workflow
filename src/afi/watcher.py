import json
import time
from afi.schema import Identity, now


def discover(settings, store, airflow):
    discovered = []
    for dag in settings.dags:
        runs, truncated = airflow.runs(dag)
        truncated = truncated or len(runs) > 20
        runs = runs[:20]
        if truncated:
            print('watcher scan truncated; older runs need manual investigation', flush=True)
        for run in runs:
            if run['state'] not in ('running', 'failed', 'success'):
                continue
            identity = Identity(dag_id=dag, run_id=run['dag_run_id'])
            overview, _ = airflow.overview(identity)
            for task in overview['tasks']:
                # No root-cause substitution for upstream_failed/skipped states.
                if task['state'] != 'failed':
                    continue
                selected = Identity(dag_id=dag, run_id=run['dag_run_id'], task_id=task['task_id'],
                                    map_index=task['map_index'], attempt=task['try_number'])
                event = selected.model_dump_json()
                inv, fresh = store.create(selected, event_key=event)
                if fresh:
                    discovered.append(inv)
    store.progress({'last_scan': now(), 'new_events': discovered})
    return discovered


def watch(settings, store, airflow, launch, once=False):
    failures = 0
    while True:
        try:
            discovered = discover(settings, store, airflow)
            failures = 0
            for inv in discovered:
                print(f'watcher new investigation {inv}', flush=True)
                try:
                    launch(inv)
                except Exception as exc:
                    print(f'watcher investigation incomplete: {type(exc).__name__}; continue {inv}', flush=True)
        except Exception as exc:
            failures += 1
            store.progress({'last_error_at': now(), 'error_type': type(exc).__name__, 'failures': failures})
            print(f'watcher connection/scan error: {type(exc).__name__}; backoff active', flush=True)
            if once:
                raise
        if once:
            return
        time.sleep(min(60, settings.poll_seconds * 2 ** min(failures, 3)))
