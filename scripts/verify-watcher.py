"""Operator verification of live watcher execution and persisted deduplication.

Run from the project with its Python environment. Never registered as a tool.
Does not clear events, rerun DAGs, or change spending authorization.
"""
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid
from afi.config import Settings
from afi.schema import now
from afi.store import Store


def snapshot(store):
    with store.connect() as db:
        events = dict(db.execute('SELECT event_key,investigation_id FROM events'))
        operations = dict(db.execute('SELECT investigation_id,count(*) FROM operations GROUP BY investigation_id'))
    return {'events': events, 'operation_counts': {inv: operations.get(inv, 0) for inv in events.values()}}


def main():
    settings = Settings.load(Path(__file__).resolve().parents[1])
    store = Store(settings.state)
    target = settings.state / 'verifications' / ('watcher-' + str(uuid.uuid4()) + '.json')
    target.parent.mkdir(parents=True, exist_ok=True)
    result = {'collected_at': now(), 'scope': 'Real watcher launch and reopened-process dedup; semantic quality reviewed separately',
              'before': snapshot(store), 'status': 'incomplete'}
    started = time.monotonic()
    try:
        command = [sys.executable, '-m', 'afi.cli', 'watch', '--once', '--bounded']
        print('Watcher scan 1/2: discover real failures and run bounded investigations.', flush=True)
        first = subprocess.run(command, cwd=settings.root, timeout=1200)
        result['first_returncode'] = first.returncode
        result['after_first'] = snapshot(store)
        print('Watcher scan 2/2: new process checks persisted event deduplication.', flush=True)
        second = subprocess.run(command, cwd=settings.root, timeout=120)
        result['second_returncode'] = second.returncode
        result['after_second'] = snapshot(store)
        old = result['before']['events']
        current = result['after_first']['events']
        added = {key: inv for key, inv in current.items() if key not in old}
        result['new_investigations'] = [{'investigation_id': inv, 'status': store.get(inv)['status']}
                                       for inv in added.values()]
        checks = {'both_scans_exit_zero': first.returncode == second.returncode == 0,
                  'previous_events_preserved': all(current.get(key) == inv for key, inv in old.items()),
                  'new_live_events_observed': bool(added),
                  'second_scan_no_new_events_or_operations': result['after_first'] == result['after_second'],
                  'new_investigations_complete': bool(added) and all(store.get(inv)['status'] == 'complete' for inv in added.values())}
        result.update(checks=checks, status='pass' if all(checks.values()) else 'incomplete')
    except Exception as exc:
        result['error_type'] = type(exc).__name__
    finally:
        result['elapsed_seconds'] = time.monotonic() - started
        target.write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps({key: result.get(key) for key in ('status', 'checks', 'new_investigations', 'error_type', 'elapsed_seconds')} |
                         {'output_path': str(target)}, indent=2))
    return 0 if result['status'] == 'pass' else 1


if __name__ == '__main__':
    raise SystemExit(main())
