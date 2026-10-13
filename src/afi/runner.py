"""Killable process boundary for bounded provider execution."""
import sys
from afi.config import Settings
from afi.store import Store
from afi.airflow import Airflow
from afi.agent import run_agent
from afi.errors import provider_error
import json


def main():
    settings = Settings.load()
    store = Store(settings.state)
    api = Airflow(settings)
    try:
        op, folder = run_agent(settings, store, sys.argv[1], api,
                              question=sys.argv[3] if len(sys.argv) > 3 else None,
                              patch=sys.argv[2] == 'patch', logs_only=sys.argv[2] == 'logs-only')
        print(f'Saved operation {op}: {folder}', flush=True)
    except Exception as exc:
        print(f'Incomplete: {type(exc).__name__}. Saved evidence remains; no diagnosis claimed.', flush=True)
        diagnostic = provider_error(exc)
        # Full bounded invalid output is saved in the failure artifact, not dumped
        # into terminal progress. This does not hide or relabel a failed outcome.
        diagnostic.pop('unvalidated_output', None)
        print(json.dumps(diagnostic), flush=True)
        try:
            op, _ = store.latest(sys.argv[1], 'failure')
            print(f'Failure details: {settings.state / "investigations" / sys.argv[1] / op / "result.json"}', flush=True)
        except ValueError:
            pass
        raise SystemExit(1)
    finally:
        api.close()


if __name__ == '__main__':
    main()
