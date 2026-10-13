"""Trusted entry point, mounted read-only. No install or arbitrary shell commands."""
import json
import sys
import time
from datetime import datetime, timezone
from importlib.metadata import version

started = time.monotonic()


def marker(phase, **details):
    print(json.dumps({'validation_phase': phase, 'at': datetime.now(timezone.utc).isoformat(),
                      'elapsed_seconds': time.monotonic() - started, **details}), flush=True)


if __name__ == '__main__':
    marker('python_started', python=sys.version.split()[0])
    assert sys.version_info[:2] == (3, 12), 'Prepared Python version mismatch'
    assert version('apache-airflow') == '3.3.1', 'Prepared Airflow version mismatch'
    assert version('pytest') == '9.1.1', 'Prepared pytest version mismatch'
    marker('pytest_import_started')
    import pytest
    marker('pytest_import_complete')

    class Progress:
        def pytest_sessionstart(self, session):
            marker('collection_started')

        def pytest_collection_finish(self, session):
            marker('collection_complete', tests=len(session.items))

        def pytest_collection_modifyitems(self, session, config, items):
            # Run fast behavior checks before the heavier framework loading check.
            # No assertion/test is skipped or altered by this ordering.
            items.sort(key=lambda item: ('test_dag_loading' in item.nodeid, item.nodeid))

        def pytest_runtest_logstart(self, nodeid, location):
            marker('test_started', nodeid=nodeid)

    code = pytest.main(['-vv', '-s', '-p', 'no:cacheprovider', '/checks', *sys.argv[1:]], plugins=[Progress()])
    marker('pytest_complete', returncode=int(code))
    raise SystemExit(code)
