"""Trusted fixed checks. Generated code cannot modify or weaken this directory."""
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import batch_ingest
import enrichment
import profile_groups
import pytest


def db():
    connection = sqlite3.connect(':memory:')
    connection.executescript(Path('/patch/sql/schema.sql').read_text())
    return connection


def test_dag_loading():
    # Same container limits/mounts/version, without pytest's imported runtime state.
    completed = subprocess.run([sys.executable, '-u', '/checks/check_dags.py'], timeout=100)
    assert completed.returncode == 0, f'Isolated DAG loading exited {completed.returncode}'


def test_requested_append_remediation_retry_completeness():
    connection = db()
    def fetch(page):
        if page == 2:
            raise ConnectionError('bounded failure')
        return [{'event_id': str(page * 2 + i), 'value': str(i)} for i in range(2)]
    for _ in range(2):
        with pytest.raises(ConnectionError):
            batch_ingest.append_pages(connection, fetch, 'validation')
    # A proposed ingestion repair must prevent duplicate output and align commits.
    total, unique = connection.execute('SELECT count(*), count(DISTINCT business_key) FROM output').fetchone()
    assert total == unique
    assert batch_ingest.reconcile(connection)['checkpoint'] == 2
    batch_ingest.append_pages(connection, lambda page: [{'event_id': str(page * 2 + i), 'value': str(i)} for i in range(2)] if page < 3 else [], 'validation')
    batch_ingest.append_pages(connection, lambda page: [], 'validation')
    assert connection.execute('SELECT count(*) FROM output').fetchone()[0] == 6


def test_requested_append_remediation_checkpoint_failure_rolls_back():
    connection = db()
    connection.executescript("""CREATE TRIGGER reject_checkpoint BEFORE UPDATE ON checkpoint
                            BEGIN SELECT RAISE(ABORT, 'bounded checkpoint failure'); END;""")
    fetch = lambda page: [{'event_id': 'e', 'value': 'v'}] if page == 0 else []
    with pytest.raises(sqlite3.IntegrityError, match='bounded checkpoint failure'):
        batch_ingest.append_pages(connection, fetch, 'validation')
    assert not connection.in_transaction
    assert connection.execute('SELECT count(*) FROM output').fetchone()[0] == 0
    assert batch_ingest.reconcile(connection)['checkpoint'] == 0
    connection.execute('DROP TRIGGER reject_checkpoint')
    connection.commit()
    batch_ingest.append_pages(connection, fetch, 'validation')
    assert connection.execute('SELECT count(*) FROM output').fetchone()[0] == 1
    assert batch_ingest.reconcile(connection)['checkpoint'] == 1


def test_requested_append_remediation_partial_page_insert_rolls_back():
    connection = db()
    connection.executescript("""CREATE TRIGGER reject_record BEFORE INSERT ON output
                            WHEN NEW.value = 'reject'
                            BEGIN SELECT RAISE(ABORT, 'bounded row failure'); END;""")
    fetch = lambda page: [{'event_id': 'first', 'value': 'ok'},
                          {'event_id': 'second', 'value': 'reject'}] if page == 0 else []
    with pytest.raises(sqlite3.IntegrityError, match='bounded row failure'):
        batch_ingest.append_pages(connection, fetch, 'validation')
    assert not connection.in_transaction
    assert connection.execute('SELECT count(*) FROM output').fetchone()[0] == 0
    assert batch_ingest.reconcile(connection)['checkpoint'] == 0


def test_profiles_and_relational_semantics(tmp_path):
    input_path = tmp_path / 'rows.jsonl'
    input_path.write_text('\n'.join(json.dumps({'group': str(i % 4), 'value': i}) for i in range(100)))
    assert profile_groups.materialized(input_path) == profile_groups.streaming(input_path)
    rows, refs = tmp_path / 'rows.json', tmp_path / 'refs.json'
    rows.write_text(json.dumps([{'event_id': 'e', 'key': None}, {'event_id': 'f', 'key': 'a'}]))
    refs.write_text(json.dumps([{'key': None, 'label': 'should-not-match'}, {'key': 'a', 'label': 'A'}, {'key': 'a', 'label': 'B'}]))
    one, two = db(), db()
    enrichment.enrich_python(one, rows, refs)
    enrichment.enrich_sql(two, rows, refs, Path('/patch/sql/enrichment.sql'))
    assert one.execute('SELECT * FROM output ORDER BY business_key,value').fetchall() == two.execute('SELECT * FROM output ORDER BY business_key,value').fetchall()
