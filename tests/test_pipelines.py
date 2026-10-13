import json
from pathlib import Path
import sqlite3
import pytest
import batch_ingest
import enrichment
import profile_groups

SCHEMA = (Path(__file__).resolve().parents[1] / 'demo/source/sql/schema.sql').read_text()


def database():
    db = sqlite3.connect(':memory:')
    db.executescript(SCHEMA)
    return db


def fetch(page, fail=True):
    if page == 2 and fail:
        raise ConnectionError('Synthetic source failure')
    return [{'event_id': str(page * 2 + i), 'value': str(i)} for i in range(2)] if page < 3 else []


def test_append_observed_duplicates_after_retry():
    db = database()
    for _ in range(2):
        with pytest.raises(ConnectionError):
            batch_ingest.append_pages(db, fetch, 'opaque')
    assert batch_ingest.reconcile(db) == {'checkpoint': 0, 'observed_page_watermark': 2,
                                        'rows': 8, 'distinct_event_ids': 4, 'requires_review': True}


def test_atomic_rolls_back_partial_failure():
    db = database()
    with pytest.raises(ConnectionError):
        batch_ingest.atomic_pages(db, fetch, 'opaque')
    assert db.execute('SELECT count(*) FROM output').fetchone()[0] == 0
    assert batch_ingest.reconcile(db)['checkpoint'] == 0


def test_keyed_retries_resume_without_duplicates():
    db = database()
    with pytest.raises(ConnectionError):
        batch_ingest.keyed_pages(db, fetch, 'opaque')
    assert batch_ingest.reconcile(db)['checkpoint'] == 2
    batch_ingest.keyed_pages(db, lambda page: fetch(page, False), 'opaque')
    batch_ingest.keyed_pages(db, lambda page: fetch(page, False), 'opaque')
    assert batch_ingest.reconcile(db)['rows'] == 6
    assert batch_ingest.reconcile(db)['distinct_event_ids'] == 6
    assert not batch_ingest.reconcile(db)['requires_review']


def test_profiles_equal_on_bounded_input(tmp_path):
    path = tmp_path / 'rows.jsonl'
    path.write_text('\n'.join(json.dumps({'group': str(i % 5), 'value': i}) for i in range(1000)))
    assert profile_groups.materialized(path) == profile_groups.streaming(path)


def test_sql_preserves_reference_duplicates_nulls_and_unmatched(tmp_path):
    records, refs = tmp_path / 'records.json', tmp_path / 'refs.json'
    records.write_text(json.dumps([{'event_id': 'e1', 'key': 'a'}, {'event_id': 'e2', 'key': None}, {'event_id': 'e3', 'key': 'missing'}]))
    refs.write_text(json.dumps([{'key': 'a', 'label': 'one'}, {'key': 'a', 'label': 'two'}, {'key': None, 'label': 'never'}]))
    python, sql = database(), database()
    enrichment.enrich_python(python, records, refs)
    enrichment.enrich_sql(sql, records, refs, Path(__file__).resolve().parents[1] / 'demo/source/sql/enrichment.sql')
    assert python.execute('SELECT * FROM output ORDER BY business_key,value').fetchall() == sql.execute('SELECT * FROM output ORDER BY business_key,value').fetchall()
    assert sql.execute('SELECT count(*) FROM output').fetchone()[0] == 4
