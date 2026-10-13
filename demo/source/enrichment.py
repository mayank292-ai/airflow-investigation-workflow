"""Reference duplicates intentionally multiply matches; null keys do not match.

The local demonstration uses SQL-like null equality semantics, not pandas semantics.
"""
import json


def enrich_python(db, records_path, references_path):
    records = json.loads(records_path.read_text(encoding="utf-8"))
    references = json.loads(references_path.read_text(encoding="utf-8"))
    results = []
    for row in records:
        matches = [ref for ref in references if row["key"] is not None and row["key"] == ref["key"]]
        for ref in matches or [{"label": None}]:
            results.append((row["event_id"], ref["label"], 0))
    db.executemany("INSERT INTO output(business_key,value,page) VALUES (?,?,?)", results)
    db.commit()


def enrich_sql(db, records_path, references_path, sql_path):
    db.executescript("CREATE TABLE records(event_id TEXT,key TEXT); CREATE TABLE reference(key TEXT,label TEXT);")
    db.executemany("INSERT INTO records VALUES (?,?)", [(r["event_id"], r["key"]) for r in json.loads(records_path.read_text())])
    db.executemany("INSERT INTO reference VALUES (?,?)", [(r["key"], r["label"]) for r in json.loads(references_path.read_text())])
    db.execute(sql_path.read_text(encoding="utf-8"))
    db.commit()
