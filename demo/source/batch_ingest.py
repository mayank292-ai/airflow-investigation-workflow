"""Pages contain immutable events; business_key identifies one event, not a customer.

Replacing mutable records would require a different update contract.
"""
import logging


def append_pages(db, fetch, run_id):
    page = db.execute("SELECT page FROM checkpoint WHERE singleton=1").fetchone()[0]
    while True:
        records = fetch(page)
        if not records:
            break
        db.executemany("INSERT INTO output(business_key,value,page) VALUES (?,?,?)",
                       [(r["event_id"], r["value"], page) for r in records])
        db.commit()
        logging.warning("run=%s committed page=%s rows=%s", run_id, page, len(records))
        page += 1
    db.execute("UPDATE checkpoint SET page=? WHERE singleton=1", (page,))
    db.commit()


def atomic_pages(db, fetch, run_id):
    page = db.execute("SELECT page FROM checkpoint WHERE singleton=1").fetchone()[0]
    try:
        db.execute("BEGIN")
        while records := fetch(page):
            db.executemany("INSERT INTO output(business_key,value,page) VALUES (?,?,?)",
                           [(r["event_id"], r["value"], page) for r in records])
            page += 1
        db.execute("UPDATE checkpoint SET page=? WHERE singleton=1", (page,))
        db.commit()
    except BaseException:
        db.rollback()
        raise


def keyed_pages(db, fetch, run_id):
    # Appropriate only for this documented immutable-event contract.
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS event_key ON output(business_key)")
    page = db.execute("SELECT page FROM checkpoint WHERE singleton=1").fetchone()[0]
    while records := fetch(page):
        try:
            db.execute("BEGIN")
            db.executemany("INSERT INTO output(business_key,value,page) VALUES (?,?,?) ON CONFLICT(business_key) DO NOTHING",
                           [(r["event_id"], r["value"], page) for r in records])
            page += 1
            db.execute("UPDATE checkpoint SET page=? WHERE singleton=1", (page,))
            db.commit()
        except BaseException:
            db.rollback()
            raise


def reconcile(db):
    checkpoint = db.execute("SELECT page FROM checkpoint WHERE singleton=1").fetchone()[0]
    watermark = db.execute("SELECT coalesce(max(page)+1,0) FROM output").fetchone()[0]
    count, distinct = db.execute("SELECT count(*),count(DISTINCT business_key) FROM output").fetchone()
    return {"checkpoint": checkpoint, "observed_page_watermark": watermark,
            "rows": count, "distinct_event_ids": distinct,
            "requires_review": checkpoint != watermark or count != distinct}
