import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from filelock import FileLock
from afi.schema import Identity, Evidence, now


class Store:
    def __init__(self, state: Path):
        self.state = state
        state.mkdir(parents=True, exist_ok=True)
        self.path = state / "workflow.sqlite"
        with self.connect() as db:
            db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS investigations (
              id TEXT PRIMARY KEY, identity TEXT NOT NULL, status TEXT NOT NULL,
              created_at TEXT NOT NULL, thread_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
              event_key TEXT PRIMARY KEY, investigation_id TEXT NOT NULL, seen_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS evidence (
              id TEXT PRIMARY KEY, investigation_id TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations (
              id TEXT PRIMARY KEY, investigation_id TEXT NOT NULL, kind TEXT NOT NULL,
              status TEXT NOT NULL, created_at TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS watcher (name TEXT PRIMARY KEY, payload TEXT NOT NULL);
            """)

    def connect(self):
        return sqlite3.connect(self.path, timeout=10)

    def create(self, identity: Identity, event_key: str | None = None):
        inv = str(uuid.uuid4())
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if event_key:
                row = db.execute("SELECT investigation_id FROM events WHERE event_key=?", (event_key,)).fetchone()
                if row:
                    return row[0], False
            db.execute("INSERT INTO investigations VALUES (?,?,?,?,?)",
                       (inv, identity.model_dump_json(), "pending", now(), str(uuid.uuid4())))
            if event_key:
                db.execute("INSERT INTO events VALUES (?,?,?)", (event_key, inv, now()))
        return inv, True

    def get(self, inv: str):
        with self.connect() as db:
            db.row_factory = sqlite3.Row
            row = db.execute("SELECT * FROM investigations WHERE id=?", (inv,)).fetchone()
        if not row:
            raise ValueError("Unknown investigation")
        return dict(row) | {"identity": Identity.model_validate_json(row["identity"])}

    def status(self, inv: str, status: str):
        with self.connect() as db:
            db.execute("UPDATE investigations SET status=? WHERE id=?", (status, inv))

    def evidence(self, inv: str):
        with self.connect() as db:
            rows = db.execute("SELECT payload FROM evidence WHERE investigation_id=?", (inv,)).fetchall()
        return {item.id: item for item in (Evidence.model_validate_json(r[0]) for r in rows)}

    def save_evidence(self, evidence: Evidence):
        with self.connect() as db:
            db.execute("INSERT INTO evidence VALUES (?,?,?)",
                       (evidence.id, evidence.investigation_id, evidence.model_dump_json()))

    def operation(self, inv: str, kind: str, status: str, payload: dict):
        op = str(uuid.uuid4())
        self.get(inv)  # Restrict output paths to known IDs.
        folder = self.state / "investigations" / inv / op
        folder.mkdir(parents=True)
        (folder / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        with self.connect() as db:
            db.execute("INSERT INTO operations VALUES (?,?,?,?,?,?)", (op, inv, kind, status, now(), json.dumps(payload)))
        return op, folder

    def latest(self, inv: str, kind="report"):
        self.get(inv)
        with self.connect() as db:
            row = db.execute("SELECT id,payload FROM operations WHERE investigation_id=? AND kind=? ORDER BY rowid DESC LIMIT 1", (inv, kind)).fetchone()
        if not row:
            raise ValueError("No saved operation of that kind")
        return row[0], json.loads(row[1])

    def progress(self, payload: dict):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO watcher VALUES ('default',?)", (json.dumps(payload),))

    @contextmanager
    def lock(self, inv: str):
        self.get(inv)
        with FileLock(str(self.state / f"{inv}.lock"), timeout=1):
            yield
