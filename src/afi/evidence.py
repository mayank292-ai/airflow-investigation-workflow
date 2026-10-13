import ast
import hashlib
import json
from pathlib import Path
from typing import Annotated
from pydantic import Field
import sqlite3
import time
import uuid
from afi.schema import Evidence, now


def run_key(dag_id: str, run_id: str):
    return hashlib.sha256((dag_id + "\0" + run_id).encode()).hexdigest()[:32]


def safe_file(root: Path, relative: str):
    # Windows and Unix traversal, device paths and symlink escape are all rejected.
    if "\\" in relative or ":" in relative or relative.startswith("/") or ".." in Path(relative).parts:
        raise ValueError("Path is outside the evidence capability")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Path escapes capability root")
    return path


class EvidenceRequestError(ValueError):
    def __init__(self, details):
        self.details = details
        super().__init__(details['reason'])


class SourceSelectorError(EvidenceRequestError):
    def __init__(self, files):
        super().__init__({'code': 'source_path_not_allowlisted',
                        'reason': 'Use a path exactly as listed in allowed_paths. Runtime infrastructure outside this manifest is unavailable.',
                        'allowed_paths': sorted(files)})


def snapshot_path(path):
    """Known deployed aliases resolve ONLY into the same verified snapshot.

    This never reads from /opt/airflow or arbitrary absolute paths on the host.
    Traversal still passes through safe_file and the manifest allowlist.
    """
    if len(path) > 300:
        raise ValueError('Source selector exceeds bound')
    if path.startswith('/opt/airflow/deployment/source/'):
        return path.removeprefix('/opt/airflow/deployment/source/')
    if path.startswith('/opt/airflow/dags/'):
        return 'dags/' + path.removeprefix('/opt/airflow/dags/')
    return path


class EvidenceTools:
    def __init__(self, settings, store, inv, airflow, seconds=None):
        self.settings, self.store, self.inv, self.airflow = settings, store, inv, airflow
        self.identity = store.get(inv)["identity"]
        self.run_dir = settings.state / "runs" / run_key(self.identity.dag_id, self.identity.run_id)
        self.deadline = time.monotonic() + (seconds or settings.investigation_seconds)
        self.calls = 0
        self.coverage = {}

    def capture(self, kind, args, collect):
        self.calls += 1
        if self.calls > self.settings.max_tool_calls or time.monotonic() > self.deadline:
            raise RuntimeError("Investigation tool budget exhausted")
        availability, truncated = "available", False
        try:
            data, truncated = collect()
        except FileNotFoundError:
            data, availability = {"reason": "Required artifact unavailable"}, "missing"
        except EvidenceRequestError as exc:
            data, availability = exc.details, 'error'
        except Exception as exc:
            # Do not put provider credentials, raw URLs or internal tracebacks in model context.
            data, availability = {"error_type": type(exc).__name__, "reason": "Retrieval failed; no conclusion can be drawn from this tool failure"}, "error"
        evidence = Evidence(id="ev_" + uuid.uuid4().hex, investigation_id=self.inv, kind=kind,
                            provenance={"identity": self.identity.model_dump(), "selector": args,
                                        "basis": "execution artifact" if kind in ("source", "checkpoint", "destination", "binding") else "retrieved current API state"},
                            collected_at=now(), availability=availability, truncated=truncated, data=data)
        self.store.save_evidence(evidence)
        category = 'source_read' if kind == 'source' and 'path' in args else kind
        self.coverage.setdefault(category, set()).add(availability)
        print(f"tool {kind} -> {evidence.id} [{availability}; truncated={truncated}]", flush=True)
        return evidence.model_dump()

    def binding(self):
        path = self.run_dir / "binding.json"
        if path.stat().st_size > 32000:
            raise ValueError("Binding exceeds size bound")
        binding = json.loads(path.read_text(encoding="utf-8"))
        if binding.get("dag_id") != self.identity.dag_id or binding.get("run_id") != self.identity.run_id:
            raise ValueError("Binding belongs to a different execution")
        return binding

    def source_root(self):
        binding = self.binding()
        revision = binding["revision"]
        if revision is None:
            raise FileNotFoundError("Executed revision is unavailable")
        if len(revision) != 64 or any(c not in "0123456789abcdef" for c in revision):
            raise ValueError("Invalid revision identifier")
        root = self.settings.state / "snapshots" / revision / "source"
        manifest_path = root.parent / "manifest.json"
        if manifest_path.stat().st_size > 32000:
            raise ValueError("Source manifest exceeds size bound")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if len(manifest["files"]) > 64:
            raise ValueError("Source file count exceeds bound")
        total = 0
        for name, expected in manifest["files"].items():
            path = safe_file(root, name)
            size = path.stat().st_size
            total += size
            if size > 256000 or total > 2000000:
                raise ValueError("Source content exceeds bounded inspection scope")
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ValueError("Source snapshot content mismatch")
        encoded = json.dumps(manifest["files"], sort_keys=True, separators=(",", ":")).encode()
        if hashlib.sha256(encoded).hexdigest() != revision:
            raise ValueError("Source revision does not match manifest")
        return root, manifest

    def overview(self):
        return self.capture("overview", {}, lambda: self.airflow.overview(self.identity))

    def attempts(self, task_id: str, map_index: int):
        return self.capture("attempts", {"task_id": task_id, "map_index": map_index},
                            lambda: self.airflow.attempts(self.identity, task_id, map_index))

    def logs(self, task_id: str, attempt: int, map_index: int, needle: str):
        return self.capture("logs", {"task_id": task_id, "attempt": attempt, "map_index": map_index, "needle": needle},
                            lambda: self.airflow.logs(self.identity, task_id, attempt, map_index, needle))

    def source_binding(self, revision):
        """Expose the binding already checked during source access, not current Git.

        Called only after source_root verifies identity and every manifest hash.
        A verified content snapshot alone does not verify deployment alignment.
        """
        binding = self.binding()
        aligned = binding.get('deployment_verified') and binding.get('revision') == revision
        return {'dag_id': binding['dag_id'], 'run_id': binding['run_id'], 'revision': revision,
                'alignment': 'verified' if aligned else 'unknown',
                'entrypoint': binding.get('entrypoint'), 'input_contract': binding.get('input_contract'),
                'basis': 'Execution-recorded binding plus verified snapshot content; collected now'}

    def source(self, path: Annotated[str, Field(max_length=300)],
               start: Annotated[int, Field(ge=1)], count: Annotated[int, Field(ge=1, le=150)]):
        def collect():
            if start < 1 or not 1 <= count <= 150:
                raise EvidenceRequestError({'code': 'invalid_source_range', 'reason': 'Retry with start >= 1 and count between 1 and 150. This is a request limit, not proof that the source is unavailable.', 'max_count': 150})
            root, manifest = self.source_root()
            relative = snapshot_path(path)
            target = safe_file(root, relative)
            if relative not in manifest["files"]:
                raise SourceSelectorError(manifest['files'])
            lines = target.read_text(encoding="utf-8").splitlines()
            selected = lines[start - 1:start - 1 + count]
            text = "\n".join(f"{start + i}: {line}" for i, line in enumerate(selected))
            return {"revision": manifest["revision"], "path": relative, "requested_path": path, "start": start,
                    "end": start + len(selected) - 1, "text": text[:16000],
                    'source_binding': self.source_binding(manifest['revision'])}, len(text) > 16000 or start - 1 + count < len(lines)
        return self.capture("source", {"path": path, "start": start, "count": count}, collect)

    def search(self, needle: str):
        def collect():
            if not 1 <= len(needle) <= 100:
                raise ValueError("Search is a bounded literal, not arbitrary regex")
            root, manifest = self.source_root()
            matches, imports = [], {}
            for name in sorted(manifest["files"]):
                text = safe_file(root, name).read_text(encoding="utf-8")
                for line, content in enumerate(text.splitlines(), 1):
                    if needle.casefold() in content.casefold():
                        matches.append({"path": name, "line": line, "text": content[:250]})
                if name.endswith(".py"):
                    tree = ast.parse(text)
                    imports[name] = [ast.unparse(n) for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))][:30]
            return {"revision": manifest["revision"], "matches": matches[:30], "imports": imports,
                    "files": sorted(manifest["files"])}, len(matches) > 30
        return self.capture("source", {"needle": needle}, collect)

    def configuration(self):
        def collect():
            binding = self.binding()
            try:
                schema = (self.source_root()[0] / "sql/schema.sql").read_text(encoding="utf-8")
            except FileNotFoundError:
                schema = None
            return {"binding": binding, "alignment": "verified" if binding.get("deployment_verified") and schema is not None else "unknown",
                    "schema": schema, "schema_available": schema is not None,
                    "input_size_caveat": "Captured input_rows alone is not authoritative upstream completeness evidence for a paginated source; corroborate it against relevant source/input observations."}, False
        return self.capture("binding", {}, collect)

    def checkpoint(self, task_id: str, attempt: int, map_index: int):
        def collect():
            folder = safe_file(self.run_dir, f"attempts/{task_id}.{map_index}.{attempt}")
            return json.loads((folder / "checkpoint.json").read_text(encoding="utf-8")), False
        return self.capture("checkpoint", {"task_id": task_id, "attempt": attempt, "map_index": map_index}, collect)

    def destination(self, task_id: str, attempt: int, map_index: int):
        def collect():
            path = safe_file(self.run_dir, f"attempts/{task_id}.{map_index}.{attempt}/output.sqlite")
            if not path.is_file():
                raise FileNotFoundError(path)
            # Only fixed diagnostic statements; never accept SQL from the agent.
            with sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True) as db:
                total, distinct = db.execute("SELECT count(*), count(DISTINCT business_key) FROM output").fetchone()
                duplicates = db.execute("SELECT business_key,count(*) FROM output GROUP BY business_key HAVING count(*)>1 LIMIT 10").fetchall()
                exact_duplicates = db.execute('SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) AS n FROM output GROUP BY business_key,value,page HAVING count(*)>1)').fetchone()[0]
                checkpoint = db.execute("SELECT page FROM checkpoint WHERE singleton=1").fetchone()
            return {"scope": "Frozen destination snapshot at this attempt's exit", "rows": total,
                    "distinct_business_keys": distinct, "duplicate_rows": exact_duplicates,
                    "excess_rows_by_business_key": total - distinct,
                    "duplicate_key_sample": duplicates, "committed_checkpoint": checkpoint[0] if checkpoint else None,
                    "business_key_caveat": "Generic statistical caveat, not this run's business contract: repeated keys can represent legitimate reference multiplicity. duplicate_rows counts repeated complete stored tuples. Apply the retrieved source/configuration contract to determine whether these are undesirable."}, False
        return self.capture("destination", {"task_id": task_id, "attempt": attempt, "map_index": map_index}, collect)

    def langchain_tools(self):
        from langchain.tools import tool
        return [tool("run_overview", description="Read run/task states and current dependencies; no root-cause inference.")(self.overview),
                tool("attempts", description="Read recorded task attempt metadata, including mapped identity.")(self.attempts),
                tool("attempt_logs", description="Read or search logs for one exact attempt. needle empty reads bounded text.")(self.logs),
                tool("source_search", description="Search only executed-source manifest and inspect static Python imports.")(self.search),
                tool("source_read", description="Read up to 150 numbered lines. Use snapshot-relative paths returned by source_search.files; known /opt/airflow/dags/ and /opt/airflow/deployment/source/ aliases map into that same snapshot. Private runtime source is unavailable.")(self.source),
                tool("configuration_schema", description="Read deployment/run binding and destination schema; excludes private fault controls.")(self.configuration),
                tool("checkpoint", description="Read frozen checkpoint evidence from one task attempt.")(self.checkpoint),
                tool("destination_summary", description="Read frozen output counts, duplicates and checkpoint for one attempt.")(self.destination)]
