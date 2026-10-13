"""Airflow 3.3.1 public API. No metadata database writes or guessed attempts."""
import json
import time
from urllib.parse import quote
import httpx
from afi.schema import Identity


class AirflowError(RuntimeError):
    pass


def q(value: str) -> str:
    return quote(value, safe="")


def run_path(identity: Identity):
    return f"/api/v2/dags/{q(identity.dag_id)}/dagRuns/{q(identity.run_id)}"


def public_task(row: dict):
    # Never expose executor_config, user conf, operator arguments or fault controls.
    keys = ("task_id", "dag_id", "dag_run_id", "map_index", "try_number", "max_tries",
            "state", "start_date", "end_date", "duration", "operator", "pool", "dag_version")
    return {key: row.get(key) for key in keys}


class Airflow:
    def __init__(self, settings, transport=None):
        self.settings = settings
        self.client = httpx.Client(base_url=settings.airflow_url, timeout=10,
                                   follow_redirects=False, transport=transport)
        self.token = None

    def close(self):
        self.client.close()

    def request(self, method, path, **kwargs):
        attempts = 2 if method.upper() == 'GET' else 1
        for attempt in range(attempts):
            try:
                return self._request(method, path, **kwargs)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt + 1 == attempts:
                    raise AirflowError(f'Airflow {method} transport failure ({type(exc).__name__}); '
                                       f'{attempts} attempt(s); no complete response available') from exc
                print(f'Airflow GET transport error ({type(exc).__name__}); bounded retry 1/1', flush=True)
                time.sleep(0.25)

    def _request(self, method, path, **kwargs):
        for refresh in range(2):
            if path.startswith("/api/") and not self.token:
                response = self.client.post("/auth/token", json={"username": self.settings.airflow_username,
                                                                "password": self.settings.airflow_password})
                if response.status_code not in (200, 201):
                    raise AirflowError(f"Airflow authentication HTTP {response.status_code}")
                self.token = response.json()["access_token"]
            headers = {"Accept": "application/json"}
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            with self.client.stream(method, path, headers=headers, **kwargs) as response:
                if response.status_code == 401 and refresh == 0:
                    self.token = None
                    continue
                if response.status_code >= 400:
                    raise AirflowError(f"Airflow HTTP {response.status_code}; {method} request failed")
                data = bytearray()
                for chunk in response.iter_bytes():
                    data.extend(chunk)
                    if len(data) > 512_000:
                        raise AirflowError("Airflow response exceeds 512 KB bound; unavailable, not silently complete")
                return json.loads(data)
        raise AirflowError("Airflow authentication failed after refresh")

    def _unauthorized(self):
        raise AirflowError("Airflow authentication failed after refresh")

    def collection(self, path, key, limit=100, pages=5, params=None):
        items = []
        total = 0
        for page in range(pages):
            response = self.request("GET", path, params={"limit": limit, "offset": page * limit, **(params or {})})
            batch = response[key]
            items.extend(batch)
            total = response.get("total_entries") or len(items)
            if len(batch) < limit or len(items) >= total:
                return items, False
        return items, len(items) < total

    def runs(self, dag):
        return self.collection(f"/api/v2/dags/{q(dag)}/dagRuns", "dag_runs", params={"order_by": "-start_date"})

    def overview(self, identity):
        raw = self.request("GET", run_path(identity))
        tasks, truncated = self.collection(run_path(identity) + "/taskInstances", "task_instances")
        run = {key: raw.get(key) for key in ("dag_id", "dag_run_id", "state", "run_type", "logical_date",
                                            "start_date", "end_date", "data_interval_start", "data_interval_end", "dag_versions")}
        definitions, dt = self.collection(f"/api/v2/dags/{q(identity.dag_id)}/tasks", "tasks")
        dependencies = [{key: task.get(key) for key in ("task_id", "downstream_task_ids", "trigger_rule", "retries")}
                        for task in definitions]
        return {"run": run, "tasks": [public_task(t) for t in tasks],
                "current_task_definitions": dependencies,
                "definition_warning": "Definitions retrieved now; executed source must be checked separately"}, truncated or dt

    def attempts(self, identity, task_id, map_index):
        path = run_path(identity) + f"/taskInstances/{q(task_id)}/{map_index}/tries"
        rows, truncated = self.collection(path, "task_instances")
        return {"attempts": [public_task(row) for row in rows]}, truncated

    def logs(self, identity, task_id, attempt, map_index, needle="", max_chars=12000):
        if attempt < 1 or map_index < -1 or len(needle) > 100:
            raise ValueError("Invalid log selector")
        token = None
        seen = set()
        chunks = []
        truncated = False
        for page in range(5):
            response = self.request("GET", run_path(identity) + f"/taskInstances/{q(task_id)}/logs/{attempt}",
                                    params={"map_index": map_index, "full_content": False, **({"token": token} if token else {})})
            content = response.get("content", [])
            text = content if isinstance(content, str) else "\n".join(json.dumps(item, ensure_ascii=False) for item in content)
            chunks.append(text)
            token = response.get("continuation_token")
            if not token:
                break
            if token in seen:
                truncated = True
                break
            seen.add(token)
            if sum(map(len, chunks)) > 40_000:
                truncated = True
                break
        else:
            truncated = bool(token)
        text = "\n".join(chunks)
        if needle:
            text = "\n".join(line for line in text.splitlines() if needle.casefold() in line.casefold())
        truncated = truncated or len(text) > max_chars
        return {"task_id": task_id, "attempt": attempt, "map_index": map_index,
                "text": text[:max_chars], "searched": needle, "pages_fetched": page + 1,
                "scope": "Only this attempt; absence of matching text does not establish absence of an event"}, truncated
