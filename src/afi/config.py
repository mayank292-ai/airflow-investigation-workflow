from pathlib import Path
import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class Settings(BaseModel):
    root: Path
    model: str = "gpt-5.4-mini"
    reasoning_effort: str = "low"
    max_model_calls: int = Field(default=8, ge=1, le=12)
    max_tool_calls: int = Field(default=24, ge=1, le=40)
    max_output_tokens: int = Field(default=4000, ge=500, le=6000)
    context_bytes: int = Field(default=100000, ge=40000, le=160000)
    investigation_seconds: int = Field(default=180, ge=10, le=300)
    max_usd: float = Field(default=1.5, gt=0, le=3)
    approved_total_usd: float = Field(default=3.0, gt=0, le=25)
    airflow_url: str = "http://127.0.0.1:8080"
    airflow_username: str = "airflow"
    airflow_password: str = "airflow"
    dags: list[str] = ["local_pipeline_01", "local_pipeline_02", "local_pipeline_03"]
    poll_seconds: int = Field(default=10, ge=5, le=60)

    @property
    def state(self) -> Path:
        return self.root / "state"

    @classmethod
    def load(cls, root: Path | None = None):
        root = (root or Path.cwd()).resolve()
        load_dotenv(root / ".env", override=False)
        values = {name: os.environ[f"AFI_{name.upper()}"] for name in cls.model_fields
                  if name != "root" and f"AFI_{name.upper()}" in os.environ}
        if "dags" in values:
            values["dags"] = values["dags"].split(",")
        return cls(root=root, **values)
