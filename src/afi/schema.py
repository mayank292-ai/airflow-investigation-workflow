from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Identity(StrictModel):
    dag_id: str = Field(min_length=1, max_length=150)
    run_id: str = Field(min_length=1, max_length=250)
    task_id: str | None = None
    map_index: int = Field(default=-1, ge=-1)
    attempt: int | None = Field(default=None, ge=1)


class Evidence(StrictModel):
    id: str
    investigation_id: str
    kind: str
    provenance: dict
    collected_at: str
    availability: Literal["available", "missing", "error"]
    truncated: bool
    data: dict


class Claim(StrictModel):
    text: str
    support: Literal["supported", "plausible", "unresolved"]
    evidence_ids: list[str]


class Remediation(StrictModel):
    proposal: str
    evidence_ids: list[str]
    preconditions: list[str]
    verification_steps: list[str]


class Diagnosis(StrictModel):
    source_alignment: Literal["verified", "approximate", "unknown"]
    task_purpose: Claim
    observed_failure: Claim
    confirmed_findings: list[Claim]
    hypotheses: list[Claim]
    missing_or_conflicting_evidence: list[str]
    downstream_effects: list[Claim]
    data_write_retry_assessment: list[Claim]
    remediation_options: list[Remediation]
    limitations: list[str]


class Report(StrictModel):
    investigation_id: str
    identity: Identity
    created_at: str
    diagnosis: Diagnosis
    runtime: dict


class Edit(StrictModel):
    path: str
    old: str = Field(min_length=1, max_length=12000)
    new: str = Field(max_length=16000)


class PatchProposal(StrictModel):
    approach: str
    assumptions: list[str]
    source_revision: str
    evidence_ids: list[str]
    edits: list[Edit] = Field(min_length=1, max_length=5)


class ReportValidationError(ValueError):
    """Application integrity failure; the message never includes provider secrets."""


def validate_references(diagnosis: Diagnosis, evidence: dict[str, Evidence]):
    claims = [diagnosis.task_purpose, diagnosis.observed_failure,
              *diagnosis.confirmed_findings, *diagnosis.hypotheses,
              *diagnosis.downstream_effects, *diagnosis.data_write_retry_assessment]
    for claim in claims:
        if claim.support == "supported" and not claim.evidence_ids:
            raise ReportValidationError("Supported claim has no evidence")
        for ref in claim.evidence_ids:
            if ref not in evidence:
                raise ReportValidationError(f"Unknown or foreign evidence reference: {ref}")
            if claim.support == "supported" and evidence[ref].availability != "available":
                raise ReportValidationError(f"Supported claim cites unavailable evidence: {ref}. Describe absence in missing_or_conflicting_evidence or use unresolved support; do not claim execution facts from unavailable records.")
    for option in diagnosis.remediation_options:
        if not option.evidence_ids or any(ref not in evidence for ref in option.evidence_ids):
            raise ReportValidationError("Remediation has invalid evidence references")
    if any(c.support != "supported" for c in diagnosis.confirmed_findings):
        raise ReportValidationError("Confirmed finding must be supported")
