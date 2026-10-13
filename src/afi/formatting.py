"""Constrain final report labels/references to retrieved facts, never diagnoses."""
from copy import deepcopy
from afi.schema import Diagnosis


def verified_alignment(evidence):
    source = any(e.kind == 'source' and e.availability == 'available' for e in evidence.values())
    binding = any(e.kind == 'binding' and e.availability == 'available' and
                  e.data.get('alignment') == 'verified' for e in evidence.values())
    embedded = False
    for record in evidence.values():
        receipt = record.data.get('source_binding') or {}
        identity = record.provenance.get('identity', {})
        if (record.kind == 'source' and record.availability == 'available' and 'text' in record.data and
            receipt.get('alignment') == 'verified' and receipt.get('revision') == record.data.get('revision') and
            receipt.get('dag_id') == identity.get('dag_id') and receipt.get('run_id') == identity.get('run_id')):
            embedded = True
    return source and (binding or embedded)


def report_schema(evidence):
    """Final-call schema: categorical alignment and IDs are machine-checkable.

    Text/diagnosis/remediation are still chosen by the model. At most80 recent
    records enter enums, bounding schema size. New investigations cannot include
    another investigation's evidence. Existing application validation still runs.
    """
    records = list(evidence.values())[-80:]
    known = [e.id for e in records]
    available = [e.id for e in records if e.availability == 'available']
    schema = Diagnosis.model_json_schema()
    definitions = schema['$defs']
    claim = definitions['Claim']
    supported, uncertain = deepcopy(claim), deepcopy(claim)
    supported['properties']['support']['enum'] = ['supported']
    uncertain['properties']['support']['enum'] = ['plausible', 'unresolved']

    def constrain_ids(target, name, values, required=False):
        ids = target['properties']['evidence_ids']
        if values:
            definitions[name] = {'type': 'string', 'enum': values}
            ids['items'] = {'$ref': '#/$defs/' + name}
            if required:
                ids['minItems'] = 1
        else:
            ids['maxItems'] = 0  # Never create an invalid empty enum.

    constrain_ids(supported, 'AvailableEvidenceId', available, required=True)
    constrain_ids(uncertain, 'KnownEvidenceId', known)
    constrain_ids(definitions['Remediation'], 'KnownEvidenceId', known, required=True)
    if available:
        definitions['SupportedClaim'] = supported
        definitions['Claim'] = {'anyOf': [supported, uncertain]}
        schema['properties']['confirmed_findings']['items'] = {'$ref': '#/$defs/SupportedClaim'}
    else:
        definitions['Claim'] = uncertain
        schema['properties']['confirmed_findings']['maxItems'] = 0
    if not verified_alignment(evidence):
        schema['properties']['source_alignment']['enum'] = ['approximate', 'unknown']
    return schema
