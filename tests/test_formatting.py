import json
from afi.formatting import report_schema
from afi.schema import Evidence, now


def record(key, status='available'):
    return Evidence(id=key, investigation_id='one', kind='logs', provenance={}, collected_at=now(),
                    availability=status, truncated=False, data={})


def test_final_schema_uses_retrieved_ids_and_does_not_supply_findings():
    schema = report_schema({'a': record('a'), 'b': record('b', 'missing')})
    definitions = schema['$defs']
    assert definitions['AvailableEvidenceId']['enum'] == ['a']
    assert definitions['KnownEvidenceId']['enum'] == ['a', 'b']
    supported, uncertain = definitions['Claim']['anyOf']
    assert supported['properties']['support']['enum'] == ['supported']
    assert supported['properties']['evidence_ids']['minItems'] == 1
    assert uncertain['properties']['support']['enum'] == ['plausible', 'unresolved']
    assert schema['properties']['source_alignment']['enum'] == ['approximate', 'unknown']
    assert not any('default' in branch['properties']['text'] for branch in (supported, uncertain))


def test_no_evidence_never_emits_invalid_empty_enum_and_history_is_bounded():
    empty = report_schema({})
    assert empty['$defs']['Claim']['properties']['evidence_ids']['maxItems'] == 0
    assert empty['properties']['confirmed_findings']['maxItems'] == 0
    assert '"enum": []' not in json.dumps(empty)
    schema = report_schema({str(n): record(str(n)) for n in range(150)})
    assert len(schema['$defs']['KnownEvidenceId']['enum']) == 80
