import json


def markdown(report: dict, evidence=None):
    diagnosis = report["diagnosis"]
    lines = [f"# Investigation {report['investigation_id']}", "",
             f"Run: `{report['identity']['dag_id']}` / `{report['identity']['run_id']}`", "",
             f"Source alignment: **{diagnosis['source_alignment']}**", "",
             "This report is a saved artifact. Displaying it is not a live investigation.", ""]
    for name, value in diagnosis.items():
        if name == "source_alignment":
            continue
        lines.extend([f"## {name.replace('_', ' ').title()}", ""])
        values = value if isinstance(value, list) else [value]
        for item in values:
            if isinstance(item, dict):
                text = item.get("text", item.get("proposal", ""))
                lines.append(f"- {text} [{item.get('support', 'proposal')}] " + ", ".join(item.get("evidence_ids", [])))
                for sub in ("preconditions", "verification_steps"):
                    if item.get(sub):
                        lines.append(f"  - {sub.replace('_', ' ')}: " + "; ".join(item[sub]))
            else:
                lines.append(f"- {item}")
        lines.append("")
    if evidence:
        lines += ['## Evidence provenance', '']
        for item in evidence:
            data = item.data
            detail = ''
            if item.kind == 'source':
                detail = f"revision `{data.get('revision', 'unavailable')}`"
                if data.get('path'):
                    detail += f"; `{data['path']}` lines {data.get('start')}–{data.get('end')}"
                elif data.get('matches'):
                    detail += '; matches: ' + ', '.join(f"{m['path']}:{m['line']}" for m in data['matches'][:12])
            else:
                detail = json.dumps(item.provenance.get('selector', {}), sort_keys=True)
            lines.append(f"- `{item.id}` — {item.kind}; {item.availability}; truncated={item.truncated}; {detail}; collected {item.collected_at}; {item.provenance.get('basis', 'unknown')}")
        lines.append('')
    lines += ["## Runtime metadata", "", "```json", json.dumps(report["runtime"], indent=2), "```", "",
              "Valid references establish retrieval provenance, not semantic correctness."]
    return "\n".join(lines)
