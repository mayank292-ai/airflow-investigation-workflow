import json
from collections import defaultdict


def materialized(path):
    records = [json.loads(line) for line in path.open(encoding="utf-8")]
    groups = defaultdict(list)
    for record in records:
        groups[record["group"]].append(record)
    return [(group, sum(item["value"] for item in items), len(items))
            for group, items in sorted(groups.items())]


def streaming(path):
    # Memory proportional to the number of groups, which is bounded by input contract.
    totals = defaultdict(lambda: [0, 0])
    with path.open(encoding="utf-8") as rows:
        for line in rows:
            item = json.loads(line)
            totals[item["group"]][0] += item["value"]
            totals[item["group"]][1] += 1
    return [(group, total, count) for group, (total, count) in sorted(totals.items())]
