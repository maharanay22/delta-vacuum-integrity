import json,csv
from pathlib import Path
root=Path(__file__).resolve().parents[1]
rows=[]
for p in sorted((root/'code'/'results').glob('runs_*.jsonl')):
    rows.extend(json.loads(x) for x in p.read_text().splitlines() if x)
keys=[tuple(r[k] for k in ['impl','family','seed','mode','retention_h']) for r in rows]
assert len(rows)==len(set(keys))==3200
totals=json.loads((root/'evidence'/'pass10'/'audit_totals.json').read_text())
assert totals['original_histories']==200 and totals['original_versions']==2416
assert totals['history_join_mismatch_runs']==0
with (root/'code'/'results'/'runs.csv').open(newline='',encoding='utf-8') as f: assert len(list(csv.DictReader(f)))==3200
print('PASS: 3200 distinct original matrix outcomes; 200 histories; 2416 versions; no archived join mismatch')
