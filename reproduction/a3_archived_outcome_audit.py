"""New read-only analysis of archived outcomes; NOT a recovered original A3 script."""
import json,collections,argparse
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def main():
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args()
 if a.output.exists():ap.error('Refusing to overwrite an existing result')
 groups=collections.defaultdict(lambda:collections.Counter());counts=collections.Counter();rows=[]
 for p in sorted((ROOT/'code/results').glob('runs_*.jsonl')):
  for line in p.read_text().splitlines():
   if not line.strip():continue
   r=json.loads(line);err=bool(r.get('error'));missing=any(k not in r for k in ['error','plan','deleted','plan_vs_actual_diff'])
   # Recorded difference excludes _delta_log bookkeeping; independently recompute it.
   recomputed=sorted(set(r.get('plan',[])) ^ {n for n in r.get('deleted',{}) if not n.startswith('_delta_log/')})
   same=not recomputed;g=groups[(r['impl'],r['family'],r['mode'],r['retention_h'])]
   counts['runs']+=1;g['runs']+=1;g['error_runs' if err else 'error_free_runs']+=1
   for c in [counts,g]:
    c['error_runs' if err else 'error_free_runs']+=int(c is counts)
    c['error_free_differences' if not err else 'error_stratum_differences']+=int(not same)
    c['missing_fields']+=int(missing);c['difference_record_disagreements']+=int(recomputed!=r.get('plan_vs_actual_diff'))
   rows.append({'impl':r['impl'],'family':r['family'],'seed':r['seed'],'mode':r['mode'],'retention_h':r['retention_h'],'error':err,'missing_fields':missing,'computed_difference':recomputed,'recorded_difference':r.get('plan_vs_actual_diff')})
 out={'provenance':'new_analysis_of_original_outcomes_not_recovered_original_A3_executable','method':'set(plan) symmetric_difference(non-log deleted paths), stratified by recorded error; no VACUUM executed','totals':dict(counts),'groups':[dict(zip(['impl','family','mode','retention_h'],k),**dict(v)) for k,v in sorted(groups.items())],'per_run':rows}
 a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(out,indent=2),encoding='utf8');print(json.dumps(out['totals']))
if __name__=='__main__':main()
