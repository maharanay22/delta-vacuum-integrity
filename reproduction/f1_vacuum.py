"""Optional destructive F1 rerun on a NEW disposable copy; never modifies fixtures.
Use the selected delta-rs environment. No action occurs without --execute.
This supplemental fixture test is separate from the original 3,200-run matrix.
"""
import argparse,json,sys,shutil,time,hashlib,traceback,os
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.dont_write_bytecode=True
sys.path[:0]=[str(ROOT/'code'/'harness'),str(ROOT/'code'/'checker')]
def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--output',required=True,type=Path);ap.add_argument('--execute',action='store_true');args=ap.parse_args()
    if not args.execute:ap.error('Real VACUUM requires --execute and a new disposable --output')
    out=args.output.resolve()
    if out.is_relative_to(ROOT.resolve()):ap.error('Use an output directory outside the repository')
    if out.exists():ap.error('Output must not exist')
    from oracle import replay,read_state,referenced_files
    from aging import age_table
    import deltalake
    fixture=ROOT/'evidence'/'fixtures'/'f1_168'/'pre'
    mtimes=json.loads((ROOT/'evidence'/'f1_168'/'prefix_native_aged_live_dv_inventory_before.json').read_text())
    out.mkdir(parents=True);table=out/'table';shutil.copytree(fixture,table)
    result={'kind':'new_supplemental_fixture_rerun_not_original_matrix','retention_h':168,'mode':'full','deltalake':deltalake.__version__}
    def inventory():return {p.relative_to(table).as_posix():{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mtime_ns':p.stat().st_mtime_ns} for p in table.rglob('*') if p.is_file()}
    try:
        states=replay(table)
        # Restore archived source times on this disposable copy before mapping ages.
        for name,info in mtimes.items():
            p=table/name
            if p.is_file():os.utime(p,ns=(info['mtime_ns'],info['mtime_ns']))
        now=int(time.time()*1000)
        result['target_commit_timestamps']=age_table(table,[s.commit_ts for s in states],[212,202],now)
        result['run_now_ms']=now;result['cutoff_ms']=now-168*3600000
        state=replay(table)[-1];before=read_state(state,table)
        assert before.ok and before.rows==16 and before.digest=='1f3e17711306b5c9e2682199e33b89b3854fc0b45037e1812b6b9aaa0b02b5b7'
        result['before_read']=vars_result(before);result['inventory_before']=inventory();result['latest_references']=sorted(referenced_files(state,table))
        result['plan_raw']=list(deltalake.DeltaTable(str(table)).vacuum(retention_hours=168,dry_run=True,enforce_retention_duration=True,full=True))
        try:
            result['vacuum_return']=list(deltalake.DeltaTable(str(table)).vacuum(retention_hours=168,dry_run=False,enforce_retention_duration=True,full=True))
        except Exception:result['vacuum_exception_chain']=traceback.format_exc();time.sleep(2)
        result['inventory_after']=inventory();result['deleted']=sorted(set(result['inventory_before'])-set(result['inventory_after']))
        result['after_read']=vars_result(read_state(replay(table)[-1],table))
    except Exception:result['fatal_exception_chain']=traceback.format_exc();raise
    finally:(out/'result.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
def vars_result(r):return {'ok':r.ok,'rows':r.rows if r.ok else None,'digest':r.digest if r.ok else None,'error':r.error,'status':'successful_read' if r.ok else 'failed_read'}
if __name__=='__main__':main()
