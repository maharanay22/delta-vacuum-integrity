"""Optional portable restoration/removal controls. Requires Spark and --execute.
All interventions occur in NEW copies; original fixture data is never changed.
"""
import sys,argparse,json,hashlib,shutil,traceback,platform,importlib.metadata as md
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.dont_write_bytecode=True
sys.path[:0]=[str(ROOT/'code'/'harness'),str(ROOT/'code'/'checker')]
def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--output',required=True,type=Path);ap.add_argument('--execute',action='store_true');a=ap.parse_args()
    if not a.execute:ap.error('Restoration/removal requires --execute and new disposable --output')
    out=a.output.resolve()
    if out.exists() or out.is_relative_to(ROOT.resolve()):ap.error('Use a new nonexistent output outside repository')
    out.mkdir(parents=True)
    from oracle import replay,read_state,canonical_hash,referenced_files
    from sparkenv import spark_session
    fixture=ROOT/'evidence'/'fixtures'/'f1_168';spark=None
    expected='1f3e17711306b5c9e2682199e33b89b3854fc0b45037e1812b6b9aaa0b02b5b7'
    result={'kind':'new_supplemental_causal_rerun_not_original_matrix','states':{},'python':sys.version,'packages':{p:md.version(p) for p in ['pyspark','delta-spark','pyarrow']}}
    def inventory(table):return {p.relative_to(table).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in table.rglob('*') if p.is_file()}
    def read(table,label):
        r=read_state(replay(table)[-1],table)
        rec={'independent':{'ok':r.ok,'rows':r.rows if r.ok else None,'digest':r.digest if r.ok else None,'error':r.error},'inventory':inventory(table)}
        try:
            spark.catalog.clearCache();spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
            rows=spark.read.format('delta').load(table.as_posix()).collect();h=canonical_hash(tuple(x) for x in rows)
            rec['spark']={'ok':True,'rows':len(rows),'digest':h}
        except Exception as exc:
            rec['spark']={'ok':False,'rows':None,'digest':None,'exception_chain':traceback.format_exc()}
            java=getattr(exc,'java_exception',None)
            if java is not None:
                writer=spark._jvm.java.io.StringWriter();printer=spark._jvm.java.io.PrintWriter(writer);java.printStackTrace(printer);printer.flush();rec['spark']['java_exception_chain']=str(writer.toString())
        result['states'][label]=rec
        return rec
    try:
        spark=spark_session(out/'warehouse')
        restores=out/'restored';removes=out/'removed'
        shutil.copytree(fixture/'post',restores);shutil.copytree(fixture/'pre',removes)
        dv=next(p.name for p in (fixture/'pre').glob('deletion_vector_*.bin'))
        original_sha=hashlib.sha256((fixture/'pre'/dv).read_bytes()).hexdigest()
        assert original_sha=='c8546f48f1e03590247c799f97b8ed3c05327a81adafcb47ce8bac9d0abb8ccc'
        first=read(restores,'restoration_before');assert all(not first[k]['ok'] for k in ['independent','spark'])
        shutil.copy2(fixture/'pre'/dv,restores/dv);assert hashlib.sha256((restores/dv).read_bytes()).hexdigest()==original_sha
        after=read(restores,'restoration_after');assert all(after[k]['ok'] and after[k]['rows']==16 and after[k]['digest']==expected for k in ['independent','spark'])
        before=read(removes,'removal_before');assert all(before[k]['ok'] and before[k]['rows']==16 and before[k]['digest']==expected for k in ['independent','spark'])
        (removes/dv).unlink()
        after=read(removes,'removal_after');assert all(not after[k]['ok'] for k in ['independent','spark'])
        result.update(completed=True,dv_sha256=original_sha,expected_rows=16,expected_digest=expected)
    except Exception:result['fatal_exception_chain']=traceback.format_exc();raise
    finally:
        (out/'results.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
        if spark is not None:spark.stop()
if __name__=='__main__':main()
