"""Read-only Spark cross-check of a named disposable F1 result table."""
import sys,json,argparse,traceback
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'code/harness'),str(ROOT/'code/checker')]
from oracle import canonical_hash
from sparkenv import spark_session
ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--table',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args()
if a.output.exists():ap.error('Output exists')
spark=None;result={}
try:
 spark=spark_session(a.output.parent/'read_warehouse')
 rows=spark.read.format('delta').load(a.table.resolve().as_posix()).collect();result={'ok':True,'rows':len(rows),'digest':canonical_hash(tuple(x) for x in rows),'status':'successful_read'}
except Exception as e:
 result={'ok':False,'rows':None,'digest':None,'status':'failed_read','exception_chain':traceback.format_exc()}
 j=getattr(e,'java_exception',None)
 if j is not None and spark is not None:
  w=spark._jvm.java.io.StringWriter();p=spark._jvm.java.io.PrintWriter(w);j.printStackTrace(p);p.flush();result['java_exception_chain']=str(w.toString())
finally:
 a.output.write_text(json.dumps(result,indent=2),encoding='utf8')
 if spark is not None:spark.stop()
