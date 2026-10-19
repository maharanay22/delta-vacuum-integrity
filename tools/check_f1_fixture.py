import sys,json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(root/'code'/'harness'),str(root/'code'/'checker')]
from oracle import replay,read_state
base=root/'evidence'/'fixtures'/'f1_168'
pre=read_state(replay(base/'pre')[-1],base/'pre')
post=read_state(replay(base/'post')[-1],base/'post')
assert pre.ok and pre.rows==16 and pre.digest=='1f3e17711306b5c9e2682199e33b89b3854fc0b45037e1812b6b9aaa0b02b5b7'
assert not post.ok
print(json.dumps({'pre':{'readable':True,'rows':pre.rows,'digest':pre.digest},'post':{'readable':False,'rows':None,'digest':None,'error':post.error}},indent=2))
