import hashlib,json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
manifest=json.loads((root/'ARTIFACT_MANIFEST.json').read_text())
bad=[n for n,h in manifest['files'].items() if not (root/n).is_file() or hashlib.sha256((root/n).read_bytes()).hexdigest()!=h]
assert not bad,bad
print('PASS:',len(manifest['files']),'candidate file hashes')
