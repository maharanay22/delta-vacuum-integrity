"""Which commits did vacuum itself leave in the log? (VACUUM START / VACUUM END)

usage: python vacuum_commits.py WORK_DIR OUT_CSV

Reads the kept work directories (all failing runs and a sample of passing ones) of every runs_*.jsonl
row and records the operations committed after the history's last version."""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parents[1] / "results"


def main() -> None:
    work, out = Path(sys.argv[1]), Path(sys.argv[2])
    rows = []
    for f in sorted(RESULTS.glob("runs_*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            log = work / r["impl"] / f"{r['family']}_{r['seed']}_{r['mode']}_{r['retention_h']}" / "_delta_log"
            if not log.is_dir() or "n_versions" not in r:
                continue
            ops = []
            for v in range(r["n_versions"], r["n_versions"] + 5):
                commit = log / f"{v:020d}.json"
                if commit.exists():
                    first = json.loads(commit.read_text(encoding="utf-8").splitlines()[0])
                    ops.append(first.get("commitInfo", {}).get("operation", "?"))
            rows.append({"impl": r["impl"], "family": r["family"], "seed": r["seed"], "mode": r["mode"],
                         "retention_h": r["retention_h"], "vacuum_error": bool(r["error"]), "commits": "+".join(ops) or "none"})
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(len(rows), "rows")


if __name__ == "__main__":
    main()
