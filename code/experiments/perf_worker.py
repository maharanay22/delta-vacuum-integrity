"""One timed measurement in a fresh process; prints a JSON line.

usage: python perf_worker.py deltars TABLE MODE RETENTION dry|real
       python perf_worker.py checker TABLE PLAN_FILE RETENTION
       python perf_worker.py baseline-deltars | baseline-checker"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "checker"))


def peak_mb() -> float:
    info = psutil.Process().memory_info()
    return getattr(info, "peak_wset", info.rss) / 2**20


def main() -> None:
    what = sys.argv[1]
    if what == "baseline-deltars":
        import deltalake  # noqa: F401

        print(json.dumps({"seconds": 0.0, "peak_mb": peak_mb()}))
    elif what == "baseline-checker":
        import vacuumcheck  # noqa: F401
        import pyarrow.parquet  # noqa: F401

        print(json.dumps({"seconds": 0.0, "peak_mb": peak_mb()}))
    elif what == "deltars":
        import deltalake

        table, mode, retention, kind = sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
        t0 = time.perf_counter()
        dt = deltalake.DeltaTable(table)
        plan = dt.vacuum(retention_hours=retention, dry_run=kind == "dry", enforce_retention_duration=False, full=mode == "full")
        secs = time.perf_counter() - t0
        print(json.dumps({"seconds": secs, "peak_mb": peak_mb(), "plan": list(plan) if kind == "dry" else None, "n": len(plan)}))
    elif what == "checker":
        from vacuumcheck import check_plan

        table, plan_file, retention = Path(sys.argv[2]), Path(sys.argv[3]), float(sys.argv[4])
        plan = json.loads(plan_file.read_text(encoding="utf-8"))
        t0 = time.perf_counter()
        res = check_plan(table, plan, retention)
        secs = time.perf_counter() - t0
        print(json.dumps({"seconds": secs, "peak_mb": peak_mb(), "n": len(res.unsafe), "actions": res.actions_read, "protected": res.protected_files}))
    else:
        raise SystemExit(f"unknown {what}")


if __name__ == "__main__":
    main()
