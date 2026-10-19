"""RQ4: runtime and memory of vacuum (dry run and real) vs the safety checker on larger tables.

usage (Spark venv): python perf_run.py PERF_TABLES_DIR WORK_DIR OUT_CSV [--reps 3] [--no-spark]

Every delta-rs and checker measurement runs in a fresh subprocess so the peak working set belongs
to that measurement alone. Spark runs in one warm JVM; its memory is not measured."""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "harness"))

from aging import age_table  # noqa: E402

VENVS = HERE.parents[1] / "venvs"
DELTARS = {"deltars-1.6.6": VENVS / "prefix", "deltars-main": VENVS / "main", "deltars-main+4858": VENVS / "pr4858"}
CHECKER_PY = VENVS / "main" / "Scripts" / "python.exe"
RETENTIONS = (0, 168)
FIELDS = ["table", "files", "actions", "impl", "what", "mode", "retention_h", "rep", "seconds", "peak_mb", "n", "error"]


def worker(python: Path, *args: str) -> dict:
    proc = subprocess.run([str(python), str(HERE / "perf_worker.py"), *args], capture_output=True, text=True, timeout=3600)
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("{")]
    if proc.returncode or not lines:
        return {"seconds": None, "peak_mb": None, "n": None, "error": (proc.stderr or proc.stdout)[-300:].replace("\n", " ")}
    return json.loads(lines[-1])


def fresh_copy(src: Path, meta: dict, work: Path) -> Path:
    shutil.rmtree(work, ignore_errors=True)
    shutil.copytree(src / "table", work)
    age_table(work, [v["commit_ts"] for v in meta["versions"]], meta["offsets_hours"], int(time.time() * 1000))
    return work


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("tables", type=Path)
    ap.add_argument("work", type=Path)
    ap.add_argument("out", type=Path)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--no-spark", action="store_true")
    args = ap.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    spark = None
    if not args.no_spark:
        from sparkenv import spark_session

        spark = spark_session(args.work.resolve() / "_spark_wh")
    rows = []
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        out = csv.DictWriter(fh, FIELDS)
        out.writeheader()

        def emit(**row):
            out.writerow({k: row.get(k) for k in FIELDS})
            fh.flush()
            rows.append(row)
            print(row, flush=True)

        for label, venv in DELTARS.items():
            b = worker(venv / "Scripts" / "python.exe", "baseline-deltars")
            emit(table="-", files=0, actions=0, impl=label, what="import-baseline", seconds=0, peak_mb=b.get("peak_mb"))
        b = worker(CHECKER_PY, "baseline-checker")
        emit(table="-", files=0, actions=0, impl="checker", what="import-baseline", seconds=0, peak_mb=b.get("peak_mb"))

        for src in sorted(p for p in args.tables.iterdir() if (p / "meta.json").exists()):
            meta = json.loads((src / "meta.json").read_text(encoding="utf-8"))
            for rep, retention in ((r, ret) for r in range(args.reps) for ret in RETENTIONS):
                base = dict(table=meta["name"], files=meta["files"], retention_h=retention)
                for label, venv in DELTARS.items():
                    py = venv / "Scripts" / "python.exe"
                    for mode in ("lite", "full"):
                        work = fresh_copy(src, meta, args.work / "perf")
                        dry = worker(py, "deltars", str(work), mode, str(retention), "dry")
                        plan_file = args.work / "plan.json"
                        plan_file.write_text(json.dumps(dry.get("plan") or []), encoding="utf-8")
                        chk = worker(CHECKER_PY, "checker", str(work), str(plan_file), str(retention))
                        real = worker(py, "deltars", str(work), mode, str(retention), "real")
                        common = dict(base, actions=chk.get("actions"), impl=label, mode=mode, rep=rep)
                        emit(**common, what="dry_run", seconds=dry["seconds"], peak_mb=dry["peak_mb"], n=dry.get("n"), error=dry.get("error"))
                        emit(**common, what="checker", seconds=chk["seconds"], peak_mb=chk["peak_mb"], n=chk.get("n"), error=chk.get("error"))
                        emit(**common, what="vacuum", seconds=real["seconds"], peak_mb=real["peak_mb"], n=real.get("n"), error=real.get("error"))
                if spark is not None:
                    for mode in ("lite", "full"):
                        work = fresh_copy(src, meta, args.work / "perf")
                        sql = f"VACUUM delta.`{work.resolve().as_posix()}` {mode.upper()} RETAIN {retention} HOURS"
                        t0 = time.perf_counter()
                        n = len(spark.sql(sql + " DRY RUN").collect())
                        dry_s = time.perf_counter() - t0
                        spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
                        t0 = time.perf_counter()
                        spark.sql(sql).collect()
                        real_s = time.perf_counter() - t0
                        spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
                        emit(**base, actions=None, impl="spark-4.4.1", what="dry_run", mode=mode, rep=rep, seconds=dry_s, n=n)
                        emit(**base, actions=None, impl="spark-4.4.1", what="vacuum", mode=mode, rep=rep, seconds=real_s)
    shutil.rmtree(args.work / "perf", ignore_errors=True)


if __name__ == "__main__":
    main()
