"""Validate the oracle's post-vacuum verdicts with Delta Lake for Spark as an independent reader.

usage (Spark venv): python crosscheck_spark.py WORK_DIR [WORK_DIR ...] --out CSV --histories DIR [--max-per-label N]

For every kept work directory (all failing runs plus a sample of passing runs), Spark reads each
pre-vacuum version with versionAsOf and the result is compared with the oracle's verdict that the
runner stored (readable? same rows as before vacuum?). Spark 4.4 refuses time travel to versions
older than delta.deletedFileRetentionDuration by default; such refusals are recorded and the read is
retried with that guard disabled so that file-level readability can be compared."""
from __future__ import annotations

import argparse
import csv
import json
import sys
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "harness"))

from oracle import canonical_hash  # noqa: E402
from sparkenv import spark_session  # noqa: E402

RESULTS = HERE.parent / "results"
GUARD = "spark.databricks.delta.vacuum.enforceTimeTravelWithinDeletedFileRetentionDuration"
LABEL_FILES = {"deltars-1.6.6": "prefix", "deltars-main": "main", "deltars-main+4858": "pr4858", "spark-4.4.1": "spark"}


def load_rows() -> dict[tuple[str, str], dict]:
    rows = {}
    for f in list(RESULTS.glob("runs_*.jsonl")) + list(RESULTS.glob("minimal_*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            rows[(r["impl"], f"{r['family']}_{r['seed']}_{r['mode']}_{r['retention_h']}")] = r
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("work", type=Path, nargs="+")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--histories", type=Path, required=True)
    ap.add_argument("--max-per-label", type=int, default=25)
    ap.add_argument("--max-passing", type=int, default=10)
    args = ap.parse_args()
    rows = load_rows()
    spark = spark_session(args.work[0].resolve() / "_spark_wh_crosscheck")
    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        out = csv.writer(fh)
        out.writerow(["impl", "run", "version", "protected", "oracle_ok", "oracle_same", "spark_guard_refused", "spark_ok", "spark_same", "agree", "spark_error"])
        for work_root in args.work:
            for label_dir in sorted(p for p in work_root.iterdir() if p.is_dir() and p.name in LABEL_FILES):
                runs = sorted((p for p in label_dir.iterdir() if p.is_dir() and (label_dir.name, p.name) in rows),
                              key=lambda p: zlib.crc32(p.name.encode()))
                failing = [p for p in runs if rows[(label_dir.name, p.name)].get("wrongly_deleted")]
                passing = [p for p in runs if p not in failing]
                chosen = failing[: args.max_per_label] + passing[: args.max_passing]
                for run in chosen:
                    r = rows[(label_dir.name, run.name)]
                    for read in r["reads"]:
                        refused = False
                        spark.conf.set(GUARD, "true")
                        try:
                            got = spark.read.format("delta").option("versionAsOf", read["v"]).load(run.resolve().as_posix()).collect()
                        except Exception as exc:
                            refused = "BEYOND_DELETED_FILE_RETENTION" in str(exc)
                            got = None
                            first_error = str(exc).splitlines()[0][:200]
                        if refused:
                            spark.conf.set(GUARD, "false")
                            try:
                                got = spark.read.format("delta").option("versionAsOf", read["v"]).load(run.resolve().as_posix()).collect()
                            except Exception as exc:
                                first_error = str(exc).splitlines()[0][:200]
                        spark_ok = got is not None
                        err = "" if spark_ok else first_error
                        digest = canonical_hash(tuple(x) for x in got) if spark_ok else ""
                        hist = json.loads((args.histories / r["family"] / f"seed_{r['seed']}" / "meta.json").read_text())
                        spark_same = spark_ok and digest == hist["versions"][read["v"]]["digest"]
                        agree = spark_ok == read["ok"] and spark_same == read["same"]
                        out.writerow([label_dir.name, run.name, read["v"], read["protected"], read["ok"], read["same"], refused, spark_ok, spark_same, agree, err])
                    fh.flush()
                    spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
                    print(label_dir.name, run.name, flush=True)


if __name__ == "__main__":
    main()
