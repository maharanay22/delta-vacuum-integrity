"""Build the larger synthetic DV tables used for RQ4 (run with the Spark venv).

usage: python perf_tables.py OUT_DIR

wide-N: one write of N small files, then 3 DELETEs that each give every file a new DV
        (remove(X, oldDV) + add(X, newDV) per file), then 1 append of N/10 files.
long-200xK: 200 files, then K DELETEs that each touch every file (K commits of 400 actions)."""
from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "harness"))

from generate import checkpoint, frame, strip_hidden  # noqa: E402
from oracle import replay  # noqa: E402
from sparkenv import spark_session  # noqa: E402

TABLES = (("wide-1000", 1000, 3), ("wide-3000", 3000, 3), ("wide-10000", 10000, 3), ("long-200x60", 200, 60))


def build(spark, out: Path, name: str, files: int, deletes: int) -> None:
    table = out / name / "table"
    if (out / name / "meta.json").exists():
        return
    shutil.rmtree(out / name, ignore_errors=True)
    path = table.as_posix()
    t0 = time.time()
    rows_per_file = 40
    (
        frame(spark, 0, files * rows_per_file, files)
        .write.format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.checkpointInterval", "1000")
        .save(path)
    )
    for r in range(deletes):
        modulus = 97 if deletes > 3 else 7
        spark.sql(f"DELETE FROM delta.`{path}` WHERE id % {modulus} = {r % modulus}")
    frame(spark, files * rows_per_file, max(files // 10, 1) * rows_per_file, max(files // 10, 1)).write.format(
        "delta"
    ).mode("append").save(path)
    if name.startswith("long"):
        checkpoint(spark, path)
    strip_hidden(table)
    states = replay(table)
    n = len(states)
    first_half_gap = 100 if name.startswith("wide") else 10
    gaps = [first_half_gap if v < n // 2 else 2 for v in range(n)]
    offsets = [2.5 + sum(gaps[v + 1 : n]) for v in range(n)]
    meta = {
        "name": name,
        "files": files,
        "deletes": deletes,
        "versions": [{"version": s.version, "commit_ts": s.commit_ts} for s in states],
        "offsets_hours": offsets,
        "build_seconds": round(time.time() - t0, 1),
    }
    (out / name / "meta.json").write_text(json.dumps(meta, indent=1), encoding="utf-8")
    print(name, "built in", meta["build_seconds"], "s", flush=True)


def main() -> None:
    out = Path(sys.argv[1]).resolve()
    spark = spark_session(out / "_spark_wh")
    for name, files, deletes in TABLES:
        build(spark, out, name, files, deletes)


if __name__ == "__main__":
    main()
