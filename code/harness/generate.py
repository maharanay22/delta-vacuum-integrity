"""Seeded workload generator: builds DV-enabled table histories with Delta Lake for Spark.

usage: python generate.py OUT_DIR --seeds 0-99 [--families nockpt,ckpt]

For every seed, the data operations are drawn from random.Random(seed), so the "nockpt" and "ckpt"
families share the same logical history; "ckpt" additionally writes checkpoints at positions drawn
from an independent stream. Each history is stored as OUT_DIR/<family>/seed_<n>/table plus meta.json."""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import time
import traceback
import uuid
from pathlib import Path

from oracle import canonical_hash, read_state, replay
from sparkenv import spark_session

OPS = (
    ("append", 15),
    ("delete_mod", 25),
    ("delete_range", 10),
    ("update", 15),
    ("merge", 15),
    ("optimize", 7),
    ("purge", 3),
    ("orphan", 5),
)


def draw_plan(seed: int) -> dict:
    rng = random.Random(seed)
    n_rows = rng.randint(100, 400)
    plan = {"seed": seed, "initial_rows": n_rows, "initial_files": rng.randint(3, 6), "ops": []}
    next_id = n_rows
    names, weights = zip(*OPS)
    for _ in range(rng.randint(8, 16)):
        op = rng.choices(names, weights)[0]
        if op == "append":
            k = rng.randint(20, 120)
            plan["ops"].append({"op": op, "start": next_id, "rows": k, "files": rng.randint(1, 2)})
            next_id += k
        elif op in ("delete_mod", "update"):
            m = rng.randint(3, 9)
            plan["ops"].append({"op": op, "m": m, "r": rng.randrange(m)})
        elif op == "delete_range":
            a = rng.randrange(max(next_id, 1))
            plan["ops"].append({"op": op, "lo": a, "hi": a + rng.randint(5, 60)})
        elif op == "merge":
            a = rng.randrange(max(next_id, 1))
            w = rng.randint(10, 80)
            plan["ops"].append({"op": op, "lo": a, "hi": a + w, "bump": rng.randint(1, 1000)})
            next_id = max(next_id, a + w)
        else:
            plan["ops"].append({"op": op, "pick": rng.random()})
    crng = random.Random(seed + 1_000_003)
    n_ops = len(plan["ops"])
    picks = sorted(crng.sample(range(2, n_ops + 1), k=min(n_ops - 1, crng.randint(1, 2))))
    plan["checkpoint_after_op"] = picks
    plan["gaps_hours"] = [random.Random(seed * 7919 + i).randint(6, 30) for i in range(n_ops + 1)]
    return plan


def frame(spark, start: int, rows: int, files: int):
    return (
        spark.range(start, start + rows)
        .selectExpr("id", "cast(id % 7 as int) as grp", "cast(id * 1.5 as double) as val")
        .repartition(files)
    )


def apply_op(spark, path: str, op: dict) -> None:
    t = f"delta.`{path}`"
    kind = op["op"]
    if kind == "append":
        frame(spark, op["start"], op["rows"], op["files"]).write.format("delta").mode("append").save(path)
    elif kind == "delete_mod":
        spark.sql(f"DELETE FROM {t} WHERE id % {op['m']} = {op['r']}")
    elif kind == "delete_range":
        spark.sql(f"DELETE FROM {t} WHERE id BETWEEN {op['lo']} AND {op['hi']}")
    elif kind == "update":
        spark.sql(f"UPDATE {t} SET val = val + 1 WHERE id % {op['m']} = {op['r']}")
    elif kind == "merge":
        spark.sql(
            f"MERGE INTO {t} t USING (SELECT id, cast(id * 1.5 + {op['bump']} as double) AS v FROM range({op['lo']}, {op['hi']})) s "
            "ON t.id = s.id WHEN MATCHED AND s.id % 3 = 0 THEN DELETE WHEN MATCHED THEN UPDATE SET val = s.v "
            "WHEN NOT MATCHED THEN INSERT (id, grp, val) VALUES (s.id, cast(s.id % 7 as int), s.v)"
        )
    elif kind == "optimize":
        spark.sql(f"OPTIMIZE {t}")
    elif kind == "purge":
        spark.sql(f"REORG TABLE {t} APPLY (PURGE)")
    elif kind == "orphan":
        files = sorted(p for p in os.listdir(path) if p.endswith(".parquet"))
        src = files[int(op["pick"] * len(files))]
        shutil.copyfile(os.path.join(path, src), os.path.join(path, f"part-00000-{uuid.uuid4()}-c000.snappy.parquet"))
    else:
        raise ValueError(kind)


def checkpoint(spark, path: str) -> None:
    jvm = spark._jvm
    jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, path).checkpoint()


def spark_hash(spark, path: str, version: int) -> tuple[int, str]:
    rows = spark.read.format("delta").option("versionAsOf", version).load(path).collect()
    return len(rows), canonical_hash(tuple(r) for r in rows)


def strip_hidden(table: Path) -> None:
    for root, dirs, files in os.walk(table):
        for name in files:
            if name.startswith(".") or name.endswith(".tmp"):
                os.remove(os.path.join(root, name))
    staged = table / "_delta_log" / "_staged_commits"
    if staged.is_dir() and not any(staged.iterdir()):
        staged.rmdir()


def generate(spark, out: Path, family: str, seed: int, plan: dict | None = None) -> dict:
    plan = plan or draw_plan(seed)
    hist = out / family / f"seed_{seed}"
    shutil.rmtree(hist, ignore_errors=True)
    table = hist / "table"
    table.parent.mkdir(parents=True)
    path = table.as_posix()
    t0 = time.time()
    (
        frame(spark, 0, plan["initial_rows"], plan["initial_files"])
        .write.format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.checkpointInterval", "1000")
        .save(path)
    )
    ckpts = []
    for i, op in enumerate(plan["ops"], start=1):
        apply_op(spark, path, op)
        if family == "ckpt" and i in plan["checkpoint_after_op"]:
            checkpoint(spark, path)
            ckpts.append(i)
    strip_hidden(table)
    states = replay(table)
    versions = []
    for st in states:
        own = read_state(st, table)
        n_spark, h_spark = spark_hash(spark, path, st.version)
        versions.append(
            {
                "version": st.version,
                "operation": st.operation,
                "commit_ts": st.commit_ts,
                "files": len(st.adds),
                "dv_files": sum(a.dv is not None for a in st.adds),
                "rows": own.rows,
                "digest": own.digest,
                "own_reader_ok": own.ok,
                "spark_rows": n_spark,
                "spark_digest": h_spark,
                "readers_agree": own.ok and own.digest == h_spark,
            }
        )
    n_versions = len(states)
    gaps = plan["gaps_hours"][:n_versions]
    offsets = [2.5 + sum(gaps[v + 1 : n_versions]) for v in range(n_versions)]
    mtimes = {
        p.relative_to(table).as_posix(): int(p.stat().st_mtime * 1000) for p in table.rglob("*") if p.is_file()
    }
    meta = {
        "family": family,
        "seed": seed,
        "plan": plan,
        "checkpoints_written_after_op": ckpts,
        "versions": versions,
        "offsets_hours": offsets,
        "orig_mtimes_ms": mtimes,
        "generation_seconds": round(time.time() - t0, 2),
    }
    (hist / "meta.json").write_text(json.dumps(meta, indent=1), encoding="utf-8")
    return meta


def parse_seeds(text: str) -> list[int]:
    out = []
    for part in text.split(","):
        a, _, b = part.partition("-")
        out.extend(range(int(a), int(b or a) + 1))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out", type=Path)
    ap.add_argument("--seeds", default="0-9")
    ap.add_argument("--families", default="nockpt,ckpt")
    args = ap.parse_args()
    args.out = args.out.resolve()
    spark = spark_session(args.out / "_spark_wh")
    for seed in parse_seeds(args.seeds):
        for family in args.families.split(","):
            if (args.out / family / f"seed_{seed}" / "meta.json").exists():
                continue
            (args.out / family).mkdir(parents=True, exist_ok=True)
            try:
                open(args.out / family / f"seed_{seed}.claim", "x").close()
            except FileExistsError:
                continue
            try:
                meta = generate(spark, args.out, family, seed)
                agree = all(v["readers_agree"] for v in meta["versions"])
                print(f"{family} seed={seed} versions={len(meta['versions'])} agree={agree} {meta['generation_seconds']}s", flush=True)
            except Exception:
                print(f"{family} seed={seed} FAILED", flush=True)
                traceback.print_exc()
            spark.catalog.clearCache()
            spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()


if __name__ == "__main__":
    sys.exit(main())
