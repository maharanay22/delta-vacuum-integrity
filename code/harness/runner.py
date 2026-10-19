"""Vacuum runner: one row per (history, implementation, mode, retention).

usage: python runner.py --impl deltars|spark --label LABEL --histories DIR --out FILE.jsonl
                        [--families nockpt,ckpt] [--seeds 0-99] [--modes lite,full] [--retentions 0,168]

Each run works on an independent copy of the stored history that is aged relative to the current
time right before vacuum (see aging.py). The oracle state is taken before the dry run; the real
vacuum follows the dry run and the safety checker runs in between on the same copy."""
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import sys
import time
import traceback
import zlib
from pathlib import Path
from typing import Callable

from aging import age_table
from generate import parse_seeds
from oracle import all_logged_files, classify, inventory, read_state, referenced_files, references, replay

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "checker"))
from vacuumcheck import check_plan  # noqa: E402
from vacuumcheck.dv import table_relative  # noqa: E402

HOUR_MS = 3_600_000
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def describe(exc: BaseException) -> str:
    text = ANSI.sub("", f"{type(exc).__name__}: {exc}")
    return " ".join(text.replace("↳", "->").split())[:600]
VacuumFn = Callable[[Path, str, int, bool], list[str]]


def deltars_vacuum() -> tuple[VacuumFn, dict]:
    import deltalake

    def run(table: Path, mode: str, retention: int, dry_run: bool) -> list[str]:
        dt = deltalake.DeltaTable(str(table))
        return list(
            dt.vacuum(retention_hours=retention, dry_run=dry_run, enforce_retention_duration=False, full=mode == "full")
        )

    return run, {"deltalake": deltalake.__version__, "deltalake_file": deltalake.__file__}


def spark_vacuum(work_root: Path) -> tuple[VacuumFn, dict]:
    from sparkenv import spark_session

    spark = spark_session(work_root / "_spark_wh")

    def run(table: Path, mode: str, retention: int, dry_run: bool) -> list[str]:
        sql = f"VACUUM delta.`{table.as_posix()}` {mode.upper()} RETAIN {retention} HOURS{' DRY RUN' if dry_run else ''}"
        rows = spark.sql(sql).collect()
        spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
        return [r[0] for r in rows] if dry_run else []

    return run, {"spark": spark.version, "delta_spark": "4.4.1"}


def run_one(vacuum: VacuumFn, label: str, hist: Path, meta: dict, mode: str, retention: int, work: Path) -> dict:
    shutil.rmtree(work, ignore_errors=True)
    shutil.copytree(hist / "table", work)
    versions = meta["versions"]
    now_ms = int(time.time() * 1000)
    target = age_table(work, [v["commit_ts"] for v in versions], meta["offsets_hours"], now_ms)
    cutoff = now_ms - retention * HOUR_MS
    states = replay(work)
    latest = states[-1].version
    protected = [s.version for s in states if s.version == latest or target[s.version + 1] >= cutoff]
    needed = frozenset().union(*(referenced_files(states[v], work) for v in protected))
    data_files, dv_files = all_logged_files(work)
    before = inventory(work)
    has_ckpt = any(".checkpoint." in p for p in before)
    reclaimable = {p for p, m in before.items() if not p.startswith("_delta_log/") and p not in needed and m * 1000 < cutoff}

    row = {
        "impl": label,
        "family": meta["family"],
        "seed": meta["seed"],
        "mode": mode,
        "retention_h": retention,
        "n_versions": len(states),
        "n_protected": len(protected),
        "has_checkpoint": has_ckpt,
        "ops": [o["op"] for o in meta["plan"]["ops"]],
        "files_before": sum(1 for p in before if not p.startswith("_delta_log/")),
        "needed_files": len(needed),
        "reclaimable_files": len(reclaimable),
        "error": "",
    }
    try:
        t0 = time.perf_counter()
        plan_raw = vacuum(work, mode, retention, True)
        row["dry_run_s"] = time.perf_counter() - t0
    except Exception as exc:
        row["error"] = "dry_run: " + describe(exc)
        plan_raw = None
    plan_list = [table_relative(p, work) for p in plan_raw or []]
    plan = sorted(set(plan_list))
    row["plan"] = plan
    row["plan_raw_len"] = len(plan_list)
    row["plan_duplicates"] = sorted(p for p in plan if plan_list.count(p) > 1)
    row["plan_size"] = len(plan)
    row["plan_unsafe_oracle"] = sorted(set(plan) & needed)
    if plan_raw is not None:
        t0 = time.perf_counter()
        check = check_plan(work, plan, retention)
        row["checker_s"] = time.perf_counter() - t0
        row["checker_flagged"] = sorted(p for p, _ in check.unsafe)
        row["checker_reasons"] = {p: why.reason for p, why in check.unsafe}
        row["checker_protected_files"] = check.protected_files
        row["checker_actions"] = check.actions_read
    try:
        t0 = time.perf_counter()
        vacuum(work, mode, retention, False)
        row["vacuum_s"] = time.perf_counter() - t0
    except Exception as exc:
        row["error"] += "vacuum: " + describe(exc)
        time.sleep(2)
    after = inventory(work)
    deleted = sorted(set(before) - set(after))
    row["deleted"] = {p: classify(p, data_files, dv_files) for p in deleted}
    row["wrongly_deleted"] = {p: classify(p, data_files, dv_files) for p in deleted if p in needed}
    refs = references(work, latest)
    latest_files = referenced_files(states[-1], work)
    row["wrong_detail"] = {
        p: {"active_in_latest": p in latest_files, "refs": refs.get(p, []), "cutoff": cutoff}
        for p in row["wrongly_deleted"]
    }
    row["wrongly_retained"] = {p: classify(p, data_files, dv_files) for p in sorted(reclaimable) if p in after}
    row["plan_vs_actual_diff"] = sorted(set(plan) ^ {p for p in deleted if not p.startswith("_delta_log/")})
    reads = []
    for s in states:
        res = read_state(s, work)
        expected = versions[s.version]["digest"]
        reads.append(
            {
                "v": s.version,
                "protected": s.version in protected,
                "ok": res.ok,
                "same": res.ok and res.digest == expected,
                "rows_lost": versions[s.version]["rows"] - res.rows if res.ok else versions[s.version]["rows"],
                "error": res.error,
            }
        )
    row["reads"] = reads
    row["unreadable_protected"] = [r["v"] for r in reads if r["protected"] and not r["ok"]]
    row["changed_protected"] = [r["v"] for r in reads if r["protected"] and r["ok"] and not r["same"]]
    row["latest_readable"] = reads[-1]["ok"] and reads[-1]["same"]
    return row


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--impl", choices=("deltars", "spark"), required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--histories", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--families", default="nockpt,ckpt")
    ap.add_argument("--seeds", default="0-99")
    ap.add_argument("--modes", default="lite,full")
    ap.add_argument("--retentions", default="0,168")
    ap.add_argument("--keep-every", type=int, default=10, help="keep 1 in N passing work dirs for cross-checks")
    args = ap.parse_args()
    work_root = args.work.resolve() / args.label
    work_root.mkdir(parents=True, exist_ok=True)
    vacuum, versions = deltars_vacuum() if args.impl == "deltars" else spark_vacuum(work_root)
    done = set()
    if args.out.exists():
        for line in args.out.read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            done.add((r["family"], r["seed"], r["mode"], r["retention_h"]))
    env = {"label": args.label, "python": sys.version, "platform": platform.platform(), **versions}
    (work_root / "runner_env.json").write_text(json.dumps(env, indent=1), encoding="utf-8")
    print(json.dumps(env), flush=True)
    with open(args.out, "a", encoding="utf-8") as out:
        for seed in parse_seeds(args.seeds):
            for family in args.families.split(","):
                hist = args.histories / family / f"seed_{seed}"
                if not (hist / "meta.json").exists():
                    continue
                meta = json.loads((hist / "meta.json").read_text(encoding="utf-8"))
                for mode in args.modes.split(","):
                    for retention in map(int, args.retentions.split(",")):
                        if (family, seed, mode, retention) in done:
                            continue
                        run_id = f"{family}_{seed}_{mode}_{retention}"
                        work = work_root / run_id
                        try:
                            row = run_one(vacuum, args.label, hist, meta, mode, retention, work)
                        except Exception:
                            row = {"impl": args.label, "family": family, "seed": seed, "mode": mode,
                                   "retention_h": retention, "error": "harness: " + traceback.format_exc()[-800:]}
                        failed = bool(row.get("wrongly_deleted") or row.get("unreadable_protected") or row.get("error"))
                        if not failed and zlib.crc32(run_id.encode()) % args.keep_every:
                            shutil.rmtree(work, ignore_errors=True)
                        row["kept_work_dir"] = work.exists()
                        out.write(json.dumps(row) + "\n")
                        out.flush()
                        print(
                            f"{run_id} plan={row.get('plan_size')} wrong={len(row.get('wrongly_deleted', {}))} "
                            f"unreadable={row.get('unreadable_protected')} flagged={len(row.get('checker_flagged', []))} "
                            f"err={row.get('error', '')[:120]}",
                            flush=True,
                        )


if __name__ == "__main__":
    main()
