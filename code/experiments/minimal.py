"""Hand-written minimal histories, one per failure class (run with the Spark venv).

usage: python minimal.py HISTORIES_DIR

They are stored as family "minimal" (seed = id below) and run through harness/runner.py exactly like
the generated histories. Ages are explicit: offsets_hours[v] is how long ago version v was committed.

  1  [write 1 file, DELETE]                  -> active DV; full vacuum must keep it (F1, #4839)
  2  [write, DELETE, DELETE]                 -> X keeps a newer DV; the older DV is needed by v1 (F4)
  3  [write, DELETE, DELETE, REORG PURGE]    -> X has an expired and an unexpired tombstone (F3 and F4)"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "harness"))

from generate import generate  # noqa: E402
from sparkenv import spark_session  # noqa: E402

DELETE_A = {"op": "delete_mod", "m": 5, "r": 0}
DELETE_B = {"op": "delete_mod", "m": 5, "r": 1}
PURGE = {"op": "purge", "pick": 0.0}

CASES = {
    1: ([DELETE_A], [0, 10]),
    2: ([DELETE_A, DELETE_B], [0, 198, 200]),
    3: ([DELETE_A, DELETE_B, PURGE], [0, 100, 100, 98]),
}


def main() -> None:
    out = Path(sys.argv[1]).resolve()
    spark = spark_session(out / "_spark_wh_minimal")
    for seed, (ops, gaps) in CASES.items():
        if (out / "minimal" / f"seed_{seed}" / "meta.json").exists():
            continue
        plan = {"seed": seed, "initial_rows": 20, "initial_files": 1, "ops": ops, "checkpoint_after_op": [], "gaps_hours": gaps}
        meta = generate(spark, out, "minimal", seed, plan)
        print(seed, [v["operation"] for v in meta["versions"]], meta["offsets_hours"], flush=True)


if __name__ == "__main__":
    main()
