"""RQ3 ablation: which parts of the retained-reference invariant are necessary?

usage (any venv with pyarrow): python ablation.py HISTORIES_DIR WORK_DIR OUT_CSV

For every history and retention, the log of an aged copy is evaluated under the full invariant and
under weakened variants, and each protected set is compared with the oracle's needed set (union
of data and DV files referenced by protected versions). A variant is unsafe for a history when it
leaves a needed file unprotected; it over-protects when it protects a file no protected version
needs. Only the _delta_log is copied: neither the oracle's needed set nor the checker reads data."""
from __future__ import annotations

import csv
import json
import shutil
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "harness"))
sys.path.insert(0, str(HERE.parent / "checker"))

from aging import age_table  # noqa: E402
from oracle import referenced_files, replay  # noqa: E402
from vacuumcheck.check import _references, active_adds, protected_set  # noqa: E402
from vacuumcheck.log import snapshot_file_actions  # noqa: E402

HOUR_MS = 3_600_000


def variant_sets(table: Path, cutoff: int) -> dict[str, set[str]]:
    actions = snapshot_file_actions(table / "_delta_log")
    active = active_adds(actions)
    removes = [a for a in actions if a.kind == "remove"]
    unexpired = [a for a in removes if (a.deletion_timestamp or 0) >= cutoff]
    expired_paths = {a.path for a in removes if (a.deletion_timestamp or 0) < cutoff}

    def refs(acts, with_dv: bool) -> set[str]:
        out = set()
        for a in acts:
            for i, (p, _) in enumerate(_references(a, table, "active-data", "active-dv")):
                if i == 0 or with_dv:
                    out.add(p)
        return out

    active_data = refs(active, False)
    active_all = refs(active, True)
    tomb_data = refs(unexpired, False)
    released = {p for p in tomb_data if p not in expired_paths}
    return {
        "A: latest snapshot only (data + DV)": active_all,
        "B: no DV files (data of latest + unexpired tombstones)": active_data | tomb_data,
        "C: latest data + DV, unexpired tombstone data, no tombstone DVs": active_all | tomb_data,
        "D: C, but an expired tombstone for a path releases it": active_all | released,
        "E: D without DVs of the latest snapshot": active_data | released,
    }


def main() -> None:
    histories, work, out_csv = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    work.mkdir(parents=True, exist_ok=True)
    rows = []
    for meta_file in sorted(histories.glob("*/seed_*/meta.json")):
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        if meta["family"] == "minimal":
            continue
        for retention in (0, 24, 168):
            copy = work / "abl"
            shutil.rmtree(copy, ignore_errors=True)
            shutil.copytree(meta_file.parent / "table" / "_delta_log", copy / "_delta_log")
            now = int(time.time() * 1000)
            target = age_table(copy, [v["commit_ts"] for v in meta["versions"]], meta["offsets_hours"], now)
            cutoff = now - retention * HOUR_MS
            states = replay(copy)
            latest = states[-1].version
            protected_versions = [s.version for s in states if s.version == latest or target[s.version + 1] >= cutoff]
            needed = set().union(*(referenced_files(states[v], copy) for v in protected_versions))
            full, _, _ = protected_set(copy, retention, now)
            sets = {"retained-reference invariant (checker)": set(full), **variant_sets(copy, cutoff)}
            for name, prot in sets.items():
                rows.append(
                    {
                        "family": meta["family"],
                        "seed": meta["seed"],
                        "retention_h": retention,
                        "variant": name,
                        "needed": len(needed),
                        "protected": len(prot),
                        "missed": len(needed - prot),
                        "missed_dv": sum(1 for p in needed - prot if p.startswith("deletion_vector_") or "/deletion_vector_" in p),
                        "over_protected": len(prot - needed),
                    }
                )
    shutil.rmtree(work / "abl", ignore_errors=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} rows -> {out_csv}")


if __name__ == "__main__":
    main()
