"""python -m vacuumcheck TABLE --retention-hours H [--plan FILE] [PATH ...]

Exits 1 when the vacuum plan would delete a file protected by the retained-reference invariant."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .check import check_plan


def main() -> int:
    ap = argparse.ArgumentParser(prog="vacuumcheck")
    ap.add_argument("table", type=Path)
    ap.add_argument("--retention-hours", type=float, required=True)
    ap.add_argument("--plan", type=Path, help="file with one path per line, e.g. vacuum dry-run output")
    ap.add_argument("--now-ms", type=int)
    ap.add_argument("paths", nargs="*")
    args = ap.parse_args()
    from_file = args.plan.read_text(encoding="utf-8").splitlines() if args.plan else []
    plan = [p.strip() for p in [*args.paths, *from_file] if p.strip()]
    result = check_plan(args.table, plan, args.retention_hours, args.now_ms)
    report = {
        "safe": result.safe,
        "cutoff_ms": result.cutoff_ms,
        "protected_files": result.protected_files,
        "unsafe": [{"path": p, "reason": why.reason, "version": why.version} for p, why in result.unsafe],
    }
    print(json.dumps(report, indent=2))
    return 0 if result.safe else 1


if __name__ == "__main__":
    sys.exit(main())
