# Staged revision: preserve full independent-reader exception chains; algorithm unchanged.
"""Ground-truth oracle, independent of any Delta implementation under test.

Replays the JSON commits to reconstruct the file set of every version, reads a version with a
small pure-Python reader (pyarrow + the DV decoder from vacuumcheck.dv) and hashes its rows."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "checker"))

from vacuumcheck.dv import DvDescriptor, dv_file_path, read_deleted_rows, table_relative  # noqa: E402
from vacuumcheck.log import commit_actions, commit_versions  # noqa: E402

LOG_DIR = "_delta_log"


@dataclass(frozen=True, slots=True)
class AddRef:
    path: str
    dv: DvDescriptor | None


@dataclass(frozen=True, slots=True)
class VersionState:
    version: int
    commit_ts: int
    operation: str
    adds: tuple[AddRef, ...]


def replay(table: Path) -> tuple[VersionState, ...]:
    log = table / LOG_DIR
    live: dict[tuple[str, str | None], AddRef] = {}
    states = []
    for v in commit_versions(log):
        ts, op = 0, ""
        for kind, body in commit_actions(log, v):
            if kind == "commitInfo":
                ts = int(body.get("inCommitTimestamp") or body.get("timestamp") or 0)
                op = str(body.get("operation", ""))
            elif kind in ("add", "remove"):
                raw_dv = body.get("deletionVector")
                dv = DvDescriptor.from_json(raw_dv) if raw_dv else None
                key = (body["path"], dv.unique_id if dv else None)
                if kind == "add":
                    live[key] = AddRef(body["path"], dv)
                else:
                    live.pop(key, None)
        states.append(VersionState(v, ts, op, tuple(live.values())))
    return tuple(states)


def referenced_files(state: VersionState, table: Path) -> frozenset[str]:
    data = (table_relative(a.path, table) for a in state.adds)
    dv_paths = (dv_file_path(a.dv, table) for a in state.adds if a.dv is not None)
    dvs = (p.resolve().relative_to(table.resolve()).as_posix() for p in dv_paths if p is not None)
    return frozenset((*data, *dvs))


def all_logged_files(table: Path) -> tuple[frozenset[str], frozenset[str]]:
    """(data files, DV files) referenced by any add/remove action anywhere in the JSON log."""
    log = table / LOG_DIR
    data: set[str] = set()
    dvs: set[str] = set()
    for v in commit_versions(log):
        for kind, body in commit_actions(log, v):
            if kind not in ("add", "remove"):
                continue
            data.add(table_relative(body["path"], table))
            raw_dv = body.get("deletionVector")
            target = dv_file_path(DvDescriptor.from_json(raw_dv), table) if raw_dv else None
            if target is not None:
                dvs.add(target.resolve().relative_to(table.resolve()).as_posix())
    return frozenset(data), frozenset(dvs)


def canonical_hash(rows: Iterable[tuple]) -> str:
    ordered = sorted(rows, key=lambda r: tuple((x is None, x) for x in r))
    return hashlib.sha256(json.dumps(ordered).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class ReadResult:
    ok: bool
    rows: int
    digest: str
    error: str


def read_state(state: VersionState, table: Path) -> ReadResult:
    try:
        rows: list[tuple] = []
        for add in state.adds:
            t = pq.read_table(table / table_relative(add.path, table))
            cols = [c.to_pylist() for c in t.columns]
            deleted = read_deleted_rows(add.dv, table) if add.dv is not None else frozenset()
            rows.extend(r for i, r in enumerate(zip(*cols)) if i not in deleted)
        return ReadResult(True, len(rows), canonical_hash(rows), "")
    except Exception as exc:  # any failure to read a version makes it unreadable
        return ReadResult(False, 0, "", __import__("traceback").format_exc())


def inventory(table: Path) -> dict[str, float]:
    """Visible files under the table root (relative path -> mtime), hidden files excluded."""
    out = {}
    for root, dirs, files in os.walk(table):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for f in files:
            if f.startswith("."):
                continue
            full = Path(root) / f
            try:
                out[full.relative_to(table).as_posix()] = full.stat().st_mtime
            except FileNotFoundError:
                continue
    return out


def classify(path: str, data_files: frozenset[str], dv_files: frozenset[str]) -> str:
    if path.startswith(LOG_DIR + "/"):
        return "log"
    if path in dv_files:
        return "dv"
    if path in data_files:
        return "data"
    return "orphan"


def references(table: Path, upto: int) -> dict[str, list[dict]]:
    """For every file: each add/remove action (version <= upto) that references it, as data or DV."""
    log = table / LOG_DIR
    refs: dict[str, list[dict]] = {}
    for v in commit_versions(log):
        if v > upto:
            break
        for kind, body in commit_actions(log, v):
            if kind not in ("add", "remove"):
                continue
            ts = body.get("deletionTimestamp")
            refs.setdefault(table_relative(body["path"], table), []).append({"kind": kind, "role": "data", "v": v, "ts": ts})
            raw_dv = body.get("deletionVector")
            target = dv_file_path(DvDescriptor.from_json(raw_dv), table) if raw_dv else None
            if target is not None:
                rel = target.resolve().relative_to(table.resolve()).as_posix()
                refs.setdefault(rel, []).append({"kind": kind, "role": "dv", "v": v, "ts": ts})
    return refs
