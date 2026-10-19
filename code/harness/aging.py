"""Shift a freshly generated table history back in time so that its versions straddle a retention
boundary.

Version k is assigned the target time T_k = now - offset_hours[k]. Every original timestamp t
(commitInfo.timestamp, add.modificationTime, remove.deletionTimestamp, file mtimes) that falls in
(c_{k-1}, c_k], where c_k is the original commit time of version k, is mapped to T_k - (c_k - t).
Real operations take seconds, so every timestamp belonging to version k lands within seconds of T_k
and the mapping is monotone. Applied consistently to JSON commits, checkpoint parquet files,
_last_checkpoint and file mtimes."""
from __future__ import annotations

import bisect
import json
import os
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from oracle import LOG_DIR

TIME_FIELDS = {"commitInfo": ("timestamp", "inCommitTimestamp"), "add": ("modificationTime",), "remove": ("deletionTimestamp",)}


def make_mapping(orig_commit_ts: list[int], target_ts: list[int]) -> Callable[[int], int]:
    def f(t: int) -> int:
        k = min(bisect.bisect_left(orig_commit_ts, t), len(orig_commit_ts) - 1)
        return target_ts[k] - (orig_commit_ts[k] - t)

    return f


def _rewrite_json(path: Path, f: Callable[[int], int]) -> None:
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        action = json.loads(line)
        ((kind, body),) = action.items()
        for field in TIME_FIELDS.get(kind, ()):
            if body.get(field) is not None:
                body[field] = f(int(body[field]))
        lines.append(json.dumps(action, separators=(",", ":")))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _map_struct_field(arr: pa.ChunkedArray, field: str, f: Callable[[int], int]) -> pa.Array:
    struct = arr.combine_chunks()
    names = [struct.type.field(i).name for i in range(struct.type.num_fields)]
    children = [struct.field(i) for i in range(len(names))]
    idx = names.index(field)
    old = children[idx]
    children[idx] = pa.array([None if x is None else f(x) for x in old.to_pylist()], type=old.type)
    return pa.StructArray.from_arrays(children, fields=list(struct.type), mask=struct.is_null())


def _rewrite_checkpoint(path: Path, f: Callable[[int], int]) -> None:
    table = pq.read_table(path)
    for col, field in (("add", "modificationTime"), ("remove", "deletionTimestamp")):
        if col in table.column_names:
            i = table.column_names.index(col)
            table = table.set_column(i, table.schema.field(i), _map_struct_field(table.column(col), field, f))
    pq.write_table(table, path)


def age_table(table: Path, orig_commit_ts: list[int], offsets_hours: list[float], now_ms: int) -> list[int]:
    """Rewrite timestamps in place. Returns the target commit time of each version."""
    target = [now_ms - int(h * 3_600_000) for h in offsets_hours]
    f = make_mapping(orig_commit_ts, target)
    log = table / LOG_DIR
    for p in sorted(log.iterdir()):
        name = p.name
        if name.endswith(".json") and name[:20].isdigit():
            _rewrite_json(p, f)
            version = int(name[:20])
            os.utime(p, ns=(target[version] * 1_000_000, target[version] * 1_000_000))
        elif ".checkpoint" in name and name.endswith(".parquet"):
            _rewrite_checkpoint(p, f)
            version = int(name[:20])
            os.utime(p, ns=(target[version] * 1_000_000, target[version] * 1_000_000))
    last = log / "_last_checkpoint"
    if last.exists():
        info = json.loads(last.read_text(encoding="utf-8"))
        ckpt = log / f"{info['version']:020d}.checkpoint.parquet"
        if ckpt.exists():
            info["sizeInBytes"] = ckpt.stat().st_size
        info.pop("checksum", None)
        last.write_text(json.dumps(info, separators=(",", ":")), encoding="utf-8")
    for root, dirs, files in os.walk(table):
        if Path(root).name == LOG_DIR:
            continue
        for name in files:
            p = Path(root) / name
            new = f(int(p.stat().st_mtime * 1000))
            os.utime(p, ns=(new * 1_000_000, new * 1_000_000))
    return target
