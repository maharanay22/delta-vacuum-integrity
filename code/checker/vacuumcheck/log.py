"""Minimal Delta log reader: latest checkpoint (classic, single or multi-part) plus later JSON commits."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Iterator, Literal

import pyarrow.parquet as pq

from .dv import DvDescriptor

COMMIT_RE: Final = re.compile(r"^(\d{20})\.json$")
CHECKPOINT_RE: Final = re.compile(r"^(\d{20})\.checkpoint(?:\.\d{10}\.\d{10})?\.parquet$")


@dataclass(frozen=True, slots=True)
class FileAction:
    kind: Literal["add", "remove"]
    path: str
    dv: DvDescriptor | None
    deletion_timestamp: int | None
    version: int


def commit_versions(log_dir: Path) -> tuple[int, ...]:
    matches = (COMMIT_RE.match(p.name) for p in log_dir.iterdir())
    return tuple(sorted(int(m.group(1)) for m in matches if m))


def latest_checkpoint(log_dir: Path) -> tuple[int, tuple[Path, ...]] | None:
    parts: dict[int, list[Path]] = {}
    for p in log_dir.iterdir():
        m = CHECKPOINT_RE.match(p.name)
        if m:
            parts.setdefault(int(m.group(1)), []).append(p)
    if not parts:
        return None
    version = max(parts)
    return version, tuple(sorted(parts[version]))


def _from_dict(kind: Literal["add", "remove"], d: dict, version: int) -> FileAction:
    dv = d.get("deletionVector")
    ts = d.get("deletionTimestamp") if kind == "remove" else None
    return FileAction(
        kind=kind,
        path=d["path"],
        dv=DvDescriptor.from_json(dv) if dv else None,
        deletion_timestamp=None if ts is None else int(ts),
        version=version,
    )


def commit_actions(log_dir: Path, version: int) -> Iterator[tuple[str, dict]]:
    with open(log_dir / f"{version:020d}.json", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                ((kind, body),) = json.loads(line).items()
                yield kind, body


def commit_file_actions(log_dir: Path, version: int) -> Iterator[FileAction]:
    for kind, body in commit_actions(log_dir, version):
        if kind in ("add", "remove"):
            yield _from_dict(kind, body, version)


def checkpoint_file_actions(paths: tuple[Path, ...], version: int) -> Iterator[FileAction]:
    for part in paths:
        names = set(pq.read_schema(part).names)
        columns = [c for c in ("add", "remove") if c in names]
        table = pq.read_table(part, columns=columns)
        for kind in columns:
            for row in table.column(kind).to_pylist():
                if row is not None:
                    yield _from_dict(kind, row, version)


def snapshot_file_actions(log_dir: Path) -> tuple[FileAction, ...]:
    """Add/remove actions visible to a reader of the latest version: the latest checkpoint (if any)
    followed by every later commit."""
    ckpt = latest_checkpoint(log_dir)
    start = 0 if ckpt is None else ckpt[0] + 1
    from_ckpt = () if ckpt is None else tuple(checkpoint_file_actions(ckpt[1], ckpt[0]))
    later_versions = tuple(v for v in commit_versions(log_dir) if v >= start)
    later = tuple(a for v in later_versions for a in commit_file_actions(log_dir, v))
    return from_ckpt + later
