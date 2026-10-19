"""Retained-reference invariant.

A file may be deleted by vacuum only if no add action of the latest snapshot and no remove action
(tombstone) with deletionTimestamp >= now - retention references it, either as a data file or as a
deletion vector file. Every action for a path counts, so an expired tombstone never overrides a
newer, unexpired one for the same path."""
from __future__ import annotations

import functools
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Literal

from .dv import DvDescriptor, dv_file_path, table_relative
from .log import FileAction, snapshot_file_actions

Reason = Literal["active-data", "active-dv", "tombstone-data", "tombstone-dv", "delta-log"]


@dataclass(frozen=True, slots=True)
class Protection:
    reason: Reason
    version: int


@dataclass(frozen=True, slots=True)
class CheckResult:
    unsafe: tuple[tuple[str, Protection], ...]
    protected_files: int
    actions_read: int
    cutoff_ms: int

    @property
    def safe(self) -> bool:
        return not self.unsafe


def _rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _references(action: FileAction, root: Path, data: Reason, dv: Reason) -> Iterator[tuple[str, Protection]]:
    yield table_relative(action.path, root), Protection(data, action.version)
    if action.dv is None or action.dv.storage_type == "i":
        return
    if action.dv.storage_type == "u":
        yield _relative_dv(action.dv.path_or_inline_dv), Protection(dv, action.version)
        return
    dv_path = dv_file_path(action.dv, root)
    if dv_path is not None:
        yield _rel(dv_path, root), Protection(dv, action.version)


@functools.lru_cache(maxsize=65536)
def _relative_dv(path_or_inline_dv: str) -> str:
    rel = DvDescriptor("u", path_or_inline_dv, None, 0, 0).relative_file()
    assert rel is not None
    return rel


def active_adds(actions: Iterable[FileAction]) -> tuple[FileAction, ...]:
    live: dict[tuple[str, str | None], FileAction] = {}
    for a in actions:
        key = (a.path, a.dv.unique_id if a.dv else None)
        if a.kind == "add":
            live[key] = a
        else:
            live.pop(key, None)
    return tuple(live.values())


def protected_set(
    table_root: Path, retention_hours: float, now_ms: int | None = None
) -> tuple[dict[str, Protection], int, int]:
    now = int(time.time() * 1000) if now_ms is None else now_ms
    cutoff = now - int(retention_hours * 3600 * 1000)
    actions = snapshot_file_actions(table_root / "_delta_log")
    protected: dict[str, Protection] = {}
    for add in active_adds(actions):
        protected.update(_references(add, table_root, "active-data", "active-dv"))
    for rm in actions:
        if rm.kind == "remove" and (rm.deletion_timestamp or 0) >= cutoff:
            for path, why in _references(rm, table_root, "tombstone-data", "tombstone-dv"):
                protected.setdefault(path, why)
    return protected, len(actions), cutoff


def check_plan(
    table_root: Path, plan: Iterable[str], retention_hours: float, now_ms: int | None = None
) -> CheckResult:
    protected, n_actions, cutoff = protected_set(table_root, retention_hours, now_ms)
    rel_plan = sorted({table_relative(p, table_root) for p in plan})
    log_hits = tuple((p, Protection("delta-log", -1)) for p in rel_plan if p.startswith("_delta_log/"))
    ref_hits = tuple((p, protected[p]) for p in rel_plan if p in protected)
    return CheckResult(
        unsafe=log_hits + ref_hits, protected_files=len(protected), actions_read=n_actions, cutoff_ms=cutoff
    )
