"""Deletion vector descriptors: path resolution (Delta protocol, "Deletion Vector Descriptor Schema")
and decoding of the RoaringBitmapArray payload."""
from __future__ import annotations

import struct
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Mapping
from urllib.parse import unquote, urlparse

Z85_ALPHABET: Final = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
Z85_INDEX: Final = {c: i for i, c in enumerate(Z85_ALPHABET)}
DV_MAGIC: Final = 1681511377
ENCODED_UUID_LEN: Final = 20


def z85_decode(text: str) -> bytes:
    if len(text) % 5:
        raise ValueError(f"z85 input length {len(text)} is not a multiple of 5")
    chunks = (text[i : i + 5] for i in range(0, len(text), 5))
    out = bytearray()
    for chunk in chunks:
        value = 0
        for ch in chunk:
            value = value * 85 + Z85_INDEX[ch]
        out += value.to_bytes(4, "big")
    return bytes(out)


@dataclass(frozen=True, slots=True)
class DvDescriptor:
    storage_type: str
    path_or_inline_dv: str
    offset: int | None
    size_in_bytes: int
    cardinality: int

    @staticmethod
    def from_json(d: Mapping[str, object]) -> "DvDescriptor":
        off = d.get("offset")
        return DvDescriptor(
            storage_type=str(d["storageType"]),
            path_or_inline_dv=str(d["pathOrInlineDv"]),
            offset=None if off is None else int(off),
            size_in_bytes=int(d["sizeInBytes"]),
            cardinality=int(d["cardinality"]),
        )

    @property
    def unique_id(self) -> str:
        base = self.storage_type + self.path_or_inline_dv
        return base if self.offset is None else f"{base}@{self.offset}"

    def relative_file(self) -> str | None:
        """Table-relative path (forward slashes) of the file holding this DV, None for inline DVs.

        Absolute ('p') paths that point outside the table root are returned as an absolute URI string."""
        if self.storage_type == "i":
            return None
        if self.storage_type == "u":
            prefix_len = len(self.path_or_inline_dv) - ENCODED_UUID_LEN
            prefix = self.path_or_inline_dv[:prefix_len]
            uid = uuid.UUID(bytes=z85_decode(self.path_or_inline_dv[prefix_len:]))
            name = f"deletion_vector_{uid}.bin"
            return f"{prefix}/{name}" if prefix else name
        if self.storage_type == "p":
            return self.path_or_inline_dv
        raise ValueError(f"unknown deletion vector storage type {self.storage_type!r}")


def table_relative(path_or_uri: str, table_root: Path) -> str:
    """Normalise an add/remove path or absolute URI to a table-relative forward-slash path."""
    if "://" in path_or_uri or path_or_uri.startswith("file:"):
        try:
            return local_path(path_or_uri).resolve().relative_to(table_root.resolve()).as_posix()
        except ValueError:
            return path_or_uri
    return unquote(path_or_uri).replace("\\", "/")


def _parse_roaring32(buf: bytes, pos: int) -> tuple[list[int], int]:
    cookie = struct.unpack_from("<I", buf, pos)[0]
    pos += 4
    if cookie & 0xFFFF == 12347:
        size = (cookie >> 16) + 1
        run_flags = buf[pos : pos + (size + 7) // 8]
        pos += (size + 7) // 8
        has_offsets = size >= 4
    elif cookie == 12346:
        size = struct.unpack_from("<I", buf, pos)[0]
        pos += 4
        run_flags = bytes((size + 7) // 8)
        has_offsets = True
    else:
        raise ValueError(f"bad roaring cookie {cookie}")
    header = [struct.unpack_from("<HH", buf, pos + 4 * i) for i in range(size)]
    pos += 4 * size
    if has_offsets:
        pos += 4 * size
    values: list[int] = []
    for i, (key, card_minus_one) in enumerate(header):
        high = key << 16
        card = card_minus_one + 1
        if run_flags[i // 8] >> (i % 8) & 1:
            n_runs = struct.unpack_from("<H", buf, pos)[0]
            pos += 2
            for r in range(n_runs):
                start, length = struct.unpack_from("<HH", buf, pos + 4 * r)
                values.extend(high | v for v in range(start, start + length + 1))
            pos += 4 * n_runs
        elif card > 4096:
            words = struct.unpack_from("<1024Q", buf, pos)
            pos += 8192
            values.extend(high | (w * 64 + b) for w, word in enumerate(words) for b in range(64) if word >> b & 1)
        else:
            values.extend(high | v for v in struct.unpack_from(f"<{card}H", buf, pos))
            pos += 2 * card
    return values, pos


def parse_bitmap_array(payload: bytes) -> frozenset[int]:
    """Decode a serialized RoaringBitmapArray (magic + portable 64-bit roaring)."""
    magic = struct.unpack_from("<i", payload, 0)[0]
    if magic != DV_MAGIC:
        raise ValueError(f"bad deletion vector magic {magic}")
    count = struct.unpack_from("<Q", payload, 4)[0]
    pos = 12
    rows: list[int] = []
    for _ in range(count):
        high = struct.unpack_from("<I", payload, pos)[0]
        values, pos = _parse_roaring32(payload, pos + 4)
        rows.extend((high << 32) | v for v in values)
    return frozenset(rows)


def local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme not in ("", "file"):
        raise ValueError(f"only local file URIs are supported, got {uri}")
    local = unquote(parsed.path)
    if len(local) > 2 and local[0] == "/" and local[2] == ":":
        local = local[1:]
    return Path(local)


def dv_file_path(dv: DvDescriptor, table_root: Path) -> Path | None:
    rel = dv.relative_file()
    if rel is None:
        return None
    return local_path(rel) if dv.storage_type == "p" else table_root / rel


def read_deleted_rows(dv: DvDescriptor, table_root: Path) -> frozenset[int]:
    target = dv_file_path(dv, table_root)
    if target is None:
        return parse_bitmap_array(z85_decode(dv.path_or_inline_dv))
    with open(target, "rb") as fh:
        fh.seek(dv.offset or 0)
        size = struct.unpack(">i", fh.read(4))[0]
        payload = fh.read(size)
    if len(payload) != size:
        raise ValueError(f"truncated deletion vector in {target}")
    rows = parse_bitmap_array(payload)
    if len(rows) != dv.cardinality:
        raise ValueError(f"deletion vector cardinality {len(rows)} != descriptor {dv.cardinality}")
    return rows
