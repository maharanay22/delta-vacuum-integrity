import json
import shutil
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from vacuumcheck import DvDescriptor, check_plan, read_deleted_rows, z85_decode
from vacuumcheck.check import active_adds
from vacuumcheck.log import snapshot_file_actions

FIXTURES = Path(__file__).parent / "fixtures"
HOUR_MS = 3_600_000


def test_z85_matches_zeromq_spec_vector():
    # ZeroMQ RFC 32 (https://rfc.zeromq.org/spec/32/) test vector.
    assert z85_decode("HelloWorld") == bytes([0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B])


def test_relative_dv_with_random_prefix_matches_protocol_example():
    # Delta PROTOCOL.md, "Deletion Vector Descriptor Schema" example, checked 2026-10-13.
    dv = DvDescriptor("u", "ab^-aqEH.-t@S}K{vb[*k^", 4, 40, 6)
    assert dv.relative_file() == "ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"


def test_inline_and_absolute_descriptors():
    assert DvDescriptor("i", "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L", None, 40, 6).relative_file() is None
    absolute = "s3://bucket/table/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin"
    assert DvDescriptor("p", absolute, 4, 40, 6).relative_file() == absolute


def test_fixture_dv_resolves_to_existing_file_and_deletes_the_deleted_values():
    table = FIXTURES / "table-with-dv-small"
    (add,) = active_adds(snapshot_file_actions(table / "_delta_log"))
    assert add.dv is not None
    assert (table / add.dv.relative_file()).is_file()
    rows = read_deleted_rows(add.dv, table)
    values = pq.read_table(table / add.path).column("value").to_pylist()
    assert sorted(values[i] for i in rows) == [0, 9]
    assert len(rows) == add.dv.cardinality


def test_every_active_dv_of_checkpointed_fixture_resolves_to_existing_file():
    table = FIXTURES / "table_with_deletion_logs"
    adds = active_adds(snapshot_file_actions(table / "_delta_log"))
    dvs = [a.dv for a in adds if a.dv is not None]
    assert dvs
    assert all((table / dv.relative_file()).is_file() for dv in dvs)
    assert all(len(read_deleted_rows(dv, table)) == dv.cardinality for dv in dvs)


def _encode_uuid(u: uuid.UUID) -> str:
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
    out = ""
    for i in range(0, 16, 4):
        value = int.from_bytes(u.bytes[i : i + 4], "big")
        digits = []
        for _ in range(5):
            value, rem = divmod(value, 85)
            digits.append(alphabet[rem])
        out += "".join(reversed(digits))
    return out


def _dv(u: uuid.UUID) -> dict:
    return {"storageType": "u", "pathOrInlineDv": _encode_uuid(u), "offset": 1, "sizeInBytes": 10, "cardinality": 1}


def _write_log(table: Path, commits: list[list[dict]]) -> None:
    log = table / "_delta_log"
    log.mkdir(parents=True)
    for version, actions in enumerate(commits):
        (log / f"{version:020d}.json").write_text("\n".join(json.dumps(a) for a in actions) + "\n")


@pytest.fixture
def dv_history(tmp_path: Path) -> tuple[Path, int, uuid.UUID, uuid.UUID]:
    """v0 adds X; v1 deletes rows (remove X, add X+dv1) long ago; v2 deletes again (remove X+dv1,
    add X+dv2) one hour ago. X has an expired and an unexpired tombstone."""
    now = 1_800_000_000_000
    dv1, dv2 = uuid.uuid4(), uuid.uuid4()
    x = {"path": "x.parquet", "size": 1, "partitionValues": {}, "modificationTime": 0, "dataChange": True}
    _write_log(
        tmp_path,
        [
            [{"add": x}],
            [{"remove": {"path": "x.parquet", "deletionTimestamp": now - 500 * HOUR_MS}}, {"add": {**x, "deletionVector": _dv(dv1)}}],
            [
                {"remove": {"path": "x.parquet", "deletionTimestamp": now - HOUR_MS, "deletionVector": _dv(dv1)}},
                {"add": {**x, "deletionVector": _dv(dv2)}},
            ],
        ],
    )
    return tmp_path, now, dv1, dv2


def test_active_data_and_dv_files_are_unsafe(dv_history):
    table, now, _, dv2 = dv_history
    result = check_plan(table, ["x.parquet", f"deletion_vector_{dv2}.bin"], 0, now)
    assert {p: why.reason for p, why in result.unsafe} == {
        "x.parquet": "active-data",
        f"deletion_vector_{dv2}.bin": "active-dv",
    }


def test_dv_of_unexpired_tombstone_is_unsafe_even_if_older_tombstone_expired(dv_history):
    table, now, dv1, _ = dv_history
    result = check_plan(table, [f"deletion_vector_{dv1}.bin"], 168, now)
    assert [(p, why.reason, why.version) for p, why in result.unsafe] == [(f"deletion_vector_{dv1}.bin", "tombstone-dv", 2)]


def test_dv_of_expired_tombstone_is_safe(dv_history):
    table, now, dv1, _ = dv_history
    assert check_plan(table, [f"deletion_vector_{dv1}.bin"], 0.5, now).safe


def test_absolute_uri_plan_entries_are_normalised(dv_history):
    table, now, _, dv2 = dv_history
    uri = (table / f"deletion_vector_{dv2}.bin").resolve().as_uri()
    assert not check_plan(table, [uri], 0, now).safe


def test_log_files_in_plan_are_unsafe(dv_history):
    table, now, _, _ = dv_history
    assert not check_plan(table, ["_delta_log/00000000000000000000.json"], 0, now).safe


def test_checkpoint_is_used_instead_of_older_commits(tmp_path: Path):
    shutil.copytree(FIXTURES / "table_with_deletion_logs", tmp_path / "t")
    table = tmp_path / "t"
    for p in (table / "_delta_log").glob("0000000000000000000[0-9].json"):
        p.unlink()
    adds = active_adds(snapshot_file_actions(table / "_delta_log"))
    original = active_adds(snapshot_file_actions(FIXTURES / "table_with_deletion_logs" / "_delta_log"))
    assert {(a.path, a.dv) for a in adds} == {(a.path, a.dv) for a in original}
