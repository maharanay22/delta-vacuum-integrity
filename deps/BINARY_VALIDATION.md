> Historical staging build/provenance reference. This candidate contains no compiled runtime binaries; see docs/ENVIRONMENT.md and deps/PATCH_VERIFICATION.json.

# Experimental Binary Validation

## Environment

Validation performed on Windows x86-64 using:

- Python 3.12.10
- PyArrow 26.0.0
- Locally compiled delta-rs experimental wheels
- Separate fresh Python virtual environments

## Experimental builds

| Build | Installation | Import | Delta table write/read |
|---|---|---|---|
| delta-rs main (`640dc893`) | PASS | PASS | PASS |
| delta-rs main + locally rebased PR #4858 (`407201ca`) | PASS | PASS | PASS |

## Functional smoke test

For each experimental build:

1. Created a disposable local Delta table.
2. Wrote three integer rows using `write_deltalake`.
3. Loaded the table using `DeltaTable`.
4. Read the rows using `to_pyarrow_table`.
5. Verified that the returned values were `[1, 2, 3]`.

Both builds completed the smoke test successfully.

## Binary integrity

The wheels were previously checked against their original SHA-256 hashes, recorded in `deps/wheels.sha256`.

The PR #4858 experimental source patch was verified to apply cleanly to the recorded main commit.

## Limitations

These tests establish installation, import, and basic Delta table write/read functionality only.

They do not establish:

- Reproducibility of the original 3,200-run experiment matrix.
- Correctness of vacuum deletion-vector retention behavior.
- Byte-for-byte reproducibility of newly compiled wheels.
- Cross-platform compatibility or production safety.

The original experiment results and supporting source code are provided separately in this repository.
