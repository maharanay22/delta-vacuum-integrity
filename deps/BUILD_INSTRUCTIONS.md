> Historical staging build/provenance reference. This candidate contains no compiled runtime binaries; see docs/ENVIRONMENT.md and deps/PATCH_VERIFICATION.json.

# Delta Lake Experimental Build Instructions

## Purpose

This document records the environment and source revisions used to construct the locally compiled Delta Lake wheels evaluated in the vacuum integrity study.

## Verified local build tools

| Tool | Version |
|---|---|
| Python | 3.12.10 |
| uv | 0.12.23 |
| Rust | 1.98.0 |
| Cargo | 1.98.0 |
| GCC (WinLibs MinGW-w64) | 16.2.0 |
| Maturin | 1.15.0 |

Platform: Windows x86-64, GNU Rust target.

These versions were verified on the original research machine. They do not establish byte-for-byte reproducibility of the compiled wheels.

## Source revisions

- Main: `640dc8937cd6d5c49c16fbf203d72083d86c5b21`
- Main plus locally rebased PR #4858: `407201ca5bb795d85e7f4b0e35a688a0a9477bdc`

The second revision contains the main revision as an ancestor.

## Source reconstruction

Clone the upstream delta-rs repository:

```bash
git clone https://github.com/delta-io/delta-rs.git
cd delta-rs
git checkout 640dc8937cd6d5c49c16fbf203d72083d86c5b21
```

To reconstruct the locally rebased experimental revision, apply `deps/pr4858-rebased.patch` from this research repository. Replace the research-checkout prefix in the example with its actual location:

```bash
git apply /path/to/delta-vacuum-integrity/deps/pr4858-rebased.patch
```

The patch was verified to apply cleanly against the specified main commit.

## Build command

Run from the `python` subdirectory of the delta-rs checkout after configuring Rust, GCC, Python, and Maturin:

```bash
maturin build --release --target x86_64-pc-windows-gnu -i /path/to/python3.12 -o /path/to/output
```

Use Rust toolchain `1.98.0-x86_64-pc-windows-gnu` and the WinLibs GCC linker.

The original build used custom `RUSTUP_HOME`, `CARGO_HOME`, `CARGO_TARGET_DIR`, and linker environment variables.

## Verification

Compare the resulting wheel against the reference hashes in `deps/wheels.sha256`.

A different binary hash does not necessarily indicate different runtime behavior, because build metadata and toolchain details can affect binary reproducibility.

## Limitations

- The experimental PR revision is locally rebased and is not represented here as an official delta-rs release.
- Fresh rebuilding has not yet been performed using these instructions.
- Byte-for-byte reproducible builds have not been established.
- The original wheels are Windows x86-64 binaries.
- The original study used local NTFS storage and synthetic workloads.
- The binary release must preserve applicable license and third-party notices.
