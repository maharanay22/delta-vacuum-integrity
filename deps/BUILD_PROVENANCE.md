> Historical staging build/provenance reference. This candidate contains no compiled runtime binaries; see docs/ENVIRONMENT.md and deps/PATCH_VERIFICATION.json.

# Experimental Delta Lake Build Provenance

## Source revisions

| Experimental build | Git commit |
|---|---|
| delta-rs main | `640dc8937cd6d5c49c16fbf203d72083d86c5b21` |
| delta-rs main + locally rebased PR #4858 | `407201ca5bb795d85e7f4b0e35a688a0a9477bdc` |

The locally rebased commit has the message `fix: keep files of unexpired tombstones during vacuum`.

A local `git merge-base --is-ancestor` check confirmed that commit `640dc8937cd6d5c49c16fbf203d72083d86c5b21` is an ancestor of commit `407201ca5bb795d85e7f4b0e35a688a0a9477bdc`.

## Original build environment

- Operating system: Windows
- Python: 3.12
- Rust toolchain: `1.98.0-x86_64-pc-windows-gnu`
- Linker: WinLibs GCC
- Build tool: maturin
- Build mode: release
- Target: `x86_64-pc-windows-gnu`

The original build used machine-specific Rust, Cargo, GCC, Python, and output paths. These paths are not required to identify the source revisions.

## Binary artifact verification

See `deps/wheels.sha256` for the three recorded wheel checksums.

See `deps/jars.sha256` for the eleven Spark JAR checksums, all of which matched the files in the original local research environment.

## Reproducibility limitations

The original experimental wheels have been located and hashed, but byte-for-byte reproducible rebuilding has not been established.

The locally rebased PR commit is an experimental source revision and must not be represented as an official upstream release without separate verification.

The inexact build's exact source revision remains to be documented.

Binary redistribution rights and availability of the required build dependencies must be reviewed before public artifact release.
