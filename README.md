# Delta Lake VACUUM Integrity Research Artifact

Independently developed research by Maha Rana Yadavalli on deletion vectors, VACUUM and retained snapshot readability. **Proposed public repository â€” awaiting explicit owner approval.** No release or DOI is asserted.

The original matrix contains 3,200 runs, 200 paired checkpoint/non-checkpoint histories, 100 seed numbers and 2,416 versions. Path-redacted original outcome views are preserved separately from regenerated examples and later supplemental experiments. Exact original bytes remain in the private review archive, excluded from public upload.

| Failure class | Direct exposure at 0 h: runs / seeds | Direct exposure at 168 h: runs / seeds |
|---|---:|---:|
| F1 active DV | 720 / 90 | 0 / 0 |
| F3 repeated tombstone data path | 0 / 0 | 608 / 38 |
| F4 tombstone-only DV | 0 / 0 | 464 / 58 |

F1/F4 direct exposure uses FULL; F3 includes both modes. Broader latent exposure and all configuration denominators are in the Pass10 CSVs. The oldest original live DV was 146.50043028 hours old: original zero F1 failures at 168 h therefore do not demonstrate a fix.

A supplemental deliberately aged F1/168 table reproduces active-DV deletion. This revision reruns that fixture with clean venv installations: the hash-matched pre-fix native build deletes its active DV, while the patched build preserves it. Spark and the independent reader agree. Restoring the exact DV recovers the expected 16-row hash; removing it reproduces failed reads. These are new supplemental fixture-copy experiments, not original matrix runs or a fresh Rust build. Failed reads have null row/hash results and are never counted as successful empty results.

Original A1 and A3 code bodies were recovered from command history, with raw excerpts and normalization provenance. New A1 output agrees with the saved original result. A3 confirms 2,696 error-free comparisons with zero plan/execution differences, and separately 504 error runs; 367 of the error runs differ. Original stdout and command timestamps were not recovered.

See [reproduction instructions](docs/REPRODUCTION.md), [actual clean-install scope](docs/CLEAN_ENVIRONMENT_REPRODUCTION.md), [readiness report](docs/PUBLICATION_READINESS.md), [remaining limits](docs/REMAINING_BLOCKERS.md), [licensing scope](LICENSING.md), and [ownership audit](docs/OWNERSHIP_AND_LICENSE_AUDIT.md).

Layout: `code/` holds generator, checker, oracle, harness, analyses and original outcome views; `evidence/pass10/` holds the replay snapshot and exposure outputs; `evidence/f1_168/` and `evidence/causal_restoration/` preserve earlier supplemental observations; `evidence/publication_revision/` contains this revision's new results/logs/audits; `evidence/recovered_original_tests/` contains recovered original text; `evidence/regenerated_seed1/` remains explicitly regenerated. `deps/` records independently obtained dependencies; no runtime binaries or environments are shipped. `ARTIFACT_MANIFEST.json` hashes the final tree. `ARTIFACT_LINEAGE.json` and `REVISION_LINEAGE.json` distinguish inputs and revisions.

Proposed licenses are Apache-2.0 for authored software/documentation and CC BY 4.0 for author-owned data/results/figures, with upstream exceptions in `LICENSING.md` and `NOTICE`. The 52 upstream checker fixture files and delta-rs patch retain Apache-2.0. No manuscript is included. `CITATION.cff` contains retained author/title metadata without invented identifiers.

All tests use small synthetic, single-writer local Windows data. Same-host clean installs passed; fresh native compilation, independent-machine execution, cloud storage and concurrency generalization remain unverified. No full matrix rerun, original-table VACUUM, system-clock change, push, publication or DOI creation occurred.
