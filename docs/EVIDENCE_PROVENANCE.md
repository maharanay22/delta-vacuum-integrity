> Historical prior-review context. Current results and superseding conclusions are in PUBLICATION_READINESS.md, CLEAN_ENVIRONMENT_REPRODUCTION.md and A1_A3_RECOVERY.md. Earlier missing-test/skipped-fixture/engine-not-run statements below describe the prior candidate only.

> Historical prior-review context. Current results and superseding conclusions are in PUBLICATION_READINESS.md, CLEAN_ENVIRONMENT_REPRODUCTION.md and A1_A3_RECOVERY.md. Earlier missing-test/skipped-fixture/engine-not-run statements below describe the prior candidate only.

> Historical prior-review context. Current results and superseding conclusions are in PUBLICATION_READINESS.md, CLEAN_ENVIRONMENT_REPRODUCTION.md and A1_A3_RECOVERY.md. Earlier missing-test/skipped-fixture/engine-not-run statements below describe the prior candidate only.

# Evidence provenance and presentation transformations

The new source tree was prepared from the original research directory after inspecting the existing private/review/source-only staging folders. It does not import their `.git` histories or change existing repositories. Current original research HEAD and existing repository inventories are recorded in the separate private audit.

`ARTIFACT_LINEAGE.json` maps candidate files to original input hashes and evidence categories. `MODIFICATIONS.json` records final portability, safety, fixture-exclusion and path-redaction changes. `ARTIFACT_MANIFEST.json` hashes final candidate bytes. These manifests have different scopes: an original hash must not be used to validate a redacted or edited copy.

Original matrix outcome quantities, seed IDs, mode/retention labels, relative file names, candidate/deletion sets, logical row counts and digests are preserved. Local home-directory prefixes in logs, CSV/JSON metadata and paths are replaced by `<LOCAL_HOME>` in public presentation copies; original source SHA-256 identifiers remain unchanged. Structured snapshot keys and values are redacted consistently so offline replay remains possible. No regenerated outcome is merged into matrix JSONL shards.

The original byte-preserved matrix JSONL/CSV, original F1/Pass 8/Pass 10 archives, and unredacted provenance are outside this public tree in the private reviewer archive. That archive must not be uploaded as the GitHub candidate. It preserves exact original errors and filenames while the candidate provides path-redacted full-chain views. Public audit-time hashes do not create a contemporaneous historical checksum chain that did not exist.

Evidence categories: original matrix and auxiliary observations; separate original minimal/repeat/performance studies; supplemental A1/A2 tests; supplemental F1/168 and causal controls; read-only Pass 10 analysis of original histories; later regenerated seed-1 evidence. A3 is missing. Stale older staging manifests and stale wheel-distribution notices are audited privately and are not silently reused as final manifests.
