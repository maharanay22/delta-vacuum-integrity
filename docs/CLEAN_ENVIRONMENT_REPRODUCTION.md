# Targeted clean-install reproduction

This revision actually executed the portable F1 and causal drivers. Three new uv environments use Python 3.12.10 with user site packages disabled and no inherited PYTHONPATH. Dependencies were installed offline from the existing cache, and the patched wheel from the original recorded archive. Import locations, versions, native module hashes, installation output and all executions are recorded under `evidence/publication_revision/`. No site-packages directory was copied from an old venv.

This is a clean installation on the same Windows host, not a fresh machine or a fresh native rebuild. Existing hash-verified JARs and JDK were used as external runtime inputs. Minimal resolved packages differ from the full historical matrix environment (for example wrapt 2.5.1 rather than 2.5.0). The tested native modules exactly match the recorded original pre-fix and patched modules. Reproduction of source builds without the private wheel/cache remains unverified. No binary dependencies are included in the proposed public archive.

| Experiment | Plan and physical outcome | Latest snapshot |
|---|---|---|
| delta-rs 1.6.6, FULL, 168 h | Live DV selected and actually deleted | Independent reader and Spark fail; nested Spark cause names missing DV |
| main+4858 recorded native build, FULL, 168 h | Empty plan; no deletion | Independent reader and Spark return expected 16-row hash |
| Restore exact DV into new post-VACUUM copy | Restored SHA matches archived source | Both readers change from failure to correct 16 rows |
| Remove only active DV from new pre-VACUUM copy | All other file content hashes unchanged | Both readers change from correct 16 rows to failure |

The input was a new copy of the archived supplemental synthetic table, not a newly generated random history. Only copied table timestamps/mtimes were aged to the 212/202-hour history; retention enforcement was enabled for the F1 delta-rs VACUUM calls. The system clock was unchanged. No original table or original raw result was modified. This confirms this F1 fixture and these binary builds, not every matrix configuration or a fresh source compilation.

Expected rows: 16. Canonical row SHA-256: `1f3e17711306b5c9e2682199e33b89b3854fc0b45037e1812b6b9aaa0b02b5b7`. DV SHA-256: `c8546f48f1e03590247c799f97b8ed3c05327a81adafcb47ce8bac9d0abb8ccc`. Failed reads record null rows/hash; they are not successful empty results. Full Python and available Java chains are retained, including Spark's outer data-file error and nested missing-DV cause.

The originally skipped fixtures are now matched to 52 upstream Apache-licensed Git blobs. Restoring them into staging allows the unchanged original checker test file to pass 11 tests with zero skips. A final rerun also passes after commitInfo privacy redaction and checksum-sidecar exclusion. A final rerun also passes after commitInfo privacy redaction and checksum-sidecar exclusion. A final rerun also passes after commitInfo privacy redaction and checksum-sidecar exclusion. Earlier 9-pass/2-skip logs, a wrong-working-directory collection error, the initial sandbox venv error, the CC download 403 and extraction encoding error are preserved as failed attempts, not discarded.
