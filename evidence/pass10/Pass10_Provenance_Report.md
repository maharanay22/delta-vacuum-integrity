# Pass 10 — Provenance and reconstruction report

## Sources and lineage

Primary original root: `<LOCAL_HOME>\Documents\Research\delta-vacuum-integrity`.
Validation root inspected: `<LOCAL_HOME>\Documents\Research\Delta_IEEE_Validation`.
Original histories: `data/histories/{nockpt,ckpt}/seed_0..99/table` with adjacent `meta.json`.

Recovered: **200 original generated histories**, **2,416 JSON-logged versions**, and **160 checkpoint Parquet files** across the 100 checkpoint-family histories. Original `meta.json` records workload plans, gap hours, offset hours, commit timestamps, original physical mtimes and per-version independent/Spark row hashes. Every logged reference exists in the retained generation inventory. Original current file mtimes agree with recorded generation mtimes to millisecond conversion tolerance; no metadata history was regenerated.

All 12 `runs_*.jsonl` shards total exactly 3,200 unique expected keys. The `_b` suffixes are seed-range shards, not a license to discard them: delta-rs main/prefix/PR shards cover seeds 0–49 and 50–99; six Spark shards cover all 100 seeds. Original `experiments/analyze.py` loads these shards by glob; its `runs.csv` has the same 3,200 keys. Minimal cases, repeats, `data/test_*.jsonl` pilots and later `fresh_*.jsonl` reproductions are excluded. The README reports earlier discarded pre-fix/main runs after an inventory-wait correction; no missing discarded-history identity is fabricated. Current final shards are the reported dataset, and several backup ZIPs contain matching result bytes.

Every history has at least one exact UUID/path linkage to a matrix outcome. All 3,200 rows also match generation version counts, physical-file counts, checkpoint flags, protected-version counts, unsafe-plan needed sets and deleted needed sets. The 748 zero-path rows are linked through their retained original family/seed history and matching batch invariants rather than a unique observed filename in that particular row. This is strong retained-original lineage evidence, not a claim that an unrecorded contemporaneous cryptographic chain exists.

## Search scope and archived evidence

`discovery.json` enumerates relevant top-level Research/Delta/IEEE/ICDE directories and ZIPs under Documents and Downloads, recursively inventories transaction logs, checkpoints, metadata, inventories and matrix result shards, and records access errors. Runtime/dependency repositories, `.git`, node_modules and virtual environments are pruned. Archive members are inspected without extraction or execution. Scope is recorded, not claimed as an exhaustive forensic search of every unrelated personal directory or deleted disk file. Relevant later fresh reproduction files were separately hashed in `excluded_and_supplemental_provenance.json`.

Archives with content matches to consumed original sources:

| Archive | Matching members | Archive SHA-256 |
|---|---|---|
| Delta_Lake_GitHub_Private_Staging.zip | 13 | c45eae940194f74577b04bb89174454660fde3493f830910b0d182863d3c6caa |
| Delta_Lake_GitHub_Reviewed_Private_Upload.zip | 12 | fbe024224994bee6837d97e49c702bfd648c827f77a5e27b39e8f851796b46ca |
| Delta_Lake_Research_Artifact_Package.zip | 13 | 07c8112992257c7561d2723d54ed97d15aa3793dae2c65f0bc8c030a2f2bc230 |
| Delta_Lake_Research_Evidence.zip | 13 | b28673e3842c747dfe34b14561f4f4578152631ae173426b9924ae46abe13cfd |
| Delta_Research_Results.zip | 12 | 4599d004398180220d87456b73bb8441a06a7e17442741c160b7d0d4904065c9 |

The backup ZIPs establish copies of result/code content; none supplies a second complete independently checksummed copy of all 200 generation histories. The research-evidence ZIP also has 45 table-log members, but they do not match the original matrix-generation logs. Do not treat those as replacement matrix histories. Later standalone F1 and Pass 8 causal ZIPs are separately identified and excluded from original exposure counts. An older Downloads Pass 8 static-only note is superseded for causal-validation status by this chat's actual local Pass 8 evidence; neither changes the original matrix.

## Timestamp reconstruction: in memory only

For original commit times `c[k]`, saved offsets `offset[k]`, and timestamp `t`, choose `k = min(bisect_left(c,t),last)` exactly as the original `aging.make_mapping`. Then:

`aged(t) - run_now_ms = -offset[k]*3,600,000 - (c[k]-t)`.

For retention `r`, the relative cutoff is `-r*3,600,000`. Expired remove timestamps and file mtime eligibility use strict `<`; unexpired removes use `>=`. A snapshot version is protected if it is latest or its successor commit is at/after the cutoff. This algebra cancels the unknown run clock, so the saved original mtimes and offsets recover exposure without changing a file or relying on current wall time. It is analytical timestamp reconstruction, **not regeneration** of physical histories.

The latest checkpoint's remove rows plus later JSON removes are examined; no unexpired reference found in full JSON replay is missing from this checkpoint-plus-tail view. The original source logs remain available for every version. For 1,125 matched retained run directories, the original-prefix aged JSON content and all surviving physical file mtimes exactly match the predicted transformation (1 ms tolerance for float/NTFS conversion). The now/cutoff can be recovered for those runs and cross-recorded wrong-detail cutoffs; it remains unknown for 2075 others. Complete per-run pre-VACUUM inventories generally were not saved, but the original generation physical inventory is present in all 200 metadata files. Post-run inventories are survivors and are not substituted for pre-run file presence.

## CSV schema and denominators

- `structural_present`: F1 has a live external DV; F3 has a physical data path with both expired and unexpired removes; F4 has an external DV referenced by an unexpired remove and not kept by a live DV reference.
- `age_eligible`: at least one structural path existed in the saved physical inventory and its mapped mtime is strictly earlier than the cutoff.
- `exposed`: an eligible path is needed by a protected snapshot and visible from checkpoint-plus-tail tombstones; F3 additionally excludes live data paths (those retain independent protection, and an active-data deletion would be F2).
- `mechanism_exposed`: `exposed` plus FULL mode for F1/F4, either mode for F3.
- `selected`: a class-exposed, protected path appears in the successful recorded dry-run plan. A dry-run error would yield unknown, never a fabricated empty plan.
- `deleted`: an exposed path occurs in the recorded filesystem deletion diff, checked against original `wrongly_deleted` and original `runs.csv` taxonomy.
- `read_failure_cooccurred`: a recorded protected failed snapshot references a deleted file of this class. This permits multiple contributing classes.
- `failed_read_exact_path`: at least one stored error explicitly contains a deleted class path. A zero here means no explicit match in saved errors, not proof of no class contribution.
- `unknown` denotes unavailable evidence. Zero is an established negative under this reconstruction. `historical_run_now_ms` and cutoff are unknown for 2,075 runs; this does not make their relative-age exposure unknown.


`Pass10_Per_Run_Exposure.csv`: 9,600 rows, one per original run × class. It includes implementation, mode, retention, family, seed, original source file/line/SHA-256, history directory/meta hash, raw and class-specific path sets, reference/candidate/deletion stages, read evidence, and any unknown reason. The 3,200 runs remain the denominator; class rows do not expand it to 9,600 trials.

`Pass10_Per_Seed_Exposure.csv`: 4,800 rows, one numeric seed × implementation × mode × retention × class, reporting both family-run counts. `Pass10_Exposure_Summary.csv`: all 32 exact configurations × three classes, plus explicitly labelled `ALL` rollups. Every stage includes its known denominator, unknown count, unique numeric seeds and unique `(family,seed)` histories. Do not sum rollups with detail rows. Repeated runs and overlapping failure classes are not independent observations.

`Pass10_File_Level_Preconditions.csv` exposes every structural path with age, live status, remove versions/ages, physical file hash, checkpoint visibility and final class-exposure flag. For F3, raw repeated-tombstone paths that are independently active remain in this file even when excluded from F3-specific exposure.

## Integrity, reproducibility and exclusions

`evidence_snapshot.json` captures original raw JSON actions, metadata, checkpoint rows, file inventories/hashes, raw outcomes, discovery and backup-member hashes; it supports offline replay without creating or aging a Delta table. `source_integrity.json` records 21829 consumed source files/archives with no size/mtime changes during acquisition; their SHA-256 hashes and timestamps are stored in the snapshot. Inputs were opened only for reading. Reference code and raw matrix JSONL/CSV are copied into this package, never edited in their original location.

Run `python -B scripts/audit.py --output NEW_DIR --replay evidence_snapshot.json` for offline reproduction. Standard Python is sufficient for replay. For live re-acquisition, specify `--research ORIGINAL_ROOT --discovery discovery.json`; the installed research Python with PyArrow is required for checkpoint Parquet reads (bundled Python lacked PyArrow). It refuses existing output directories and outputs under either original research root. No original module is executed. The optional artifact-tool CSV export script has a bundled Node dependency documented in `REPRODUCE.md`; the Python replay independently emits equivalent final CSVs.

Analysis development/execution attempts are preserved under `execution_logs/`: initial syntax errors, missing bundled PyArrow, and an interrupted sandboxed acquisition are explicitly retained. Final source capture was `analysis_execution_005.log`; final class-filtered analysis with absolute-time fields and mode applicability was `analysis_execution_008_final.log`. Intermediate broad F3 mixed-tombstone counts are retained as stage 1/2, not silently passed off as final class-specific exposure. Only final CSVs are delivered at the package root. No new experiment, VACUUM, table deletion, regeneration, system-clock change, manuscript edit, publication or GitHub push occurred.

## Evidence limits

Exact retained original generation histories are recoverable as current bytes/metadata with strong cross-result lineage. Exact per-run pre-VACUUM copies are not recoverable for every run, and absolute timing is unknown where post-run copies/cutoffs were not saved. Exposure counts are nonetheless determinate under the recovered original harness and metadata; all 3,200 stage-1/2/class-exposure statuses are known. Original runtime selection may have small elapsed-time drift relative to the captured run reference; no file is near a cutoff (boundary margins are recorded in verification output). Historical timestamps and current hashes are evidence, not proof against undocumented prior tampering.

If original data were missing, limited regeneration could test a mechanism or the seeded logical design, but new random file UUIDs, operation durations, layouts, checkpoints and mtimes would require a separate labelled experiment. It could not repair original exposure denominators or be joined to archived original plan/deletion/read outcomes. No such regeneration was necessary or performed here. Original F3/F4 controlled causal isolation remains incomplete; the audit associates observed failures but does not invent unperformed reads or counterfactuals.

Observed minimum structural-file mtime margin from its cutoff is 1799.324 seconds (about 29 min 59.324 s). Thus the README's nominal 30-minute description is approximate at subsecond level; no exposure depends on a near-zero boundary. The independent F1 maximum age is 146.50043028 h, leaving over 21 h before the 168 h cutoff.

Additional root-member check: Delta_Research_Results.zip also contains a root-level runs.csv identical to the primary flattened results (SHA-256 34c3c73be1bad1ae64b936174d0a9b01acea0b6e72377608ab1d6a462f6f60bf). This is additional to its 12 matched raw result shards tabulated above. No root-level meta.json or _delta_log members were found in the searched ZIPs. See archive_root_member_check.json.

Recorded dry-run/checker/vacuum phase sums peak at 191.725 s across the original matrix, compared with a 1,799.324 s minimum structural-file mtime cutoff margin. Aging/replay overhead was not separately timed and is not invented. See cutoff_timing_sanity_check.json.

## Packaged raw originals

`reference_sources/data/histories` contains byte-identical copies of all 200 generation histories (metadata, Parquet data, DV binaries, JSON logs and checkpoints), with source hashes and original mtimes in `source_file_manifest.json`. `reference_sources/data/work` contains the consumed retained post-run JSON logs, not complete post-run data directories. Original final result shards and analysis code are under `reference_sources/code`. Copies were SHA-256 verified against acquisition before packaging. ZIP timestamps have limited precision; the manifest retains original nanosecond values. Backup archives are inventoried and hashed rather than redundantly repackaged. These are archived original bytes, not regenerated tables.
