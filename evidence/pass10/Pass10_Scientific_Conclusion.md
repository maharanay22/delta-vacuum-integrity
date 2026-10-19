# Correction Pass 10 — Scientific conclusion

## 1. Was F1 exposure present in the original 168-hour matrix?

**No. All 1,600 original 168-hour runs had zero age-eligible active-DV exposure.** Of 200 original histories, 180 had live external DVs, but the oldest live DV was 146.50043028 hours old at the harness's run reference time. None crossed the 168-hour cutoff. This is an exposure result from original transaction logs and saved file timestamps, independently checked; it is not inferred from the absence of F1 failures.

Consequently, zero original F1 failures at 168 hours cannot demonstrate that the pre-fix implementation is safe at that retention or quantify a fix there. The later targeted aged F1 test and Correction Pass 8 restoration/removal tests are separate synthetic validation, excluded from every matrix count below.

## 2. Exposure counts and denominators

The original matrix contains 3,200 distinct runs: four implementations × two modes × two retentions × two checkpoint families × 100 seed numbers. Each retention has 1,600 runs. A numeric seed is counted once across families; `(family, seed)` is a distinct physical history. Repeated implementation/mode runs are not independent seeds.

| Retention h | Class | Structural runs | Old-file runs | Class-exposed runs | Direct-mode exposed runs | Unique exposed seeds | Exposed histories | Unsafe-plan runs | Deletion runs | Associated failed-read runs |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | F1 | 1440 | 1440 | 1440 | 720 | 90 | 180 | 180 | 109 | 109 |
| 0 | F3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 0 | F4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 168 | F1 | 1440 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 168 | F3 | 928 | 928 | 608 | 608 | 38 | 76 | 304 | 304 | 304 |
| 168 | F4 | 1600 | 928 | 928 | 464 | 58 | 116 | 232 | 180 | 180 |

Definitions: **structural** means raw reference preconditions, **old-file** adds physical mtime eligibility, and **class-exposed** also requires a protected snapshot reference, visibility through the latest checkpoint plus subsequent JSON, and no independent active-path protection for the class. **Direct-mode exposed** further requires FULL for F1/F4; the studied F3 tombstone-data mechanism can apply in both FULL and LITE. Exposure in a history is therefore not a claim that LITE performs orphan-DV cleanup. Counts are run events, not numbers of deleted files.

At 168 hours, F3's raw mixed-tombstone condition occurs in 116 histories / 58 seeds, but only **76 histories / 38 seeds** have the tombstone-only data-path exposure relevant to F3. Forty histories have only independently live-protected mixed paths; eight additional histories contain both active and inactive mixed paths. The file-level CSV retains all such paths rather than hiding the distinction. F4 has **116 histories / 58 seeds** with old tombstone-only DVs. Because F4 is a FULL physical-listing mechanism in these runs, its direct-mode denominator is 800 runs and its exposed count is 464; the other 464 latent exposures are in LITE.

At 0 hours, F1 has 180 exposed histories / 90 seeds, replicated across configurations: 1,440 latent class-exposed runs, of which **720 FULL runs** directly exercise the F1 mechanism. F3 and F4 have no unexpired remove references and therefore zero exposure at 0 hours.

## 3. Configurations: zero exposure versus zero observed failures

The following 168-hour rows each combine the two families, denominator 200 runs / 100 numeric seeds. F3 exposure is 38/100 in each family; F4 latent exposure is 58/100 in each family. F1 age exposure is zero in every row.

| Implementation | Mode | F1 exposed | F3 exposed | F3 plan | F3 deleted | F4 latent exposed | F4 plan | F4 deleted |
|---|---|---|---|---|---|---|---|---|
| deltars-1.6.6 | full | 0 | 76/200 | 76 | 76 | 116/200 | 116 | 91 |
| deltars-1.6.6 | lite | 0 | 76/200 | 76 | 76 | 116/200 | 0 | 0 |
| deltars-main | full | 0 | 76/200 | 76 | 76 | 116/200 | 116 | 89 |
| deltars-main | lite | 0 | 76/200 | 76 | 76 | 116/200 | 0 | 0 |
| deltars-main+4858 | full | 0 | 76/200 | 0 | 0 | 116/200 | 0 | 0 |
| deltars-main+4858 | lite | 0 | 76/200 | 0 | 0 | 116/200 | 0 | 0 |
| spark-4.4.1 | full | 0 | 76/200 | 0 | 0 | 116/200 | 0 | 0 |
| spark-4.4.1 | lite | 0 | 76/200 | 0 | 0 | 116/200 | 0 | 0 |

For main+4858 and Spark, zero F3 deletions occurred despite 76 exposed runs per mode; zero FULL F4 deletions occurred despite 116 exposed runs. Those are **zero observed failures under exposure**. Every implementation's 168-hour F1 result and every 0-hour F3/F4 result are **zero exposure**, so they do not test the corresponding mechanism. LITE F1/F4 rows report latent reference/file-age conditions but have no direct orphan-DV-cleanup mechanism; their zero failures must not be presented as a FULL-mode fix demonstration.

## 4. Snapshot-read evidence and attribution

The original outcomes record 109 F1 deletion runs at 0 hours, 304 F3 deletion runs at 168 hours, and 180 F4 deletion runs at 168 hours. Protected snapshots referencing a class-deleted file subsequently fail in those same numbers of runs. These class counts can overlap and must not be summed as distinct failing runs.

For F3, all 304 associated failed reads explicitly name a deleted F3 path in the saved error. For F4, only 82 of the 180 associated failed-read runs explicitly name a class-deleted DV (41 in pre-fix FULL and 41 in main FULL); other deleted files can prevent a read first, and stored errors are truncated. The 180 value is therefore co-occurrence with a needed missing DV, not 180 isolated causal demonstrations. The remaining class-specific causal attribution is **unknown**, not zero. Raw errors and per-version outcomes remain in the archived result JSONL. Failed reads are distinct from successful reads with a changed row hash; those are separate fields in the per-run output.

## 5. Are the results sufficient for the reviewers?

This audit resolves the exposure-denominator question for the retained original matrix, with all 200 histories recovered, 3,200 matching result joins and 1,125 consistent retained aged run directories. No regeneration is needed for these exposure counts. All original class/run deleted-file counts reconcile exactly with the original flattened results.

It supports a corrected account of F3/F4 coverage and establishes that original 168-hour F1 coverage was absent. It does **not**, by itself, resolve every reviewer concern or make the original matrix an F1-at-168 experiment. A response should state this limitation, report the later targeted F1 causal validation separately, and avoid treating matrix repetitions as additional independent seeds. No manuscript edits were made.

Unresolved limits: 2075 exact absolute run clocks and most complete per-run pre-VACUUM copies were not archived; relative cutoff exposure remains reconstructible from original metadata. Current hashes are audit-time provenance, not proof of an original contemporaneous checksum chain. The original results often contain operational vacuum errors; attempted selection and actual deletion differ. This audit adds no independent new engine reads, concurrency tests, cloud/object-store experiments or exhaustive causal isolation for F3/F4. It cannot establish broader safety or failure rates beyond this local synthetic design.
