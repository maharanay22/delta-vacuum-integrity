# Reproduction guide

Start with Python 3.12 and a new checkout; do not point experiment scripts at original research data. Offline checks need no engine installation:

```powershell
python -B tools/verify_artifact.py
python -B tools/check_matrix.py
python -B reproduction/pass10/audit.py --replay evidence/pass10/evidence_snapshot.json.gz --output ../new_pass10_replay
python -B reproduction/a3_archived_outcome_audit.py --output ../new_A3_reanalysis.json
python -B evidence/recovered_original_tests/A3_original_analysis.py .
```

Install PyArrow 26.0.0, PyRoaring 1.2.0 and pytest 9.1.1 into a new venv for reader/checker work. Run checker tests from `code/checker` (`python -B -m pytest -q -p no:cacheprovider tests`). All original 11 tests now pass with provenance-verified upstream fixtures. `tools/check_f1_fixture.py` only reads the pre/post fixtures.

Recovered original A1's later body takes two arguments: candidate repository root and an EXISTING new disposable result directory. It creates temporary synthetic JSON logs only there, then refuses to overwrite A1_result.json. Recovered A1 attempt 1 retains an incomplete historical shell invocation and is evidence, not a successful-run claim. See `docs/A1_A3_RECOVERY.md`.

For engine tests obtain dependencies separately: read `docs/ENVIRONMENT.md`, `deps/BUILD_INSTRUCTIONS.md`, requirements and the hash-verifying JAR acquisition script. Set JAVA_HOME and DELTA_DEPS_DIR. Use separate clean environments for delta-rs 1.6.6 and the experimental patched build. Both main builds report Python package 1.6.5, so identify the source/native artifact by recorded commit and hash. The patched wheel is deliberately not redistributed. The source patch was apply-checked and matched to the recorded tip; a fresh native build has not been performed here.

```powershell
python -B reproduction/f1_vacuum.py --execute --output ../new_f1_prefixed_build_output
python -B reproduction/f1_causal.py --execute --output ../new_f1_controls
```

Run the first command independently under each selected delta-rs environment, using a different nonexistent output directory each time. The drivers copy archived supplemental synthetic input and age only disposable copies; they do not generate new random histories. The causal driver requires Spark/PySpark 4.2.0, Delta Spark 4.4.1, JDK17 and the verified external JARs. Capture complete stdout/stderr to NEW logs. Optional `reproduction/read_new_f1_with_spark.py --table NEW_F1_OUTPUT/table --output NEW_F1_OUTPUT/spark_read.json` reads that new current snapshot. Expected row/DV hashes and actual tested results are in `docs/CLEAN_ENVIRONMENT_REPRODUCTION.md`.

The full 3,200 pipeline is retained for inspection, not rerun in this preparation. Do not invoke `code/experiments/run_all.sh` for a smoke test. Its explicit disposable-VACUUM opt-in and the original generator/aging/runner must be reviewed before any separate experiment. No original raw file should ever be used as writable output.
