#!/bin/bash
# Full pipeline, as run for the paper. Paths are relative to this repository's parent directory
# (<LOCAL_HOME>\Documents\Research\delta-vacuum-integrity). Every step resumes where it stopped.
set -e
if [ "${ALLOW_DISPOSABLE_VACUUM:-}" != "YES" ]; then
  echo "Opt-in required: use only a new disposable research workspace; see docs/REPRODUCTION.md" >&2
  exit 2
fi
cd "$(dirname "$0")/../.."
export PYTHONUTF8=1
V=venvs
H=data/histories
R=code/results
SEEDS=${SEEDS:-0-99}
SPARK_SEEDS=${SPARK_SEEDS:-0-99}

# 1. Histories (Delta Lake for Spark writes the DVs). Several generators may run in parallel; each
#    claims a (family, seed) with a .claim file.
$V/spark/Scripts/python.exe code/harness/generate.py $H --seeds $SEEDS
$V/spark/Scripts/python.exe code/experiments/minimal.py $H

# 2. Vacuum matrix: lite/full x 0h/168h on an aged copy of every history, per implementation.
cd code/harness
for pair in prefix:deltars-1.6.6 main:deltars-main pr4858:deltars-main+4858; do
  v=${pair%%:*}; label=${pair#*:}
  ../../$V/$v/Scripts/python.exe runner.py --impl deltars --label $label --histories ../../$H \
    --out ../results/runs_$v.jsonl --work ../../data/work --seeds $SEEDS
  ../../$V/$v/Scripts/python.exe runner.py --impl deltars --label $label --histories ../../$H \
    --out ../results/minimal_$v.jsonl --work ../../data/work_min --families minimal --seeds 1-3 --keep-every 1
done
../../$V/spark/Scripts/python.exe runner.py --impl spark --label spark-4.4.1 --histories ../../$H \
  --out ../results/runs_spark.jsonl --work ../../data/work --seeds $SPARK_SEEDS
../../$V/spark/Scripts/python.exe runner.py --impl spark --label spark-4.4.1 --histories ../../$H \
  --out ../results/minimal_spark.jsonl --work ../../data/work_min --families minimal --seeds 1-3 --keep-every 1
# Repeatability of the pre-fix build: two more trials on nockpt seeds 0-24.
for r in 1 2; do
  ../../$V/prefix/Scripts/python.exe runner.py --impl deltars --label deltars-1.6.6 --histories ../../$H     --out ../results/repeat_prefix_r$r.jsonl --work ../../data/work_repeat_r$r --families nockpt --seeds 0-24
done
cd ../..

# 3. Cross-check the oracle's readability verdicts with Spark on kept work directories.
$V/spark/Scripts/python.exe code/experiments/crosscheck_spark.py data/work data/work_min --out $R/crosscheck_spark.csv --histories $H

# 4. RQ4 performance on larger synthetic tables.
$V/spark/Scripts/python.exe code/experiments/perf_tables.py data/perf_tables
$V/spark/Scripts/python.exe code/experiments/perf_run.py data/perf_tables data/perf_work $R/perf.csv --reps 3

# 5. Tables and figures.
$V/spark/Scripts/python.exe code/experiments/vacuum_commits.py data/work $R/vacuum_commits.csv
$V/main/Scripts/python.exe code/experiments/ablation.py $H data/abl_work $R/ablation.csv
$V/spark/Scripts/python.exe code/experiments/environment.py $R/environment.json
$V/spark/Scripts/python.exe code/experiments/analyze.py $R code/figures $H
