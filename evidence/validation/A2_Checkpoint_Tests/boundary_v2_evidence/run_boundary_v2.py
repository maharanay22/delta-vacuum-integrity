import json
import pathlib
import shutil
import sys
import time
import traceback

import pyarrow.parquet as pq

root = pathlib.Path(sys.argv[1])
base = pathlib.Path(sys.argv[2])

sys.path.insert(0, str(root / "code" / "harness"))
sys.path.insert(0, str(root / "code" / "checker"))

from sparkenv import spark_session
from vacuumcheck import check_plan

HOUR_MS = 3600000

cases = [
    ("boundary_v2_short", "interval 1 hours", 1),
    ("boundary_v2_long", "interval 168 hours", 168),
]

evidence = base / "boundary_v2_evidence"
output = evidence / "A2_boundary_v2_results.json"
warehouse = evidence / "spark_warehouse"

if output.exists() or warehouse.exists():
    raise RuntimeError("STOP: Existing output or warehouse")

for name, _, _ in cases:
    if (base / name).exists():
        raise RuntimeError(f"STOP: Table already exists: {name}")

results = []
spark = None

try:
    spark = spark_session(warehouse)

    for name, retention, hours in cases:
        original = base / name
        adjusted = evidence / (name + "_aged")

        if adjusted.exists():
            raise RuntimeError(f"STOP: Aged copy exists: {adjusted}")

        print(f"\n=== {name} ===", flush=True)

        spark.range(0, 10).coalesce(1).write \
            .format("delta") \
            .option("delta.deletedFileRetentionDuration", retention) \
            .mode("errorifexists") \
            .save(str(original))

        spark.sql(
            f"DELETE FROM delta.`{original.as_posix()}` WHERE id >= 0"
        )

        # Preserve the original Delta-generated history.
        shutil.copytree(original, adjusted)

        log = adjusted / "_delta_log"

        # Modify only the disposable aged copy.
        adjusted_ts = int(time.time() * 1000) - 2 * HOUR_MS
        candidate = None

        for commit in sorted(log.glob("*.json")):
            lines = []
            changed = False

            for line in commit.read_text(
                encoding="utf-8"
            ).splitlines():
                action = json.loads(line)

                if "remove" in action:
                    action["remove"]["deletionTimestamp"] = adjusted_ts
                    candidate = action["remove"]["path"]
                    changed = True

                lines.append(json.dumps(action))

            if changed:
                commit.write_text(
                    "\n".join(lines) + "\n",
                    encoding="utf-8"
                )

        if candidate is None:
            raise RuntimeError("No remove action found")

        # The copied local Hadoop checksum files are stale after
        # the controlled metadata adjustment. Remove them ONLY
        # from the disposable aged copy.
        for crc in log.glob(".*.crc"):
            crc.unlink()

        spark._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()

        delta_log = spark._jvm.org.apache.spark.sql.delta.DeltaLog \
            .forTable(spark._jsparkSession, str(adjusted))

        delta_log.checkpoint()

        checkpoints = sorted(log.glob("*.checkpoint*.parquet"))

        if not checkpoints:
            raise RuntimeError("No checkpoint generated")

        checkpoint_removes = []

        for checkpoint in checkpoints:
            data = pq.read_table(checkpoint, columns=["remove"])

            for action in data.column("remove").to_pylist():
                if action is not None:
                    checkpoint_removes.append(action)

        checked = check_plan(
            adjusted,
            [candidate],
            retention_hours=hours
        )

        result = {
            "case": name,
            "retention_hours": hours,
            "adjusted_age_hours": 2,
            "checkpoint_remove_count": len(checkpoint_removes),
            "checker_safe": checked.safe,
            "flagged_files": len(checked.unsafe),
            "timestamp_adjusted": True,
            "vacuum_executed": False
        }

        results.append(result)

        print("Checkpoint removes:",
              len(checkpoint_removes), flush=True)
        print("Checker safe:",
              checked.safe, flush=True)
        print("Flagged files:",
              len(checked.unsafe), flush=True)

    output.write_text(
        json.dumps(results, indent=2) + "\n",
        encoding="utf-8"
    )

    print("\nPASS: A2 boundary experiment completed", flush=True)
    print("Evidence:", output, flush=True)

except Exception:
    print(traceback.format_exc(), flush=True)
    raise

finally:
    if spark is not None:
        spark.stop()
