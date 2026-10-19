import glob
import os
import sys
from pathlib import Path

RESEARCH = Path(__file__).resolve().parents[2]
DEPS = Path(os.environ.get("DELTA_DEPS_DIR", str(RESEARCH / "deps"))).resolve()


def configure_env() -> None:
    if not os.environ.get("JAVA_HOME"):
        raise RuntimeError("Set JAVA_HOME to an installed JDK 17 before starting Spark")
    os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.environ["PATH"]
    os.environ.setdefault("HADOOP_HOME", str(DEPS / "hadoop"))
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYTHONUTF8", "1")


def spark_session(warehouse: Path):
    configure_env()
    from pyspark.sql import SparkSession

    wh = str(warehouse).replace("\\", "/")
    cp = str(DEPS / "jars" / "*")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("vacuum-integrity")
        .config("spark.driver.extraClassPath", cp)
        .config("spark.driver.memory", "3g")
        .config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
        .config("spark.delta.logStore.file.impl", "io.delta.storage.LocalLogStore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", wh)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "2")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", "1000")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + wh)
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session
