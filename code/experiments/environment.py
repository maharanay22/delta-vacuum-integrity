"""Record the exact environment of the experiments into results/environment.json.

usage (any venv): python environment.py OUT_JSON"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESEARCH = HERE.parents[1]
VENVS = RESEARCH / "venvs"
WHEELS = RESEARCH / "wheels"
JDK = Path(r"<LOCAL_HOME>\tools\jdk-17.0.20.1+1")
RUSTC = Path(r"<LOCAL_HOME>\tools\rust\rustup\toolchains\1.98.0-x86_64-pc-windows-gnu\bin\rustc.exe")
DELTA_RS = Path(r"<LOCAL_HOME>\source\delta-rs")


def run(*cmd: str) -> str:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return (p.stdout + p.stderr).strip()
    except Exception as exc:
        return f"unavailable: {exc}"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def venv_packages(name: str) -> dict:
    py = VENVS / name / "Scripts" / "python.exe"
    code = (
        "import importlib.metadata as m, json; "
        "print(json.dumps({d: m.version(d) for d in ['deltalake','pyarrow','pandas','psutil','pyspark','delta-spark','matplotlib'] "
        "if any(x.metadata['Name'].lower().replace('_','-')==d for x in m.distributions())}))"
    )
    out = run(str(py), "-c", code)
    try:
        return json.loads(out.splitlines()[-1])
    except Exception:
        return {"error": out[-300:]}


def git(*args: str) -> str:
    return run("git", "-C", str(DELTA_RS), *args)


def main() -> None:
    import psutil

    env = {
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu": run("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"),
        "logical_cpus": psutil.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / 2**30, 1),
        "filesystem": "local NTFS (Windows), single writer",
        "python": sys.version,
        "java": run(str(JDK / "bin" / "java.exe"), "-version"),
        "rustc": run(str(RUSTC), "--version"),
        "systems_under_test": {
            "deltars-1.6.6": {"source": "PyPI deltalake==1.6.6 (pre-fix)", "packages": venv_packages("prefix")},
            "deltars-main": {
                "source": "delta-io/delta-rs main, includes #4839 (merged 2026-10-13) and #4850",
                "commit": git("rev-parse", "640dc893"),
                "commit_subject": git("log", "-1", "--format=%s", "640dc893"),
                "wheel": str(WHEELS / "main"),
                "wheel_sha256": [sha256(p) for p in (WHEELS / "main").glob("*.whl")],
                "build": "maturin build --release --target x86_64-pc-windows-gnu (rustc 1.98.0, WinLibs GCC); Cargo.lock saved next to the wheel",
                "packages": venv_packages("main"),
            },
            "deltars-main+4858": {
                "source": "delta-io/delta-rs PR #4858 (branch fix-vacuum-tombstone-deletion-vectors, head 6ebfd483) rebased locally onto main 640dc893",
                "rebased_commit": run("git", "-C", r"<LOCAL_HOME>\source\delta-rs", "rev-parse", "paper-4858-rebased"),
                "wheel": str(WHEELS / "pr4858"),
                "wheel_sha256": [sha256(p) for p in (WHEELS / "pr4858").glob("*.whl")],
                "build": "same as deltars-main",
                "packages": venv_packages("pr4858"),
            },
            "spark-4.4.1": {
                "source": "delta-spark 4.4.1 on PySpark 4.2.0, local[2], JDK 17; jars on spark.driver.extraClassPath",
                "packages": venv_packages("spark"),
                "jars_sha256": {p.name: sha256(p) for p in sorted((RESEARCH / "deps" / "jars").glob("*.jar"))},
                "conf": {
                    "spark.hadoop.fs.file.impl": "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
                    "spark.delta.logStore.file.impl": "io.delta.storage.LocalLogStore",
                    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
                },
            },
        },
        "checker": "vacuumcheck 0.1.0 (this repository, checker/)",
        "env": {k: os.environ.get(k) for k in ("JAVA_HOME", "HADOOP_HOME")},
    }
    Path(sys.argv[1]).write_text(json.dumps(env, indent=2), encoding="utf-8")
    print(json.dumps(env, indent=2)[:3000])


if __name__ == "__main__":
    main()
