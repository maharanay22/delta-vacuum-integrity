# Environment and dependency provenance

Recorded original platform: Windows 11 x86-64 / local NTFS / single writer; Python 3.12.10; Temurin JDK 17.0.20.1; PySpark 4.2.0; delta-spark 4.4.1; PyArrow 26.0.0. Original recorded environment details are in `code/results/environment.json`. This is historical provenance, not a claim that every pin is obtainable today.

Offline Pass 10 replay and manifest checks need standard Python 3.12. Checker/oracle verification needs PyArrow 26.0.0 and a pytest runner; the actual verification environment/version is recorded in the readiness audit. Existing `deps/requirements` files are preserved observations. `main.txt` and `pr4858.txt` report the same deltalake package version and must not be used to select different local builds from PyPI. Use the source commits, patch, lockfiles and separately verified wheel/module identities.

No virtual environments, wheels, JARs, executable binaries or complete third-party source repositories are distributed. Eleven JAR reference hashes and Maven retrieval instructions are retained. The two Cargo.lock files are preserved exactly. The old Rust licensing inventory describes declared metadata; it does not prove final binary composition or redistribution clearance. The separate inexact build is not substituted for any of the four original matrix implementations.

Matrix environment labels are `deltars-1.6.6`, `deltars-main`, `deltars-main+4858`, `spark-4.4.1`. The last is the Delta Spark label; the actual Spark engine was 4.2.0. Pinned native builds target Windows GNU and are not claimed portable to other systems.
