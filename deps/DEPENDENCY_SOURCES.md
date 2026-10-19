> Historical staging build/provenance reference. This candidate contains no compiled runtime binaries; see docs/ENVIRONMENT.md and deps/PATCH_VERIFICATION.json.

# Research Dependency Sources

## GlobalMentor Hadoop Bare Naked Local FileSystem

**Artifact:** `com.globalmentor:hadoop-bare-naked-local-fs:0.1.0`

**Purpose:** Provides local Hadoop filesystem access on Windows without requiring the standard Hadoop `winutils.exe` executable.

**License:** Apache License 2.0, as declared in the artifact's Maven metadata.

**Source repository:** https://github.com/globalmentor/hadoop-bare-naked-local-fs

**Official Maven Central artifact:**

https://repo.maven.apache.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.jar

### Integrity verification

Expected SHA-256:

`e0cc30fb0531eb0b59468dc0abf5b257533d2365b5e9f45e795edd707aa78c62`

The JAR was downloaded from Maven Central and its SHA-256 checksum was compared against the original research dependency.

**Verification result: PASS**

The publicly downloadable JAR matches the original research artifact byte-for-byte.

### Reproduction

Download the JAR from Maven Central and place it in:

`deps/jars/`

Verify its checksum against `deps/jars.sha256` before running the Spark experiments.

### Other dependencies

The remaining Spark, Delta Lake, Unity Catalog, and RoaringBitmap JARs are recorded in `deps/jars.sha256`.

Their individual public download locations and checksum matches must be verified separately before claiming that the entire dependency set is independently retrievable.
