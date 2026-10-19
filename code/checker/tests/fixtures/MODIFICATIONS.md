# Modified upstream test fixtures

Derived from delta-rs at the revision and paths in `third_party/FIXTURE_PROVENANCE.json`, under Apache-2.0. This publication copy removes userName, userId, notebook, clusterId and job fields from commitInfo in 21 JSON commits, and omits 21 local filesystem .crc sidecars. Data actions, DV descriptors, Parquet and checkpoint contents are unchanged. Original and staged hashes are recorded. These metadata redactions are not original experimental changes.
