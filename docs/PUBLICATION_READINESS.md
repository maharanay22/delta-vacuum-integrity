# Publication-readiness report

**Status: public research repository, published with explicit owner approval. No versioned release, tag or DOI has been created.** The initial reviewed publication is commit `0b047c401796685030e1bd82384703ad5410ee62`. These documentation corrections preserve the research code and all experimental evidence.

| Status | Finding | Evidence and limits |
|---|---|---|
| PASS | Publication and licensing approval | The owner approved publication of the reviewed package. Current Apache-2.0 / CC BY 4.0 scope and upstream exceptions are recorded in LICENSING.md, LICENSE, DATA_AND_FIGURES_LICENSE.md and NOTICE. No additional legal rights are asserted. |
| PASS | Initial public artifact integrity | Anonymous verification of the initial publication matched all 336 approved files: 335 manifest entries and the manifest itself. |
| PASS | Documentation corrections | Approval-stage wording is current, build instructions identify deps/pr4858-rebased.patch, and current descriptions distinguish 52 verified original fixture files from 31 published derived files. Encoding damage in current-facing text and escaped documentation examples is corrected. |
| PASS | Original results and evidence | The preserved original matrix contains 3,200 unique outcomes. Published original, regenerated and supplemental observations remain separate. Independent offline replay reproduced Pass10 totals and A3 counts. These documentation edits do not rerun an engine experiment. |
| PASS | Previously executed targeted validation | Fresh same-host installs reproduced F1 FULL/168 pre-fix DV deletion, patched preservation and DV restoration/removal controls. The preserved final checker log reports 11 passing tests and no skips. These are historical checks, not new results from this correction. |
| PASS | Fixture provenance and privacy | All 52 original fixture files matched upstream blobs. Publication omits 21 checksum sidecars; 31 derived files remain, with nonessential identities removed from 21 commitInfo records. Hashes and modification records are retained. |
| WARNING | Independent ownership evidence | The author's statement and Git provenance support the scope; no independent legal-title, employer-assignment or patent certification is claimed. |
| WARNING | Build reproducibility | Fresh native compilation and independent-machine engine reproduction remain unverified. Existing cached artifacts were used in the recorded clean-install tests. |
| WARNING | Historical execution records | Original A1/A3 stdout, timestamps and exit status were not recovered; recovered code and clearly labelled new reruns are retained. |
| WARNING | Security review limits | Text scans are heuristic and do not exhaustively inspect every binary encoding. Private archives and unreviewed runtime distributions must not be uploaded. |
| WARNING | Citation and release metadata | Author and repository URLs are verified. No versioned release date, DOI or ORCID is invented. Full CFF schema validation remains unverified. |

Historical candidate-state reports and logs under evidence/ describe their capture time and remain unchanged. Refer to the current README and LICENSING.md for publication status and scope. A versioned release or Zenodo action requires separate authorization.
