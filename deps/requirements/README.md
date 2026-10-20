> Historical staging build/provenance reference. This candidate contains no compiled runtime binaries; see docs/ENVIRONMENT.md and deps/PATCH_VERIFICATION.json.

# Python Environment Reproduction

This directory records the Python package versions used in the Delta Lake VACUUM integrity experiments.

## Environments

- `spark.txt` — Spark history generation and analysis

- `main.txt` — delta-rs main experimental environment

- `pr4858.txt` — locally patched PR #4858 environment

- `prefix.txt` — delta-rs 1.6.6 baseline environment

- `main-install.txt` and `pr4858-install.txt` — supporting packages excluding the experimental deltalake wheels

The main and PR #4858 environments use different experimental wheels even though both report deltalake version 1.6.5.

## Experimental wheels

The following experimental wheel archives were used in the original research but are NOT included in this source-only public release. Researchers must independently build the corresponding wheels from the documented source revisions before using the installation commands below:

- `deltalake-main-windows-wheel.zip`

- `deltalake-pr4858-windows-wheel.zip`

Extract them into separate `wheels/main` and `wheels/pr4858` directories.

Both archives contain a wheel named `deltalake-1.6.5-cp311-abi3-win_amd64.whl`.

Never install both wheels into the same environment.

## Installation (Windows PowerShell)

Run these commands from the repository root with Python 3.12 installed.

Create separate environments:

```powershell

py -3.12 -m venv venvs/main

py -3.12 -m venv venvs/pr4858

```

Install supporting dependencies:

```powershell

.\venvs\main\Scripts\python.exe -m pip install -r deps\requirements\main-install.txt

.\venvs\pr4858\Scripts\python.exe -m pip install -r deps\requirements\pr4858-install.txt

```

Install the corresponding experimental wheels:

```powershell

.\venvs\main\Scripts\python.exe -m pip install --no-deps .\wheels\main\deltalake-1.6.5-cp311-abi3-win_amd64.whl

.\venvs\pr4858\Scripts\python.exe -m pip install --no-deps .\wheels\pr4858\deltalake-1.6.5-cp311-abi3-win_amd64.whl

```

Verify imports:

```powershell

.\venvs\main\Scripts\python.exe -c "import deltalake; print(deltalake.__version__)"

.\venvs\pr4858\Scripts\python.exe -c "import deltalake; print(deltalake.__version__)"

```

Both should report version 1.6.5. The version number alone does not establish binary identity; verify the wheel checksums against the release documentation.

## Additional requirements

The full research workflow also requires JDK 17, Spark dependencies, and environment configuration described elsewhere in the repository.

The Spark and prefix package inventories are recorded in `spark.txt` and `prefix.txt`. Their complete installation procedures must be validated separately.

## Limitations

These requirements files record the installed versions used in the original research. They are not independently validated cross-platform lockfiles.

The experimental wheels target Windows x86-64. The complete setup procedure has not yet been independently verified from a fresh checkout.

Original low-retention matrix experiments disabled retention-duration checks. The supplemental FULL/168-hour driver enables retention-duration enforcement. Use disposable synthetic data only, never production tables.
