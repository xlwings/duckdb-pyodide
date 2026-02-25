# duckdb-pyodide

A GitHub Actions workflow that builds DuckDB WASM wheels for [Pyodide](https://pyodide.org/) since DuckDB dropped their pyodide build (see [duckdb/duckdb-pyodide#7](https://github.com/duckdb/duckdb-pyodide/issues/7)).

## Triggering a build

Builds run automatically on every push to `main`, or can be triggered manually via **Actions → Build DuckDB Pyodide Wheel → Run workflow**.

The matrix of duckdb/pyodide version combinations is hardcoded in [`.github/workflows/build.yml`](.github/workflows/build.yml) under `jobs.build.strategy.matrix.include`. Edit that list to add or change versions — only exact version tags from [duckdb/duckdb-python](https://github.com/duckdb/duckdb-python) are supported (e.g. `v1.4.4`), not branch names.

- Each combination produces its own artifact, uploaded to the workflow run and retained for 90 days.
- Extension downloading does not work in the Pyodide runtime. Built-in extensions (json, parquet, icu, core_functions) are bundled.
- The build takes ~20 minutes because it compiles DuckDB from source with Emscripten.

## How it works

The [build workflow](.github/workflows/build.yml) clones [duckdb/duckdb-python](https://github.com/duckdb/duckdb-python) with its duckdb submodule, applies a few patches, then runs `pyodide build --exports=whole_archive`.

Four issues had to be worked around since the upstream build workflow was abandoned:

1. **Missing cross-build env URL** — the new `duckdb-python` repo is missing the pyodide xbuildenv URL that was added to the old repo via [duckdb/duckdb#18183](https://github.com/duckdb/duckdb/pull/18183) but never carried over. Passed via the `DEFAULT_CROSS_BUILD_ENV_URL` environment variable at build time.

2. **`wheel<0.44` required** — `auditwheel-emscripten` (pulled in by `pyodide-build`) calls `wheel.cli` which was removed in wheel 0.44.

3. **Full git clone required** — `setuptools_scm` needs git tags to produce a valid version string. Shallow clones fall back to `0.0.1.dev1`, which duckdb's custom version scheme rejects with `ValueError: Invalid version format`.

4. **`CMakeLists.txt` linker flag incompatibility** — the `elseif(UNIX AND NOT APPLE)` branch passes `--export-dynamic-symbol` to the linker, which is a GNU ld flag unknown to `wasm-ld`. Since Emscripten sets `UNIX=1`, this branch fires. Patched via [`scripts/patch_cmake.py`](scripts/patch_cmake.py) to add an `elseif(EMSCRIPTEN)` no-op guard before it.

5. **Dirty working tree breaks versioning** — patching `CMakeLists.txt` marks tracked files as modified, so `setuptools_scm` sees `distance=0, dirty=True` and falls through to `_bump_dev_version`, which rejects a distance of 0. Fixed by setting `OVERRIDE_GIT_DESCRIBE` to the git tag (e.g. `v1.4.4`), which duckdb's own version scheme uses to bypass `setuptools_scm` detection entirely. Only exact version tags are supported as `duckdb_version`; branch names like `main` will not produce a valid version.
