# duckdb-pyodide

A GitHub Actions workflow that builds DuckDB WASM wheels for [Pyodide](https://pyodide.org/) since DuckDB dropped their pyodide build (see [duckdb/duckdb-pyodide#7](https://github.com/duckdb/duckdb-pyodide/issues/7)). Original support for Pyodide was added in https://github.com/duckdb/duckdb/pull/11531.

## Adding a new version

Edit the `matrix.include` list in [`.github/workflows/build.yml`](.github/workflows/build.yml) — only exact version tags from [duckdb/duckdb-python](https://github.com/duckdb/duckdb-python) are supported (e.g. `v1.4.4`), not branch names. Each pyodide version requires a specific Python version; check the [pyodide changelog](https://pyodide.org/en/stable/project/changelog.html) when adding new entries.

```yaml
matrix:
  include:
    - duckdb_version: 'v1.5.0'
      pyodide_version: '0.29.3'
      python_version: '3.13'
```

## Build & Release

A single [build workflow](.github/workflows/build.yml) runs on every push (except README-only changes) and on manual dispatch:

- **Non-main branches** — builds, runs tests, and uploads wheels as artifacts (90-day retention).
- **`main` branch** — builds, runs tests, and publishes wheels as GitHub releases tagged `duckdb-vx.x.x-pyodide-x.x.x`.

The build takes ~20 minutes per combination since it compiles DuckDB from source with Emscripten. Extension downloading does not work in the Pyodide runtime; built-in extensions (json, parquet, icu, core_functions) are bundled.

## Tests

### Smoke tests

Verifies core functionality (platform detection, table CRUD, pandas integration). Must pass for the build to succeed.

```bash
uv run -m http.server
```
Now open http://localhost:8000/test-smoke.html.

`wasm-dist/` must contain the built wheel.

### Official test suite

Runs ~180 test files from [duckdb/duckdb-python](https://github.com/duckdb/duckdb-python) `tests/fast/` via pytest inside Pyodide. Results are informational only and do not block the build.

```bash
uv run -m http.server
```
Now open http://localhost:8000/test-official.html.

`wasm-dist/` must contain the built wheel.

`test-official.html`: results pending v1.5.0 build.

Tests that are excluded from the run (incompatible with Pyodide):

| Category | Reason |
|----------|--------|
| Threading (`test_6584`, `test_parallel`, `test_alex_multithread`, `test_multithread`, `test_query_progress`) | `can't start new thread` — no threading in Pyodide |
| ADBC (`test_adbc`) | ADBC driver not available in wasm |
| Spark (`test_spark`) | PySpark not available in Pyodide |
| fsspec / httpfs (`test_fsspec`, `test_read_csv_httpfs`) | `fsspec` not available in Pyodide; httpfs extension not bundled |
| Subprocess (`test_startup`, `test_connection_interruption`) | `emscripten does not support processes` |
| PyTorch / TensorFlow (`test_torch`, `test_tf`) | Not available in Pyodide |
| psutil (`test_query_profiler`) | Not available in Pyodide |
| Incompatible pytest API (`test_json_logging`) | Uses `pytest.raises(check=...)` added in pytest 8.4; Pyodide ships an older version |
| Windows-only (`test_windows_path`) | N/A in wasm |

### Updating tests

The test files in [`tests/`](tests/) are copied from [duckdb/duckdb-python](https://github.com/duckdb/duckdb-python). To update them, copy the `tests/conftest.py` and `tests/fast/` directory from the matching duckdb-python tag. Tests that are incompatible with Pyodide (threading, filesystem, ADBC, Spark, fsspec, pytorch, tensorflow) are excluded in the `TEST_FILES` list in `test-official.html`.

## How it works

The [build workflow](.github/workflows/build.yml) clones [duckdb/duckdb-python](https://github.com/duckdb/duckdb-python) with its duckdb submodule, applies a few patches, then runs `pyodide build --exports=whole_archive`.

## Fixes

1. **Missing cross-build env URL** — the new `duckdb-python` repo is missing the pyodide xbuildenv URL that was added to the old repo via [duckdb/duckdb#18183](https://github.com/duckdb/duckdb/pull/18183) but never carried over. Passed via the `DEFAULT_CROSS_BUILD_ENV_URL` environment variable at build time.

2. **`wheel<0.44` required** — `auditwheel-emscripten` (pulled in by `pyodide-build`) calls `wheel.cli` which was removed in wheel 0.44.

3. **Full git clone required** — `setuptools_scm` needs git tags to produce a valid version string. Shallow clones fall back to `0.0.1.dev1`, which duckdb's custom version scheme rejects with `ValueError: Invalid version format`.

4. **`CMakeLists.txt` linker flag incompatibility** — the `elseif(UNIX AND NOT APPLE)` branch passes `--export-dynamic-symbol` to the linker, which is a GNU ld flag unknown to `wasm-ld`. Since Emscripten sets `UNIX=1`, this branch fires. Patched via [`scripts/patch_cmake.py`](scripts/patch_cmake.py) to add an `elseif(EMSCRIPTEN)` no-op guard before it.

5. **Dirty working tree breaks versioning** — patching `CMakeLists.txt` marks tracked files as modified, so `setuptools_scm` sees `distance=0, dirty=True` and falls through to `_bump_dev_version`, which rejects a distance of 0. Fixed by setting `OVERRIDE_GIT_DESCRIBE` to the git tag (e.g. `v1.4.4`), which duckdb's own version scheme uses to bypass `setuptools_scm` detection entirely. Only exact version tags are supported as `duckdb_version`; branch names like `main` will not produce a valid version.

## Credits

This repo was developed with [Claude Code](https://claude.ai/claude-code) (claude-sonnet-4-6).
