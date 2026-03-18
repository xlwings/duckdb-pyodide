# Adding httpfs Extension Support

## The Core Challenge

`httpfs` depends on `libcurl` and `OpenSSL` — native C libraries that need to be compiled for WASM/Emscripten. This is non-trivial but not impossible.

## How DuckDB Extensions Are Selected

The currently bundled extensions (json, parquet, icu, core_functions) are whatever `duckdb-python`'s upstream CMake builds by default. We don't configure which extensions to include — they come along for free.

`httpfs` is **not** built by default; it must be explicitly enabled via CMake:

```bash
CMAKE_ARGS: "-DDUCKDB_EXPLICIT_PLATFORM=wasm_eh_pyodide -DBUILD_HTTPFS_EXTENSION=ON"
```

But that alone won't work, because `httpfs` needs native libs.

## What duckdb-wasm does (reference: `patches/duckdb/duckdb.patch`)

The duckdb-wasm patch is ~700 lines and implements `WASM_LOADABLE_EXTENSIONS` — a **runtime extension download** system, not static linking. It does **not** compile httpfs into the binary. Instead:

1. **Replaces `.duckdb_extension` → `.duckdb_extension.wasm`** throughout extension install/load paths.
2. **Adds a JS-based loader** via `EM_ASM_PTR` that fetches extension `.wasm` files at runtime using `XMLHttpRequest` (browser) or `Worker` + `SharedArrayBuffer` + `fetch` (Node.js, for synchronous blocking).
3. **Adds `preloaded_httpfs` flag** so `ExtensionIsLoaded("httpfs")` returns true when httpfs was loaded via the JS path.
4. **Adds `SetPreferredRepository`/`GetPreferredRepository`** to route `INSTALL` statements to custom endpoints.
5. **Removes extension signature/metadata checks** in the WASM path (commented out).

So `httpfs` in duckdb-wasm is a separately hosted `.duckdb_extension.wasm` file that gets downloaded and `dlopen`'d at runtime — it still avoids the libcurl problem by using JS fetch for the download of the extension itself, but httpfs's own network calls (the ones that read S3/HTTP files) would separately need to work in WASM.

The patch targets a different DuckDB version than v1.4.4 and is invasive enough that it would need careful porting.

## Two Approaches

### Option A: Static compilation (no runtime loading)

Compile httpfs into the wheel statically with `-DBUILD_HTTPFS_EXTENSION=ON`. This requires Emscripten-compatible replacements for `libcurl` and `OpenSSL`. There is no official Emscripten port of either, so this would mean either:
- Porting them to WASM (very high effort)
- Replacing httpfs's curl calls with `emscripten_fetch` (requires patching httpfs source)

**Pros:** No runtime download, self-contained wheel, simpler mental model.
**Cons:** Significant porting effort, larger binary, uncertain Pyodide network compatibility (Pyodide Web Workers have `fetch` but not synchronous XHR).

### Option B: Runtime loading (what duckdb-wasm does)

Port the `WASM_LOADABLE_EXTENSIONS` patch from duckdb-wasm to duckdb v1.4.4, host a `httpfs.duckdb_extension.wasm` file somewhere, and let users `LOAD 'httpfs'` at runtime.

**Pros:** Follows proven duckdb-wasm approach, separates concerns, httpfs binary already exists in duckdb-wasm releases.
**Cons:** Invasive patch (~700 lines) touching core extension loading, needs porting to v1.4.4, requires hosting extension files, synchronous fetch in Pyodide Web Worker context is a known problem (no `XMLHttpRequest`, `SharedArrayBuffer` may not be available depending on COOP/COEP headers).

## Pyodide-specific concern

Both approaches hit the same wall: **Pyodide runs in a Web Worker, which doesn't support synchronous XHR**. The duckdb-wasm Node.js path uses `SharedArrayBuffer` + `Worker` threads to simulate synchronous fetch — this requires `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp` headers, which many Pyodide deployments don't set.

This means any network call that DuckDB expects to be synchronous (either for loading the extension itself, or for httpfs reads) would need async bridging that isn't straightforward from C++/WASM.

## Honest Assessment

This is **hard**. The duckdb-wasm patch is not directly usable as a template for the Pyodide case because their deployment assumptions differ significantly. The most promising path would be:

1. Check whether duckdb-wasm publishes pre-built `httpfs.duckdb_extension.wasm` artifacts for v1.4.4.
2. Investigate whether Pyodide's JS bridge (`pyodide.runPython`, `js` module) could be used to make async fetch calls from within a synchronous C++ context via Atomics.
3. Only then decide whether Option A or B is more tractable.
