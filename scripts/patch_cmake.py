#!/usr/bin/env python3
"""
Patch duckdb-python/CMakeLists.txt to skip GNU ld-specific export flags
under Emscripten. wasm-ld doesn't support --export-dynamic-symbol; symbol
exports are handled by pyodide-build via --exports=whole_archive instead.
"""
import pathlib
import sys

cmake = pathlib.Path("CMakeLists.txt")
original = cmake.read_text()

old = (
    "elseif(UNIX AND NOT APPLE)\n"
    "  target_link_options(\n"
    '    _duckdb PRIVATE "LINKER:--export-dynamic-symbol=duckdb_adbc_init"\n'
    '    "LINKER:--export-dynamic-symbol=PyInit__duckdb")'
)

new = (
    "elseif(EMSCRIPTEN)\n"
    "  # wasm-ld does not support --export-dynamic-symbol; symbol exports are\n"
    "  # handled by pyodide-build via --exports=whole_archive\n"
    "elseif(UNIX AND NOT APPLE)\n"
    "  target_link_options(\n"
    '    _duckdb PRIVATE "LINKER:--export-dynamic-symbol=duckdb_adbc_init"\n'
    '    "LINKER:--export-dynamic-symbol=PyInit__duckdb")'
)

if old in original:
    cmake.write_text(original.replace(old, new, 1))
    print("Patched CMakeLists.txt: added EMSCRIPTEN guard before UNIX branch")
else:
    print("WARNING: expected pattern not found in CMakeLists.txt - patch skipped")
    print("Relevant lines:")
    for i, line in enumerate(original.splitlines(), 1):
        if "export-dynamic-symbol" in line or "UNIX AND NOT APPLE" in line:
            print(f"  {i}: {line}")
    sys.exit(1)
