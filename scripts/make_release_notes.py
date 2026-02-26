#!/usr/bin/env python3
import hashlib
import pathlib
import sys

dv = sys.argv[1]
pv = sys.argv[2]

rows = []
for whl in sorted(pathlib.Path("dist").glob("*.whl")):
    digest = hashlib.sha256(whl.read_bytes()).hexdigest()
    rows.append(f"| `{whl.name}` | `{digest}` |")

lines = [
    f"DuckDB {dv} wheel for Pyodide {pv}",
    "",
    "## SHA-256 checksums",
    "",
    "| File | SHA-256 |",
    "|------|---------|",
    *rows,
]
print("\n".join(lines))
