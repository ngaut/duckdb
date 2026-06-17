# DuckDB Compiled-Region SQL Trace Harness

This directory contains focused trace tooling for compiled-region SQL coverage.
TPC-H traces answer workload performance questions; this harness answers whether
the focused SQL test flows still produce honest region, scalar-IR, boundary,
counter, runtime, and IR evidence through the `jit_*` SQL surface.

Run:

```sh
python3 benchmark/jit/jit_sql_trace.py --out-dir /tmp/duckdb_jit_sql_trace
python3 benchmark/jit/verify_jit_sql_trace.py /tmp/duckdb_jit_sql_trace
python3 benchmark/jit/verify_jit_architecture.py
```

Use a fresh or empty `--out-dir` for each run. The harness refuses non-empty
trace directories, and the verifier rejects files that are not listed in
`trace_manifest.json`.

The generated `summary.csv` has one row per focused case:

- `region_native_filter_projection`
- `region_unsupported_unnest_boundary`
- `sql_equivalence_matrix`

Each case also emits event, cumulative counter, and kernel-counter CSVs.
`test_surface_coverage.csv` inventories the checked-in compiled-region
sqllogictest files and every `TEST_CASE` in `test/api/test_jit.cpp`, mapping
each surface to the unit/sqllogictest route and any focused trace cases that
exercise the same flow.
`flow_step_summary.csv` groups each focused case by target, phase, status,
execution mode, policy decision, candidate shape, and candidate ABI, then
reconciles event counts, stage timing, runtime rows, kernel reachability, and
boundary counters against the raw event and kernel-counter CSVs.
The trace directory includes `trace_manifest.json`, which records the schema
version, configuration, database ownership mode, run-owned artifacts, CSV
columns, row counts, byte sizes, and content hashes. The verifier checks the
manifest and test-surface coverage before reading trace content, then checks
validation results, flow-step reconciliation, expected compiled/unsupported
surfaces, runtime counters, nonzero native code, deterministic region pipeline
shape and ABI, IR presence for generated code, event ID ordering, typed scalar
IR coverage, complex-type boundary IR coverage, and compiled
projection-sink/filter-projection-sink region shapes.

The architecture verifier checks source-level boundaries: database-owned
execution-region manager registration, static `jit_sljit` integration,
core-owned source contracts, manifest-backed trace-contract presence,
full-pipeline/source contracts, and reverse dependencies between core execution
region code, backend code, and DuckDB executor internals.
