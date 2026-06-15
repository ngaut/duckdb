# DuckDB JIT SQL Trace Harness

This directory contains focused trace tooling for the existing JIT SQL coverage.
TPC-H traces answer workload performance questions; this harness answers whether
the core JIT test flows still produce honest region, scalar-IR, fallback,
counter, runtime, and IR evidence.

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
- `region_unsupported_join_fallback`
- `region_resume_state_fallback`
- `sql_equivalence_matrix`

Each case also emits event, cumulative counter, and kernel-counter CSVs.
`test_surface_coverage.csv` inventories the checked-in JIT sqllogictest files
and every `TEST_CASE` in `test/api/test_jit.cpp`, mapping each surface to the
unit/sqllogictest route and any focused trace cases that exercise the same flow.
`flow_step_summary.csv` groups each focused case by target, phase, status,
execution mode, policy decision, candidate shape, and candidate scope, then
reconciles event counts, stage timing, runtime rows, kernel reachability, and
fallback counters against the raw event and kernel-counter CSVs.
The trace directory includes `trace_manifest.json`, which records the schema
version, configuration, database ownership mode, run-owned artifacts, CSV
columns, row counts, byte sizes, and content hashes. The verifier checks the
manifest and test-surface coverage before reading trace content, then checks
validation results, flow-step reconciliation, expected compiled/unsupported
surfaces, runtime counters, nonzero native code, deterministic region pipeline
shape and scope, IR presence for generated code, event ID ordering, typed scalar
IR coverage, complex-type fallback IR coverage, and compiled
projection/filter-projection region shapes.

The architecture verifier checks source-level boundaries: database-owned JIT
manager registration, static `jit_sljit` integration, core source registration,
manifest-backed trace-contract presence, microbenchmark admission inventory,
forbidden-label guards, and reverse dependencies between core JIT, backend code,
and DuckDB executor internals.
