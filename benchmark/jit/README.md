# DuckDB Execution-Region Verification

This directory keeps the execution-region verification surface intentionally
small:

- `verify_jit_architecture.py` checks source-level architecture invariants.
- `compile_overhead_benchmark.py` measures small-query decision and compile overhead
  with focused execution-region workloads.
- `direct_ctas_benchmark.py` measures CTAS direct materialization fast paths for
  FLOAT, DOUBLE, homogeneous INTEGER, mixed INTEGER/BIGINT fused groups,
  DECIMAL64 fused groups, DATE fused groups, and mixed fixed-width TPC-H-like
  projections with and without VARCHAR source append.
- `benchmark/tpch/jit/tpch_benchmark.py` is the canonical correctness,
  performance, and profiling harness for TPC-H execution regions.

Run the architecture verifier directly:

```sh
python3 benchmark/jit/verify_jit_architecture.py
```

Run the TPC-H harness when validating performance or region-selection behavior:

```sh
python3 benchmark/tpch/jit/tpch_benchmark.py --duckdb build/reldebug/duckdb --queries 1 6 12 --policies off auto --out-dir /tmp/duckdb_jit_tpch_benchmark
python3 benchmark/tpch/jit/verify_tpch_benchmark.py /tmp/duckdb_jit_tpch_benchmark
```

Run the CTAS direct-materialization harness when changing projection append codegen:

```sh
python3 benchmark/jit/direct_ctas_benchmark.py --rows 5000000 --policies off auto --out-dir /tmp/duckdb_jit_direct_ctas
```

`direct_ctas_benchmark.py` writes `summary.csv`, `runs.csv`, and `counters.csv`.
By default it uses coverage-style JIT CBO settings so the direct-materialization
stages are reached and verified. Use `--production-cbo` to measure the current
production CBO decision instead. The harness fails when AUTO silently misses the
expected generated stage:

- FLOAT/DOUBLE: `op0:projection.direct_materialize_generated`
- homogeneous INTEGER, mixed INTEGER/BIGINT groups, DECIMAL64 groups, and DATE groups: `op0:projection.direct_materialize_fixed_fused_generated`
- mixed fixed-width, including VARCHAR source append: `op0:projection.direct_materialize_fixed_generated`

Run the compile-overhead harness when optimizing planner or SLJIT code generation:

```sh
python3 benchmark/jit/compile_overhead_benchmark.py --policies off auto --out-dir /tmp/duckdb_jit_compile_overhead
```

`compile_overhead_benchmark.py` writes `summary.csv`, `runs.csv`, and
`counters.csv` for scalar filter/projection, filtered aggregate, grouped
aggregate, hash join probe, and full-pipeline scan/project/sink workloads.

`tpch_benchmark.py` defaults to `--timing-mode=production`. In that mode the
measured query uses the DuckDB shell timer without detailed JSON profiling, while
correctness checks and `duckdb_jit_counters()` still run after the timed
statement. Use `--timing-mode=profile` only when the profiler JSON itself is the
artifact under inspection.

`tpch_benchmark.py` writes:

- `summary.csv`: per-query/per-policy timing and execution-region totals.
- `runs.csv`: one row per measured query execution.
- `counters.csv`: raw typed `duckdb_jit_counters()` rows annotated with query, policy, and repeat.
- `performance_gaps.csv`: query-by-query performance summary with AUTO speedup, compiled coverage,
  primary AUTO blocker, and aggregate AUTO runner-cost benefit/cost/net-benefit evidence.

The verifier fails by default when AUTO makes JIT decisions and drops below
0.98x of OFF median runtime. When AUTO makes no JIT decisions, the verifier
applies an absolute 5 ms median slowdown noise floor instead; use
`--auto-no-decision-noise-s` to adjust that tolerance for a specific machine.
Use `--min-auto-speedup` to tighten or relax the compiled-decision gate.

Production `auto` stays on DuckDB's vectorized path unless core execution has
selected the compiled-vectorized runner. Benchmark artifacts expose the selected runner,
region shape, blocker class, and runtime breakdown from the same core execution
selection path.
