# DuckDB Execution-Region Verification

This directory keeps the execution-region verification surface intentionally
small:

- `verify_jit_architecture.py` checks source-level architecture invariants.
- `benchmark/tpch/jit/tpch_benchmark.py` is the canonical correctness,
  performance, and profiling harness for TPC-H execution regions.

Run the architecture verifier directly:

```sh
python3 benchmark/jit/verify_jit_architecture.py
```

Run the TPC-H harness when validating performance or region-selection behavior:

```sh
python3 benchmark/tpch/jit/tpch_benchmark.py --policies off auto force --out-dir /tmp/duckdb_jit_tpch_benchmark
python3 benchmark/tpch/jit/verify_tpch_benchmark.py /tmp/duckdb_jit_tpch_benchmark
```

`tpch_benchmark.py` defaults to `--timing-mode=production`. In that mode the
measured query uses the DuckDB shell timer without detailed JSON profiling, while
correctness checks and `duckdb_jit_counters()` still run after the timed
statement. Use `--timing-mode=profile` only when the profiler JSON itself is the
artifact under inspection.

`tpch_benchmark.py` writes:

- `summary.csv`: per-query/per-policy timing and execution-region totals.
- `runs.csv`: one row per measured query execution.
- `counters.csv`: raw typed `duckdb_jit_counters()` rows annotated with query, policy, and repeat.
- `performance_gaps.csv`: query-by-query performance summary with AUTO/FORCE speedups, compiled coverage,
  primary AUTO blocker, aggregate AUTO runner-cost benefit/cost/net-benefit, and FORCE planning/compile/code-size
  evidence.

Production `auto` stays on DuckDB's vectorized path unless core execution has
selected the compiled-vectorized runner. Benchmark artifacts expose the selected runner,
region shape, blocker class, and runtime breakdown from the same core execution
selection path.
