# DuckDB JIT Verification

This directory contains the local guardrails for execution-region JIT work.
Architecture and policy rules live in
`benchmark/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`.

## Commands

Architecture verifier:

```sh
python3 benchmark/jit/verify_jit_architecture.py
```

Refactor guard:

```sh
python3 benchmark/jit/run_jit_refactor_guard.py
```

Install repo-local guard hooks:

```sh
python3 benchmark/jit/install_refactor_guard_hooks.py
```

TPC-H benchmark and comparison gate:

```sh
python3 benchmark/tpch/jit/tpch_benchmark.py --duckdb build/reldebug/duckdb --queries all --policies off auto
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --queries all
```

Generic production workload gate:

```sh
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 4
```

The generic gate covers arithmetic, filters, CASE-heavy expressions, multiple
aggregate lanes, filtered scans, persistent column-vs-column comparisons,
single- and multi-source nullable persistent scans, grouped DISTINCT, dense
computed grouping, sorted-run grouping, and joins. Arithmetic, CASE,
multi-aggregate, scan-expression, scan-filter, column-comparison, both nullable
classes, and the proven grouped workloads require compiled speedups at their
configured thread counts. The column-comparison gate requires at least 1.25x
at both one and four threads. Join workloads without a proven compiled route
have a bounded auto-policy slowdown and may remain vectorized.
Short production failures receive an automatic focused high-sample recheck
before the gate decides.

Runtime proof is a separate traced mode:

```sh
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --trace-runtime
```

Traced mode verifies that compiled regions execute and records runtime paths;
it does not enforce performance thresholds. Use production timing for
regression decisions.
