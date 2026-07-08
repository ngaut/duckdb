# DuckDB JIT Verification

This directory contains the local guardrails for execution-region JIT work.
Architecture and policy rules live in
`benchmark/tpch/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`.

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

Use profile timing only for attribution artifacts. Use production timing for
regression decisions.
