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
computed multi-aggregate grouping, sorted-run grouping, and joins. Arithmetic, CASE,
multi-aggregate, scan-expression, scan-filter, column-comparison, both nullable
classes, and the proven grouped workloads require compiled speedups at their
configured thread counts. The column-comparison gate requires at least 1.25x
at both one and four threads. Dense multi-aggregate grouping requires at least
1.80x at one thread and 1.60x at four threads; sorted-run grouping requires
1.20x and 1.05x respectively. Join workloads without a proven compiled route
have a bounded auto-policy slowdown and may remain vectorized.
Short production failures receive an automatic focused high-sample recheck
before the gate decides.

Join-heavy validation also covers exact perfect-hash dynamic filters. Storage
executes exact PHJ conjuncts before generic residual predicates, and a
query-local filter/table identity lets compiled probes reuse that proof without
repeating membership work. Runtime tracing reports
`hash_join_probe.perfect_probe.exact_source_filter` when this contract fires.
The generic exact-filter join preserves a 1.15x single-thread compiled speedup;
the four-thread gate retains its separate 1.08x floor because its shorter raw
runtime has a larger proportional noise envelope.

When a production run verifies a durable performance improvement, the same
increment must tighten the corresponding checked-in speedup floor or refresh
the accepted comparison artifact. A performance change is incomplete while
the regression gate still accepts the older, slower performance level.
The selective grouped multi-aggregate floor is 1.11x at both one and four
threads after removing unprofitable hybrid SIMD setup for simple predicates.

Filtered hash-build validation covers source-filter selection composition and
direct build ingress. Runtime tracing reports `filter.selected_input_zero_copy`
when one generated filter evaluates a previous selection against the original
producer chunk, and
`hash_join_build.selected_source_view` when the terminal consumes a selection
without an intervening projection. These selected-view paths must have no
runtime delegation entries.

Workload preparation and expected-result materialization always run with JIT
off. The gate resets events and counters after preparation, immediately before
the measured query, so compiled/runtime coverage can only be credited to the
target workload.

Runtime proof is a separate traced mode:

```sh
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --trace-runtime
```

Traced mode verifies that compiled regions execute and records runtime paths;
it does not enforce performance thresholds. Use production timing for
regression decisions.
