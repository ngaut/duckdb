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

The hooks divide responsibility instead of repeating the same work.
Pre-commit validates the staged tree with the build, architecture, Python, and
JIT unit ratchet, then publishes the verified Git tree hash. When HEAD matches
that receipt, pre-push reuses the result and adds only the generic and TPC-H
production gates required by performance-sensitive branch changes. A missing
or stale receipt makes pre-push run the complete guard. Candidate timing gates
use five alternating repetitions; focused triage and promotion use ten.

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
aggregate lanes, filtered scans, mixed numeric/date plus nullable-string
predicates, persistent column-vs-column comparisons, single- and multi-source
nullable persistent scans, grouped DISTINCT, dense computed multi-aggregate
grouping, sorted-run grouping, and joins. Arithmetic, CASE, multi-aggregate,
scan-expression, scan-filter, mixed-predicate, column-comparison, both nullable
classes, and the proven grouped workloads require compiled speedups at their
configured thread counts. The mixed-predicate gate requires at least 1.25x at
one thread and 1.20x at four threads. The column-comparison gate requires at
least 1.25x at both one and four threads. Dense multi-aggregate grouping
requires at least 1.80x at one thread and 1.60x at four threads; sorted-run
grouping requires 1.60x and 1.35x respectively. Join workloads without a proven
compiled route have a bounded auto-policy slowdown and may remain vectorized.
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
Read-only fixtures are built once per shared setup and reopened for each sample;
the timed policies no longer rebuild multi-million-row tables. The selective
grouped multi-aggregate floors are 1.22x at one thread and 1.17x at four threads,
against independent ten-repeat proofs no lower than 1.238x and 1.195x. The
two-way conjunction carries 1.31x and 1.25x floors; the three-way variant adds
1.25x and 1.20x floors. The neighboring non-null grouped workload has 1.16x and
1.13x thread-specific floors. The mixed-predicate promotion proves 1.338x at
one thread and 1.286x at four threads.
Generated sorted-run aggregation proves 1.680x at one thread and 1.425x at four
threads over ten alternating production repetitions; its checked-in floors are
1.60x and 1.35x. The backend kernel streams flat, all-valid fixed-width group
keys and one primitive aggregate lane into fixed-capacity pending storage, while
unsupported selections, nulls, casts, and lane shapes retain the vectorized
fallback.
Hybrid SIMD admission uses one shared scalar-operation cost contract: fully
packed select/count/sum kernels retain simple comparisons, while scalar-terminal
hybrids require enough predicate work to amortize mask dispatch. AND hybrids
evaluate their packed masks branchlessly and classify only the completed mask;
OR remains available to fully packed kernels but stays scalar in hybrids after a
neutral matched proof. Mixed masks visit only set lanes with count-trailing-zero
iteration. Specialized predicates may use the same loop for the longest ordered
SIMD-supported AND prefix, then execute their generic residual directly without
an intermediate selection vector.

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
