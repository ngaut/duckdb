# JIT Broad-Workload Gate

Last updated: 2026-07-12

This file records the current TPC-H milestone and verification commands. The
generic production contracts live in
`benchmark/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`. TPC-H is evidence, not a source
of query-specific execution routes.

## Current milestone

The active architecture work is generic grouped and join execution:

- one payload descriptor owns primitive kind, storage ABI, typed-lowering kind,
  and DuckDB aggregate-state layout;
- payload runtime and lane scratch consume descriptors directly; planner
  aggregate records stop at capability and native-sink binding;
- grouped reduction bindings validate that descriptor against live DuckDB lanes
  once in one per-operator runtime owner, then direct, dense, run, row-pointer,
  projected, and perfect-hash terminals consume the same bound contract;
- grouped execution binds one immutable perfect-hash fused, grouped-state fused,
  or grouped-state per-payload family; chunk execution does not rediscover that
  family from flags;
- exact INT128 reducers update both words of DuckDB's ordinary hugeint state;
- semantic aggregate and DISTINCT state use pipeline-local ownership, never
  disposable compiled-region scratch;
- DuckDB core CBO admits stateful recipes only when backend-neutral work facts
  cover startup and runtime ownership can prove the selected work.

## Accepted SF1 evidence

Configuration: TPC-H SF1, one thread, production timing, tracing disabled, 31
alternating repeats, result verification enabled. Accepted artifact:
`benchmark/tpch/jit/tmp/sf1_baseline_refresh_20260712/promotion_recheck`.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.072 | 0.063 | 1.143x |
| Q2 | 0.011 | 0.012 | 0.917x |
| Q3 | 0.064 | 0.062 | 1.032x |
| Q4 | 0.046 | 0.042 | 1.095x |
| Q5 | 0.054 | 0.050 | 1.080x |
| Q6 | 0.042 | 0.022 | 1.909x |
| Q7 | 0.062 | 0.058 | 1.069x |
| Q8 | 0.044 | 0.041 | 1.073x |
| Q9 | 0.155 | 0.130 | 1.192x |
| Q10 | 0.069 | 0.063 | 1.095x |
| Q11 | 0.013 | 0.013 | 1.000x |
| Q12 | 0.071 | 0.063 | 1.127x |
| Q13 | 0.151 | 0.139 | 1.086x |
| Q14 | 0.057 | 0.054 | 1.056x |
| Q15 | 0.039 | 0.030 | 1.300x |
| Q16 | 0.032 | 0.030 | 1.067x |
| Q17 | 0.031 | 0.031 | 1.000x |
| Q18 | 0.120 | 0.093 | 1.290x |
| Q19 | 0.033 | 0.031 | 1.065x |
| Q20 | 0.066 | 0.062 | 1.065x |
| Q21 | 0.118 | 0.106 | 1.113x |
| Q22 | 0.024 | 0.022 | 1.091x |

All results are correct. Auto compiles 20 queries and deliberately keeps Q2
and Q11 vectorized because their stateful work is below the generic startup
floor. Summed medians improve from 1.374s to 1.217s (1.129x); the per-query
geometric-mean speedup is 1.118x. The gate preserves 18 material JIT wins with
no accepted-baseline regression.

## Accepted SF10 evidence

Configuration: TPC-H SF10, one thread, production timing, tracing disabled, 31
alternating repeats, result verification enabled. Accepted artifact:
`benchmark/tpch/jit/tmp/sf10_baseline_refresh_20260712/promotion_recheck`.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.659 | 0.560 | 1.177x |
| Q2 | 0.064 | 0.058 | 1.103x |
| Q3 | 0.638 | 0.600 | 1.063x |
| Q4 | 0.503 | 0.470 | 1.070x |
| Q5 | 0.597 | 0.530 | 1.126x |
| Q6 | 0.392 | 0.193 | 2.031x |
| Q7 | 0.583 | 0.545 | 1.070x |
| Q8 | 0.412 | 0.355 | 1.161x |
| Q9 | 1.761 | 1.389 | 1.268x |
| Q10 | 1.000 | 0.876 | 1.142x |
| Q11 | 0.071 | 0.068 | 1.044x |
| Q12 | 0.640 | 0.565 | 1.133x |
| Q13 | 1.772 | 1.723 | 1.028x |
| Q14 | 0.422 | 0.387 | 1.090x |
| Q15 | 0.356 | 0.246 | 1.447x |
| Q16 | 0.186 | 0.161 | 1.155x |
| Q17 | 0.283 | 0.279 | 1.014x |
| Q18 | 1.485 | 0.971 | 1.529x |
| Q19 | 0.306 | 0.278 | 1.101x |
| Q20 | 0.619 | 0.539 | 1.148x |
| Q21 | 1.427 | 1.297 | 1.100x |
| Q22 | 0.191 | 0.159 | 1.201x |

All results are correct and all 22 queries execute compiled regions with traced
runtime ownership. Summed medians improve from 14.367s to 12.249s (1.173x),
the per-query geometric-mean speedup is 1.175x, and the gate records 21
material JIT wins. Q11 now lowers its widening decimal product and exact
INT128 sum to native machine code; the same recipe is rejected at SF1 because
there are not enough batches to amortize stateful startup.

## Performance direction

JIT wins by deleting real execution work:

- join-output and projection materialization;
- copied selected batches and state-address vectors;
- repeated grouped hash probes when dense or run-compressed deltas suffice;
- repeated validity, cast, and selection work after a typed proof;
- per-row calls that can be represented as vector-sized primitive updates.
- independent code handles, entry points, and lazy-publication flags that can
  drift across runtime specializations;
- benchmark-side reconstruction of runtime obligations already known by CBO.

CBO tuning must expose real backend capability and publish its typed runtime
proof ledger. Gates consume that ledger and must not hide a slow selected route
or name a benchmark query.

## Required verification

For every runtime, planner, backend, or performance change:

```bash
python3 benchmark/jit/verify_jit_architecture.py
cmake --build build/reldebug -j12
build/reldebug/test/unittest "[jit]"
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 1
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 4
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --no-build
```

Promote a baseline only from a complete, correct, production-mode high-sample
run. Promotion is a ratchet, not permission to accept a regression:

```bash
python3 benchmark/tpch/jit/run_tpch_regression_gate.py \
  --promote-baseline --promotion-repeats 31 --no-build
```

When a selected direction is architecturally correct but misses the performance
bar, profile the generated and source stages, fix the owning layer, and rerun
the complete gate. Do not weaken correctness or accounting thresholds.
