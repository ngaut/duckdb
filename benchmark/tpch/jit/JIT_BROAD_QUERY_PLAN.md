# JIT Broad-Workload Gate

Last updated: 2026-07-13

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
- selected hash-join output carries one backend-neutral proof object for
  selection identity, exact RHS aliases, and safe key narrowing; filters
  invalidate only the facts they actually change;
- low-cardinality generated string search is rejected from physical-pipeline
  facts before graph construction, IR lowering, or backend analysis;
- hybrid SIMD is selected only when the packed predicate has enough work to
  amortize mask setup before the remaining scalar grouped update.

## Accepted SF1 evidence

Configuration: TPC-H SF1, one thread, production timing, tracing disabled, and
result verification enabled. This historical artifact used 31 alternating
repeats and remains only as the accepted comparison receipt:
`benchmark/tpch/jit/tmp/sf1_proof_batch_pregraph_simd_promotion_20260713/promotion_recheck`.
Current candidate gates use five repeats and focused triage or promotion uses
ten; the workflow never reruns 31 repetitions.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.072 | 0.058 | 1.244x |
| Q2 | 0.011 | 0.011 | 0.942x |
| Q3 | 0.062 | 0.060 | 1.022x |
| Q4 | 0.045 | 0.040 | 1.100x |
| Q5 | 0.053 | 0.049 | 1.086x |
| Q6 | 0.040 | 0.021 | 1.965x |
| Q7 | 0.060 | 0.056 | 1.076x |
| Q8 | 0.042 | 0.040 | 1.051x |
| Q9 | 0.148 | 0.121 | 1.221x |
| Q10 | 0.065 | 0.057 | 1.148x |
| Q11 | 0.012 | 0.012 | 0.972x |
| Q12 | 0.070 | 0.062 | 1.133x |
| Q13 | 0.145 | 0.094 | 1.549x |
| Q14 | 0.052 | 0.049 | 1.044x |
| Q15 | 0.038 | 0.029 | 1.324x |
| Q16 | 0.031 | 0.029 | 1.070x |
| Q17 | 0.030 | 0.030 | 1.013x |
| Q18 | 0.105 | 0.087 | 1.198x |
| Q19 | 0.031 | 0.029 | 1.067x |
| Q20 | 0.065 | 0.061 | 1.076x |
| Q21 | 0.115 | 0.103 | 1.116x |
| Q22 | 0.023 | 0.021 | 1.103x |

All results are correct. Auto compiles 20 queries and deliberately keeps Q2
and Q11 vectorized because their stateful work is below the generic startup
floor. Summed medians improve from 1.314s to 1.119s (1.175x); the per-query
geometric-mean speedup is 1.144x. The promoted gate increases material JIT
wins from 18 to 19 with no accepted-baseline regression.

## Accepted SF10 evidence

Configuration: TPC-H SF10, one thread, production timing, tracing disabled, and
result verification enabled. This historical artifact used 31 alternating
repeats and remains only as the accepted comparison receipt:
`benchmark/tpch/jit/tmp/sf10_proof_batch_pregraph_simd_promotion_20260713/promotion_recheck`.
Current candidate gates use five repeats and focused triage or promotion uses
ten; the workflow never reruns 31 repetitions.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.651 | 0.503 | 1.295x |
| Q2 | 0.063 | 0.057 | 1.099x |
| Q3 | 0.629 | 0.581 | 1.083x |
| Q4 | 0.485 | 0.449 | 1.081x |
| Q5 | 0.550 | 0.500 | 1.100x |
| Q6 | 0.384 | 0.187 | 2.054x |
| Q7 | 0.559 | 0.523 | 1.069x |
| Q8 | 0.391 | 0.334 | 1.172x |
| Q9 | 1.694 | 1.276 | 1.328x |
| Q10 | 0.962 | 0.824 | 1.167x |
| Q11 | 0.070 | 0.067 | 1.050x |
| Q12 | 0.628 | 0.558 | 1.126x |
| Q13 | 1.661 | 1.081 | 1.537x |
| Q14 | 0.401 | 0.367 | 1.092x |
| Q15 | 0.341 | 0.235 | 1.452x |
| Q16 | 0.176 | 0.154 | 1.142x |
| Q17 | 0.271 | 0.251 | 1.080x |
| Q18 | 1.311 | 0.947 | 1.385x |
| Q19 | 0.296 | 0.268 | 1.105x |
| Q20 | 0.609 | 0.529 | 1.152x |
| Q21 | 1.392 | 1.250 | 1.114x |
| Q22 | 0.185 | 0.141 | 1.311x |

All results are correct and all 22 queries execute compiled regions with traced
runtime ownership. Q22 is ratcheted from the focused ten-repeat artifact
`benchmark/tpch/jit/tmp/q22_derived_in_stats_fix_promotion10_20260713`; its two
compiled regions are present in every JIT run. The other rows retain the
accepted all-query receipt above. With the ratcheted Q22 result, summed medians
improve from 13.710s to 11.080s (1.237x), the per-query geometric-mean speedup
is 1.211x, and the promoted gate increases
material JIT wins from 21 to all 22 queries. Q11 lowers its widening decimal
product and exact INT128 sum to native machine code; the same recipe is
rejected at SF1 because there are not enough batches to amortize stateful
startup.

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
  --promote-baseline --promotion-repeats 10 --no-build
```

When a selected direction is architecturally correct but misses the performance
bar, profile the generated and source stages, fix the owning layer, and rerun
the complete gate. Do not weaken correctness or accounting thresholds.
