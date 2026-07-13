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
- specialized native predicates retain their original backend-neutral typed IR
  and remap it into the same dense executable-input ABI as scalar predicate
  lowering. A generic planner may split only the longest SIMD-supported leading
  prefix of an ordered AND; the existing specialized predicate owns the
  residual. No query, relation, or column identity participates in admission;
- partial-predicate execution is single-pass. Uniform ARM64 masks bypass a full
  movemask, mixed masks visit only set lanes with count-trailing-zero iteration,
  and the residual writes directly to one hoisted output selection. It never
  mutates `execute_sel` or builds an intermediate selection vector. Fully packed
  select/count/sum kernels retain their independent SIMD plans;
- generic benchmark fixtures are materialized once per read-only setup identity
  and reused across alternating samples, eliminating repeated table rebuilds and
  measuring stable persisted storage.
- profitable fixed-width grouped runs execute in a resumable backend-generated
  kernel over operator-lifetime fixed storage; exact and proven narrowing key
  specializations share one runtime ABI and all unsupported shapes fall back
  before publishing pending state;
- a complete untraced 10-repeat candidate is itself promotion-qualified. If a
  focused 10-repeat noise recheck clears a comparison, the gate merges those
  query rows and revalidates the complete artifact instead of rerunning the
  unchanged 22-query matrix.

## Accepted SF1 evidence

Configuration: TPC-H SF1, one thread, production timing, tracing disabled, and
result verification enabled. The accepted ten-repeat comparison receipt is
`benchmark/tpch/jit/tmp/generated_run_full_sf1_promotion_20260713`.
Candidate gates use five repeats and focused triage or promotion uses ten; no
higher repetition count is scheduled.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.072 | 0.057 | 1.271x |
| Q2 | 0.010 | 0.011 | 0.932x |
| Q3 | 0.061 | 0.061 | 0.995x |
| Q4 | 0.044 | 0.040 | 1.100x |
| Q5 | 0.052 | 0.049 | 1.061x |
| Q6 | 0.040 | 0.020 | 2.070x |
| Q7 | 0.059 | 0.056 | 1.064x |
| Q8 | 0.041 | 0.039 | 1.057x |
| Q9 | 0.146 | 0.119 | 1.222x |
| Q10 | 0.064 | 0.054 | 1.187x |
| Q11 | 0.012 | 0.012 | 0.954x |
| Q12 | 0.071 | 0.054 | 1.326x |
| Q13 | 0.142 | 0.092 | 1.539x |
| Q14 | 0.051 | 0.048 | 1.054x |
| Q15 | 0.038 | 0.028 | 1.326x |
| Q16 | 0.031 | 0.029 | 1.088x |
| Q17 | 0.030 | 0.029 | 1.020x |
| Q18 | 0.103 | 0.069 | 1.480x |
| Q19 | 0.031 | 0.029 | 1.071x |
| Q20 | 0.065 | 0.061 | 1.068x |
| Q21 | 0.114 | 0.101 | 1.120x |
| Q22 | 0.022 | 0.020 | 1.112x |

All results are correct. Auto compiles 20 queries and deliberately keeps Q2
and Q11 vectorized because their stateful work is below the generic startup
floor. Summed medians improve from 1.300s to 1.080s (1.204x), the per-query
geometric-mean speedup is 1.167x, and the gate reports 19 material wins. Q18
improves from 1.177x to 1.480x; Q12 also advances from 1.302x to 1.326x.

## Accepted SF10 evidence

Configuration: TPC-H SF10, one thread, production timing, tracing disabled, and
result verification enabled. The accepted ten-repeat comparison receipt is
`benchmark/tpch/jit/tmp/generated_run_full_sf10_promotion_20260713/accepted_baseline`.
Candidate gates use five repeats and focused triage or promotion uses ten; no
higher repetition count is scheduled.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.652 | 0.502 | 1.297x |
| Q2 | 0.060 | 0.055 | 1.104x |
| Q3 | 0.611 | 0.559 | 1.093x |
| Q4 | 0.475 | 0.435 | 1.091x |
| Q5 | 0.541 | 0.497 | 1.089x |
| Q6 | 0.386 | 0.180 | 2.142x |
| Q7 | 0.552 | 0.514 | 1.076x |
| Q8 | 0.387 | 0.332 | 1.166x |
| Q9 | 1.636 | 1.242 | 1.317x |
| Q10 | 0.944 | 0.809 | 1.167x |
| Q11 | 0.069 | 0.066 | 1.050x |
| Q12 | 0.635 | 0.481 | 1.321x |
| Q13 | 1.605 | 1.030 | 1.558x |
| Q14 | 0.395 | 0.360 | 1.098x |
| Q15 | 0.337 | 0.232 | 1.456x |
| Q16 | 0.173 | 0.148 | 1.172x |
| Q17 | 0.270 | 0.250 | 1.081x |
| Q18 | 1.282 | 0.767 | 1.671x |
| Q19 | 0.294 | 0.265 | 1.109x |
| Q20 | 0.617 | 0.525 | 1.175x |
| Q21 | 1.358 | 1.238 | 1.096x |
| Q22 | 0.184 | 0.145 | 1.273x |

All results are correct and all 22 queries execute compiled regions with traced
runtime ownership. Summed medians improve from 13.464s to 10.631s (1.266x), the
per-query geometric-mean speedup is 1.234x, and all 22 queries are material JIT
wins. Q18 improves from 1.382x to 1.671x and Q12 from 1.258x to 1.321x. Q20's
full-matrix raw timing was rechecked independently at ten repeats; the focused
comparison passed and its rows were merged before the complete accepted
artifact was revalidated. Q11 lowers its widening decimal
product and exact INT128 sum to native machine code; the same recipe is rejected
at SF1 because there are not enough batches to amortize stateful startup.

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
