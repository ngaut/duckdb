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

## Accepted SF1 evidence

Configuration: TPC-H SF1, one thread, production timing, tracing disabled, and
result verification enabled. The accepted ten-repeat comparison receipt is
`benchmark/tpch/jit/tmp/partial_predicate_full_sf1_promotion_20260713/promotion_recheck`.
Candidate gates use five repeats and focused triage or promotion uses ten; no
higher repetition count is scheduled.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.071 | 0.057 | 1.249x |
| Q2 | 0.010 | 0.011 | 0.921x |
| Q3 | 0.060 | 0.059 | 1.014x |
| Q4 | 0.043 | 0.040 | 1.095x |
| Q5 | 0.052 | 0.048 | 1.077x |
| Q6 | 0.039 | 0.019 | 2.022x |
| Q7 | 0.059 | 0.055 | 1.074x |
| Q8 | 0.042 | 0.040 | 1.058x |
| Q9 | 0.147 | 0.119 | 1.236x |
| Q10 | 0.062 | 0.054 | 1.162x |
| Q11 | 0.011 | 0.012 | 0.967x |
| Q12 | 0.070 | 0.053 | 1.302x |
| Q13 | 0.143 | 0.095 | 1.508x |
| Q14 | 0.050 | 0.048 | 1.036x |
| Q15 | 0.037 | 0.028 | 1.311x |
| Q16 | 0.031 | 0.028 | 1.073x |
| Q17 | 0.030 | 0.030 | 1.003x |
| Q18 | 0.102 | 0.086 | 1.177x |
| Q19 | 0.031 | 0.030 | 1.042x |
| Q20 | 0.064 | 0.059 | 1.087x |
| Q21 | 0.113 | 0.102 | 1.110x |
| Q22 | 0.023 | 0.021 | 1.091x |

All results are correct. Auto compiles 20 queries and deliberately keeps Q2
and Q11 vectorized because their stateful work is below the generic startup
floor. Summed medians improve from 1.289s to 1.093s (1.179x); the per-query
geometric-mean speedup is 1.147x. There are 18 material wins. Q3's raw JIT
runtime improves versus the previous receipt, but its faster non-JIT sample
moves the normalized ratio below the material-win threshold; this is not a JIT
runtime regression. Q12 improves from 1.133x to 1.302x.

## Accepted SF10 evidence

Configuration: TPC-H SF10, one thread, production timing, tracing disabled, and
result verification enabled. The accepted ten-repeat comparison receipt is
`benchmark/tpch/jit/tmp/partial_predicate_full_sf10_promotion_20260713/promotion_recheck`.
Candidate gates use five repeats and focused triage or promotion uses ten; no
higher repetition count is scheduled.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.650 | 0.505 | 1.287x |
| Q2 | 0.060 | 0.055 | 1.103x |
| Q3 | 0.614 | 0.564 | 1.088x |
| Q4 | 0.478 | 0.443 | 1.078x |
| Q5 | 0.542 | 0.492 | 1.101x |
| Q6 | 0.380 | 0.179 | 2.119x |
| Q7 | 0.552 | 0.514 | 1.074x |
| Q8 | 0.385 | 0.332 | 1.160x |
| Q9 | 1.654 | 1.254 | 1.319x |
| Q10 | 0.952 | 0.805 | 1.184x |
| Q11 | 0.069 | 0.066 | 1.049x |
| Q12 | 0.620 | 0.493 | 1.258x |
| Q13 | 1.604 | 1.037 | 1.548x |
| Q14 | 0.391 | 0.357 | 1.095x |
| Q15 | 0.332 | 0.232 | 1.430x |
| Q16 | 0.177 | 0.152 | 1.166x |
| Q17 | 0.267 | 0.249 | 1.073x |
| Q18 | 1.274 | 0.922 | 1.382x |
| Q19 | 0.291 | 0.264 | 1.104x |
| Q20 | 0.597 | 0.517 | 1.154x |
| Q21 | 1.350 | 1.222 | 1.105x |
| Q22 | 0.182 | 0.143 | 1.273x |

All results are correct and all 22 queries execute compiled regions with traced
runtime ownership. Summed medians improve from 13.421s to 10.796s (1.243x), the
per-query geometric-mean speedup is 1.217x, and all 22 queries are material JIT
wins. Q12 improves from 1.126x to 1.258x in the complete promotion artifact;
its focused ten-repeat proof reached 1.280x. Q11 lowers its widening decimal
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
