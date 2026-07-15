# JIT Broad-Workload Gate

Last updated: 2026-07-15

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
- perfect-hash string groups retain dictionary-domain identity in the aggregate
  runtime owner; generated lookup caches normalized group contributions by
  dictionary source index and invalidates only observed entries when the
  dictionary has no persistent identity;
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
- selected one-key regular hash probes compose a persisted range proof with the
  runtime no-chain layout, reading BIGINT source keys directly against a
  compressed INTEGER table. The loop preserves selection, Bloom, salt, prefetch,
  and row-pointer semantics without relation or benchmark identity;
- low-cardinality complementary string reductions keep a bounded eight-group
  direct tier in their pipeline-local accumulator, retain a fixed-capacity hash
  fallback, and use one fused all-valid 16-byte RHS matcher rather than a
  Cartesian specialization matrix;
- profitable fixed-width grouped runs execute in a resumable backend-generated
  kernel over one operator-lifetime pending owner shared by projected and
  materialized direct inputs; exact, proven narrowing, and integral-compression
  key specializations share one runtime ABI. Pipeline-local state preserves the
  unpublished boundary group across fairness yields, while bounded exact
  intervals coalesce to a conservative hull at capacity so parallel radix
  finalization can still distinguish provably disjoint ownership from real
  cross-worker duplicates without unbounded proof state. Single-lane kernels
  retain tuned all-valid and nullable variants; a generated multi-lane kernel
  binds one cached runtime lane array and handles nullable primitive payloads
  without a specialization matrix. Unsupported shapes fall back before
  publishing pending state;
- a complete untraced 10-repeat candidate is itself promotion-qualified. If a
  focused 10-repeat noise recheck clears a comparison, the gate merges those
  query rows and revalidates the complete artifact instead of rerunning the
  unchanged 22-query matrix.

## Accepted SF1 evidence

Configuration: TPC-H SF1, one thread, production timing, tracing disabled, and
result verification enabled. The accepted ten-repeat comparison receipt is
`benchmark/tpch/jit/tmp/dictionary_group_owner_scope_full_sf1_promotion10_20260715`.
Candidate gates use five repeats. A focused ten-repeat triage or promotion is
an explicit follow-up command; no failed candidate schedules it automatically.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.070 | 0.056 | 1.268x |
| Q2 | 0.011 | 0.011 | 0.946x |
| Q3 | 0.061 | 0.059 | 1.029x |
| Q4 | 0.044 | 0.040 | 1.105x |
| Q5 | 0.052 | 0.047 | 1.094x |
| Q6 | 0.039 | 0.020 | 1.964x |
| Q7 | 0.059 | 0.055 | 1.062x |
| Q8 | 0.042 | 0.040 | 1.054x |
| Q9 | 0.146 | 0.112 | 1.306x |
| Q10 | 0.063 | 0.055 | 1.134x |
| Q11 | 0.012 | 0.012 | 0.976x |
| Q12 | 0.069 | 0.054 | 1.283x |
| Q13 | 0.143 | 0.092 | 1.552x |
| Q14 | 0.051 | 0.049 | 1.053x |
| Q15 | 0.038 | 0.028 | 1.323x |
| Q16 | 0.031 | 0.030 | 1.039x |
| Q17 | 0.030 | 0.030 | 1.010x |
| Q18 | 0.101 | 0.055 | 1.831x |
| Q19 | 0.031 | 0.029 | 1.065x |
| Q20 | 0.064 | 0.060 | 1.076x |
| Q21 | 0.112 | 0.101 | 1.119x |
| Q22 | 0.023 | 0.021 | 1.088x |

All results are correct. Auto compiles 20 queries and deliberately keeps Q2
and Q11 vectorized because their stateful work is below the generic startup
floor. Summed medians improve from 1.292s to 1.056s (1.224x), the per-query
geometric-mean speedup is 1.176x, and the gate reports 18 material wins. Q1 is
1.268x, Q9 is 1.306x, Q13 is 1.552x, and Q18 is 1.831x.

## Accepted SF10 evidence

Configuration: TPC-H SF10, one thread, production timing, tracing disabled, and
result verification enabled. The accepted ten-repeat comparison receipt is
`benchmark/tpch/jit/tmp/nullable_codegen_full_sf10_promotion10_20260713`.
Candidate gates use five repeats. A focused ten-repeat triage or promotion is
an explicit follow-up command; no failed candidate schedules it automatically.

| Query | Non-JIT (s) | JIT (s) | Speedup |
| ---: | ---: | ---: | ---: |
| Q1 | 0.647 | 0.491 | 1.316x |
| Q2 | 0.060 | 0.055 | 1.099x |
| Q3 | 0.614 | 0.567 | 1.083x |
| Q4 | 0.471 | 0.438 | 1.076x |
| Q5 | 0.533 | 0.489 | 1.092x |
| Q6 | 0.381 | 0.179 | 2.134x |
| Q7 | 0.548 | 0.510 | 1.075x |
| Q8 | 0.385 | 0.330 | 1.167x |
| Q9 | 1.636 | 1.202 | 1.362x |
| Q10 | 0.945 | 0.807 | 1.170x |
| Q11 | 0.069 | 0.066 | 1.051x |
| Q12 | 0.621 | 0.482 | 1.288x |
| Q13 | 1.601 | 0.937 | 1.708x |
| Q14 | 0.391 | 0.357 | 1.097x |
| Q15 | 0.332 | 0.228 | 1.457x |
| Q16 | 0.172 | 0.149 | 1.155x |
| Q17 | 0.268 | 0.249 | 1.077x |
| Q18 | 1.274 | 0.767 | 1.660x |
| Q19 | 0.293 | 0.264 | 1.111x |
| Q20 | 0.599 | 0.519 | 1.154x |
| Q21 | 1.361 | 1.220 | 1.115x |
| Q22 | 0.181 | 0.141 | 1.284x |

All results are correct and all 22 queries execute compiled regions with traced
runtime ownership. Summed medians improve from 13.382s to 10.445s (1.281x), the
per-query geometric-mean speedup is 1.238x, and all 22 queries are material JIT
wins. Q9 is 1.362x, Q13 is 1.708x, and Q18 is 1.660x. Q11 lowers its widening
decimal product and exact INT128 sum to native machine code; the same recipe is
rejected at SF1 because there are not enough batches to amortize stateful
startup.

The generic production matrix is sourced directly from
`benchmark/jit/generic_benchmark.py`; this plan intentionally does not mirror a
hard-coded workload count. The post-review
five-repeat artifacts are
`benchmark/jit/tmp/full_generic_review_root_fixes_t1_candidate5_20260713` and
`benchmark/jit/tmp/full_generic_review_root_fixes_t4_candidate5_20260713`; both
pass correctness and raw-runtime gates. A separate 16-lane grouped-run
promotion proves that bounded code generation remains a win outside TPC-H:
1.223x at one thread and 1.311x at four threads over ten repetitions, with
1.15x and 1.20x checked-in floors.

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
