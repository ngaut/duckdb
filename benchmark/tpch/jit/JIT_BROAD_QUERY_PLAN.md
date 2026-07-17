# JIT Broad-Workload Plan

Last updated: 2026-07-17

This file defines the current broad-workload direction and TPC-H qualification
contract. Stable architecture lives in
`benchmark/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`; measured values live in the
accepted CSV artifacts. TPC-H is evidence, never an execution-route identity.

## Current direction

Improve generic execution by removing physical work at semantic boundaries:

- bind one immutable primitive recipe instead of rediscovering shape at
  runtime;
- finalize recipe grammar and all runtime ownership metadata in one pass;
- publish that recipe once from executable binding and move the same tagged
  plan into the kernel;
- reduce regular hash matches directly into eligible ungrouped aggregate
  states without a matched row-pointer batch;
- expose row and dictionary RHS storage through one backend-neutral core ABI;
- reuse operator-lifetime payload, group-source, and reduction-lane bindings;
- retain generated grouped-run state across scheduler yields;
- use exact range, ordering, nullability, and ownership proofs to remove
  repeated per-row checks;
- keep native execution when capability or startup economics cannot prove a
  compiled win.

The current direct regular-hash terminal supports semantic `COUNT(*)`,
`COUNT(rhs)`, and BIGINT `SUM(rhs)` over both row and dictionary RHS storage.
The recipe records probe, optional filter, join, aggregate, and terminal
identities. The canonical aggregate descriptor records RHS output identity and
type. Runtime resolves only the current physical source and aggregate state.
Pipeline-local dispatch caches stable materialized routes, keeps direct routes
for eligible chunks, and becomes hybrid only when a later physical input shape
requires the materialized implementation.

## Workload coverage

TPC-H qualification is paired with the generic matrix. The generic suite is
the protection against query-specific architecture and covers:

- arithmetic, CASE, filters, and nullable expressions;
- persistent scans and column comparisons;
- packed and scalar-terminal predicate paths;
- grouped, DISTINCT, perfect-hash, projected, affine, nullable, sparse, and
  wide-lane aggregates;
- selected and identity vector views;
- numeric and string joins;
- one-thread and parallel execution.

Workload definitions, speedup floors, and raw runtime ceilings live in
`benchmark/jit/generic_benchmark.py`. This document does not duplicate their
counts or thresholds.

## Accepted TPC-H evidence

Accepted SF1 and SF10 states are local, ignored, and relocatable:

- SF1: `benchmark/tpch/jit/local_baselines/tpch_refactor_guard_sf1_state.json`
- SF10: `benchmark/tpch/jit/local_baselines/tpch_refactor_guard_state.json`

Each state binds all 22 queries, one thread, scale factor, production timing,
repeat count, and relevant JIT configuration. Its relative `current_baseline`
points to the complete accepted artifact containing `summary.csv`, `runs.csv`,
`counters.csv`, and `performance_gaps.csv`.

The CSVs are the source of truth for JIT-versus-native tables. Do not paste
rounded timing snapshots into this plan; they drift after every valid baseline
promotion.

## Qualification protocol

A candidate uses five alternating policy pairs. Promotion uses ten. No failed
candidate automatically acquires more samples.

For every query, the gate requires:

- identical SQL results;
- no compile errors;
- raw JIT-auto runtime within its independent accepted ceiling;
- preserved speedup and component contracts;
- runtime proof for every compiled or accelerated-runner-selected query.

`jit_policy=off` normalization is secondary noise evidence and cannot make a
raw JIT regression pass. Traced proof runs are separate from production timing.

## Commands

Build and correctness:

```sh
cmake --build build/reldebug --config RelWithDebInfo -j12
python3 benchmark/jit/verify_jit_architecture.py
build/reldebug/test/unittest "[jit]"
```

Generic candidates:

```sh
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 1 --repeats 5
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 4 --repeats 5
```

TPC-H SF10 and SF1 candidates:

```sh
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --no-build --queries all --repeats 5
python3 benchmark/tpch/jit/run_tpch_regression_gate.py \
  --no-build --queries all --scale-factor 1 --repeats 5 \
  --baseline-state benchmark/tpch/jit/local_baselines/tpch_refactor_guard_sf1_state.json
```

Promote only after the complete candidate, artifact verification, comparison,
and runtime proof pass:

```sh
python3 benchmark/tpch/jit/run_tpch_regression_gate.py \
  --no-build --queries all --promote-baseline --promotion-repeats 10
```

When an architecturally correct direction misses the performance bar, profile
the generated and source stages, fix the owning layer, and rerun the complete
gate. Do not weaken correctness, runtime proof, or raw-runtime ceilings.
