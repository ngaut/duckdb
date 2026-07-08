# JIT Production Recipe Runtime Design

Last updated: 2026-07-07

This is the active architecture contract for DuckDB execution-region JIT. It is
not a history log. Source, tests, counters, and gates are the implementation
truth.

## Goal

JIT is a DuckDB execution backend for first-class execution regions. It must win
through generic capability facts and bound primitives, not TPC-H-shaped routes.

Production flow:

```text
DuckDB physical pipeline
  -> execution-region IR
  -> backend capability facts
  -> CBO runner selection
  -> SLJIT recipe binding
  -> bound primitive execution over runtime batch views
```

## Hard Contracts

- Recipe binding is the admission boundary.
- A compiled recipe must have a non-empty primitive sequence.
- Every primitive must pass its binding contract before execution.
- Native-only execution is explicit plan state, not an empty recipe.
- CBO facts and runtime counters must agree.
- CBO must not credit work that runtime counters cannot prove.
- Unsupported work must surface as a blocker, explicit terminal boundary, or counter.
- Production JIT code must not mention query numbers, table names, column names,
  or benchmark-specific shape strings.

## Runtime Batch View

All selected, materialized, and source-owned flows pass row ownership through
`SljitRuntimeBatchView`.

Consumers must read the producer view for:

- selected hash-join consumers;
- post-join projection chains;
- projection-fed aggregate updates;
- mark-filter boundaries;
- terminal handoff.

Full output materialization is allowed only when a producer cannot publish the
view needed by the next primitive.

## Primitive Ownership

Each primitive owns its input view, output view, advancement contract, and
runtime counters. Execution runs the bound sequence directly and must not
rediscover route predicates per batch.

Current primitive families:

- `ProjectionChain`
- `GeneratedFilter`
- `HashJoinProbe`
- `MarkProbeFilterBoundary`
- `GroupedAggregateUpdate`
- `ProjectionAggregate`
- `NativeTailDelegation`

## Aggregate Backends

Grouped aggregate updates use explicit strategies:

- direct primitive payload update;
- filtered primitive payload update;
- count-star preaggregation;
- row-pointer grouped update over native state addresses;
- perfect-hash grouped update when the domain contract is valid;
- explicit `DISTINCT_KEY_SINK` for delegated distinct-key sinks.

Regular hash aggregate lookup is native state-address resolution plus generated
payload update. It must not be modeled as generated lookup until a generated
lookup backend exists.

Distinct aggregate lookup remains DuckDB-owned until a real generated distinct
backend exists. Mixed distinct/non-distinct, filtered distinct, and ordered
aggregate cases must surface as explicit delegated-terminal work.
Computed distinct-key input projections are generated work, not proof of
materialization elision by themselves; the generated distinct backend is the
right layer to remove those projection batches.

## Hash Join Backends

Hash join generated work is admitted from key-source and probe-layout facts:

- key count and physical layout;
- all-valid input;
- selected or unselected runtime view;
- chain or no-chain table layout;
- bloom, salt, dictionary, mark-build, and output-mode facts.

Regular and perfect probe paths must load keys from the current runtime view and
must preserve cast-overflow semantics before probe filtering.

## MARK Boundary

MARK joins followed by filter/projection publish a selected-view boundary that
downstream terminal recipes can consume:

```text
HashJoinProbe
  -> MarkProbeFilterBoundary
  -> ProjectionChain*
  -> TerminalRecipe
```

Positive marker filters can publish an LHS-only selected view when downstream
projection does not reference marker semantics. Negative/null-aware marker
filters require a separate capability.

## CBO Inputs

CBO uses backend-neutral facts:

- exact source cardinality when proven;
- physical cardinality estimates for unknown-output source contracts;
- source-contract input cardinality for DuckDB-owned scan-filter work;
- generated expression/backend stage counts;
- native operator/stage counts;
- materialization elision count;
- selected hash-join filter materialization penalty;
- materializing hash-build sink protocol penalty;
- native grouped state-address lookup penalty;
- compile/startup cost.

Query regressions are backend optimization targets, not reasons to hide selected
regions.

## Verification

Required gates:

```sh
python3 benchmark/jit/verify_jit_architecture.py
python3 benchmark/jit/run_jit_refactor_guard.py --level unit
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --baseline <accepted-baseline>
```

Runtime/materialization-elision contract checks live in
`benchmark/tpch/jit/verify_tpch_benchmark.py` and `test/api/test_jit_helpers.hpp`.

## Remaining Gaps

- Selected probe-to-grouped-aggregate paths need deeper direct ownership.
- Distinct aggregates need a generated backend.
- String and variable-width cases still delegate too much work.
- Some native source/sink protocols still leak into selected generated paths.
- Large helper files should continue to split by primitive ownership after tests
  cover the boundary.
