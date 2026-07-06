# JIT Production Recipe Runtime Design

Last updated: 2026-07-06
Branch: `codex/jit-native-duckdb-core`

## Goal

The production JIT runtime must scale across workloads by composing execution
primitives from capability facts. It must not grow by adding benchmark-shaped
routes. TPC-H remains a benchmark and regression suite, not a source of runtime
policy or query-specific branches.

The target architecture is:

```text
DuckDB region IR
  -> backend capability analysis
  -> DuckDB CBO runner selection
  -> SLJIT recipe construction
  -> bound primitive execution over explicit batch views
```

Recipe construction happens before execution. The hot path runs bound primitive
calls directly; it must not search route tables, parse strings, or rediscover
semantic facts per batch.

Recipe construction is an admission boundary, not a hint. A stored recipe must
already be a non-empty executable primitive sequence whose steps passed their
`CanBind`/`Bind` contract checks. Invalid primitive sequences are construction
errors, not executor fallbacks.

The build result is a recipe plan:

```text
SljitFullPipelineRecipePlan {
  has_recipe: bool
  recipe: non-empty primitive sequence when has_recipe=true
  uses_extended_source_fetch_budget: native-source scheduling fact
}
```

An empty primitive sequence must never mean "fall back to legacy execution".
Native-only execution is explicit plan state outside the recipe. A recipe is a
bound primitive execution contract.

## Ownership Boundary

DuckDB core owns:

- region graph and typed region IR;
- native operator contracts;
- correctness eligibility;
- DuckDB CBO runner selection;
- telemetry and counters;
- vectorized execution when no compiled region is selected.

SLJIT owns:

- backend capability analysis from backend-neutral IR;
- recipe construction from accepted native-region operators;
- generated code and runtime primitives that honor DuckDB contracts.

Production JIT code must not match on query ids, table names, column names, SQL
text, TPC-H constants, or benchmark data values.

## Runtime Batch View

All runtime primitives should communicate through one explicit batch view instead
of ad hoc `DataChunk`, `SelectionVector`, and row-pointer plumbing:

```cpp
struct SljitRuntimeBatchView {
	DataChunk *chunk;
	const SelectionVector *selection;
	idx_t count;
	optional_ptr<SljitRowPointerView> row_pointers;
	vector<idx_t> column_map;
	SljitValidityFacts validity;
	SljitOwnership ownership;
};
```

Rules:

- referenced columns must outlive the consumer primitive;
- materialized columns are owned by the current scratch batch;
- selection vectors are immutable unless a primitive explicitly produces a new
  selection;
- row pointers are valid only for the hash-table contract that produced them;
- NULL semantics are facts on the view, not hidden route-specific behavior.

The first implementation slice may introduce a minimal view with chunk,
selection, count, and ownership. Additional facts should be added only when a
converted primitive needs them.

The selected hash-join slice now carries an optional hash-join output column map.
This lets a downstream producer such as a MARK boundary expose its LHS columns
as a semantic view over an upstream hash-join selection without materializing the
full upstream join output. The map is a producer fact: current input column `i`
maps back to upstream hash-join output column `map[i]`. A missing map means
identity.

Consumers of a selected hash-join view must normalize through that producer map
before executing projection or aggregate logic. The generic path is:

```text
semantic projection
  -> hash-join output-column remap
  -> materialize only referenced producer output columns
  -> execute projection over the producer-output schema
```

This is the architectural fallback for selected hash-join consumers. Specialized
row-pointer aggregate backends may bypass it only when their descriptor can prove
every group/payload source from producer facts: probe-side input vectors,
build-side row-pointer fields, or selected producer output columns materialized
by explicit reference. A descriptor miss is not a CBO bug and must not throw; it
must fall back to the normalized selected-view backend.

A projection-fed two-join recipe is also a selected-view consumer. When the
first hash join feeds a `ProjectionChain` before the second hash join, recipe
binding must preserve the first join as a selected producer whenever the
selection primitive can bind. The `ProjectionChain` then materializes only the
columns it references through the output-column map and hands a normal
materialized view to the second probe. Full first-join output materialization is
the generic fallback only for first probes that cannot publish a selected view;
it must not be the default shape for projection-fed two-join recipes.

## Primitive Contract

Each primitive owns one semantic contract and exposes three phases:

```text
CanBind(context, input facts) -> capability result
Bind(context, input view) -> bound primitive
Execute(runtime, input view) -> output view
```

Core primitives:

- `SourceFetch`
- `GeneratedFilter`
- `FilterAsSelection`
- `HashJoinProbeSelectionOnly`
- `HashJoinProbeMaterialize`
- `MarkProbeFilterBoundary`
- `ProjectionChain`
- `DirectJoinProjection`
- `GroupedAggregateUpdate`
- `NativeTailHandoff`

`NativeTailHandoff` is the only primitive allowed to jump from a converted
recipe back into DuckDB's native operator chain. It consumes an unselected,
materialized `SljitRuntimeBatchView`; selected views must first be converted by a
primitive that explicitly owns that selection contract.

Source-fetch sink advancement is also a primitive contract. `SourceFetch`
advances the source batch when the next primitive consumes the source view
directly. `SourceBatchBoundary` owns advancement for its coalesced source batch
instead. This ownership must be explicit in primitive contracts and must not be
inferred from a route or terminal aggregate helper.

A scan-filtered source is not, by itself, a reason to insert
an opaque materialization boundary. `SourceFetch` publishes the source-contract
output chunk as the source view. Recipes that need to reduce per-batch overhead
before a stateful hash-probe/aggregate consumer use an explicit
`SourceBatchBoundary` primitive with its own counters. That primitive may
coalesce materially short source chunks and pass near-full chunks through
without copying. It must not be hidden behind `uses_scan_filters`.
DuckDB-owned table-scan filter pushdown stays in the DuckDB scan contract. SLJIT
must not replace it with generated source filters because that loses scan
pruning, adaptive scan filtering, and dynamic-filter ownership. Generated
filters may still run as explicit post-source `FILTER` primitives over the
scan-filtered source output.
A scan-filtered primitive aggregate update is real generated/backend work. The
backend capability pass must not downgrade primitive aggregate updates to weak
accelerated work just because the source batch came from DuckDB scan filters.
Native-only execution must remain native-only: if source-contract coalescing is
needed before a native tail, recipe binding must emit
`SourceBatchBoundary -> NativeTailHandoff` instead of passing an executor side
flag.

Projected grouped aggregate recipes require a dedicated aggregate backend, such
as count-star preaggregation or descriptor-backed direct primitive payload
update. If recipe binding cannot prove such a backend, the recipe must use an
explicit `NativeTailHandoff` before the projection/aggregate tail. A grouped
aggregate primitive must not hide native sink execution as its normal backend
for a selected projection recipe.
For count-star grouped aggregate, high-cardinality batches that cannot fit the
local compact preaggregation table use the same prepared grouped state-address
backend with one count delta per input row. That row-delta path is part of the
dedicated count-star backend; it is not a native aggregate fallback.
Regular hash aggregate lookup must not be modeled as a generated backend stage
until a real generated hash-lookup backend exists. The production path is native
grouped state-address resolution plus generated primitive payload update. That
path must not publish permanent `native_hash_aggregate_lookup` blockers, layout
IR, or CBO penalties for a backend that cannot become ready. Perfect hash
aggregate lookup remains the generated-lookup backend because its address
calculation is owned by the compiled primitive.
When a selected reference view is followed by projection(s) and count-star
grouped update, recipe binding may bind those projection semantics directly into
`GroupedAggregateUpdate` as a projected-input contract. That is not a separate
projected aggregate primitive and not a route-era shortcut: the aggregate
primitive owns only the aggregate update strategy, the composed group projection,
and the selected input view it consumes. If the projected-input contract cannot
prove a supported group expression, recipe binding must keep explicit
`ProjectionChain` materialization before the aggregate or hand off to a native
tail.

Projection chains over variable-width reference columns must preserve reference
lifetimes instead of copying strings into an intermediate batch. A pure reference
projection with `VARCHAR` or other variable-width outputs should flush any
pending projection batch and hand a referenced or sliced view directly to the
next primitive. Batching remains appropriate for fixed-width materialization and
for computed projections that produce owned values; it must not turn a reference
projection into a string-copy stage. This is a primitive contract, not a query
special case.

No primitive should know a whole query shape. A primitive can know a DuckDB
operator contract and the value-lifetime facts it consumes or produces.

Pre-join projection is not a recipe-level primitive. The correct generic model is
`ProjectionChain`: it owns projection execution, owns its output batch, and hands
a normal materialized view to the next primitive. A selected hash-join recipe may
elide a pre-join projection only after the producer and every downstream consumer
share an explicit map-aware LHS contract:

- a preserved upstream selected hash-join view carries a semantic output-column
  map: downstream LHS column `i` maps to upstream hash-join output column
  `map[i]`, and the upstream hash-join binding still resolves physical probe
  vectors or build-side row-pointer fields;
- an elided pre-join projection needs a separate physical source-column map:
  projected LHS column `i` maps to the source input column or expression that
  replaces materializing projection output `i`;
- hash-probe key binding must use a physical key-input remap, not an implicit
  assumption that projected key indexes equal source indexes. The primitive
  carries `SljitHashJoinProbeInputRemap`, and primitive binding prepares the
  remapped plan/operator-info view so DuckDB's
  `ExecutionHashJoinProbeBinding` and SLJIT's generated key slots agree.
  Execution consumes that prepared view directly; it must not copy or patch
  hash-join semantic metadata on the hot path;
- post-join projection/aggregate descriptor binding must compose projection
  semantics through that map, preserving casts and target types;
- direct reference, direct computed, and fallback materialization paths must all
  consume the same map;
- residual hash-join predicates are eligible only when every probe-side residual
  source has an explicit reference-preserving source-column remap. Build-side
  residual sources remain row-pointer sources;
- int64-source to int32-key projection elision is a hash-probe key-source
  contract. Regular and perfect probe backends both load the physical source
  width explicitly, preserve cast-overflow semantics before probe filtering,
  and publish the same post-match cast proof;
- if any consumer cannot prove the map contract, the recipe must use
  `ProjectionChain` instead of elision.

The selected post-join aggregate runtime may consume remapped hash-join output
views only through the descriptor-backed direct aggregate path. If that descriptor
cannot bind the mapped producer, execution stops instead of falling through to
stale full-output materialization.
Selected aggregate recipes with no explicit post-join projection and
projection-aggregate recipes with a post-join projection must share the same
pre-join projection descriptor and prepared hash-probe remap path. The
descriptor is the contract; recipe shape must not decide whether pre-join
materialization is required.

Grouped aggregate update binds one explicit update strategy during recipe
binding and validates it through the primitive contract. Execution must not own
a hard-coded route list or rediscover the semantic shape per batch. The current
generated strategies are count-star preaggregation, direct primitive payload
update, and filtered primitive payload update; all other aggregate shapes must
use an explicit native handoff until a generated backend owns their full
contract.
For regular grouped aggregates, typed-expression payload reducers bind the
fused grouped payload strategy once in the executable aggregate update. That
path goes directly to grouped state-address find/update plus fused payload
reduction; it must not probe reference-payload preaggregation, append-only, or
direct-reference update strategies first on every batch. Reference payloads keep
the default strategy order because preaggregation and append-new are their real
backend strategies, not fallbacks.

Distinct aggregates deliberately do not have a generated backend today. DuckDB's
regular distinct aggregate path owns distinct-key lookup, duplicate filtering,
and aggregate state update. A future generated distinct backend may be admitted
only as a new `GroupedAggregateUpdate` strategy that owns both distinct lookup
and payload update with explicit descriptor facts; until then JIT lowering must
not invent a partial distinct update path or alter DuckDB's physical distinct
aggregate implementation.

Direct row-pointer grouped aggregate lookup is also a backend contract, not a
TPC-H route. Descriptor lookup may use row-pointer identity only when the
descriptor proves every group source repeats with that row pointer. Arbitrary
duplicate row-pointer compaction is an optional backend inside that contract:
it must first prove repeats with allocation-free batch evidence, then allocate
its map/compact vectors only for batches that can actually shrink. The normal
no-duplicate path must go directly to descriptor hash/probe/append without a
speculative hash map.

Runtime trace counters are diagnostic, not the performance contract. Nested
stage timings can charge the outer stage for recorder overhead that is not part
of production execution. Production-mode benchmark timing is the source of truth
for user-visible speed; profile-mode timing is used to identify the next
bottleneck.

Diagnostic settings must not select different physical backends. A backend
strategy such as projected direct payload update or count-star preaggregation
may be gated by execution capability and policy, but not by
`jit_trace_decisions`, `jit_trace_runtime`, `jit_dump_ir`, retained event-log
size, or `EXPLAIN ANALYZE`. Tracing can expose facts; it cannot create facts.

The CBO cost surface must preserve the same ownership boundary. A generated
aggregate update backend is generated/backend work, even when it updates a
DuckDB-owned aggregate state contract. It must not be counted as native
aggregate operator work. `native_aggregate_stage_count` is reserved for actual
native aggregate work or an explicit native handoff, while materialization
elision can still be costed for generated aggregate backends that consume a
filtered batch directly.

Generated backend stages are costed separately from generated expression stages.
Expression-stage benefit is capped by expression work; backend-stage benefit is
not capped by expression cost because a generated aggregate update, hash lookup,
or similar primitive can replace non-expression vectorized work. The explicit
`generated_backend_stage_count` fact keeps this aggressive without leaking back
into native aggregate accounting.

Exact source cardinality is a runtime contract fact, not a stale operator
estimate. A finalized stateful source such as sort, TopN, or aggregate state
scan may publish an exact output row count only after the backing state can prove
that count. Physical-pipeline runner cost must use that exact count when the
source bounds the pipeline and all downstream physical operators are
row-preserving or row-reducing, such as filters, projections, and sinks. It must
not take the maximum of stale source/operator estimates in that shape. A
row-expanding operator such as a join breaks the cap and requires normal
cardinality estimation.
Scan-filtered source estimates are already estimates of the source output. CBO
must not apply another synthetic selectivity reduction for each DuckDB-owned scan
filter, or large scan-filtered native-tail candidates collapse to tiny one-batch
costs and skip backend analysis.

Stateful native source-to-sink protocol is not, by itself, generated/backend
ownership. A region that only wraps a stateful native source and a native sink
with generated selection or projection glue must not claim
`native_operator_stage_benefit` until recipe binding proves at least one
generated backend stage. Real backend ownership, such as a row-pointer grouped
aggregate update, remains aggressively admissible; MARK-to-delimiter-sink glue
does not.

Hash-join build sinks are native-tail capability boundaries. A pipeline with
generated compute before a hash-join build sink must not be rejected solely by
pre-graph route cost because the pipeline model cannot prove whether the
generated prefix and native sink can share a valid execution contract. The
pre-graph decision should require region-graph/backend analysis; post-lowering
CBO still owns profitability and can reject unsupported or unprofitable
build-sink recipes. The native hash-build sink protocol penalty must not erase
generated compute-prefix benefit by default: the vectorized baseline also calls
the same native build sink, so the incremental compiled-runner cost is the
handoff contract, not the entire sink protocol.

## Projection Aggregate Descriptor

Projected aggregate input analysis must live in one reusable descriptor layer:

```text
projection semantics + aggregate contract + producer facts
  -> projection aggregate descriptor
  -> input-vector or row-pointer grouped update backend
```

Direct join-output aggregation, selected MARK-boundary aggregation, and future
post-projection aggregate producers should share this descriptor boundary. A
producer may add facts such as row pointers, selected input columns, or proven
cast ranges, but it must not own aggregate payload or group-key analysis.

The descriptor layer must not depend on direct join-output producer state. It
binds from projection semantics, aggregate contracts, and producer facts. Single
projection and projection-chain composition are projection facts, not aggregate
backend modes.

Row-pointer update batching is an explicit aggregate update schedule selected at
recipe binding time. It must not be inferred from a projection-chain enum in the
runtime.

Distinct aggregate descriptors are deliberately outside this layer until there
is a real generated distinct backend. A future implementation must bind from the
same descriptor facts as other grouped aggregate updates: materialized input
vectors, row-pointer group-key sources, payload sources, group reserve facts,
and exact storage ownership. It must not be a Q16 special case, a count-only
side path, or a CBO threshold that changes DuckDB's distinct aggregate contract.

This keeps the architecture generic:

- projection composition happens once before descriptor binding;
- projection-chain composition is a primitive binding responsibility: a composed
  executable projection is stored in the bound `ProjectionChain` primitive, and
  execution only resolves that bound projection or runs the already-bound
  sequential primitive path;
- group key sources are explicit input-vector or row-pointer sources;
- input-vector sources derived from selected hash-join build rows carry an
  explicit repeat-with-row-pointer fact, so descriptor hash/probe reuse is
  driven by producer facts rather than by string value comparisons;
- sliced or dictionary input-vector group sources may reuse descriptor
  hash/probe work when every group source repeats the same underlying source
  index as the previous row;
- payload source indices are bound by aggregate contract;
- dense-domain and cast proofs are facts attached to the descriptor execution;
- selected reference projections can be sliced into a batch view without an
  append/copy materialization boundary;
- direct join-output is a producer mode, not the owner of the aggregate backend.

## Recipe Builder

The recipe builder walks accepted native-region operators and composes
primitives. A current route such as:

```text
HASH_JOIN_PROBE -> HASH_JOIN_PROBE(MARK_PROBE) -> FILTER -> PROJECTION* -> AGGREGATE
```

should become a recipe:

```text
SourceFetch
HashJoinProbeMaterialize
HashJoinProbeSelectionOnly
MarkProbeFilterBoundary
NativeTailHandoff(filter_idx)
```

The recipe builder uses a recipe-pattern registry. Each registry entry analyzes
operator facts, binds primitive contracts, and either returns a complete
primitive sequence or declines. Source-batch native-tail execution is a registry
entry, not an out-of-band branch after recipe matching. Projection aggregate
variants live in one projection-aggregate registry keyed by prefix facts
(`SOURCE`, `SINGLE_JOIN`, `TWO_JOIN`); they must not be split into separate
single-join/two-join branch ladders.

The builder returns a primitive recipe only after binding the primitive
contracts. If no primitive recipe applies, it returns a native-only plan. This
keeps admission/fallback policy separate from recipe execution and prevents
zero-step recipes from becoming hidden control flow.

Native-tail topology recognition is a facts pass. Shapes such as
`FILTER -> PROJECTION -> native tail` and
`PROJECTION -> FILTER -> PROJECTION -> native tail` produce explicit native-tail
facts before recipe binding. The builder consumes those facts and binds
primitives; it must not keep local route-predicate helpers for each prefix.

A projection-filter-projection native-tail prefix must be composed as:

```text
SourceFetch
ProjectionChain
GeneratedFilter
ProjectionChain
NativeTailHandoff
```

It must not use a combined pre-projected filter/projection primitive. Projection
and filtering remain separate primitive contracts; any fused implementation must
live behind one of those primitive contracts, not as a route-shaped runtime path.

The primitive-sequence executor must flush materializing primitive batches in
primitive step order. It must not track a single global pending projection or
materialization slot. A flush from an earlier primitive is allowed to feed a
later materializing primitive, and that later batch must be flushed by the same
generic sequence pass.

When a recipe carries `uses_extended_source_fetch_budget`, the primitive
sequence executor must extend both source fetches and materialized downstream
batches by the same computed recipe budget. When the recipe does not carry that
fact, both source fetches and downstream batches must use the normal runtime
budget. The primitive executor must not independently extend source fetches or
independently clamp terminal row budgets; otherwise the plan fact no longer owns
runtime scheduling.

## MARK Probe Boundary

The generic rule is:

```text
MARK_PROBE + filter(marker or NOT marker)
  -> MarkProbeFilterBoundary
```

`MarkProbeFilterBoundary` produces a batch view with:

- referenced left-hand-side columns;
- one boolean marker vector only when a downstream consumer still reads marker
  semantics;
- DuckDB-compatible MARK NULL validity;
- optional marker or non-marker selection;
- no full MARK join output unless required by the downstream consumer.

After the marker or non-marker filter has been applied, a downstream projection
that does not reference the marker consumes an LHS-only view. That view is a
normal boundary mode, not a query-specific optimization: recipe binding proves
the marker is unreferenced, `MarkProbeFilterBoundary` emits
`mark_filter_lhs_view`, and no `mark_filter_vector` is built.

When the filter is a positive marker filter and the marker is unreferenced, the
regular MARK probe may use an explicit filtered-output contract:
`FILTERED_MARK_MATCHES`. That contract emits the matching source-row selection
directly and records `mark_match_selection_reference`; it must not write
per-input marker flags only to rescan them. Negative marker filters remain a
separate null-aware capability because SQL `NOT IN` semantics depend on build
NULLs and nullable probe keys.

When the MARK input is itself a selected hash-join view and the downstream
consumer can preserve that view, the boundary keeps the upstream row pointers
alive and composes the MARK selection into the source selection. It materializes
only columns needed to probe the MARK hash table, not downstream aggregate group
columns. Downstream projection/aggregate primitives consume the column map and
row pointers as producer facts.

The empty-build side is the same contract, not a special result shape. In
selected-view mode an empty MARK probe must populate the selection-vector mark
flags for every input row and preserve the input cardinality. It must not
materialize a boolean output vector while leaving the selection-vector mark flags
undefined.

This applies to any workload with that operator contract:

- MARK join followed by filter/projection;
- MARK join followed by aggregate;
- multi-join pipelines with a downstream MARK filter;
- anti/semi-style subquery plans lowered through a MARK probe.

MARK join followed by filter/projection and an arbitrary native sink is a
native-tail recipe:

```text
SourceFetch
MarkProbeFilterBoundary
ProjectionChain*
NativeTailHandoff
```

It must not materialize full MARK join output before the marker filter.

## Capability Facts

Runtime capability analysis should expose explicit facts rather than route names:

- `can_selection_only_probe`
- `can_emit_row_pointers`
- `can_reference_lhs`
- `can_materialize_marker_only`
- `can_consume_selection`
- `can_preserve_variable_width_refs`
- `can_native_tail_handoff`
- `requires_materialized_chunk`

Decisions remain separate:

```text
Can compile? capability facts
Should compile? DuckDB CBO
How execute? bound recipe
```

Correctness must never depend on profitability.

## CBO Inputs

DuckDB CBO should score facts, not route names:

- rows and batches;
- expression cost;
- hash probe count;
- materialization bytes avoided;
- selection density;
- grouped lookup cost;
- distinct aggregate cost, through the regular aggregate/stateful protocol model;
- string, NULL, and decimal cost;
- native-tail handoff cost;
- stateful native source/sink protocol cost, including whether a generated
  backend stage actually owns work across that boundary;
- compile/startup cost.

Route names, TPC-H query ids, and benchmark-specific constants must not appear in
CBO logic.

Distinct aggregates must stay on the regular DuckDB aggregate-key path unless a
real generated distinct backend owns both lookup and update work. JIT eligibility
must not change DuckDB's physical distinct aggregate implementation. Coverage
tests may overfund future backend shapes to exercise mechanics, but production
CBO must admit only generated work that pays for itself against the vectorized
parallel baseline.

Aggregate group-count estimates are group estimates only. They must not be
promoted from input row counts to force local hash-table reservation. Runtime
growth remains owned by the aggregate hash table; recipe lowering may pass exact
or statistically derived group facts, but it must not turn a row upper bound into
a preallocation contract. Dense-domain facts are lookup/backend facts, not proof
that every input row is a new group.

Aggregate reserve is a bound recipe fact, not a batch-local heuristic. Executable
binding may create `SljitAggregateGroupReservePlan` only from propagated
group-key distinct reserve facts. Core derives reserve facts conservatively from
sampled source statistics only when source cardinality is available to cap the
estimate; the backend does not reinterpret raw `approx_unique` as an exact group
count. The runtime applies that plan once per aggregate operator and local
pipeline state with
`ReserveGroups(total_group_count)`. The hash table owns the concrete capacity
contract: a total-group reservation must also leave one vector of append slack,
because the append path must be able to fit an incoming vector before it
discovers how many rows are duplicate groups.
Runtime telemetry records the bound reserve target as a JIT runtime counter so
reserve misses can be diagnosed from facts instead of inferred from resize time.
The once-only flag belongs to the execution-region runtime, not per-call scratch,
because a compiled kernel may re-enter the same local sink state many times.
It must not reserve from `estimated_input_count`, `compact_groups.size()`,
selected row count, or any other input-row upper bound.

## Verification Gates

Required gates for this design:

- architecture verifier rejects TPCH/query tokens in production JIT files;
- each primitive has focused unit coverage;
- each converted recipe has at least one synthetic non-TPCH regression;
- runtime counters prove the intended boundary disappeared;
- fallback paths are visible counters, not silent behavior;
- `build/release/test/unittest "*jit*"` passes;
- SF10 all-query benchmark remains non-regressing before broadening admission.

## Migration Order

1. Add the minimal runtime batch view.
2. Wrap existing MARK helpers as `MarkProbeFilterBoundary`.
3. Convert the two-join MARK route to the boundary primitive.
4. Convert the single MARK aggregate route.
5. Extract `NativeTailHandoff`.
6. Extract `ProjectionChain`.
7. Replace route enum entries one family at a time.
8. Delete obsolete route-specific runtime functions after each conversion.

## End State

Good production architecture:

```text
capability facts
  -> bound recipe
  -> direct primitive execution
  -> explicit counters
```

Bad long-term architecture:

```text
more route enums
  -> more shape ladders
  -> more bespoke functions
  -> benchmark-shaped maintenance
```
