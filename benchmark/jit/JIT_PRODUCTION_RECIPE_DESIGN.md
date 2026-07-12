# JIT Production Recipe Architecture

Last updated: 2026-07-11

This is the active architecture contract for DuckDB execution-region JIT. It
defines ownership, admission, runtime-view, and accounting rules. It is not a
TPC-H history log or a list of query-specific optimizations. Source, tests,
runtime counters, and the architecture verifier are the implementation truth.

## Purpose

JIT accelerates generic execution regions by composing backend capabilities and
bound primitives. TPC-H is a workload and regression suite; it must not become
the source of production routes or policy.

The production path is:

```text
DuckDB physical pipeline
  -> execution-region graph and typed IR
  -> backend capability analysis
  -> CBO runner selection
  -> SLJIT recipe binding
  -> executable primitive sequence over runtime batch views
  -> optional explicit native tail
```

## Ownership

DuckDB core owns:

- the execution-region graph, typed IR, and correctness contracts;
- source and sink protocols for native operators;
- cost-model inputs, runner selection, telemetry, and runtime proofs;
- vectorized/native execution when no compiled runner is selected.

SLJIT owns:

- backend capability analysis over backend-neutral region IR;
- recipe construction and primitive binding;
- generated code and runtime primitives that honor DuckDB contracts;
- explicit runtime path, proof, and delegation reporting.

Production JIT code must not branch on query ids, table or column names, SQL
text, benchmark constants, or benchmark-shaped route names.

## Recipe contract

`SljitFullPipelineRecipePlan` has two explicit states:

- `has_recipe = true`: the plan contains a non-empty executable primitive
  sequence;
- `has_recipe = false`: execution is native-only.

An empty sequence is never a fallback encoding. Recipe construction is the
admission boundary:

1. analyze operator facts;
2. check every primitive's binding contract;
3. bind a sequence with `SourceFetch` first and a terminal primitive last;
4. validate operator indices and the complete sequence before execution.

Invalid sequences are construction errors. The executor must not rediscover a
route, silently replace a broken primitive, or credit work that was not bound.
`uses_extended_source_fetch_budget` is a source scheduling/coalescing fact; it
does not mean that execution delegated to native code.

The terminal primitive is one of the following:

- `PostJoinProjectionAggregateUpdate`;
- `UngroupedAggregateUpdate`;
- `GroupedAggregateUpdate`;
- `HashJoinBuildSink`;
- `AppendSink`;
- `DelimJoinSink`;
- `NativeTailDelegation`.

`NativeTailDelegation` is an explicit terminal boundary. It may consume only a
materialized, unselected input view, and it must report delegated runtime work.
It is not whole-executor delegation.

## Runtime batch ownership

All primitives communicate through `SljitRuntimeBatchView`.

| View | Contents | Ownership rule |
| --- | --- | --- |
| Materialized | `chunk`, `count`, no selection | The chunk owns the complete input batch. |
| Selected reference | `chunk`, match selection, build selection, row pointers, and count | The view references the producer chunk and hash-table state; consumers must respect both lifetimes. |
| Selected join view | A selected reference plus an optional output-column map | The map translates semantic hash-join output columns back to producer input columns. |

Selection is not output materialization. A consumer that needs a materialized
view must materialize only the referenced producer columns, after applying the
producer output-column map. A specialized aggregate or projection path may
consume the selected view directly only when its descriptor proves every source
and lifetime.

Terminal append and delimiter sinks consume selected hash-join views directly
when every output column can be represented as a reference, slice, or
dictionary vector. A required RHS gather remains an explicit materialization
path. The selected-view path records its own runtime proof and never reports
delegation; the gather fallback records both materialization and delegated
work.

`SourceFetch` owns source-contract execution and may coalesce short source
chunks into a standard-sized batch when the downstream sink has no ordered
partition requirement. A generated source filter can make an input chunk sparse;
if the sink requires ordered partition information, that filtered path publishes
each source-fetch boundary separately. It must not carry rows from a later source
batch under an earlier batch's partition identity. This is an explicit source
primitive rule, not a hidden scan-filter route. A native tail receives a
materialized, unselected view. No view may outlive its chunk, scratch storage,
selection vectors, row pointers, or hash-table contract.

Selected-view join terminals are the complementary case: `SourceFetch` streams
each sparse chunk without copying wide pre-join rows, and the terminal coalesces
only selected output columns before calling `AppendSink` or `DelimJoinSink`.
This keeps sink calls vector-sized without paying to copy rows or columns that
the join discards.

`PhysicalTableScan` exposes one source-contract entry point for both storage
backed `seq_scan` and stable-layout ordinary table functions, including in/out
functions such as `range`. The latter delegates to the table function's normal
callback and state machinery; it does not reinterpret table-function bind data
as storage scan state. This keeps source ownership generic while preserving each
function's existing async, filter, and input-state semantics. Table functions
that independently project their output remain an explicit boundary until a
source adapter proves the requested input layout.

DuckDB core derives this capability once through
`GetExecutionSourceContractCapability`. The descriptor carries the source kind,
whether the source contract is ready, whether the requested input layout is
supported, and whether storage-scan state owns execution. Contract construction,
source-state opening, and source execution all consume that descriptor. This
prevents planner/runtime drift and keeps storage-specific bind-data casts out of
the generic table-function path.

## Primitive sequence

The current primitive kinds are:

### Source and intermediate primitives

- `SourceFetch`
- `GeneratedFilter`
- `HashJoinProbeMaterialize`
- `HashJoinProbeSelection`
- `MarkProbeFilterBoundary`
- `ProjectionChain`

### Terminal primitives

- `PostJoinProjectionAggregateUpdate`
- `UngroupedAggregateUpdate`
- `GroupedAggregateUpdate`
- `HashJoinBuildSink`
- `AppendSink`
- `DelimJoinSink`
- `NativeTailDelegation`

Each primitive has one semantic contract: what it consumes, what it publishes,
which operator facts it requires, and which counters it records. Binding must
capture those facts once. The hot path executes the bound sequence directly; it
must not parse strings, search route tables, or re-evaluate recipe admission
per batch.

## Backend rules

### Expressions, filters, and projections

- Capability checks are based on typed expression facts, input validity, and
  exception/cast semantics.
- Scalar and SIMD implementations are two implementations of the same
  generated contract. SIMD admission must preserve the scalar SQL semantics,
  including NULL handling, overflow, and cast behavior.
- Typed-expression source IDs are adapter-local. Every generated selector reads
  the adapter's source arrays; a runtime specialization may touch the input
  chunk only after explicitly mapping those local IDs back to input columns.
- A SIMD-eligible boolean root uses packed evaluation when result extraction is
  profitable. On ARM64, a single integer or DATE comparison uses an eight-row
  unrolled branchless scalar selector: NEON has no cheap integer movemask. Each
  lane overwrites the current candidate selection slot, and condition flags
  advance the cursor only for a match. Conjunctions and filtered reductions
  retain their proven packed SIMD loops. This is a target capability decision,
  not a query-name rule.
- A flat all-valid `(integer_reference % positive_constant) compare constant`
  predicate also uses an eight-row unrolled selector. It hoists the source and
  selection cursors and reuses the exact signed magic-multiply remainder
  lowering, including truncated-toward-zero behavior for negative values.
  Compound modulo expressions remain on the general typed-expression path.
- Filter primitives represent `selected_count == input_count` as an identity
  view and may leave the all-true selection vector implicit. Direct selector
  consumers that require concrete indices materialize that identity once at
  the C++ boundary. The generated mixed-mask path backfills a deferred identity
  prefix at the first false lane.
- Consecutive generated filters compose absolute selections over the original
  producer chunk. When a source contract or prior filter publishes a selected
  batch, the next selector receives that selection as its execution domain and
  emits original-row indices for the survivors. It does not build a dictionary
  chunk merely to restore a materialized input invariant. The runtime proof is
  `filter.selected_input_zero_copy`.
- A selected hash-join result is a different ownership class: it can combine
  probe selection, build selection, row pointers, and a semantic output map.
  A generated filter materializes that virtual join view only when its
  expression cannot execute against the referenced producer chunk directly.
  Such scratch storage is owned per filter operation so nested filters cannot
  alias or overwrite another operation's input.
- Projection chains may consume selected views through the producer map and
  materialize only referenced columns. Reference lifetimes, especially for
  variable-width values, must remain valid until the consumer finishes.
- Unsupported expression shapes block binding or form an explicit native tail;
  they must not be hidden as successful generated work.

### Hash joins

Join admission is driven by backend-neutral facts, including key count and
layout, key types and validity, selected versus materialized input, output mode,
and available table features such as all-valid keys, bloom filters, salts, and
dictionary-backed output.

Adjacent materializing probe primitives hand each produced batch directly to
the next probe. A partially buffered batch is flushed before the direct handoff,
so row order is preserved and no output is duplicated. Buffer append remains
the terminal/general path; `hash_join_probe.direct_materialized_handoff`
records the specialized boundary at runtime.

Generated regular and perfect-hash probes load keys from the current runtime
view and preserve DuckDB cast-overflow and NULL semantics. A selected probe
publishes match selection, build selection, row pointers, and any output-column
map needed by downstream projection or aggregate consumers. Full join-output
materialization is an explicit requirement of a consumer, not the default
representation of a selected probe.

Exact perfect-hash runtime filters and their owning join table share a
query-local identity. Storage applies an exact PHJ conjunct as a mandatory
prefilter, including checked integral input conversion, even when another
conjunct still requires the generic expression executor. The source contract
may carry that identity through filters and reference-only projections;
computed projections, joins, and native boundaries invalidate it. When the
downstream perfect-hash probe binds the same identity, the backend derives the
match and build dictionary selections directly from the already-validated key
and skips the duplicate NULL, range, and membership probe. Identity mismatch
always uses the normal generated probe. This is an execution proof, not a
cardinality estimate or a TPC-H-specific rule.

The proof consumer distinguishes representation identity from checked
conversion. A same-type key uses the exact-membership proof directly; a wider
input still performs checked conversion and validates the perfect-table domain
before deriving an offset. Proof reuse therefore removes only work already
established by the producer.

The storage-side exact-filter loop selects its input/source vector layouts once
per batch. An identity selection is represented explicitly as absent input
selection, so the dominant all-valid, same-width sparse path contains no
selection-vector load. That path probes four bitmap entries before publishing
matches, hiding membership-load latency without duplicating cast or NULL
semantics in the uncommon paths.

Storage expression filtering analyzes an AND expression before mutating its
selection. Fully supported internal expressions lower once per filter state to
a typed operation list; vector execution never rediscovers function identity,
rebuilds numeric intervals, or reallocates expression-analysis scratch.
Mandatory exact membership operations are ordered before ranges and other
conjuncts, making every later selector consume only surviving rows instead of
rescanning the raw source cardinality. Mixed expressions split exact membership
from a cached residual executor, so the generic residual never rechecks exact
membership.

Compression codecs consume that same typed operation plan rather than parsing a
second copy of the filter tree. Bitpacked integral scans fuse a same-width exact
perfect-hash membership operation, plus immediately following signed ranges,
into the shared bitmap-lookup decompression loop. Full compression groups still
decompress directly into the result vector; partial groups use the scan scratch
buffer. The codec then resumes the canonical plan at the first unfused
operation. This preserves one operation order and one fallback boundary while
avoiding a decode-copy-filter pipeline for sparse exact joins.

An exact-filter perfect-hash probe followed by `HashJoinBuildSink` is also an
executable primitive sequence when every operation is backend-owned but no
machine-code body remains. Admission requires the exact source-filter identity,
scan-filter ownership, a matched probe/build view, and the explicit build sink.
Runtime must prove both exact-filter backend work and selected build ingress;
the sequence never receives synthetic machine-code credit.

A pure hash-probe chain may terminate in `AppendSink` without first publishing
a full output chunk. The final probe remains selected, while intermediate probes
materialize only when the next join contract requires it. The common
`SourceFetch -> HashJoinProbeSelection -> AppendSink|DelimJoinSink` shape has a
lowered executor that streams sparse probe input and coalesces selected terminal
output, avoiding both wide pre-join copies and tiny sink calls without hiding a
separate execution route.

A source pipeline ending in a hash-build sink uses the same explicit recipe
model: `SourceFetch`, any linear generated filter/projection prefix, then
`HashJoinBuildSink`. The terminal accepts materialized projection handoffs,
selected source views, and selected hash-probe views. Selected source input is
represented with dictionary vectors; it is not copied into an intermediate
filtered chunk. The backend reports `backend_join.direct_hash_build` only when
the fully fused lowering has this primitive shape. CBO then replaces the native
sink-protocol charge with one generated backend stage. Direct hash-build
ownership does not automatically claim materialization elision: projection
buffer work remains visible and must be priced independently.

A source hash-build with positive string equality/set predicates or string
prefix/suffix predicates composes DuckDB's filtered scan and filter-column
pruning with the explicit backend build primitive. A string match may carry
additional positive reference/constant equality or `IN` conjuncts, allowing
mixed storage filters without moving standalone numeric/date ranges out of
generated code. The backend primitive is sufficient executable work for this
shape even when no machine-code filter is needed, and runtime telemetry must
prove its direct source ingress. Contains, general LIKE, OR, range, and
computed predicates remain eligible for the generated source-build recipe.

The physical and candidate prechecks use a direct-hash-build upper bound only
for source-prefix build pipelines with no upstream join stage, and only to
permit backend capability analysis. Final admission still consumes the
backend's explicit `direct_hash_build` fact. An upper bound is never runtime
credit and cannot select a compiled runner by itself.

MARK joins use `MarkProbeFilterBoundary` to publish the filtered LHS view. The
boundary supports match and non-match marker predicates, marker omission or
reference, and materialized marker flags when downstream semantics require
them. NULL-aware non-match behavior remains part of the DuckDB hash-join
contract.

### Aggregates

Grouped aggregate updates bind only when a dedicated strategy is proven:

- `COUNT_STAR_PREAGGREGATION`;
- `DIRECT_PRIMITIVE_PAYLOAD_UPDATE`;
- `FILTERED_PRIMITIVE_PAYLOAD_UPDATE`;
- `DISTINCT_KEY_SINK` for a proven all-distinct key-ingestion contract.

Ungrouped aggregate updates use direct or filtered primitive payload updates.
For a flat typed nullable payload, the generated reducer uses a hybrid loop:
rows whose referenced sources are all valid take register-only expression
lowering, while only rows containing an actual null take the generic semantic
path. A null anywhere in a vector must not force every row through stack-slot
evaluation. The hybrid path keeps the validity-array register reserved and
uses only data-pointer hoists that do not alias it, so the same contract covers
single- and multi-source `COALESCE` or `CASE` expressions. Regular hash grouping
resolves native grouped state addresses and uses generated payload updates.
Perfect-hash grouping owns its generated group lookup when the domain contract
is valid.

Computed perfect-hash keys may carry a signed integer cast into the generated
lookup. The group plan keeps the pre-cast source width, loads that source
directly, and lets the perfect-hash domain check reject values outside the
proven target domain. It does not materialize a narrowed key vector or resolve
one DuckDB state address per input row. Raw reference keys still require a
native load kind matching their physical storage width; transformed keys do
not invent a storage kind for their narrowed result. CBO credits this path only
when generated grouped stages and materialization-elision facts prove that the
generated lookup replaces the native grouped-state lookup.

Projected grouped updates may compose projection semantics into the grouped
primitive when the descriptor proves the required sources. Otherwise recipe
binding chooses an explicit projection materialization path or a native tail.

Dense grouped preaggregation has two generic scopes:

- batch-local preaggregation is admitted when a fixed-width dense group range
  compresses the current batch by at least 8x. It supports primitive payloads
  and generated fused payload expressions, and flushes one exact delta per
  touched key;
- pipeline-lifetime count-one preaggregation is admitted for one fixed-width
  integer group key, one `COUNT` or `COUNT_STAR` lane, a proven dense domain,
  at least 2x estimated compression, and at most 1,048,576 domain slots. It
  accumulates across source and join-output batches, then flushes standard-sized
  unique-key chunks through the same grouped-state contract.

Pipeline accumulation is transactional per input batch: key rejection rolls
back that batch before another route can run. The fallback grouped primitive
keeps lookup and random state updates as separate phases to preserve CPU
memory-level parallelism. Both direct projected pipelines and direct
join-output aggregate strategies own the same pending accumulator and flush it
at their explicit strategy boundary.

Dense grouped lookup keeps one address array. An empty slot is zero, a real
group is an aligned row pointer, and an in-batch pending group is encoded with
the pointer's otherwise-unused low bit. This removes the former full-domain
parallel pending array while preserving exact duplicate detection. Pending
tags never survive a batch: successful append replaces them with row pointers,
and failure clears every tag before another strategy can run. Low-bit tagging
is admitted only for an even row stride; odd-stride group-only layouts use the
general exact lookup path.

Pending preaggregated groups use 2,047 entries and retain the final compact
group across source invocations. If an input run crosses a vector boundary, its
next delta merges into that unpublished carry before any hash-table append.
Overflow within one already-compacted input range flushes the exact prefix and
carries only the suffix. This makes every published batch strictly increasing
without a special duplicate case in the grouped hash table.

Direct append reports exact new-group success to DuckDB's radix adaptivity.
After 131,072 attempted compact groups, more than 95% proven-new groups switch
the local table to append-only mode. This statistical route always marks the
table for final duplicate reconciliation; it never relies on the single-table
finalize shortcut. Raw-tuple HLL adaptation retains its independent 1,048,576
row threshold and ownership.

Sorted compact-key streams can establish a stronger contract. The grouped hash
table, rather than a JIT backend, owns a monotonic proof for one all-valid,
fixed-width integer key. Every compact batch must be strictly increasing and
its first key must be greater than the preceding batch's last key. Once proven,
the local table can append without duplicate lookups. The proof spans backend
invocations and applies equally to serial and parallel local tables; parallel
tables reconcile cross-thread overlap during normal radix finalization. When
every local proof remains intact and the exact local key ranges are disjoint,
the radix owner marks the already-aggregated partitions ready to scan without
rehashing them during finalize.

Any null, type change, key repetition or regression, unsupported route, or
fallback permanently requires final combination for that local table. A
single-table finalize shortcut is legal only while the proof remains intact.

`DISTINCT_KEY_SINK` is backend-owned key ingestion through a DuckDB hash-table
contract. Lowering reports `distinct_key_fast_insert` only when the complete
shape is supported, and CBO converts only that aggregate stage from native work
to generated backend work. Runtime selection is cardinality-driven:

- high-uniqueness, single-thread `COUNT(DISTINCT value)` may materialize the
  outer group once, deduplicate exact `(group-state address, value)` pairs, and
  increment the main count state directly;
- duplicate-heavy or parallel input uses DuckDB's vectorized distinct table,
  followed by an explicitly owned fast DISTINCT finalize lookup;
- nullable values, fixed- and variable-width values, selections, and hash
  collisions retain exact SQL semantics. Unsupported shapes remain native or
  explicitly delegated.

Fast grouped state-address lookup is never a global aggregate default. The
ordinary aggregate API uses DuckDB's regular lookup; only the direct DISTINCT
count and DISTINCT finalize owners request the fast lookup explicitly. This
prevents a specialized optimization from taxing unrelated grouped workloads.

## CBO and runtime accounting

CBO uses backend-neutral, measurable facts rather than query identity:

- proven source-contract cardinality, or a physical estimate when source output
  cardinality is unknown;
- finalized dynamic-filter cardinality evidence, kept separate from raw source
  input cardinality;
- generated expression, generated stage, and generated backend-stage counts;
- backend-owned distinct-key fast-insert stages and propagated distinct-count
  upper bounds;
- native join, aggregate, grouped-state-address, build-sink, and sort stages;
- materialization-elision opportunities and selected-join materialization
  penalties;
- source-contract scan penalties, stateful protocol costs, and startup cost;
- whether the candidate is a full pipeline and which native protocol class it
  uses.

Finalized runtime membership filters expose a build-side distinct-key upper
bound through the core runtime layout. This includes perfect-hash membership
and regular-hash prefix-range filters. The execution-region graph combines that
bound with scan-column distinct statistics to estimate filtered source output.
It unwraps optional/selectivity wrappers, composes multiple filters, and pads
the result by 2x to remain conservative when filtered columns are correlated.
This is an explicit `finalized_dynamic_filter_cardinality_estimate` fact, not a
guess inferred later from row counts.

The table-scan contract retains raw source cardinality for scan accounting.
Only the source node and provably non-expanding probe candidates may use the
finalized filtered estimate as downstream cardinality. The proof covers
unique-key perfect-hash probes and regular SEMI, ANTI, and MARK probes. Operators
that can expand or otherwise invalidate that estimate stop its propagation.
This keeps source work honest while preventing stale optimizer cardinality from
pricing a small dynamic-filter tail as hundreds of batches.

Unknown-output join/group admission has a separate backend-neutral proof. A
region with no hash-build sink, at least one generated join, at least two
generated backend stages, and enough generated grouped work to cover every
native grouped-state lookup may amortize that lookup and receive backend-stage
credit. This admits large fused join-to-group pipelines such as ordinary
correlated and dynamic-filter workloads without naming a benchmark query.

Foldable equality predicates use statistics for the filtered expression, not
only the underlying column. Integral modulo by a positive constant propagates
a conservative remainder range and distinct-count bound, so `x % d = k`
estimates the modulo domain instead of pretending the source column's full
distinct count survives. Dense filtered reductions can therefore fund JIT
startup, while sparse modulo predicates remain below the admission floor. The
rule is type- and expression-driven and applies to ordinary workloads as well
as benchmark queries.

An ungrouped generated filtered reduction with full-pipeline ownership and no
join, grouped lookup, native aggregate, sort, or selected-view materialization
does not pay the generic source-contract scan penalty. DuckDB's raw storage
scan is shared by both runners; the generated terminal consumes filtered
batches directly and removes the intermediate projection. Grouped and
stateful tails keep the penalty.

When source output cardinality is unknown, CBO caps batch credit only while the
source input represents more batches than the candidate estimate and no
runtime-owned fusion proves that the downstream work amortizes the uncertainty.
It uses the physical estimate directly when the source input is no larger than
the candidate, except for a direct hash-build sink with a high estimated join
expansion over the source: that estimate is too uncertain to fund startup and
is capped as well. A single generated probe feeding one terminal hash-build
sink may use the candidate estimate when it has no aggregate or sort tail and
does not have high estimated expansion. This direct path has no intermediate
operator that can multiply unknown filtered output, so it also pays no generic
scan-filter materialization penalty. Multi-join and aggregate tails retain both
guards. Other qualifying fusions are a deep probe-only tail, a serial stateless
probe tail, or a serial grouped-state lookup/probe tail with no hash-build sink.
For a large, selective source, the lookup case also requires at least two
native joins; the one-join case remains conservative. Parallel grouped lookup
does not qualify.

A finalized dynamic-filter estimate bypasses the unknown-output batch cap and
sparse-source scan penalty because those mechanisms protect against uncertain
downstream cardinality, while raw scan work is already represented separately.
For serial grouped aggregation, native grouped-state address lookup may be
amortized only when generated stage depth is more than twice the lookup count.
A large input with less than the calibrated expression-work floor receives no
generated-stage or materialization-elision credit: state lookup remains the
dominant protocol even when several shallow generated stages are present.
Parallel lookup remains charged. Conversely, a finalized region producing at
most two standard vectors and ending in a native aggregate receives neither
generated-backend credit nor native aggregate-tail credit unless the aggregate
itself is generated. This startup floor rejects tiny native tails without
penalizing the same generic shape at material cardinality.

Production startup accounting also rejects an expression-free, join-only
finalized tail of at most four standard vectors, even when capability analysis
can represent a generated join backend stage. Above that floor, an
expression-free finalized join with no hash-build sink and no payable generated
backend stage may use native-operator credit only when an explicit delimiter
sink proves the materialization-eliding terminal contract. An explicitly
configured zero startup cost disables these two join startup guards for focused
coverage and deliberate no-startup deployments; it does not weaken the native
aggregate floor.

Hash-build sinks never receive generated backend-stage credit when source output
is unknown, and every candidate pays the native sink protocol. A direct build
sink binds and materializes only the selected required source columns; when a
join expands the source view, CBO charges a selected-view expansion cost rather
than full-output materialization. A generated expression-only prefix still
pays its materializing protocol cost. If binding has to fall back to a full
output view or a native tail, runtime must expose that materialization or
delegation rather than treating it as generated work. Candidates without these
ownership and cardinality facts remain on the conservative path.

CBO credit must match typed runtime proofs. The proof vocabulary is:

| Runtime fact | Required runtime proof |
| --- | --- |
| Generated-stage work | `GENERATED_STAGE_WORK` |
| Generated-backend work and native-operator ownership | `GENERATED_BACKEND_WORK` |
| Materialization elision | `MATERIALIZATION_ELISION` plus the applicable generated-work proof |
| Full-pipeline ownership | `FULL_PIPELINE_OWNERSHIP` |
| Explicit native/delegated work | `DELEGATED_RUNTIME_WORK`; it must not be counted as generated or elided work |

`NO_WORK` is used for compiled invocations that did not process input. A
selected candidate must not receive credit for work that runtime counters cannot
prove. Materialization-elision proof rows must contain no materialization,
buffer-append, or delegated runtime work.

Unsupported work must be visible as one of:

- a capability blocker and native-only plan;
- an explicit native-tail boundary;
- a typed runtime delegation counter.

## Verification

Run the architecture and unit gates for recipe or contract changes:

```bash
python3 benchmark/jit/verify_jit_architecture.py
cmake --build build/reldebug --config RelWithDebInfo -j12
python3 benchmark/jit/run_jit_refactor_guard.py --level unit --no-build --skip-architecture --skip-py-compile
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 1
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 4
```

The generic harness runs setup and reference-result creation with JIT disabled,
then resets telemetry before the timed statement. Setup CTAS compilation must
never satisfy target-workload compilation or runtime-proof requirements.

Run the TPC-H regression gate for runtime, planner, backend, or performance
changes:

```bash
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --no-build
```

The gate resolves the accepted local artifact through
`benchmark/tpch/jit/tmp/tpch_refactor_guard_state.json`. Promote a complete
22-query production artifact only after it passes correctness, runtime-proof,
old-baseline comparison, and a full-query high-sample promotion qualification:

```bash
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --promote-baseline --promotion-repeats 31 --no-build --queries all
```

Baseline promotion is a ratchet, not a way to accept a regression. Partial
query sets cannot update the accepted state unless the caller explicitly opts
into the local-only partial-baseline escape hatch. Promotion never points at
the low-sample candidate. It reruns the complete requested query set without
decision/runtime tracing and compares that high-sample artifact with the
previous baseline. A failed late query may be rerun independently at the same
high sample count to remove sustained-run thermal/order bias. Its row is used
only if the focused comparison passes, and the resulting complete artifact is
compared again in full. The state update is atomic and happens only after every
gate passes. Runtime, speedup, preserved-win, and component-ratio thresholds
are evaluated with exact decimal arithmetic: a value exactly on a configured
boundary passes, while any value beyond it fails.

If an accepted timing artifact is proven stale by a high-sample paired run,
baseline re-initialization still requires the complete 22-query, 31-repeat
production suite, architecture verification, artifact correctness, and traced
runtime-contract verification. A refresh is valid only when compiled-region
counts and physical recipe shapes are unchanged for apparent regressions and
every selected query remains at least parity with non-JIT. It must not be used
to hide a changed runtime path or a workload that became slower than non-JIT.

Correctness, runtime-proof, and materialization-elision checks are implemented
by the JIT API helpers and `benchmark/tpch/jit/verify_tpch_benchmark.py`.

The generic production gate is part of the performance contract. Arithmetic,
CASE-heavy, multi-aggregate, persistent-table expression, filtered scan,
column-vs-column comparison, single- and multi-source nullable scan, and
single-thread grouped-DISTINCT workloads must show an auto-policy speedup and
compiled-region ownership; the arithmetic-heavy and column-comparison classes
use material speedup floors. Dense computed perfect-hash grouping is also a
compiled performance contract, including both serial and parallel execution.
Other grouped and join workloads must remain within the bounded slowdown
budget; a vectorized result is valid when capability analysis cannot prove a
faster compiled route. This keeps JIT admission honest across production
workload classes instead of optimizing TPC-H query names.

Generic performance and runtime proof are separate modes. Production runs have
runtime tracing disabled and enforce speedup/slowdown thresholds. Traced runs
enforce correctness, compilation, and executed-region proof but never make a
performance decision. A production failure on a short workload triggers a
focused recheck up to the configured high-sample count before the gate decides;
correctness, compilation, and missing-runtime-proof failures are never retried
as timing noise.

## Current boundaries

- Mixed, filtered, ordered, multi-aggregate, and unsupported DISTINCT shapes
  remain DuckDB-owned outside the proven `DISTINCT_KEY_SINK` contracts.
- Variable-width and string expression coverage is narrower than fixed-width
  coverage.
- Pipeline-lifetime dense accumulation currently covers only one `COUNT` or
  `COUNT_STAR` lane. Other unordered sparse payload shapes use batch-local
  dense preaggregation when profitable, consecutive-run preaggregation when
  proven, or exact per-row grouped-state updates.
- Native source/sink protocols remain native unless an explicit execution
  contract makes them safe to compose.
- Native-only execution is a valid result of capability analysis, not a failed
  recipe or an excuse to loosen CBO accounting.
