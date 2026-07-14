# JIT Production Recipe Architecture

Last updated: 2026-07-14

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

Every top-level fused family uses one non-throwing binder for admission and
recipe construction: source-filter aggregate, join-filter aggregate, source
hash-build sink, hash-join append sink, and hash-join build sink. Each binder
validates the complete shape first, builds into local sequence state, and
publishes the recipe only after successful construction. This prevents a
failed candidate from leaving partial state and prevents capability checks
from drifting away from the primitive sequence that they admit.

Invalid sequences are construction errors. The executor must not rediscover a
route, silently replace a broken primitive, or credit work that was not bound.
`uses_extended_source_fetch_budget` is a source scheduling/coalescing fact; it
does not mean that execution delegated to native code.

The join-filter-aggregate recipe has one grammar, independent of workload
names: `SourceFetch -> Projection* -> HashJoinProbeSelection ->
GeneratedFilter -> Projection* -> PostJoinProjectionAggregateUpdate`. Either
projection run may be empty. Binding emits one immutable
`SljitHashJoinDirectAggregateConsumerContract` when the probe and its optional
generated filter feed the final post-join aggregate directly. Execution uses
that contract only for the matching probe step; all other successor shapes
remain on the materialized route and are never probed speculatively.

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

## Typed primitive recipe descriptor

Each primitive payload recipe has one type-driven descriptor. Matcher, codegen,
runtime adapter, and CBO consume the same
descriptor rather than independently inferring support from expression kind or
aggregate return type. The descriptor owns:

- input physical type, width, signedness, and nullability contract;
- result width and generated-value ABI;
- grouped and ungrouped aggregate state layout;
- supported expression forms and overflow semantics;
- runtime source adapter and generated code entry point;
- generated-stage, startup, and per-batch cost facts.

Capability is rejected when any consumer cannot honor the descriptor. In
particular, the typed expression reducer produces one signed machine-word
value; it cannot admit an INT128 reference merely because the aggregate result
state is a hugeint. INT128 values use the exact-width primitive reducer. This
single boundary eliminates the former mixed BIGINT/HUGEINT special case and is
the model for adding future widths or scalar types.

The migration order is: define the descriptor from DuckDB aggregate ABI facts,
make recipe binding the only capability decision, bind runtime adapters and
codegen from that descriptor, then delete duplicate type switches. The
descriptor exports backend-neutral work facts into `PhysicalRunnerCostInput`;
DuckDB core remains the sole owner of CBO policy. No descriptor may contain
workload, query, relation, or column identity.

The ABI and state-layout migration is complete. `SljitAggregatePayloadValueABI`
classifies payload storage once, while `SljitAggregatePayloadDescriptor` binds
the primitive kind, aggregate index, input type, state size, and state offsets.
Capability analysis, grouped and perfect-hash codegen, generic fallback codegen,
specialized ungrouped and filtered-ungrouped reducers, and standalone runtime
adapters consume that descriptor. Runtime validates the descriptor against
DuckDB's live aggregate lane before executing. Exact-width reference reducers
share one INT128 loader and one two-word add-with-carry emitter across grouped
and ungrouped state locations. The descriptor also carries logical typed-lowering
identity, so INT64 and DECIMAL64 payloads that share a machine-word storage ABI
cannot be conflated by typed-expression planning. Generic, grouped, and
perfect-hash typed plans consume the bound descriptor, including shared-payload
optimizations. Remaining descriptor work is calibrated backend-neutral cost
facts; it must not move CBO policy out of DuckDB core.

Payload execution has no planner-aggregate dependency. Fused source
classification, filtered and unfiltered adapters, perfect-hash adapters,
preaggregated count binding, and payload-lane scratch lookup all consume the
descriptor vector. Planner aggregate records stop at capability and native-sink
binding; runtime cannot reconstruct primitive kind or state ABI from them.

Aggregate payload binding has explicit monotonic ownership. A native aggregate
starts `UNBOUND`, where DuckDB sink payload indexes are the only source of
meaning. Direct primitive binding moves it to `DIRECT`; incorporating any
projection moves it to `PROJECTION_COMPOSED`. In either bound state, the native
payload expressions are authoritative and later passes may only compose those
expressions through another projection. They may never rebuild payloads from
the original sink indexes. Initializers require `UNBOUND`, composition requires
a bound state, and successful composition always publishes
`PROJECTION_COMPOSED`. No pass infers ownership from expression shape: a fused
projection can legitimately simplify to a list containing only references.

Grouped runtime strategies bind descriptors to DuckDB's live primitive lanes
through `SljitGroupedReductionLaneBinding`. That binding owns the single check
for aggregate identity, primitive kind, payload type, state size, grouped state
offset, and value/is-set offsets. One `SljitAggregateOperatorScratch` owns every
runtime-lifetime object for an aggregate operator: payload lanes, grouped
reduction lanes, adapter and preaggregation scratch, continuation state,
adaptive miss trackers, and the grouped sink binding. Ordinary, projected,
row-pointer, and direct grouped terminals all reuse that owner; none may create
a terminal-local binding. The first bind also selects one immutable execution
family: perfect-hash fused lookup, grouped-state fused payload update, or
grouped-state per-payload update. Every later chunk dispatches through that
family instead of rediscovering the combination from mutable flags. Direct-new,
append-new, state-address, and preaggregation admission consume the cached lane
bindings instead of rebuilding lane validity. Perfect-hash commit and
deferred-flag emitters consume payload descriptors directly. Dense perfect-hash
batch-reduction scratch offsets remain strategy-local storage and are not
mistaken for DuckDB aggregate-state ABI.

## Compiled artifact ownership

Generated machine code and its callable entry point are one lifetime unit.
`SljitCompiledFunction<T>` owns both; remapped expressions share that complete
immutable artifact instead of copying a raw function pointer. Eager generators
build the owner and callable locally, construct a complete artifact through
`TryCreate`, and publish it with one whole-value move. Code-generation policy
stays outside the ownership type, avoiding one builder-template body per call
site. The artifact exposes no mutable code-owner, callable, or split `Set` API.
Lazy artifacts add
their one-time publication state to the same owner. The release-store publishes
the complete artifact, and every callable read follows an acquire-load or the
`call_once` synchronization edge.

Regular hash-join code uses one specialization cache keyed by input validity,
selection, layout, bloom use, and MARK-selection mode. Named variant fields,
parallel specialization arrays, and independent publication objects are
forbidden. Perfect-hash probes use the same lazy artifact contract. Expression,
general aggregate, nested-loop, and fused-projection code use the eager artifact
contract. Primitive grouped-run specializations are lazy: runtime ordering
economics runs first, then the first accepted batch atomically publishes only
the key-cast, lane-count, and nullability specialization it executes. Core
runtime accounting records every lazily published backend artifact. Code size,
callable lifetime, and borrowed/remapped ownership therefore cannot drift apart,
and unordered input does not carry dead run-reducer code.

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

Source lowering computes the complete post-source layout before publishing
native source operators into the region. Output types, nullability, statistics,
and exact-filter identities are propagated through generated filters and source
projection repair while those plans are still owned by the lowering cursor;
only then are the plans moved into the immutable region. Downstream operators
must never derive their input contract from a moved-from source plan.

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

### Scan-filter ownership

Scan-filter ownership is an explicit mode, not a boolean. `NONE` gives the
generated source the unfiltered input contract, `ALL` keeps static and dynamic
filters in DuckDB's scan, and `DYNAMIC_ONLY` lets DuckDB apply finalized runtime
filters while the generated source owns supported static predicates. The open
request and lowering plan publish the same mode, and `PhysicalTableScan` derives
the dynamic-only filter set from the finalized dynamic-filter state. Static
filters are never silently evaluated twice or silently dropped.

Static-filter admission is an execution-contract proof, not a backend-shaped
arithmetic blacklist. A generated filter must reference only its retained scan
input, use a supported physical source type, and be unable to raise a runtime
exception. The contract discharges integral modulo and integer-division only
when a constant divisor rules out zero (and signed `-1`, which can overflow at
the minimum value). The backend then independently lowers the admitted IR; if
it cannot lower it, DuckDB retains scan ownership. This admits normal safe
predicates such as `x % 7 = 3` without allowing potentially throwing arithmetic
to bypass DuckDB semantics.

The generic four-thread scan-filter promotion measures 2.016x over ten
alternating pairs (51.823 ms off versus 25.712 ms auto). Its 1.85x
thread-specific regression floor protects the generated-source ownership win;
the global 1.08x floor remains for thread counts not yet independently promoted.

SLJIT chooses between `ALL` and `DYNAMIC_ONLY` from workload-independent source
facts and the complete candidate shape. Generated mixed string filtering
requires a downstream grouped-aggregate contract that consumes the repaired
source layout. String predicates over a source domain with at most 64 known
distinct values publish a backend cost preference for vectorized execution;
they are supported by native lowering, but DuckDB's compressed/dictionary
vector path can evaluate them once per reusable value instead of once per row.
The physical-runner CBO consumes this preference, so the choice is a normal
runner selection rather than a false backend-capability failure. The mixed
path records `source-strategy=mixed-source-filter` and
`source_contract_filter_pushdown=dynamic-only`, so runtime proof can distinguish
composed ownership from the legacy all-or-nothing path.

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
- On ARM64, percent-only constant `LIKE` fragments of at least two bytes use a
  16-position NEON candidate scan over the rarest adjacent byte pair. Candidate
  matches still use exact `memcmp`, preserving ordered-fragment semantics and
  arbitrary byte values. One-byte fragments and non-ARM64 targets retain the
  portable anchored search path.
- Typed-expression source IDs are adapter-local. Every generated selector reads
  the adapter's source arrays; a runtime specialization may touch the input
  chunk only after explicitly mapping those local IDs back to input columns.
- A specialized native predicate may retain the original backend-neutral typed
  IR as an auxiliary semantic view. Executable binding remaps that tree through
  the predicate's canonical source list into the same dense source coordinates
  as scalar predicate lowering. Missing or stale references are construction
  errors; codegen cannot infer a second input ABI.
- Partial predicate SIMD is an ordered split, not arbitrary conjunct
  extraction. Only the longest leading SIMD-supported prefix of a top-level AND
  may execute as a packed mask. A non-empty specialized residual owns all later
  children, preserving SQL evaluation order and short-circuit semantics. A
  fully supported root uses the ordinary packed selector, and an unsupported
  first child keeps the complete specialized scalar predicate.
- A SIMD-eligible boolean root uses packed evaluation when result extraction is
  profitable. On ARM64, a single integer or DATE comparison uses an eight-row
  unrolled branchless scalar selector: NEON has no cheap integer movemask. Each
  lane overwrites the current candidate selection slot, and condition flags
  advance the cursor only for a match. Conjunctions and filtered reductions
  retain their proven packed SIMD loops. This is a target capability decision,
  not a query-name rule.
- A bare reference-to-constant predicate feeding a fused perfect-hash
  multi-aggregate reducer stays on the scalar fast loop: grouped lookup and
  payload updates remain scalar, so a packed mask has too little expression
  work to repay its setup. Predicates with additional expression or conjunction
  work may use the hybrid packed-mask contract. High-selectivity predicates
  retain the separate selected-input path; these choices follow measured work
  avoided, not query identity.
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
- The partial-predicate runtime requires a flat execution domain and flat data
  for the packed prefix. It evaluates the prefix and residual in one pass,
  appending final survivors directly through one hoisted output-selection
  pointer. It never publishes an intermediate selection or mutates
  `execute_sel`; unmet layout or validity contracts enter the complete scalar
  predicate before any output is written. Uniform masks bypass lane extraction,
  while mixed masks iterate only set bits with count-trailing-zero plus
  clear-lowest-set-bit operations.
- Consecutive generated filters compose absolute selections over the original
  producer chunk. When a source contract or prior filter publishes a selected
  batch, the next selector receives that selection as its execution domain and
  emits original-row indices for the survivors. It does not build a dictionary
  chunk merely to restore a materialized input invariant. The runtime proof is
  `filter.selected_input_zero_copy`.
- A selected hash-join result is a different ownership class: it can combine
  probe selection, build selection, row pointers, and a semantic output map.
  One `ExecutionHashJoinProbeOutputProof` travels with that view and carries
  independent facts for identity probe selection, exact RHS source aliases,
  and safe probe-key narrowing. A downstream filter invalidates identity only
  when it compacts rows; exact RHS aliases survive because they describe value
  ownership, not row order. Parallel booleans for these facts are forbidden.
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

A terminal that can consume input rows in order may request an identity-preferred
selected view. Perfect-hash probing then writes only the required build selection
while every row matches; it does not write a match selection that the consumer
would immediately replace with the incremental identity vector. If any row
misses, the probe reruns its compact-selection kernel before publishing a result,
so no consumer can observe partial identity state. The exact-filter and generated
perfect-hash engines share this contract. Runtime receipts distinguish the elided
all-match path from the compact retry.

An all-valid perfect-hash RHS predicate is a separate terminal contract. Its
two complementary lanes partition every represented row, so their totals are
also the group row count; the terminal therefore does not write a redundant
per-row represented-count lane. A nullable RHS uses the ordinary accumulator,
where neither lane may account for a NULL predicate. The contract is selected
from the bound dictionary validity, never from a workload or value assumption,
and emits an explicit all-valid receipt.

Dictionary-backed RHS predicates that require exact string comparison have an
executable-owned immutable classifier. Every pipeline task using a
`SljitExecutableHashJoinProbe` has the same direct-terminal predicate descriptor,
so their direct-row volume is combined for one dictionary. After that volume
covers the larger of 64 vectors and the dictionary cardinality, one task builds
`{dictionary owner, all-valid fact, classification bytes}` while holding the
cache mutex. The artifact is published through DuckDB's atomic shared pointer;
each consumer retains that owner atomically before reading any byte. A different
dictionary starts a new observation epoch. Fixed-width values and two inline
string constants do not take this route: their direct packed comparison is
already cheaper than a full dictionary pass plus another indirection. There is
no mutable shared classification after publication, and nullable slots retain
their explicit NULL classification rather than using the all-valid terminal.

After an all-valid terminal has established one or two runtime group keys, it
may accumulate the current vector's complementary totals in scalar batch
counters and merge each known group once. An unseen key commits the pending
totals before immediately resuming the general accumulator, so the compact
path cannot lose rows across a domain transition. This is an observed group
domain rule; it neither names nor assumes any query, table, or key value.
For an inlined `VARCHAR` source compressed to one of the canonical unsigned
key widths, that observed domain is also a transactional direct-input rule:
the terminal compares canonical `string_t` storage against the known compressed
keys (the `{length, prefix}` word first and the tail only when required) while
accumulating only vector-local per-group totals. It commits those totals after
the entire vector validates, not a transformed wide key or a row-local staging
buffer. A NULL, long string, or unseen value exits before mutation and retains
the generic selected-key transform. Other physical types never instantiate this
path.

A compatible `PostJoinProjectionAggregateUpdate` may consume regular no-chain
inner-probe matches directly. The generic consumer receives the probe-row index
and matching build-row pointer, then uses the bound group and payload
descriptors to update the existing pipeline-local accumulator. It is admitted
only for the same typed key, layout, output, residual, and aggregate contracts
as the ordinary selected-view route. Unsupported shapes continue through that
route before a match is published; direct consumption is never a second join
implementation.

When a direct group transform can fail, its execution-local terminal proves the
entire bounded vector before updating the accumulator. Each converter declares
whether its transformed key is staged with that proof; this is a property of
the conversion work, not its destination width. The lossless string and
arithmetic compressors stage their transformed keys once, avoiding a second
conversion during consumption. Both routes preserve all-or-nothing fallback.

Two-constant string membership is a shared runtime primitive across direct
perfect-hash probes, regular probes, and post-join projections. It loads the
DuckDB string header once, then performs the existing full comparison for each
header-compatible constant. Prefix or length collisions therefore retain exact
equality semantics without repeating the common header load.

Complementary aggregate accumulation writes exactly one payload lane for each
valid predicate result and neither lane for a null predicate. This preserves
the SQL null contract while avoiding a zero-valued read-modify-write on the
opposite lane in every direct-probe consumer.

The filter in that recipe may move before the probe only when its complete
source map resolves to probe-input columns with identical types. The runtime
caches one remapped generated predicate per pipeline-local terminal state,
selects the probe input zero-copy, and then probes only survivors. Any RHS
reference, missing map, type mismatch, or failed generated-filter binding keeps
the ordinary post-join filter path. This is a semantic source-ownership proof,
not a predicate heuristic, and the runtime reports either direct-consumer
execution or its explicit miss reason.

Exact membership runtime filters and their owning join table share a query-local
identity. Storage applies exact perfect-hash and shift-zero numeric prefix-range
conjuncts as mandatory prefilters, including checked integral input conversion,
even when another conjunct still requires the generic expression executor. The
source contract may carry that identity through filters and reference-only
projections; computed projections, joins, and native boundaries invalidate it.
When a downstream perfect-hash probe binds the same identity, the backend
derives match and build dictionary selections directly from the validated key.
A regular probe may also disappear when the prefix bitmap is exact, the build
keys are proven unique, there is one equality condition and no residual or
marking work, and every requested RHS value is that equality key. Such RHS
outputs alias the probe-key vector; non-key payloads and duplicate builds keep
the normal row-pointer probe and gather. Identity mismatch always uses the
normal generated probe. This is an execution proof, not a cardinality estimate
or a benchmark-specific rule.

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
avoiding a decode-copy-filter pipeline for sparse exact joins. Dense versus
sparse perfect tables and presence versus absence of residual ranges are chosen
once before the loop, so invariant layout branches do not remain in each value
match.

Bloom filters have an explicit build/finalize/read lifecycle. Parallel build
uses atomic bit insertion; hash-table publication finalizes the filter before
probe readers can observe it, after which lookup is a plain immutable read.
Fixed-width, string, and interval scan inputs hash and test the Bloom filter in
one unified-vector pass. Nested values retain the canonical vector-hash
fallback. This removes the temporary hash vector and second selection pass
without creating a second Bloom implementation.

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

Perfect-hash state layout and update strategy are separate decisions. A dense
batch reducer is admitted only for a genuinely tiny domain (at most 16 slots),
where it replaces DuckDB-state traffic with compact batch-local accumulation
and one commit per seen group. A sparse scratch table is not a reduction proof:
cache residency, packed rows, and narrower accumulator words do not justify
adding per-row scratch updates plus a final commit. Sparse domains therefore
use direct generated state updates with deferred group flags unless an
independent run or batch strategy proves that it reduces update cardinality.
That strategy boundary is workload-neutral and never depends on query, table,
or column identity.

Widening decimal multiplication has an exact fixed-width recipe. It accepts two
signed INT8/INT16/INT32/INT64 references cast to an INT128-backed decimal,
lowers signed 64-by-64 multiplication to SLJIT's two-word result, and publishes
one ordinary `hugeint_t` vector. `SUM(INT128)` uses DuckDB's primitive aggregate
ABI and adds both lower and upper words, including carry, into the normal
`SumState<hugeint_t>`. Grouped and ungrouped execution share the same semantic
two-word addition contract. Wider values never enter the machine-word typed
expression ABI.

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
Producer output maps resolve an expression and its original input separately.
The map has one coordinate system: projection output to raw probe input. It is
never reinterpreted as a join-output ordinal. A downstream grouped expression
is composed with the mapped producer expression before the consumer validates
the result. Each consumer then decides which composed expression kinds it
supports. Direct-input consumers still require an identity-like reference,
while grouped-key consumers may preserve a proven integer or string-compression
transform. This keeps the semantic map generic and prevents one consumer's
representation restriction from blocking a different consumer's valid fused
path.

A mixed-source grouped reduction may consume a group key from the selected
probe input while reading an aggregate predicate from the matched build row.
The current contract recognizes two complementary `SUM(CASE ... THEN 1 ELSE
0 END)` lanes over the same constant string set. An already-typed group key
retains the producer's selected vector view; a remaining narrowing or
compression transform is read directly through the producer selection instead
of creating a full join-output vector. Fixed-width build fields use typed loads
against precompressed constants. Nullable predicates still contribute the
non-null `ELSE 0` value to both sums. Pattern binding, row-layout resolution,
and constant compression are operator-lifetime plan state; chunks contain only
data work. A source domain of at most 64 groups admits a pipeline-lifetime exact
accumulator with physical capacity for 128 groups. If stale statistics exceed
that capacity, the accumulator flushes through the ordinary pending
preaggregation owner and retries the current row, preserving exact semantics.
The accumulator index keeps the first eight distinct groups observed at runtime
in a bounded direct tier. Observation of a ninth distinct group transitions the
same index permanently to hashing, so domains of 9-64 groups do not pay eight
unsuccessful linear comparisons per later row. Both routes use the same
fixed-capacity index and operator-lifetime state. The common
all-valid 16-byte compressed RHS representation has one fused matcher variant.
Every other width or nullable layout uses the generic matcher, deliberately
bounding specialization instead of generating a width-by-nullability template
matrix.
Fallible selected-key transforms are preflighted before pipeline state changes,
so native fallback cannot duplicate a partial batch. Admission depends only on
typed expressions, source coordinates, row-layout facts, and primitive lanes.

A regular no-chain hash probe with one selected equality key may read a BIGINT
source directly against an INTEGER hash-table layout when persisted source
statistics prove that every input fits the narrowed domain. One loop composes
selection, unchecked proven narrowing, hashing, optional Bloom and salt checks,
entry prefetch, and row-pointer publication. A missing range proof, a different
key/layout contract, residual predicates, or chained output retains the generic
probe. The recipe is keyed only by typed plan and runtime layout facts; it does
not identify a benchmark, relation, or column.

Dense grouped preaggregation has two generic scopes:

- batch-local preaggregation is admitted when a fixed-width dense group range
  compresses the current batch by at least 8x. It supports primitive payloads
  and generated fused payload expressions, and flushes one exact delta per
  touched key;
- pipeline-lifetime count-one preaggregation is admitted for one fixed-width
  integer group key, one `COUNT` or `COUNT_STAR` lane, a proven dense domain,
  at least 2x estimated compression, and a compact domain within a 32 MiB local
  accumulator budget. The common path stores 32-bit deltas plus touched-key
  offsets; before the represented row count can overflow a compact delta, the
  state promotes once to 64-bit deltas. It accumulates across source and
  join-output batches, then flushes standard-sized unique-key chunks through
  the same grouped-state contract.

Pipeline accumulation is transactional per input batch: key rejection rolls
back that batch before another route can run. The fallback grouped primitive
keeps lookup and random state updates as separate phases to preserve CPU
memory-level parallelism. Direct projected, direct materialized, and direct
join-output aggregate strategies use the same pending accumulator and flush it
at their explicit strategy boundary. A materialized terminal binds its exact
group sources and payload source indexes once from the aggregate's canonical
input schema and typed payload descriptors; chunk execution never reconstructs
payload metadata from planner aggregate records.

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
next delta merges into that unpublished carry before any hash-table update.
Overflow within one already-compacted input range flushes the exact prefix and
carries only the suffix. Arbitrary compact producers may retain non-adjacent
duplicate keys; those batches use the normal exact grouped-state update and do
not publish a proven-unique append contract.

Semantic terminal state is pipeline-local, not compiled-call-local. DuckDB core
owns `ExecutionRegionLocalState` beside the pipeline executor and asks the
selected backend kernel to construct its derived state. SLJIT keeps generated
execution scratch and the full-pipeline terminal owner there, so scheduler
fairness yields reuse the unpublished boundary group. `NOT_FINISHED` flushes
only ephemeral source/intermediate materializers. `BLOCKED` or `INTERRUPTED`
flushes the terminal before vectorized fallback, and `FINISHED` performs the
final terminal flush. Reset or kernel replacement destroys the complete local
state. This lifecycle is backend-neutral and applies to every stateful compiled
terminal, not only grouped aggregation.

Consecutive fixed-width group keys have two pending representations. The
buffered representation compacts each source vector and bulk-copies its groups
and payload deltas into pending storage. The streaming representation loads the
source directly, keeps one active run delta in registers, and merges that delta
once per group. A bounded sample selects streaming only when it predicts at
least 3x run compression; lower-density streams retain the bulk-copy route.
This is a typed runtime-density decision, not a workload or query rule.

For one flat fixed-width key and primitive `COUNT_STAR`, `COUNT`, `SUM_INT64`,
or `SUM_HUGEINT` lanes, the streaming representation is generated machine code.
Its backend-owned ABI carries source pointers, a resumable input offset, fixed
output pointers, output count, and capacity. The kernel merges a source-boundary
carry, emits complete runs directly into operator-lifetime scratch, stops before
overwriting a full output batch, and resumes after the ordinary pending owner
flushes it. Exact keys, the three proven narrowing casts, and
integral-compression transforms use generated specializations selected from the
live source descriptor; generated-plan assumptions never guess which projection
source the runtime elides.

The tuned single-lane path keeps separate all-valid and nullable kernels. A
multi-lane executable instead owns one compile-time primitive-lane descriptor
list and one runtime lane-input array. Code generation unrolls supported
primitive updates while the lane list fits the eight-lane instruction-cache
budget. Wider homogeneous lists use one bounded generated loop; wider
heterogeneous lists and 32-bit targets keep the exact generic preaggregation
loop. Each runtime lane
supplies its payload, validity, and output pointers. A null validity pointer
means the current rows are all valid; a real mask is checked in generated code,
so nullable `SUM` and `COUNT` preserve SQL semantics without a
nullability-combination specialization matrix. Every lane shares one
represented-row-count vector, while values and `SUM` is-set bits remain
lane-local. The lane-input array and payload-source descriptor vector are cached
beside fixed pending scratch and rebound without per-chunk allocation. Generated
run lowering requires a 64-bit SLJIT machine word; 32-bit targets retain the
same exact generic reducer until all 64-bit operations have paired-register
lowering. Selections, unproven casts, unsupported primitive types, and non-flat
inputs take the existing buffered or scalar streaming route before any state is
published. Runtime proof distinguishes generated stage work from backend work,
and trace-only miss reasons add no production-mode string construction.

The selected run representation belongs to the aggregate runtime for the whole
input stream. It is decided once and never changes at a source-vector boundary:
a later unique-key vector is still consumed by a previously selected streaming
owner. Per-vector switching is invalid because the two representations share
the unpublished boundary group and proven-unique append contract. Strategy
admission therefore completes source, payload, and pending-layout validation
before publishing the decision.

Direct append reports exact new-group success to DuckDB's radix adaptivity.
Fresh trivially destructible primitive states may combine initialization and
their first update in the state-address callback. Canonical `SUM` layouts clear
the complete flag-and-padding tail, then store the `bool` at its semantic byte;
they never encode `is_set` by storing a native-endian integer word. Layouts with
destructors or noncanonical offsets retain DuckDB's ordinary initializer.
After 131,072 attempted compact groups, more than 95% proven-new groups switch
the local table to append-only mode. This statistical route always marks the
table for final duplicate reconciliation; it never relies on the single-table
finalize shortcut. Raw-tuple HLL adaptation retains its independent 1,048,576
row threshold and ownership.

Sorted compact-key streams can establish a stronger contract. The grouped hash
table, rather than a JIT backend, owns a monotonic proof for one all-valid,
fixed-width integer key. Every compact batch must be strictly increasing and
its first key must be greater than the preceding batch's last key. The observed
set is represented as bounded integral intervals: adjacent keys extend one
exact run and gaps create another until the one-vector interval budget is full.
At that boundary the local summary coalesces to one conservative first/last
hull and continues extending it. Coalescing can reject a globally disjoint
layout through a false overlap, but it cannot invent disjointness or invalidate
local monotonic uniqueness; proof memory therefore remains independent of input
cardinality without forcing a rehash solely because the stream is fragmented.

Once proven, the local table can append without duplicate lookups. The bounded
interval summary spans backend invocations and local pointer-table `Abandon()`
boundaries.
This matters under parallel scans, where scheduler ownership is cyclic and one
worker's broad first/last envelope overlaps other workers even though its exact
intervals do not. Radix finalization collects every local interval summary,
sorts the intervals by key, and skips rehash only when every active local table
contributed an intact proof and no intervals overlap. A shared boundary key is
an overlap and keeps normal reconciliation. External aggregation, any null,
type change, local key repetition or regression, unsupported route, or fallback
requires final combination. Ordinary single-table finalization retains its
independent no-abandonment rule.

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

Semantic aggregate state lives at aggregate-pipeline scope, not compiled-region
scratch scope. In particular, the direct grouped `COUNT(DISTINCT value)` pair
set belongs to `HashAggregateLocalSinkState` and resets only with that local
sink state. A region execution binding is a disposable adapter and must not own
deduplication state whose meaning spans multiple compiled executions.

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
- physical-pipeline vectorized-execution preferences derived from shared scan
  statistics for low-cardinality string predicate domains;
- whether the candidate is a full pipeline and which native protocol class it
  uses.

A vectorized-execution preference is cost evidence, not a capability blocker.
In production auto mode, shared scan statistics can reject an unprofitable
low-cardinality generated string search before region graph construction, IR
lowering, or backend analysis. Diagnostic modes may still build the complete
backend plan and record the same `rejected_vectorized_execution_preferred`
reason. This keeps capability truth separate from a representation-dependent
performance choice and lets the same native lowering serve high-cardinality
sources without charging low-cardinality queries for analysis they will not
use.

Physical-pipeline upper bounds and post-lowering candidate costs use the same
materialization-elision rule for generated projection-to-aggregate pipelines.
The physical precheck may admit backend analysis when a projection feeds a
generated aggregate update; final candidate admission still requires backend
ownership and runtime proof. This avoids rejecting profitable adaptive grouped
run preaggregation before capability analysis while preserving conservative
fallback for unsupported or high-uniqueness shapes.

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

Constant `IN` predicates use the same subject statistics. Non-null foldable
members are deduplicated, capped by the subject's distinct-count bound, and
scaled as `matching_distinct / subject_distinct`. Unsupported, nullable, or
non-constant lists retain the conservative estimate. A base-column distinct
count is never substituted when a derived subject lacks expression statistics;
doing so would estimate the wrong domain and can reverse join orientation.
This keeps join-order and build-side selection aligned with the real filtered
relation without adding a JIT-specific optimizer rule.

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

A stateful source/sink candidate with known input cardinality receives no
generated-backend or materialization-elision credit below 32 standard-vector
batches. Expression work may still justify a stateless recipe, but stateful
protocol startup cannot be hidden by a shallow generated prefix. The threshold
is workload-independent: it keeps SF1-sized decimal aggregation vectorized
while admitting the same exact recipe at SF10 and other sufficiently large
inputs.

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

The cost model derives a typed proof-requirement mask from the exact work
components it credits and carries that mask through event and counter
telemetry. Verification consumes `runner_cost_required_runtime_proofs`
directly; benchmark code must not reconstruct requirements from individual
cost columns. Adding a new credited work class therefore requires one core
proof mapping and one runtime satisfaction rule, rather than parallel
benchmark-specific inference paths.

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
Production timing comes from the shell's monotonic microsecond wall clock and
is emitted with six decimal places. Millisecond-rounded timing is too coarse for
the short generic contracts: a one-millisecond median shift can fabricate or
erase a material speedup without any execution change.
Each generic repetition executes both policies once and reverses their order on
the next repetition. This preserves the independent raw-median threshold while
preventing page-cache or frequency warmup from always favoring the second policy.

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
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --promote-baseline --promotion-repeats 10 --no-build --queries all
```

The default state is the accepted SF10 matrix. The independent SF1 state is
`benchmark/tpch/jit/tmp/tpch_refactor_guard_sf1_state.json`; run or promote it
with `--scale-factor 1 --baseline-state <that-path>`. Both accepted states must
remain complete, production-mode, one-thread artifacts with at least 10 repeats.
When `--scale-factor` is omitted, the gate inherits it from the selected
accepted state. An explicit scale factor, thread count, timing mode, or query
set that does not match that state is rejected before benchmarking. A candidate
must never be compared with an artifact from a different TPC-H configuration.

Baseline promotion is a ratchet, not a way to accept a regression. Partial
query sets cannot update the accepted state unless the caller explicitly opts
into the local-only partial-baseline escape hatch. Promotion never points at
the low-sample candidate. It reruns the complete requested query set without
decision/runtime tracing and compares that high-sample artifact with the
previous baseline. An operator may explicitly request a focused ten-repeat
rerun for a failed late query to remove sustained-run thermal/order bias. Its
row is used only if the focused comparison passes, and the resulting complete
artifact is compared again in full. The state update is atomic and happens only
after every gate passes. Runtime, speedup, preserved-win, and component-ratio thresholds
are evaluated with exact decimal arithmetic: a value exactly on a configured
boundary passes, while any value beyond it fails.

If an accepted timing artifact is proven stale by a high-sample paired run,
baseline re-initialization still requires the complete 22-query, 10-repeat
production suite, architecture verification, artifact correctness, and traced
runtime-contract verification. A refresh is valid only when compiled-region
counts and physical recipe shapes are unchanged for apparent regressions and
every selected query remains at least parity with non-JIT. It must not be used
to hide a changed runtime path or a workload that became slower than non-JIT.

Correctness, runtime-proof, and materialization-elision checks are implemented
by the JIT API helpers and `benchmark/tpch/jit/verify_tpch_benchmark.py`.

The generic production gate is part of the performance contract. Arithmetic,
CASE-heavy, multi-aggregate, persistent-table expression, filtered scan,
column-vs-column comparison, single- and multi-source nullable scan, selective
perfect-hash multi-aggregate, and single-thread grouped-DISTINCT workloads must
show an auto-policy speedup and compiled-region ownership; the arithmetic-heavy
and column-comparison classes use material speedup floors. High-cardinality
multi-fragment string search must compile and meet its material speedup floor;
the matching low-cardinality case must select vectorized execution and stay
inside the independent raw-runtime slowdown ceiling. Dense computed
perfect-hash grouping is also a compiled performance contract, including both
serial and parallel execution.
Other grouped and join workloads must remain within the bounded slowdown
budget; a vectorized result is valid when capability analysis cannot prove a
faster compiled route. This keeps JIT admission honest across production
workload classes instead of optimizing TPC-H query names.

Generic performance and runtime proof are separate modes. Production runs have
runtime tracing disabled and enforce speedup/slowdown thresholds. Traced runs
enforce correctness, compilation, and executed-region proof but never make a
performance decision. A production failure is reported from its candidate
sample. A focused ten-repeat recheck is an explicit operator action;
correctness, compilation, and missing-runtime-proof failures are never retried
as timing noise.

A verified performance win and its regression baseline are one change. The
corresponding workload floor or accepted comparison artifact must be tightened
before the implementation lands; otherwise the gate still permits the
performance that the implementation just replaced. Floors remain
thread-specific when parallel scheduling noise changes the demonstrated
margin. Retained exact-filter evidence proves 1.179x-1.191x at one thread and
1.110x-1.128x at four threads, so its floors remain 1.15x and 1.08x. Current
candidate gates use five order-alternating policy pairs and an explicitly
requested promotion or focused triage uses ten; no routine gate schedules a
larger sample.

Filtered perfect-hash and ungrouped scalar-terminal hybrids consume one SIMD
profitability contract based on scalar predicate operations. A lone comparison
stays on the scalar fast loop because terminal work remains row-at-a-time.
Arithmetic comparisons amortize mask dispatch directly. AND conjunctions build
the complete packed mask branchlessly and classify it once: classifying an
intermediate mask serializes the vector pipeline on broad scans. The SF10 Q6
regression gate exposed that architectural boundary when per-child horizontal
reductions cut the established JIT win from 2.046x to 1.657x; removing those
reductions restored 2.044x in the focused development proof while retaining the
grouped conjunction floors. OR remains available to fully packed kernels but
stays scalar for scalar-terminal hybrids because matched production timing did
not amortize its all-true check. Fully packed select, count, and sum kernels do
not use this hybrid gate. On ARM64, uniform completed masks use a horizontal
classifier and avoid a full movemask. Mixed groups share one sparse-mask
iterator: count-trailing-zero finds the next matching lane and
`mask &= mask - 1` removes it, so rejected lanes do not pay callback dispatch.

Generic benchmark fixtures are created once per read-only setup identity, before
the alternating samples. Reopening the stable database for every sample proves
persisted production data instead of repeatedly timing a just-inserted table.
Under that contract, independent promotion runs put the simple selective
workload at 1.238x-1.265x at one thread and 1.195x-1.215x at four threads, with
floors of 1.22x and 1.17x. After the branchless-mask root fix, the two-way
conjunction proves 1.334x-1.366x and 1.276x-1.278x, with floors of 1.31x and
1.25x. The three-way conjunction proves 1.302x-1.315x and 1.225x-1.227x, with
floors of 1.25x and 1.20x. The ten-run SF10 Q6 proof restores 2.044x and passes
the strict comparison against the prior 2.054x accepted baseline. The non-null
grouped workload proves 1.167x-1.176x at one thread and 1.143x at four
threads, with thread-specific floors of 1.16x and 1.13x.
The mixed-source complementary join promotion receipts prove 1.332x at one
thread and 1.250x at four threads in alternating 10-repeat runs. The direct
perfect-hash terminal combines a pipeline-local group accumulator with an
executable-owned RHS predicate classifier. Direct volume is combined across
pipeline tasks until it covers the dictionary, then one task publishes an
immutable dictionary-owning byte classifier through DuckDB's atomic shared
pointer for exact unpacked strings. Compact fixed-width strings retain their
direct packed comparison; the same applies when both constants are inline.
Every task retains the published owner before byte lookup; the mutex is absent
from the post-publication path. For direct output with an incremental source,
the build-side dictionary index derives as normalized key minus hash minimum.
Its all-valid one/two-key terminal accumulates incremental inline groups at
their ordinal and commits only after the entire vector proves the compact
domain; any NULL, long string, or unseen key leaves state untouched for the
general transform path. Its checked-in floors are 1.31x and 1.24x respectively.
The T4 promotion artifact is
`benchmark/jit/tmp/contiguous_build_index_t4_promotion10_20260714`.
The mixed numeric/date plus nullable-string scan proves the generic partial
predicate contract outside TPC-H: 1.338x at one thread and 1.286x at four
threads. Its checked-in floors are 1.25x and 1.20x. Disabling only partial
predicate SIMD raises the one-thread JIT median from 0.0505s to 0.0565s, proving
that the gain belongs to the split execution mechanism rather than unrelated
JIT work. The current complete ten-repeat TPC-H promotions preserve Q12 at
1.334x at SF1 and 1.288x at SF10.
Pipeline-local carry and exact parallel dense-run proofs move the generic
six-million-row projected workload from 0.118126s to 0.036658s at one thread
(3.222x) and from 0.030526s to 0.016050s at four threads (1.902x). Its checked-in
floors are now 3.00x and 1.75x. The materialized arithmetic-key variant moves
from 0.118844s to 0.048766s (2.437x) and from 0.031131s to 0.019352s (1.609x),
with floors of 2.25x and 1.45x. These are independent ten-repeat production
promotions with tracing disabled and zero correctness differences or compile
errors. The same generic generated-run mechanism keeps accepted TPC-H Q18 at
1.455x at SF1 and 1.660x at SF10 in complete ten-repeat production matrices.
The nullable multi-lane variant fuses `SUM(nullable)` and `COUNT(nullable)` in
one generated run kernel. Its prior scalar replay spent 0.156s in local
preaggregation and made JIT 1.55x slower. Generated execution lowers that stage
to 0.039s in the traced receipt and proves 1.785x at one thread and 1.505x at
four threads over ten production repetitions. The checked-in floors are 1.70x
and 1.45x. No workload identity participates in lane binding or code generation.
The focused receipts are
`benchmark/jit/tmp/pipeline_local_dense_runs_t1_promotion10_20260713` and
`benchmark/jit/tmp/pipeline_local_dense_runs_t4_promotion10_20260713`.
The nullable multi-lane receipts are
`benchmark/jit/tmp/grouped_nullable_sorted_runs_multilane_t1_promotion10_20260713`
and
`benchmark/jit/tmp/grouped_nullable_sorted_runs_multilane_t4_promotion10_20260713`.
The 16-lane shared-affine receipts are
`benchmark/jit/tmp/grouped_wide_affine_compact_scratch_t1_promotion10_20260715`
and
`benchmark/jit/tmp/grouped_wide_affine_compact_scratch_t4_promotion10_20260715`.
They prove 2.400x and 2.031x, with checked-in floors of 2.25x and 1.90x. The
generated run kernel keeps one widened shared base sum and valid count per
compact group.
Generated code publishes vector-bounded machine-word deltas; the runtime widens
them immediately so a pending group can span any number of vectors safely. The
final state-address update uses checked machine-word affine arithmetic for the
common case and widens only on overflow, avoiding both per-row expression replay
and an intermediate groups-by-lanes materialization. Code size stays bounded
independently of lane count, and payload-source layouts explicitly distinguish
direct per-lane coordinates from fused combined-source coordinates.
Eligible run kernels are generated only after the runtime sample proves useful
ordering, and only the observed key-cast/nullability specialization is published.
Pipeline-local payload-source descriptors retain their allocation across chunk
rebindings. Generated run lowering is admitted only when the SLJIT machine word
can represent every 64-bit key, payload, and state operation and the target has
the required register file; unsupported targets use the exact generic route
until a paired-register lowering is implemented.
The complementary-join receipts are
`benchmark/jit/tmp/dense_perfect_hash_predicate_cache_promotion10_20260714` and
`benchmark/jit/tmp/string_complementary_fast_probe_t4_promotion10_20260713`.

Current full-matrix generic evidence is stored in
`benchmark/jit/tmp/full_generic_review_root_fixes_t1_candidate5_20260713` and
`benchmark/jit/tmp/full_generic_review_root_fixes_t4_candidate5_20260713`. Both production
gates pass with zero correctness differences or compile errors across range
arithmetic, filters, CASE, multi-aggregate, persistent scans, nullable
expressions, grouped aggregation, DISTINCT, numeric joins, and string joins.
The branchless conjunction promotion receipts are
`benchmark/jit/tmp/branchless_conjunction_root_fix_promotion_t1_20260713`,
`benchmark/jit/tmp/branchless_conjunction_root_fix_promotion_t4_20260713`, and
`benchmark/tpch/jit/tmp/q06_branchless_root_fix_promotion_20260713`.
These artifacts are workload-class evidence; they do not authorize
workload-specific capability checks.

The current generic matrix is defined by the workload list in
`benchmark/jit/generic_benchmark.py`; documentation does not maintain a second
hard-coded workload count. Compiled one-thread
speedups range from 1.118x to 3.771x and compiled four-thread speedups range
from 1.060x to 3.728x. The low-cardinality string-search control remains
vectorized at 0.994x and 0.971x respectively, inside its independent 5%
raw-runtime ceiling. Current accepted TPC-H SF1 and SF10 promotion artifacts
are documented in `benchmark/tpch/jit/JIT_BROAD_QUERY_PLAN.md`; both require a
complete 22-query, 10-repeat production matrix plus correctness and traced
runtime-proof passes before the state file moves.

## Current boundaries

- Mixed, filtered, ordered, multi-aggregate, and unsupported DISTINCT shapes
  remain DuckDB-owned outside the proven `DISTINCT_KEY_SINK` contracts.
- Variable-width and string expression coverage is narrower than fixed-width
  coverage.
- Pipeline-lifetime dense accumulation currently covers only one `COUNT` or
  `COUNT_STAR` lane within the 32 MiB compact-domain budget. Other unordered
  sparse payload shapes use batch-local dense preaggregation when profitable,
  consecutive-run preaggregation when proven, or exact per-row grouped-state
  updates.
- Generated pending-run aggregation currently accepts one fixed-width key, one
  or more primitive lanes, exact keys, proven signed narrowing casts, integral
  compression, and nullable payloads from both projected and materialized
  direct inputs. Generated reducers unroll up to eight supported lanes on
  64-bit SLJIT targets; wider homogeneous lists use one bounded looped kernel.
  Heterogeneous wider lists and 32-bit targets retain the buffered, scalar, or
  exact vectorized route. Multi-key, selected, and non-flat inputs remain
  explicit boundaries. Source-fusing general multi-input affine group
  expressions is the next generic grouped-run boundary.
- Native source/sink protocols remain native unless an explicit execution
  contract makes them safe to compose.
- Native-only execution is a valid result of capability analysis, not a failed
  recipe or an excuse to loosen CBO accounting.
