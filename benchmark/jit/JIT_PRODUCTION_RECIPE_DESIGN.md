# JIT Execution Architecture

This document is the architecture contract for DuckDB execution-region JIT and
the plan that moves the code to it. Each part states the **contract** (rules
every change must keep), the **target** (the state the remaining work reaches),
and where the code still differs (**current state**). The plan at the end
orders the remaining work; every step lands behind the gates in
`benchmark/jit/README.md`. The document describes the code as it is: history
lives in the commit log, and benchmark results in machine-readable artifacts.

## Goal

JIT must accelerate generic physical-plan shapes while preserving DuckDB's
semantics and vectorized fallback. TPC-H is an important regression workload,
not an architecture input. Production code must never branch on query ids, SQL
text, benchmark names, table names, column names, or scale factors.

```text
physical pipeline
  -> execution-region graph and typed region IR        (core)
  -> one full-pipeline stage plan                      (core)
  -> backend lowering: artifact plan + instance facts  (backend)
  -> core cost decision                                (core)
  -> shared compiled artifact (cached)                 (backend, core-owned cache)
  -> query instance: bindings, slots, chunk step       (backend + core)
  -> chunk step inside the native pipeline executor    (core loop)
```

## Where the structure falls short of the principles

- **Execution ownership.** Several backend fast paths run precompiled C++
  rather than generated machine code. Their placement still needs qualification
  against equivalent native execution.
- **Data layer.** Rows move between steps through many batch, pending and view
  types and are translated across many index spaces by per-family remap
  helpers. Index-space confusion is the dominant wrong-result class: a layout
  that silently differs from what a consumer assumes produces garbage, not an
  error.
- **Strategy catalog.** Runtime cascades try strategies in order per call and
  re-check facts already known at bind time.
- **Representations.** Several structures describe one pipeline. Op plans now
  hold one kind's payload, but large core contracts inside those payloads are
  still deep-copied on a cache miss. The structural artifact key retains some
  estimates and statistics until runtime-only consumers use instance bindings.
- **Binding.** Backends re-check `ready` flags and layout facts per chunk.

## Principles

1. **Core owns every protocol.** Source progress, sink backpressure,
   partitions, cancellation, and errors live in the native pipeline executor.
   Compiled code is a chunk step inside it.
2. **One data layer.** Rows travel between steps as batches whose layout is a
   typed, immutable descriptor. A step that changes the layout produces a new
   layout; nothing reinterprets another space's indices.
3. **Decide once, execute directly.** A step's strategy is chosen when the step
   binds and then executed per batch. Runtime fall-through between strategies
   is a design defect, not a fallback.
4. **Artifacts are shapes; instances are facts.** A shared artifact holds only
   compile-affecting structure and is identified structurally. Everything
   about one query lives in its instance.
5. **Bindings describe, backends choose, core materializes.** Core never
   performs a binding side effect the backend did not request.
6. **Generated code earns its place.** A fused path whose hot loop runs no
   generated code is a candidate for a native DuckDB operator, not a backend
   specialization.
7. **Correctness is differential.** Results under the JIT match
   `jit_policy='off'` on random queries; tests assert properties, not route
   names.

## Layer ownership

### DuckDB core

Core owns:

- physical operators and their source, operator, and sink protocols;
- execution-region graph construction and the typed region IR;
- backend-neutral runtime ABIs for batches, joins, aggregates, and pipelines;
- the cost decision, scheduling, cancellation, and errors;
- correctness, telemetry, runtime proof, and native execution.

Core exports facts and operations. It does not expose concrete private storage
types merely to support one backend.

### Backend

SLJIT owns capability analysis over the region IR and exported ABIs, artifact
plans and their structural keys, instance binding, generated code, executable
ownership, and exact delegation reporting. The backend must not infer semantic
support from mutable runtime state; it specializes physical access only after a
semantic recipe has been admitted.

Executable-memory policy is selected at build time by platform: Apple uses
SLJIT's `MAP_JIT` allocator, Linux uses distinct pooled RW and RX mappings, and
Windows and other supported POSIX systems use per-allocation W^X transitions.
Generated code is never published through a generic RWX allocator.

The native-vector calling convention requires six saved registers that SLJIT
exposes as real addressable machine registers, and each kernel layout must fit
beside its declared scratch-register set. x86-32 virtualizes the required
`S3`-`S5` registers, so the SLJIT backend registers as unavailable there.
`SljitTargetCapabilities` is the single owner of target architecture, word
width, SIMD availability, and addressable saved registers; it builds one
process-lifetime snapshot, and generated row loops contain no capability
branches. Semantic layouts assign registers to roles; an optional role is
either assigned an addressable register or absent, and the planner selects the
register or spill layout.

Metal admits one exact typed recipe — source contract, scalar projection,
append sink — and never caches artifacts.

Backend registration freezes name, description, runner kind, and region
support before publication. The registry mutex protects only that metadata and
backend lifetime; availability callbacks run after it is released, and
backends are never unregistered, so no extension code runs under a core lock.

### Cost model

Core is the only owner of runner policy. Backends publish neutral work facts
(startup cost, row work, generated stages, materialization cost) and never
implement a second selector. `jit_policy=off` always selects native execution.
`jit_policy=auto` selects a compiled runner only when capability and cost both
approve it. A selected runner that cannot compile or execute its admitted
recipe is a failed proof, not evidence that the route was never selected.

**Target:** one decision point after the graph is built, with one typed input
and typed rejection reasons. **Current state:** five gates
(`ExecutionRegionProductionEligibilityAllowsPlanning`, the pre-graph runner
selection, `ExecutionRegionGraphMayHaveCostedAcceleration`, the cost-only
runner, and the post-analysis runner), all reaching
`DuckDBCostModel::SelectPhysicalRunner` with different inputs. Observed
runtime cardinality never feeds the decision; adaptive choices live in kernels
(see Binding contract). A complete grouped-lookup replacement is thread-local,
so parallel admission needs evidence that it amortizes: an estimated 2x group
reduction, or a single grouping key, whose runtime run sample hands batches
without runs to the native sink. A grouped aggregate counts as generated
compute because its generated body replaces native group lookups. An ungrouped
aggregate does not: a bare `sum(col)` or `count(*)` does exactly the native
vectorized update's work, so only the filters, projections, and probes it
fuses with make a pipeline generated work.

## Execution model: chunk steps

The pipeline executor owns the loop of every compiled kernel. It fetches each
chunk through the source contract the plan opened, advances a partitioned
sink's batches, retries a blocked sink, and finalizes, exactly as for the
vectorized operator chain. The kernel binds to one executor as a chunk step and
replaces the operator chain:

- `ExecuteChunk` runs one source chunk to completion. Output the sink blocks on
  queues in the executor; the executor yields after a blocked sink and sinks
  the queue first when it wakes, so one task never registers two callbacks.
- `FlushChunks` sinks every row the step holds across chunks. The executor
  calls it before a fetched chunk moves a partitioned sink to another
  partition.
- `FinishChunks` sinks what the step holds once the source is exhausted, in
  place of the caching-operator flush. A pipeline whose sink already finished
  drops held rows, as caching operators do.

The step keeps its state for the executor's lifetime; any executor state is a
legal point to continue from, so nothing in the kernel tracks yields,
partitions, or interrupt registration. A pipeline fed by its producers' pushes
(a direct CTE consumer) plans like any other; its executor binds the step when
it is created. The aggregate-state source recipe, whose source is not chunks,
keeps its own fetch loop for one entry's chunk budget.

No runner takes a running pipeline over. A kernel that cannot start (a
native-delegated probe whose runtime layout is not ready) leaves the pipeline
to the vectorized loop before the first fetch, and the latch keeps that
executor vectorized. A native operator that stops being ready mid-stream is an
internal error. `jit_debug_force_entry_defer` exercises the entry deferral.

## Data layer: batches and layouts

### Contract

- A batch is a data chunk, a logical cardinality, an optional selection, and
  its lifetime: borrowed until the next fetch, or owned by the step. Identity
  order is represented without a disposable selection; an all-match filter
  returns the unchanged batch.
- A primitive may borrow scratch only until the next scratch mutation; a
  continuation that outlives the call owns or copies its state.
- A consumer states the physical representation it accepts. A generic loader
  may dispatch every representation; a row-layout, compressed-row, or
  row-pointer consumer rejects dictionary storage. The complementary join
  consumer accepts a dictionary group vector explicitly: it resolves each
  referenced entry to its group once per vector, after proving every entry
  convertible and the groups within the accumulator, so a declined vector
  leaves no state. Silent interpretation of one representation as another is
  forbidden.
- Fast-path caches key their validity on stable identities — dictionary ids,
  value keys, epochs — never on reusable addresses, and every event that
  relocates rows disables caches holding row addresses.

### Target

**Typed layouts.** Every edge between two steps of a recipe has an immutable
`BatchLayout`: the ordered columns, each with logical type, physical type, and
the region-IR slot it carries. Recipe construction creates the layouts once.
Each step declares the layout it consumes and the layout it produces; a step
that re-lays rows out (a projection, a string-set preclassification, published
preaggregated keys, a MARK marker omission, an elided projection) produces a
new layout instead of reusing the old indices. Consumers address columns by
slot through their input layout. A binding whose layout does not match the
declared one fails at bind time, never as silent garbage. The remaining index
spaces reduce to two — region-IR slots and the column positions of one layout — and
the remap helpers to the layout's slot lookup.

**Sources fill batches.** The source contract fills a batch across storage
vectors: each vector's surviving rows are compacted into the batch at its
current offset (fixed-width values copied, strings referenced with their
buffers kept alive), until the batch is full or the scan assignment ends. A
kernel therefore never re-batches source rows, and the per-vector source work
(dictionary slices, chunk resets, fetch dispatch) disappears.

**One coalescer.** Rows are coalesced between steps in exactly one place, with
one rule based on the bytes a copy would move. Owned batches reduce to the
source batch, the step coalescer, and the typed pending states of aggregate
terminals.

### Current state

- `ExecutionChunkCoalescer` owns the source compaction policy. The table scan
  uses it to fill a chunk within one scan assignment, amortizing storage fetch
  calls. `ExecutionCoalescingChunkStep` buffers remaining partial source chunks
  at the executor's partition boundary. Backends own no source-fetch buffer.
  Full batches, storage dictionaries and FSST encodings pass through unchanged;
  selected fixed-width values are gathered, and buffered strings use the native
  owning copy. A vector's auxiliary heap does not prove ownership of every
  string, so the coalescer never infers lifetime from its presence. Partition
  changes flush buffered rows before advancing, and a finished sink consumes no
  further rows.
- Downstream batching rules still exist in
  `SljitChunkHasOutOfLineValues`, `SljitAppendChunkToInitializedBatch`,
  `SljitProjectionHasVariableWidthOutput`, `CanBatchTerminalAggregateUpdate`,
  the pending row-pointer and input-vector batches, the pending preaggregated
  group batch, the count-star pending state, the matched-row microbatch, and
  core's sink coalescing. The hash-join materialize batch and the post-join
  projected batch deep-copy strings with no width check.
- A pending row-pointer batch is typed by its layout
  (`SljitPendingRowPointerBatch<LAYOUT>`): the payload batch and the string-set
  class batch flush through their own updates, so no consumer reads a class
  column as the payload it replaced. The pending preaggregated group batch
  keeps published keys in a chunk of their own.
- MARK boundaries and elided pre-join projections share `SljitBatchLayout`:
  an immutable producer identity and ordered columns with source position,
  source type and logical result type. Physical types derive from the logical
  types; a type-changing column still requires the producer's checked
  expression, never reinterpretation. Layout construction validates source
  positions and types. These positions are producer-scoped, not yet global
  region-IR slots.
- A MARK boundary binds filter semantics, marker representation, probe strategy
  and its layout during recipe construction. Omitting the marker produces a
  new layout. LHS columns name their actual probe-input positions; the computed
  marker has no input slot. Build-side emptiness and NULL state remain query-local facts.
  Output buffers derive their type vector from the layout only at initialization.
- An elided projection binds the probe primitive directly. Its layout resolves
  key and residual positions into one immutable input plan, preserving checked
  cast chains. There is no intermediate view descriptor, parallel positional
  map, or cached compatibility flag. Recipe copies share the layout and plan;
  selected batches borrow the layout for the recipe's lifetime. Aggregate
  descriptor caches retain its identity instead of copying a map and comparing
  it per batch. Query-specific plan facts still need the instance-slot split.
- Join-to-aggregate inputs have one ordered slot descriptor: each slot owns its
  logical type and bound probe reference, build reference, or expression access.
  Sparse slots retain their declared type without a source. Input and pending
  batches derive their schemas from these slots only when initialized.
  Semantic join-output positions resolve through the producer's typed layout
  during binding; checked producer expressions compose with the consumer instead
  of being replaced by a physical index. Equality-key aliases use the semantic
  probe key's producer-output position. There is no reverse lookup by physical
  source, separate output-to-projection vector, or mapped/unmapped descriptor split.
  Computed inputs own query-local expression scratch; references need none.
- Expressions have two index spaces with one rule each. An executable
  predicate reads its sources through input slots (`input_source_indices`
  maps slot to column); an ordinary scalar projection still reads source
  columns from its plan.
  Composition, semantic remapping and re-preparation start from the semantic
  plan, which reads input columns (`SljitCopySemanticExpressionPlan`); an
  executable expression is re-addressed in place, keeping its compiled code,
  by `SljitTryRemapExecutableExpressionInputs`. Every executable aggregate
  payload reads slots through `input_source_indices`, including scalar
  references and binary operands. Re-addressing changes the input map, not the
  expression's slots. `DIRECT_PER_LANE` and `FUSED_COMBINED` describe kernel
  input-array layouts, not different meanings for expression indices. Semantic
  aggregate plans retain column indices for composition and code generation.

## Plan representation and artifact identity

### Contract

- **Artifact and instance.** The database-local execution-region manager owns
  reusable artifacts in a bounded LRU. An artifact holds compiled code,
  immutable descriptors, and concurrency-safe lazy specialization cells.
  Telemetry, counters, operator bindings, runtime-filter identities,
  hash-table dictionaries, and all other query data live in the instance (the
  kernel, its local states, and the chunk step). Live pointers never enter an
  artifact or its key. A backend that cannot make this split remains correct
  and uncached.
- **Key completeness.** The key covers every compile-affecting fact, so every
  plan that maps to a key can bind the shared artifact. Failure to instantiate
  a cache hit is a backend contract error; core does not hide an incomplete
  key with an eviction-and-retry path.
- **Publication** is transactional and single-flight: core validates the
  compile result, instantiates the kernel, validates its executable body, and
  only then publishes; a failed build or first instantiation aborts the
  reservation. Active kernels keep shared ownership after eviction.
  Generated code and its owning allocation publish as one lifetime unit;
  readers acquire the owner before reading a callable, using DuckDB's atomic
  shared ownership or one mutex for every read and write.
- **Immutable recipe.** `SljitFullPipelineRecipePlan` is either a validated,
  non-empty recipe or an explicit native-only path; the construction-only
  `INVALID` tag is rejected by runtime. Recipe construction is the single
  admission boundary: analyze complete operator facts, bind semantic
  descriptors, bind each primitive once, construct the sequence locally,
  finalize ownership in one pass, publish only after every check passes.
  Family binders expose failure-atomic `TryMake...` operations. Runtime
  preparation initializes physical state but cannot return a recipe-shape
  miss; an accepted but inconsistent recipe raises an internal error.
- **Region planning authority.** A pipeline produces at most one full-pipeline
  candidate whose ordered stage plan is the sole authority for generated
  stages, native contracts, source boundaries, and missing contracts. Backend
  analysis returns one selected-stage receipt; core rejects missing,
  duplicated, reordered, or unfused ownership before cost selection.
- **Typed dispatch boundaries.** Heavy typed runtime families are partitioned
  only at batch-level dispatch boundaries; a virtual call, callback, or
  function pointer inside a row, match, or aggregate-update loop is not
  allowed.

### Target

**One plan, moved.** The region IR stays the semantic authority. Backend
lowering produces an artifact plan of kind-tagged op descriptors (each op
holds only its own kind's data, within a size budget of about 1 KB), plus the
instance facts. Plans move from stage to stage; nothing deep-copies operator
or sink infos. A pipeline plans once after dependencies finalize, before any
local source state captures the selected layout and filter provider.

**Structural identity.** The artifact key is a canonical binary encoding of the
artifact plan, written by each descriptor from the same fields its code
generation reads, compared byte for byte together with the backend ABI.
Estimates enter only as the compile decisions derived from them; diagnostic
text, reasons, and blockers never enter. Descriptions are rendered on demand
for diagnostics only.

**Instance slots.** The instance holds a slot table indexed by artifact op:
runtime-filter identities, dynamic-filter handles, prepared input plans,
cast-error identity (query location and source alias), and the
stats-derived parameters (min/max, group domains, reserve counts) that code
reads as runtime parameters. Only facts generated code bakes in as constants
belong to the key. Two queries that differ only in literals' positions,
estimates, or column statistics within the same compiled shape share one
artifact.

**No strings as control flow.** Stage status, blockers, and readiness are typed
enums; reason text is derived from them.

### Current state

- Several structures describe one pipeline: the execution contract, region
  graph, region IR, candidate with stage plan, lowering plan, native region
  plan, executable region, artifact, cached artifact, kernel, local state, and
  chunk step.
- `sljit_artifact_key.cpp` encodes typed plan fields and contracts with binary
  serialization. Neither candidate descriptions nor diagnostic renderers,
  reasons or nested blockers participate. Binary values preserve signed zero.
  The key retains statistics used to specialize code and recipes, plus
  `emit_flat_nullable_fast_path`, which changes generated code. Aggregate input
  estimates and reservation estimates are instance parameters, not key inputs.
  Cast aliases and SQL positions are not key inputs: structural error slots are
  bound to the current execution instead. Semantic error-message literals remain
  part of expression identity.
- Semantic and executable operator descriptors each own one kind-tagged payload;
  the payload tag is their only kind authority. Inactive operator families are
  neither constructed nor copied. Core operator contracts also hold exactly one
  hash-join or nested-loop payload; the tag determines their kind, and diagnostics
  and trait analysis inspect only that family. Sink contracts use the same
  tagged ownership; delimiter sinks explicitly compose distinct aggregation
  and hash-build payloads. Sources carry no unused aggregate-update payload.
  Backend aggregate, hash-build and ordered-sink plans own their typed family
  payload directly, including through runtime binding and artifact-key encoding.
  Probe and append plans still retain broad operator/sink contracts.
  Broad source contracts and large individual payloads still prevent meeting
  the smaller descriptor budget.
- Runtime-filter identities stay out of the artifact: the probe plan
  records a binding slot, `SljitNativeHashJoinProbePlan::Copy` drops the
  identity, and the kernel hands its identity bindings to each thread's
  scratch. Source statistics used by runtime strategies also live in
  `SljitRegionInstanceBindings`, not the shared artifact's source metadata.
  Aggregate input estimates and group-reservation targets are bound once per
  instance, including on cache hits. Each thread borrows immutable parameters
  through its scratch; batch code performs no virtual parameter lookup. The
  reserve-count policy and dense-preaggregation economics remain unchanged.
  Lowering also extracts cast-error identities into those bindings and clears
  diagnostic `fields`, `reason`, and `ir` from shared operator/sink infos and
  expression fragments. Cast slot IDs survive expression copying, remapping,
  and join/aggregate composition. Error reporting resolves the slot only on the
  cold error path; generated loops do not look up query metadata.
- Post-join aggregate recipes own their semantic projection and prepared
  probe/build input expressions. Composition and any additional code generation
  finish before artifact publication; their new code allocations count toward
  artifact size. Execution-local descriptors borrow those callables and own
  only bindings and mutable adapters, not another compiled projection.
- Planning runs once at executor preparation, after dependencies finalize.
  `ConfigureExecutionSourceState` configures the existing native source before
  local state creation; it must not reopen or consume a stateful source.
  Recursive rescheduling reuses the plan and configures newly created source
  state when necessary. Changing a pipeline's input mode invalidates its plan.
- Strings decide behavior in two places: the source function names
  `"seq_scan"` and `"hash_aggregate_scan"` gate statistics and recipe
  binding, and contract `*_ready` flags are set from empty blocker strings.
  Region nodes read their lowering status from their boundary kind.

## Binding contract

### Contract

Regular hash-join compilation and execution use two contracts: semantic IR
holds reusable facts (key types, row-layout offsets, tuple and pointer
offsets, join semantics, output mapping), and the query-local binding holds
non-owning physical views (encoded entry words, pointer and salt masks,
optional chain pointers, bloom words). Core builds the physical
`TupleDataLayout` and the semantic row-layout contract through one canonical
builder, so zero-copy offsets cannot drift. The table accessor reports typed
states (`READY`, `EMPTY`, `NOT_FINALIZED`, `POINTER_TABLE_MISSING`);
diagnostic strings derive from those states.

Dictionary-slot narrowing is a one-time, per-column core layout decision shared
by native and compiled execution. Admission follows reference projections,
selection-preserving operators and join output mappings to finalized producers.
Probe lanes that can be NULL-extended or replayed flat from spill cannot narrow;
stable RHS dictionaries remain eligible independently of other output lanes.
A finalized perfect-hash or global-dictionary RHS producer establishes a new
dictionary promise, so unrelated ancestors cannot invalidate it. Materialized,
multi-source and unknown lanes require a new producer proof. A consuming table's
own spill and repartition preserve its pinned dictionaries and narrow indices.

External hash joins distinguish build-round memory from repartition workspace.
Each build round and repartition sizing use the reservation minus probe-buffer
requirements, clamped at zero.
A single probe-buffer estimate uses the actual spill schema, including its hash
column, before and after repartition; join-output types do not define this cost.
A capped reservation cannot wrap into an unlimited budget; the existing external
finalizer makes progress with one nonempty partition when none fit the budget.
Repartition sizing targets a quarter of that budget per partition but caps
fan-out by the workspace needed to split an old partition. Selection and task
scheduling share that workspace calculation and use the actual old radix width,
including after recursive reuse. No split is scheduled when even one task cannot
fit; a zero build-round budget never requests maximum fan-out.

The entry view is an array of `uint64_t` words with published pointer and salt
masks and a bucket shift: a probe hash starts at bucket
`(hash >> bucket_shift) & bitmask`, so a parallel in-memory build can align its
buckets with its radix partitions. The bloom view is a word array plus bitmask
using the shared bloom-mask primitive, published only for a sealed filter that
still rejects enough to pay for hashing. Backend code never names `ht_entry_t`, `BloomFilter`, or
`JoinHashTable`, and never dereferences the opaque hash-table handle a probe
binding may carry. `ExecutionHashJoinRHSFixedColumnSource` is the boundary for
fixed build-side columns, either `ROW` (row-layout offset plus validity) or
`DICTIONARY` (index offset, data, validity, count); core resolves and caches
these per operator and initializes every field before publishing `ready`.
Storage-dependent RHS gathering is an exported core operation.
A perfect-hash build fills its table from disjoint build-chunk ranges in
parallel and builds its prefix-range filter the same way; a payload column
without NULL values keeps an absent validity mask, the no-NULL proof that
all-valid RHS consumers read.

Execution-region backends use DuckDB's version- and platform-matched C++
extension contract plus an execution-region ABI version, currently 21. Any
backend-visible layout or virtual-interface change advances it; registration
rejects other versions, and the boundary verifier pins the value.

### Target

Binding has three phases:

1. **Describe.** Core publishes the binding view — layouts, physical sources,
   semantics — with no side effects.
2. **Choose.** The backend selects the step's strategy from the view, its
   input layout, and instance facts, and lists what it needs core to prepare.
3. **Materialize.** Core performs exactly the requested work (a bloom filter,
   dictionary arrays, append state) and validates the final view once.

Afterwards the backend never re-checks binding facts per chunk. Bindings are
narrow, typed views: one struct per layout kind instead of a union of every
kind's fields, no raw pointers to operator local state. Data-dependent choices
(bloom filters, direct versus materialized consumers, adaptive preaggregation)
are explicit monotone transitions recorded once, driven by observed counts.

### Current state

- The probe bloom filter follows the three phases: the bind sets
  `bloom_filter_requested`, the SLJIT bind requests
  `ExecutionUseHashJoinProbeBloomFilter` unless the probe key's exact-filter
  proof names this hash table, and the decision is a runtime path.
- `ExecutionHashJoinProbeBinding` embeds both the regular and the perfect
  layout. `ExecutionBindHashJoinBuild`
  mutates the local sink's payload chunk and append state; aggregate binds
  allocate state adaptors.
- The native probe bind validates readiness, output mode, key count and the
  regular or perfect layout once, using the selected input plan. Grouped
  primitive updates publish one bound strategy after validating their state;
  count-one lane compatibility is cached with the payload lanes. Selections,
  nullability, actual count-one values and checked-cast proofs remain batch facts.
- Other consumers still re-check binding facts (row-pointer and input-vector
  updates, perfect-hash output, complementary sums and aggregate consumer
  eligibility). Their strategies and typed binding views remain to be unified.

## Strategy selection and the fast-path catalog

### Target

A step is a strategy plus its descriptor, chosen once when the step binds.
Per batch the step runs that strategy. Variation within the contract — a
selection present or absent, validity known or unknown — is handled inside
the strategy by code that dispatches once per batch. Changing strategy after
binding happens only through a declared monotone transition (for example
`DIRECT` → `HYBRID` for the direct probe consumer) with its reason recorded.

Each precompiled C++ family belongs to one of three categories:

- **Generated**: its hot loop runs generated code; it stays in the backend.
- **Native candidate**: pure C++ that removes operator overhead; it moves into
  the native DuckDB operator it accelerates, where it also serves
  `jit_policy='off'`, unless measurement shows it needs region context.
- **Shape-specific**: fixed to one plan shape (such as a complementary sum
  over exactly two constants); it is generalized or deleted.

Runtime proof reports generated and C++ work separately, so each family's
share of a win is measurable.

### Current state

The MARK boundary already executes a bound strategy. Its runtime dispatch
does not revisit semantic eligibility; empty outputs are skipped by the
shared probe drain before boundary materialization.

The runtime cascades that try alternatives in order:

| Cascade | Location | On total failure |
|---|---|---|
| Grouped input-vector update | `sljit_grouped_aggregate_input_vector_update_runtime.hpp` | callers throw |
| Row-pointer grouped update | `sljit_row_pointer_grouped_aggregate_update_runtime.hpp` | caller throws |
| Direct join-output aggregate | `sljit_direct_join_output_aggregate_runtime.hpp` | native materialize |
| Probe aggregate consumer | `sljit_hash_join_probe_aggregate_consumer_runtime.hpp` | generic probe drain |
| Probe path | `sljit_hash_join_probe_executor_runtime.hpp` | generated kernel |
| Grouped bound update | `sljit_grouped_aggregate_direct_update_runtime.hpp` | generic path |
| Projection chain | `sljit_projection_chain_runtime.hpp` | expression runtime |
| Full-pipeline step | `sljit_native_pipeline_runtime.hpp` | throw |

Most runtime descriptor work runs on the first chunk and is cached; a failure
disables that strategy for the step, and adaptive strategies (row-pointer
preaggregation, pending-run buffering) switch themselves off after a rejected
batch, so falling through costs little time. Join projection aggregate inputs
dispatch bound access recipes: expression preparation and source mapping run
once before the descriptor becomes ready. Capacity, selection, and current
hash-table storage checks remain batch-dependent. The precompiled families are
preaggregation (mostly C++, with generated-kernel call sites), complementary
sums, all-valid probe templates, probe aggregate consumers, count-star
preaggregation and string-set classification, all pure C++ apart from the
first.

## Semantic rules that hold throughout

### Source filters

Source-filter selection has one output: the selected-stage receipt plus the
source recipe. The receipt says whether each static filter is generated or
storage-owned; the recipe gives the residual scan-filter mode and source input
layout:

- `STATIC_PRUNING_ONLY`: generated code evaluates static filters per row;
  storage keeps them only for zonemap pruning;
- `DYNAMIC_FILTERS_WITH_STATIC_PRUNING`: as above, and storage also evaluates
  the runtime dynamic subset as residual filters;
- `ALL`: storage owns pruning and row-level evaluation;
- `NONE`: no storage filter participation.

Prune-only filters are not executed per vector. The vectorized path still owns
the complete combined filter set, so a deferred entry preserves semantics.
Source selection is workload-independent but still uses cost heuristics:
estimated string-value reuse favors storage search predicates; ordinary string
filters additionally consider compression eligibility, mixed-predicate fusion,
and dynamic-filter survivors. These estimates do not prove that a column is
dictionary encoded. Actual dictionary execution remains storage-owned.

Selecting storage filters does not veto the accelerated runner. The selected
lowering removes those filters from generated work, and the CBO prices the
remaining projection, join, and aggregate stages. The physical precheck cannot
reject that work based on the distinct count of a source predicate. Backend
preferences for generated predicates apply only to filters remaining in the
executable body, not to predicates already owned by storage.

Moving a static predicate into a region is valid only when the retained source
layout supplies every referenced column, evaluation is exception-free for all
admitted values, NULL behavior is identical, and exactly one owner evaluates
it. If a generated selector is not part of the final recipe, the normal DuckDB
filter remains.

### Expressions

Expression support is type-driven: one descriptor defines input type, width,
signedness, nullability, result ABI, overflow semantics, code generation, and
runtime adaptation, and matcher, codegen, runtime, and cost model consume the
same facts. Integer operations sign-extend explicitly; a failing checked
operation re-runs DuckDB's own operator so the error and operands match.
Floating-point comparisons follow DuckDB's order, not IEEE's: a NaN ranks above
every other value and equals every other NaN, so a comparison against a number
reads the unordered outcome from the rank, and a comparison of two values that
may both be NaN compares their ranks. A decimal converts to DOUBLE as DuckDB
casts it: `raw / 10^scale` only while the raw value is exact in a double
(`|raw| <= 2^53`) and `10^scale` is exact (scale at most 22), DuckDB's own
correctly rounded decimal conversion otherwise. Generated conjunctions build the
complete packed mask before classifying it, and packed operations are admitted
by SLJIT's exact operation-and-width capability test.

A scalar filter loop branches on the predicate's own flags instead of
materializing a boolean, keeps rejected rows on the fall-through path to a
bottom-tested latch, and runs matching rows' payloads out of line. Division and
modulo by a constant divisor of at least 2 use a magic-number multiply; a
predicate `n % d = c` (or `<>`) with `0 <= c < d` tests divisibility instead:
`n >= c` and `d | n - c` for `c > 0`, `d | |n|` for `c = 0`, by multiplying by
the inverse of `d`'s odd part, rotating out its power of two, and comparing
against `floor((2^64 - 1) / d)`. Loop-invariant operands live outside the row
loop: hot sources' data pointers in registers, and wide constants (magic
multipliers, inverses, limits) in stack slots stored once per chunk. Packed
loops take the same data-pointer registers and the same bottom-tested latch,
one branch per lane group. Packed SUM and COUNT run two lane groups per
iteration into independent accumulators, since one accumulator chain bounds the
loop at its latency; one guarded group and the scalar tail finish the vector.

A projection may move past join selection only when it is total over its
admitted input domain. The pre-join projection binder admits references and
INT64-to-INT32 casts whose source range proves no conversion error. Unknown
or wider ranges retain the generated projection before the join, including
for non-key payloads and keys that a selection might never read. Source
statistics apply only while preceding operators preserve that source layout.

### Hash joins

Native probes require explicit facts for join type and output mode, key count,
types, NULL semantics and comparison kinds, hash-table layout kind, salt,
chain and retained-key NULL state, RHS source representation, and selected
versus identity input. Observed retained-key NULL state is distinct from
nullable schema shape and from NULLs filtered during build. Perfect-hash
dynamic filters use one exact membership definition; because a source filter
may be adaptive, a generated probe still validates NULLs, range, and sparse
membership before emitting a match.

### Aggregates

Aggregate recipes bind primitive state ABI, payload sources, group sources,
nullability, and initialization semantics once. Ungrouped and grouped routes
preserve DuckDB's behavior for empty input, NULL input, overflow,
initialization, combine, and finalize. Full-width HUGEINT inputs use checked,
ordered state updates: chunk-local reduction or grouped preaggregation cannot
reassociate them without changing intermediate overflow behavior. Narrow
integer inputs and generated values proven to fit a machine word retain their
wider-state reduction paths. Group-key range facts belong to the
executable; proven signed narrowing removes per-chunk range scans, unknown
ranges keep checked materialization. Pipeline-local preaggregation may hold a
boundary group across yields, and publication is failure-atomic: raw
equivalence keys and partial states never escape through DuckDB's grouped-state
API. Parallel finalization skips rehash only when conservative key summaries
prove disjointness. Every producer that stores rows into a shared aggregate
table stores the canonical `DataChunk::Hash` of all group columns. A
grouped-state lookup never declines a descriptor it admitted, because a batched
caller cannot replay consumed input. Key equality is core's: every lookup
strategy, and the key comparison core exports to backends, hashes and matches
keys with DuckDB's hash and equality, under which every NaN is one value and
-0.0 equals 0.0. A backend merges rows only under that equality; keys it cannot
prove equal go to core's lookup, never into a group of their own.

A generated grouped update owns group lookup only where it beats DuckDB's
vectorized sink: a bounded perfect-hash domain, or group runs in the key stream.
A single-key direct update samples its first batch for runs; when the sample
finds none, the step flushes what the sample buffered, ends any proven-unique
append claim, and from then on hands every batch to the native pipeline from
the operator it feeds (whole source chunks start at the first operator), whose
terminal is DuckDB's own aggregate sink.

Compiled aggregate plans, binding, and grouped-state updates carry only
`ExecutionRegionAggregateSinkInfo`, not the general sink union. Its operator
kind comes from the aggregate contract. Lowering checks the general IR's sink
tag against that contract before extracting the payload. Instance binding and
artifact identity traverse only the aggregate's inputs and groups.

Grouped-state publication uses the semantic sink contract: core records each
aggregate's state offset, the executable copies offsets and primitive kinds
into its plan, and the backend's call ABI carries only core-owned state
addresses, selections, and primitive deltas. An aggregate-state source
publishes only semantic primitive lanes; the backend maps them to target
aggregates once, and core combines mapped states behind
`ExecutionAggregateStateScanBatch` after validating compatibility. Direct state
consumption is an economics decision (at least 128 semantic bytes of unique
mapped states), not a workload exception.

## Observability and accounting

EXPLAIN ANALYZE carries the region story: the `JIT_EXECUTION_REGIONS` block,
`CBO_PIPELINE` decision lines, `RUNTIME_KERNELS` lines, `jit` attributions on
covered operators, and `jit_deferred` reasons on the pipeline source, applied
as a post-execution overlay. `jit_trace_decisions=true` prints full per-event
lines; `duckdb_jit_events()` is the machine-readable authority.

Accounting distinguishes the selected runner, compiled region, executed region,
native-only backend result, explicit native-tail delegation, materialization
elision, and runtime blocker or compile failure. Selection is not compilation,
and compilation is not execution. A missing proof is a failure for every query
the cost model selected, including queries that later fail compilation.
Traced proof runs are separate from production timing.

The cost model requires `ACCELERATED_STAGE_WORK` and `BACKEND_OWNED_WORK`
for the work it funds: either generated code or a precompiled backend kernel
can implement acceleration. `GENERATED_STAGE_WORK` is separate, positive
evidence from an actual generated-call boundary, not a claim inferred from
the selected runner, allocated code, a timer, or materialization elision.
Delegated native sinks and failed fast-path attempts do not earn accelerated
work proof. Proof verification requires the declared typed evidence; timing
buckets cannot substitute for it. The historical `generated_stage_*` timing
and cost field names remain artifact-schema names, not machine-code counters.

## Performance contract

Correctness and performance are both required:

- compiled routes must beat native execution by their workload-specific floor;
- native-selected routes must remain within an independent raw slowdown limit;
- a regression cannot pass because `jit_policy=off` slowed by the same amount;
- paired or normalized ratios are secondary noise evidence only;
- one-thread and parallel floors stay separate when scheduling changes the
  demonstrated margin;
- checked-in floors are one-way ratchets: a gap is closed in the code or the
  environment, never by lowering a floor.

A verified performance improvement and its regression baseline are one change.
Tighten the checked-in floor or promote the accepted comparison artifact in the
same increment; otherwise the gate still accepts the slower performance the
implementation replaced. Moving a fast path into a native operator (principle
6) speeds up `jit_policy='off'` as well; such a move is measured against the
same floors.

Benchmark commands, repetition counts, artifact schemas, host admission,
baseline locations, and promotion mechanics are operational policy, owned by
`benchmark/jit/README.md` and the gate scripts it names.

## Verification

Commands for every gate live in `benchmark/jit/README.md`. Every change passes
focused tests first, then the static architecture checks, the JIT correctness
suite, the differential fuzz campaign, the generic one-thread and parallel
workloads, and the affected TPC-H scale factors.

- **Differential first.** The refactor guard runs a fixed-seed campaign of
  random queries over a NULL-injected TPC-H schema at every pre-commit;
  longer campaigns over larger scale factors catch size-dependent classes
  such as key compression. A value domain the schema lacks and a query shape
  the generator never builds are untested classes, so the fuzz database
  derives the types TPC-H never stores (FLOAT and DOUBLE with NaN, infinities
  and both zeros; narrow signed and unsigned integers at both ends of their
  ranges; UBIGINT above BIGINT; signed HUGEINT; DECIMAL(38,6); BOOLEAN;
  TIMESTAMP), and the generator builds predicates over several columns as
  values and comparisons between two columns of one table. Coverage needs
  NULL-free data too: NULL-injected columns keep every all-valid path cold.
- **Properties, not routes.** A test asserts result parity with
  `jit_policy='off'`, order where it matters, and error parity. It may assert
  one coverage counter proving the path ran; it does not encode which route a
  legitimate improvement may change.
- **Mutation check.** A new path is verified by breaking it and watching its
  test fail.

## Remaining work

Each step is a sequence of commits that pass the full guard; no floor moves.
Steps within a phase are ordered; M1 and M2 are independent and come first,
because M3-M5 shrink once layouts and instances exist.

### M1 — data layer

1. **Source batches.** Qualify the core-owned compaction policy on generic
   workloads and TPC-H without moving any performance floor. Preserve storage
   fetch amortization and encoded-vector passthrough while extending coverage.
2. **Typed layouts.** Extend bound layouts to every recipe edge, including the
   preaggregated group batch, and give columns region-IR slot identities.
   Consumers resolve slots through layouts; MARK and elided-projection layouts
   already share one descriptor, and join-to-aggregate inputs bind through that
   producer layout. These still use producer-scoped positions.
   Exit: remaining per-family remap helpers are gone; layout mismatch fails at bind.
3. **One coalescer.** Replace per-step batch owners and per-site batching
   rules with the step coalescer and one byte-based rule. Exit: at most four
   owned batch kinds; no string deep copy decided by a step.

### M2 — artifact identity and instances

1. **Instance slots.** Move remaining stats-derived runtime parameters into the
   instance slot table, separating them from code-generation decisions. Audit
   prepared input plans for remaining execution-local inputs. Exit: artifacts
   hold no query-local data, verified by a static check.
2. **Structural key.** Move remaining runtime-only inputs to instance slots,
   then remove those inputs from the canonical key. Exit: queries
   that differ only in literal positions, estimates, or statistics within one
   compiled shape hit the same artifact; no description rendering on the
   planning path.
3. **Compact plans.** Narrow the probe and append contract payloads
   inside backend descriptors and move plans between stages. Exit: op
   descriptors within budget; planning cost per query measured and reduced.
4. **Typed status.** Contract readiness flags are derived from blocker
   strings; replace them with typed status and blocker enums. Exit: no
   behavior reads a reason or blocker string.

Planning is a small fraction of a TPC-H query and the cache already hits for
an identical query, so steps 2 and 3 pay for clarity and for short queries,
not for TPC-H throughput.

### M3 — binding

Apply describe/choose/materialize to every binding; split binding views by
layout kind; remove per-chunk `ready` and layout re-checks. Exit: no binding
side effect the backend did not request; no binding fact re-validated per
chunk.

### M4 — strategies

Select each step's strategy at bind time and convert the cascades above,
largest first, to bound strategies with declared transitions. Exit: no runtime
fall-through between strategies.

### M5 — catalog

Classify every precompiled family as generated, native candidate, or
shape-specific; move native candidates into DuckDB operators where measurement
supports it, generalize or delete shape-specific families. Exit: every
remaining backend family runs generated code on its hot path or carries a
measured justification.

Reachability comes first: a family that no test, TPC-H query, generic
workload or fuzz campaign (NULL-injected and NULL-free) reaches is deleted,
provided the cascade behind it covers its inputs. A fallback at the end of a
cascade whose callers throw on failure stays even when no workload reaches
it.

### M6 — cost model

Merge the five gates into one decision with typed input and reasons; feed
observed cardinalities to the adaptive transitions instead of re-planning.

## Current boundaries

- Variable-width expression coverage is narrower than fixed-width coverage.
- Unsupported aggregate layouts, DISTINCT shapes, and join modes remain
  native.
- Nested-loop joins are native execution boundaries. Their semantic IR remains
  available to planning, but core exports no unused compiled-runtime binding.
- A region's table-scan source reads storage synchronously: it schedules no
  async read-ahead or prefetch tasks, which native scans do for cold data
  (the storage prefetch tests fail with the JIT forced on for that reason).
- Multi-key, selected, and non-flat grouped inputs require an explicit proven
  route before generated run ownership expands. Their generated lookups have no
  run sample, so batches without runs stay on the generated lookup, which is
  slower than DuckDB's vectorized sink on random-order keys.
- Native source and sink protocols remain native unless an exported execution
  contract makes composition safe.
- Native-only selection is valid capability analysis, not a failed recipe and
  not permission to loosen cost accounting.

These are capability boundaries, not workload exceptions. New support
generalizes a descriptor, layout, or runtime ABI, removes a boundary, adds
differential and proof coverage, and ratchets performance evidence.
