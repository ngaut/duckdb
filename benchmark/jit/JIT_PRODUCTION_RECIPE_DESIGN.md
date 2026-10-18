# JIT Execution Architecture

Last updated: 2026-10-18

This document is the architecture contract for DuckDB execution-region JIT and
the plan that moves the code to it. Each part states the **contract** (rules
every change must keep), the **target** (the state the migration reaches), and
where the code still differs (**today**). The migration plan at the end orders
the work; every step lands behind the gates in `benchmark/jit/README.md`.
Benchmark results belong in machine-readable artifacts, not in this design.

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

## Why the design changes

An October 2026 review measured the code against this contract's own
principles. The principles held; the structure did not:

- **Where time goes.** In TPC-H SF10 one-thread execution, generated machine
  code is about 13% of time, precompiled C++ fast paths about 23%, and
  DuckDB's storage scan inside kernels about 35%. Most wins come from fused
  C++ and avoided operator overhead, not from generated code.
- **Data layer.** Rows move between steps through 16 batch, pending and view
  types and 19 owned batch instances, translated across 14 index spaces by
  about 60 remap helpers. Most wrong-result classes the October fuzz
  campaigns found were index-space confusions, and the preclassified
  aggregate batch bug (a class column read as the payload it replaced) was a
  layout that silently differed from what a generic dispatcher assumed.
- **Strategy catalog.** 235 `SljitTry*` functions, 194 of them reached only
  at runtime; the grouped input-vector update tries about 13 strategies per
  call and re-checks facts already known at bind time.
- **Representations.** One pipeline is described by about 10 structures.
  Op plans embed every kind's sub-plan (11.7 KB for a native op plan, 13.8 KB
  for an executable op), operator and sink infos are deep-copied 4-6 times on
  a cache miss, and the artifact key is a rendered string of 5-29 KB that
  includes estimates and blocker text.
- **Binding.** Core binds had side effects the backend could not decline (the
  eager probe bloom filter), and backends re-check `ready` flags per chunk.

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
and typed rejection reasons. **Today:** five gates
(`ExecutionRegionProductionEligibilityAllowsPlanning`, the pre-graph runner
selection, `ExecutionRegionGraphMayHaveCostedAcceleration`, the cost-only
runner, and the post-analysis runner), all reaching
`DuckDBCostModel::SelectPhysicalRunner` with different inputs. Observed
runtime cardinality never feeds the decision; adaptive choices live in kernels
(see Binding contract).

## Execution model: chunk steps

This part is the target and is in place.

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
  may dispatch every representation; a row-layout, compressed-row,
  row-pointer, or complementary consumer rejects dictionary storage. Silent
  interpretation of one representation as another is forbidden.
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
declared one fails at bind time, never as silent garbage. The 14 index spaces
reduce to two — region-IR slots and the column positions of one layout — and
the remap helpers to the layout's slot lookup.

**Sources fill batches.** The source contract fills a batch across storage
vectors: each vector's surviving rows are compacted into the batch at its
current offset (fixed-width values copied, strings referenced with their
buffers kept alive), until the batch is full or the scan assignment ends. A
kernel therefore never re-batches source rows, and the per-vector source work
(dictionary slices, chunk resets, fetch dispatch) disappears.

**One coalescer.** Rows are coalesced between steps in exactly one place, with
one rule based on the bytes a copy would move. The 19 owned batch instances
reduce to the source batch, the step coalescer, and the typed pending states
of aggregate terminals.

### Today

- The table scan's source contract copies the kept rows of successive storage
  vectors into one chunk until it holds half a vector, within one scan
  assignment and under the batch index its rows were scanned with. A vector
  that keeps most rows, has out-of-line strings, or carries a storage
  dictionary or FSST vector passes through uncopied. Since `e687a5ec637` the
  scan also copies a region source's fixed-width filtered columns flat instead
  of slicing them. Strings are copied, not referenced.
- Other sources still hand over their chunks as produced;
  `SljitSourceFetchPrimitiveRuntime` copies chunks under 1024 rows into
  `source_contract_batch`, except chunks whose strings are out of line
  (`SljitChunkHasOutOfLineValues`).
- Batching rules are spread over `ShouldBatchSourceContractChunk`,
  `SljitChunkHasOutOfLineValues`, `SljitAppendChunkToInitializedBatch`,
  `SljitProjectionHasVariableWidthOutput`, `CanBatchTerminalAggregateUpdate`,
  the pending row-pointer and input-vector batches, the pending preaggregated
  group batch, the count-star pending state, the matched-row microbatch, and
  core's sink coalescing. The hash-join materialize batch and the post-join
  projected batch deep-copy strings with no width check.
- `SljitPendingRowPointerAggregateBatch` replaces payload sources with one
  class column in preclassified mode; only the `preclassified_payload` flag
  keeps generic dispatchers from reading it as the payload (`ea83155c33c`).

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
or sink infos. A pipeline plans once; a readiness refresh updates instance
facts without re-planning.

**Structural identity.** The artifact key is a canonical binary encoding of the
artifact plan, written by each descriptor from the same fields its code
generation reads, compared byte for byte together with the backend ABI.
Estimates enter only as the compile decisions derived from them; diagnostic
text, reasons, and blockers never enter. Descriptions are rendered on demand
for diagnostics only.

**Instance slots.** The instance holds a slot table indexed by artifact op:
runtime-filter identities, dynamic-filter handles, prepared input remaps,
cast-error identity (message, query location, source alias), and the
stats-derived parameters (min/max, group domains, reserve counts) that code
reads as runtime parameters. Only facts generated code bakes in as constants
belong to the key. Two queries that differ only in literals' positions,
estimates, or column statistics within the same compiled shape share one
artifact.

**No strings as control flow.** Stage status, blockers, and readiness are typed
enums; reason text is derived from them.

### Today

- About 10 structures describe one pipeline: the execution contract, region
  graph, region IR, candidate with stage plan, lowering plan, native region
  plan, executable region, artifact, cached artifact, kernel, local state, and
  chunk step.
- `ComposeSljitRegionArtifactCacheKey` renders the key from
  `DescribeNativeRegionInternal` and contract descriptions; it includes
  `op_estimated_input_count`, the cardinality-gated `flat_nullable_fast_path`,
  blocker and reason strings, and the cast error message and query location.
  Stats-derived values are baked into the artifact and covered by a separate
  binding key.
- Runtime-filter identities already stay out of the artifact: the probe plan
  records a binding slot, `SljitNativeHashJoinProbePlan::Copy` drops the
  identity, and the kernel hands its identity bindings to each thread's
  scratch. Copies of the first query's diagnostic `fields`, `reason`, and `ir`
  still ride inside shared operator and sink infos.
- The planner runs up to three times per pipeline: the initial build, a
  rebuild when the source contract is unsupported, and a readiness refresh
  once dependencies finalize. Recursive rescheduling does not re-plan.
- Strings decide behavior in a few places: `node.blocker_reason.empty()`
  selects generated versus missing-contract stages, `"seq_scan"` and
  `"hash_aggregate_scan"` gate statistics and recipe binding, and `*_ready`
  flags are set from empty blocker strings.

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

The entry view is an array of `uint64_t` words with published pointer and salt
masks; the bloom view is a word array plus bitmask using the shared bloom-mask
primitive. Backend code never names `ht_entry_t`, `BloomFilter`, or
`JoinHashTable`, and never dereferences the opaque hash-table handle a probe
binding may carry. `ExecutionHashJoinRHSFixedColumnSource` is the boundary for
fixed build-side columns, either `ROW` (row-layout offset plus validity) or
`DICTIONARY` (index offset, data, validity, count); core resolves and caches
these per operator and initializes every field before publishing `ready`.
Storage-dependent RHS gathering is an exported core operation.

Execution-region backends use DuckDB's version- and platform-matched C++
extension contract plus an execution-region ABI version, currently 12. Any
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

### Today

- The probe bloom filter is the first three-phase binding (`ae686ed84a9`): the
  bind sets `bloom_filter_requested`, the SLJIT bind requests
  `ExecutionUseHashJoinProbeBloomFilter` unless the probe key's exact-filter
  proof names this hash table, and the decision is a runtime path.
- `ExecutionHashJoinProbeBinding` has 25 fields and embeds both the regular
  (22 fields) and perfect (21 fields) layouts. `ExecutionBindHashJoinBuild`
  mutates the local sink's payload chunk and append state; aggregate binds
  allocate state adaptors.
- Runtimes re-check `ready`, `output_mode`, key counts, and payload-source
  types on every call (row-pointer update, input-vector update, perfect-hash
  join output, complementary sums, the probe aggregate consumer, the probe
  executor); the perfect probe layout is validated on every chunk.

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

### Today

The runtime cascades, by number of alternatives tried in order:

| Cascade | Location | Alternatives | On total failure |
|---|---|---|---|
| Grouped input-vector update | `sljit_grouped_aggregate_input_vector_update_runtime.hpp` | ~13 | callers throw |
| Row-pointer grouped update | `sljit_row_pointer_grouped_aggregate_update_runtime.hpp` | 6 + nested | caller throws |
| Direct join-output aggregate | `sljit_direct_join_output_aggregate_runtime.hpp` | 8 | native materialize |
| Probe aggregate consumer | `sljit_hash_join_probe_aggregate_consumer_runtime.hpp` | ~8 | generic probe drain |
| Probe path | `sljit_hash_join_probe_executor_runtime.hpp` | 6 C++ + generated | generated kernel |
| Grouped bound update | `sljit_grouped_aggregate_direct_update_runtime.hpp` | adaptive 3 + switch | generic path |
| Projection chain | `sljit_projection_chain_runtime.hpp` | ~6 | expression runtime |
| Join projection aggregate inputs | `sljit_hash_join_projection_aggregate_input_runtime.hpp` | ~7 per column | blocked |
| Full-pipeline step | `sljit_native_pipeline_runtime.hpp` | 5 | throw |

Most runtime descriptor work runs on the first chunk and is cached; a failure
disables that strategy for the step. The join projection aggregate inputs
rebuild expressions per chunk. Families by size: preaggregation (21 files,
~7.6K lines, mostly C++ with three generated-kernel call sites), all-valid
probe templates (~2.3K lines, pure C++), complementary sums (~1.7K, pure
C++), probe aggregate consumers (~1.3K), count-star preaggregation (~1.0K,
pure C++), string-set classification (~0.6K, pure C++).

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
There is no selectivity percentage or workload-specific source selector;
storage-sensitive low-cardinality string predicates and direct hash-build
filters keep their storage recipe as a physical capability decision.
Statistics may order predicates inside a generated filter; they never choose
generated versus storage ownership.

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
may both be NaN compares their ranks. Generated conjunctions build the
complete packed mask before classifying it, and packed operations are admitted
by SLJIT's exact operation-and-width capability test.

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
initialization, combine, and finalize. Group-key range facts belong to the
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

**Target:** runtime proof separates generated work from precompiled C++ work.
**Today:** `GENERATED_STAGE_WORK` is also recorded for C++ fast paths.

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
  random queries over a NULL-injected TPC-H schema at every pre-commit
  (`2b95c8c252c`); longer campaigns over larger scale factors catch
  size-dependent classes such as key compression. A query feature added to
  the generator is a test of every existing path.
- **Properties, not routes.** A test asserts result parity with
  `jit_policy='off'`, order where it matters, and error parity. It may assert
  one coverage counter proving the path ran; it does not encode which route a
  legitimate improvement may change.
- **Mutation check.** A new path is verified by breaking it and watching its
  test fail.

## Migration plan

Each step is a sequence of commits that pass the full guard; no floor moves.
Steps within a phase are ordered; phases M1 and M2 are independent and come
first, because M3-M5 shrink once layouts and instances exist.

### M0 — foundations (done)

- Chunk-step execution with core-owned loop, sink, and partition protocol.
- Differential fuzz campaign in every pre-commit guard (`2b95c8c252c`).
- Region sources compact filtered fixed-width columns (`e687a5ec637`).
- First describe/choose/materialize binding: the probe bloom filter
  (`ae686ed84a9`, ABI 12 in `341237e1a22`).

### M1 — data layer

1. **Source batches.** The source contract fills batches across storage
   vectors with compacted survivors; delete the source-fetch batching rules.
   Exit: no per-vector work in the kernel; source batching code deleted;
   `join_exact_filter_build` and TPC-H no slower. Table scans are done; the
   kernel's source batching stays for other sources until the coalescer
   (step 3) replaces it, and strings are copied until batches can reference
   their buffers.
2. **Typed layouts.** Introduce `BatchLayout` per recipe edge; convert the
   preclassified row-pointer batch, the preaggregated group batch, the MARK
   boundary, and elided projections to explicit layouts; consumers resolve
   slots through layouts. Exit: the `preclassified_payload` flag and most
   remap helpers are gone; layout mismatch fails at bind.
3. **One coalescer.** Replace per-step batch owners and per-site batching
   rules with the step coalescer and one byte-based rule. Exit: at most four
   owned batch kinds; no string deep copy decided by a step.

### M2 — artifact identity and instances

1. **Instance slots.** Move cast-error identity, stats-derived parameters, and
   prepared remaps into the instance slot table; strip diagnostic fields from
   shared infos. Exit: artifacts hold no query-local data, verified by a
   static check.
2. **Structural key.** Encode the artifact plan canonically; drop estimates and
   text from the key; render descriptions only for diagnostics. Exit: queries
   that differ only in literal positions, estimates, or statistics within one
   compiled shape hit the same artifact; no description rendering on the
   planning path.
3. **Kind-tagged plans.** Replace composed op plans with kind-tagged
   descriptors and move plans between stages. Exit: op descriptors within
   budget; planning cost per query measured and reduced.
4. **Typed status.** Replace string control flow with typed status and blocker
   enums.

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

### M6 — cost model

Merge the five gates into one decision with typed input and reasons; feed
observed cardinalities to the adaptive transitions instead of re-planning.

## Current boundaries

- Variable-width expression coverage is narrower than fixed-width coverage.
- Unsupported aggregate layouts, DISTINCT shapes, and join modes remain
  native.
- No backend consumes the nested-loop join binding core exports, so a
  pipeline containing a nested-loop join stays native past it.
- A region's table-scan source reads storage synchronously: it schedules no
  async read-ahead or prefetch tasks, which native scans do for cold data
  (the storage prefetch tests fail with the JIT forced on for that reason).
- Multi-key, selected, and non-flat grouped inputs require an explicit proven
  route before generated run ownership expands.
- Native source and sink protocols remain native unless an exported execution
  contract makes composition safe.
- Native-only selection is valid capability analysis, not a failed recipe and
  not permission to loosen cost accounting.

These are capability boundaries, not workload exceptions. New support
generalizes a descriptor, layout, or runtime ABI, removes a boundary, adds
differential and proof coverage, and ratchets performance evidence.
