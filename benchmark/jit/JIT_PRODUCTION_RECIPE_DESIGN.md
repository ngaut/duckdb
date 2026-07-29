# JIT Production Recipe Architecture

Last updated: 2026-07-28

This document is the stable architecture contract for DuckDB execution-region
JIT. It describes layer ownership, immutable recipe binding, runtime data
binding, fallback, cost selection, and proof. Benchmark results belong in
machine-readable artifacts, not in this design.

## Goal

JIT must accelerate generic physical-plan shapes while preserving DuckDB's
semantics and vectorized fallback. TPC-H is an important regression workload,
not an architecture input. Production code must never branch on query ids, SQL
text, benchmark names, table names, column names, or scale factors.

The execution path is:

```text
physical pipeline
  -> backend-neutral execution-region graph
  -> backend capability analysis and semantic descriptors
  -> core-owned cost selection
  -> immutable backend recipe
  -> runtime physical binding
  -> compiled primitive sequence or explicit native execution
```

## Layer ownership

### DuckDB core

Core owns:

- physical operators and their source, operator, and sink protocols;
- execution-region graph construction and typed semantic IR;
- backend-neutral runtime ABIs for vectors, joins, aggregates, and pipelines;
- cost inputs, runner selection, scheduling, cancellation, and errors;
- correctness, telemetry, runtime proof, and native execution.

Core exports facts and operations. It does not expose concrete private storage
types merely to support one backend.

### Backend

SLJIT owns:

- capability analysis over backend-neutral IR and exported runtime ABIs;
- semantic descriptor binding and recipe construction;
- generated code, executable ownership, and backend runtime adapters;
- backend-specific stage receipts and exact delegation reporting.

Executable-memory policy is selected at build time by platform: Apple uses
SLJIT's `MAP_JIT` allocator, Linux uses distinct pooled RW and RX mappings, and
Windows and other supported POSIX systems use per-allocation W^X transitions.
Generated code is never published through a generic RWX allocator.

Backend availability is also an ABI admission boundary. The native-vector
calling convention requires six saved registers that SLJIT exposes as real
addressable machine registers, and each kernel layout must fit beside its
declared scratch-register set. Register count alone is not sufficient:
x86-32 virtualizes the required `S3`-`S5` registers, so the SLJIT backend is
registered as unavailable and DuckDB remains on vectorized execution.

`SljitTargetCapabilities` is the single owner of target architecture, machine
word width, SIMD availability, register-file size, and addressable saved
registers. Only that layer may read SLJIT target macros or feature probes. It
builds one immutable process-lifetime snapshot; planning and code generation
query the snapshot, and generated row loops contain no capability branches.

Kernel code does not derive fast paths from register counts. Semantic layouts
assign registers to native-vector metadata, perfect-hash state and group data,
ungrouped accumulators, primitive-run state, and perfect-hash probe invariants.
An optional role is either assigned an addressable machine register or absent;
the planner then selects the corresponding register or spill layout. This keeps
portable fallback and target-specific acceleration under one ABI instead of
duplicating architecture checks in individual emitters.

The backend must not infer semantic support from mutable runtime state. It may
specialize physical access only after a semantic recipe has been admitted.

### Cost model

DuckDB core is the only owner of runner policy. Backends publish neutral work
facts such as startup cost, row work, generated stages, and materialization
cost. They do not implement a second query selector.

`jit_policy=off` always selects native execution. `jit_policy=auto` may select a
compiled runner only when capability and cost both approve it. A selected
runner that cannot compile or execute its admitted recipe is a failed proof,
not evidence that the route was never selected.

## Immutable recipe boundary

`SljitFullPipelineRecipePlan` is a tagged plan with two publishable states:

- a non-empty, validated `SljitFullPipelineRecipe`; or
- an explicit native-only runtime path.

The default `INVALID` tag is construction-only and is rejected by runtime.
There is no independent `has_recipe` boolean or empty-string sentinel that can
disagree with the payload.

An empty primitive sequence is not a fallback encoding. Recipe construction is
the single admission boundary:

1. analyze complete operator facts;
2. bind semantic descriptors;
3. bind each primitive descriptor once;
4. construct the complete sequence in local state;
5. make one finalization pass over sequence ownership and runtime metadata;
6. publish the fully initialized recipe only after all checks pass.

Recipe-family binders expose failure-atomic `TryMake...` operations. Shape
analysis never calls a separate `CanMake...` path and then reconstructs the
same descriptor during `Make...`. In particular, projected grouped aggregate
strategy selection publishes the descriptor it selected, and the full-pipeline
binder owns one projection-aggregate family binder instead of constructing a
temporary binder for each question.

Primitive binding is the semantic authority. Recipe finalization validates
sequence grammar and cross-primitive ownership; it does not call capability
predicates again. Each step stores operator identities only in its typed
primitive descriptor—there is no parallel generic index array to reconcile.
Runtime preparation initializes physical state but cannot return a recipe-shape
miss. Partition preservation, scan-filter body ownership, fused-filter
ownership, and the direct-terminal contract are finalized together before the
recipe plan is published. Every vector-source recipe uses the same primitive
sequence executor; selected hash-join sinks are not a second runtime mode.

Prepared expression capability does not change when selector machine code is
emitted. The executable builder therefore binds the plan exactly once, emits
selectors only for filters not owned by a fused terminal, and moves that same
plan into the kernel. Runtime consumes published dispatch, source-chunk, scan
filter, and direct-terminal ownership; it does not scan neighboring primitives
to rediscover a route. A recipe that was accepted but is internally
inconsistent raises an internal error. Unsupported semantics are rejected
before publication and remain native.

## Compiled artifact lifetime

The database-local execution-region manager owns reusable backend artifacts.
The backend plan supplies a complete artifact key covering semantic structure
and every compile-affecting binding fact; an empty key explicitly opts out.
Equivalent prepared and ad-hoc plans can therefore share code without tying
artifact lifetime to mutable physical operators, while refreshed source facts
select a distinct artifact. Core performs single-flight publication, so
concurrent executions of the same artifact identity produce one artifact
rather than racing duplicate compilers.

The bounded cache stores only a backend-owned immutable semantic artifact,
plus the execution mode and diagnostics produced by that compilation. Backend
compilation returns the artifact, not an execution-bound kernel. Core sends
both a fresh artifact and a cache hit through the same backend instantiation
path outside the cache lock. Diagnostic switches such as detailed tracing, IR
dumping, and verification do not fragment the artifact key; backend settings
or binding facts that change generated behavior do.

Publication is transactional. Core validates the compile result, instantiates
the execution-bound kernel, validates its executable body and ABI, and only
then publishes the artifact. A failed build or first instantiation aborts the
in-flight reservation. Failure to instantiate a cache hit is a backend
contract error: the complete cache key must guarantee that every equivalent
plan can bind the shared artifact, so core does not hide an incomplete key with
an eviction-and-retry path. Sleeping single-flight callers retain the exact
publication even if unrelated insertions immediately evict it from the ready
LRU. Active kernels retain shared ownership after eviction.

Compiled code, immutable descriptors, and concurrency-safe lazy
specialization cells may be shared. Adaptive-runner state, telemetry, runtime
counters, operator bindings, dynamic-filter identities, hash-table
dictionaries, and all other query data live in each instantiated kernel or its
local states. A backend that cannot make this split remains correct and
uncached. Changing this contract advances the loadable execution-region ABI.

Backend-private compile ownership follows the same boundary. The concrete
SLJIT artifact and kernel declarations are private to the extension. Artifact
construction, cache instantiation, metadata, and table-filter binding compile
without the full-pipeline template graph. A dedicated execution owner alone
instantiates full-pipeline dispatch against the concrete kernel, so its typed
row loops remain statically bound without leaking backend implementation types
through the loadable backend ABI.

Heavy typed runtime families must be partitioned only at batch-level dispatch
boundaries. Moving a `PhysicalType` switch or recipe-kind switch out of a
translation unit is valid; adding a virtual call, callback, or function-pointer
dispatch inside a row, match, or aggregate-update loop is not. This rule keeps
compile ownership and object size local while preserving the direct calls that
the optimizer needs in hot loops.

Grouped run and pending preaggregation follow that rule through
`sljit_grouped_aggregate_preaggregation_api.hpp`. General pipeline orchestration
submits one vector batch through this private API. Its dedicated runtime owner
performs physical key and payload type selection and contains every resulting
typed row-loop specialization.

Direct hash-probe complementary aggregation follows the same rule through
`sljit_hash_join_aggregate_consumer_api.hpp` and
`sljit_join_input_complementary_sum_api.hpp`. Pipeline orchestration passes the
concrete native probe executor and one vector batch to the dedicated aggregate
consumer owner. Descriptor preparation, typed group accumulation,
preaggregation, materialization, and accumulator flush stay behind those
batch-level APIs. Probe code generation remains statically bound, and neither
API introduces indirect dispatch into match or aggregate-update loops.

The primitive sequence starts with `SourceFetch` and ends with exactly one of:

- `PostJoinProjectionAggregateUpdate`;
- `UngroupedAggregateUpdate`;
- `GroupedAggregateUpdate`;
- `HashJoinBuildSink`;
- `AppendSink`;
- `DelimJoinSink`;
- `NativeTailDelegation`.

`NativeTailDelegation` is an explicit terminal boundary. It accepts only its
declared materialized input view and reports delegated work. It is not an
implicit escape from a failed compiled primitive.

## Direct hash-probe terminal contract

A direct hash-probe-to-aggregate route has two immutable contracts.

`SljitHashJoinDirectAggregateConsumerContract` belongs to the primitive recipe
and records:

- probe step identity;
- terminal step identity;
- optional generated-filter identity;
- hash-join operator identity;
- aggregate operator identity.

Each recipe family creates this contract while it creates the sequence. There
is no post-construction scan for a matching shape.

`SljitHashJoinDirectUngroupedAggregateDescriptor` belongs to the canonical
join-projection-aggregate descriptor and records only semantic payload facts:

- primitive kind;
- build-side output identity;
- logical type;
- physical type.

It never stores a row pointer, dictionary pointer, validity pointer, or concrete
hash-table object. Execution resolves the current physical source from
`ExecutionHashJoinRHSFixedColumnSource` and validates that it still matches the
bound semantic descriptor.

All-valid regular-probe template dispatch has one translation-unit owner.
Pipeline orchestration sees only the generic key-reader primitive and
vector-level entry points for the two concrete direct-consumer ABIs: bounded
matched-row batches and direct ungrouped reduction. The probe loop remains
statically specialized inside that owner; there is no virtual or indirect call
per match. This keeps terminal semantics out of matcher policy and prevents
every pipeline consumer from instantiating the full key-type dispatch graph.

This separation is deliberate:

```text
recipe binding             runtime execution
semantic operator ids  ->  current operator bindings
semantic RHS output id ->  row or dictionary source
logical/physical type  ->  validated physical loader
aggregate primitive    ->  current primitive state lane
```

The direct ungrouped consumer reduces successful regular-hash matches without
materializing a matched row-pointer batch. It currently supports `COUNT(*)`,
`COUNT(rhs)`, and BIGINT `SUM(rhs)` when the regular probe's all-valid,
single-key, no-chain contract is proven. Unsupported shapes use the existing
materialized route.

Physical dispatch is retained in pipeline-local state. The first non-empty
attempt binds a stable unsupported route directly to `MATERIALIZED`, avoiding
the same probe on every chunk. A successful direct route starts as `DIRECT`;
if a later vector shape legitimately needs materialization, it becomes
`HYBRID` and keeps direct execution for eligible chunks. Empty input does not
bind a route. These transitions are monotonic and recorded once.

## Runtime physical ABI

Regular hash-join compilation and execution deliberately use two different
contracts:

- semantic IR contains only reusable facts: key types, row-layout offsets,
  tuple/pointer offsets, join semantics, and output mapping;
- query-local binding contains non-owning physical views: encoded entry words,
  pointer/salt masks, optional chain pointers, and Bloom words.

Live pointers never enter an artifact key or compiled artifact. Core constructs
the physical `TupleDataLayout` and the semantic row-layout contract through the
same canonical builder, so zero-copy offsets cannot drift between the native
table and JIT planning. The runtime table accessor reports typed physical
states (`READY`, `EMPTY`, `NOT_FINALIZED`, or `POINTER_TABLE_MISSING`);
diagnostic strings are derived from those states and are not control flow.

The entry view is an array of `uint64_t` words whose pointer and salt regions
are defined by published masks. The Bloom view is a word array plus bitmask,
and both core and backends use the shared common Bloom-mask primitive. Backend
code must not include or name `ht_entry_t`, `BloomFilter`, or
`JoinHashTable`. This preserves direct, zero-copy probing without turning
core-private C++ classes into an extension ABI.

The hash-table physical view entered the execution-region backend ABI in
version 4. ABI version 5 adds opaque aggregate-state batch consumption; a
loadable backend built against either older binding layout is rejected at
registration.

`ExecutionHashJoinRHSFixedColumnSource` is the core/backend boundary for fixed
build-side columns. It represents either:

- `ROW`: row-layout offset plus row validity metadata; or
- `DICTIONARY`: dictionary-index offset, data, validity, and count.

Core resolves and caches these operator-lifetime sources when it publishes the
probe binding. It initializes all storage-specific fields before publishing
`ready=true`.
Backend helpers depend only on this exported ABI and common validity-mask
types. They do not include `join_hashtable.hpp` or name private
`JoinHashTable` layout types.

The probe binding may carry an opaque core-owned hash-table handle, but a
backend never dereferences it. Core copies immutable probe facts such as
build-side filtered-NULL state and per-condition NULL-equality semantics into
the binding. Storage-dependent RHS gathering is an exported core operation.
This keeps backend code independent of private hash-table layout and lets core
change that layout without creating a second hidden ABI.

Consumers must state their physical requirement. A generic fixed-column loader
may dispatch both storage kinds. Row-layout projection, compressed-row string,
row-pointer grouping, and complementary row-field consumers explicitly reject
dictionary storage. Silent interpretation of dictionary indices as row values
is forbidden.

Fast-path caches must key their validity on stable identities — dictionary
ids, value keys, epochs — never on reusable addresses, and every event that
relocates rows must disable caches holding row addresses. Both rules are
enforced by static architecture checks.

## Batch and continuation ownership

Recipe execution carries an explicit batch view:

- data chunk;
- logical cardinality;
- optional selection;
- selection ownership and lifetime.

Identity order is represented without a disposable selection. All-match
filters return the unchanged input view. A primitive may borrow scratch only
until the next scratch mutation; a continuation that outlives the call must own
or copy its state.

Source progress, sink backpressure, cancellation, and recursive-pipeline state
remain core-owned. The backend can coalesce source fetches only through the
declared source budget and must preserve exact operator protocol.

Sparse source-output batching is independent of terminal kind. Small chunks
are copied into one backend-owned source batch before downstream expression or
probe work, including recipes whose terminal consumes a selected hash-join
view. The selected terminal's output batch is a separate owner and is not a
reason to re-enter the probe once per sparse source chunk. Batching stops at
the source-result boundary and is disabled whenever core requires partition
boundaries and the recipe marks its source chunks partition-sensitive.

Recursive JIT suppression covers one complete compiled-kernel execution,
including native source, operator, sink, flush, and finalize callbacks. Source
contracts do not enter and leave suppression for each chunk. This gives every
callback the same recursion rule and removes thread-local transitions from hot
batch loops.

## Runner switching

Compiled and vectorized execution consume one shared source distribution: for
storage scans both paths claim row groups from the same parallel state, and
first claims are made lazily by the scan loops, never at local-state
construction. Switching runners is legal only at claim points, where no local
holds an in-flight row group. A compiled kernel switches by decline-claim: the
source drains its current row group, declines the next claim, and the runtime
converts the declined fetch into a deferral. Deferral is one-way — deferring
latches the pipeline's compiled suppression so a deferred kernel is never
re-entered — and sources without the storage contract never decline, so
switching is inert where it is not proven. Recursive-CTE iterations run
native by design — iteration-1 pipelines compile normally, and frontier
iterations sit below the amortization floor — so reused executors keeping
compiled entry closed there is a decision, not debt.

Deferral is legal at exactly two points, enforced by the runtime: at kernel
entry before the first source fetch (nothing is claimed), and at a declined
claim boundary (the declined fetch proves nothing is in flight). Any other
deferral request throws, because the contract cursor can hold a partially-read
row group that a handoff would silently abandon. Genuine native-fallback
states — a native-delegated join probe whose runtime layout is not ready, such
as an externally partitioned hash table — are chunk-independent operator
readiness facts, so the kernel probes readiness in an entry prologue and
defers there.

Handoff is additionally a kernel capability: a recipe step that claims
exclusive ownership of sink finalization (inline distinct-key counting)
refuses mid-query handoff entirely, because the rows the other runner sinks
would be stranded under a claim it cannot see. Entry deferral remains legal
for such recipes — no claim exists before the first row. The classification is
a single authority next to the strategy definition; the kernel's capability is
the conjunction over its recipe steps. The per-sink handoff proof and its two
debug settings (forced boundary defer, forced entry defer) are regression
fixtures.

## Measured runner selection

`jit_adaptive_ab` (default on, band-gated via
`jit_adaptive_ab_band_basis_points`=1000 so only thin-margin selections are
measured) measures one native and one compiled row group per compiled-selected
pipeline and commits to the measured winner; the planner
admits a pipeline to measurement only when its static net benefit lies within
`jit_adaptive_ab_band_basis_points` of its required benefit, so confident
selections pay no measurement tax. The native leg runs first under a one-claim
budget owned by its executor and resumed across scheduler yields; the compiled
kernel then resolves the verdict at its own first row-group boundary — commit
continues in-entry, fallback defers one-way. Other threads execute natively
during a measurement, and an executor that has fetched from the vectorized
source without a claim budget is latched vectorized for the pipeline's
remainder: its cursor can hold a partially-scanned row group at any yield, so
switching it into compiled execution — including after a commit verdict —
would abandon those rows. Only executors with clean cursors take the compiled
leg or join a committed kernel. Verdicts are recorded as runtime events carrying
per-leg times and rows. A fallback verdict records runner choice but does not
replace the kernel's declared typed runtime proofs. A verdict is final for its
kernel's lifetime.

## Observability

EXPLAIN ANALYZE carries the execution-region story at three levels. The
`JIT_EXECUTION_REGIONS` block summarizes policy, backend, and the region time
split; `CBO_PIPELINE` prints one line per selection decision and
`RUNTIME_KERNELS` one aggregate line per executed kernel. The operator tree
itself is annotated: every operator covered by a compiled full-pipeline kernel
carries a `jit` attribution (which also explains their near-zero native
timings — the kernel bypasses per-operator execution), the pipeline source
carries `jit_adaptive` verdicts and `jit_deferred` reasons when those occur.
Annotations are applied as a post-execution overlay so per-thread extra-info
refreshes cannot clobber them. `jit_trace_decisions=true` switches both
sections to the full per-event lines; `duckdb_jit_events()` remains the
machine-readable authority.

The structural execution-region plan belongs to the physical pipeline and is
built at most once. Recursive rescheduling resets source state and refreshes
dynamic readiness, but it does not reconstruct the graph, rerun capability
analysis, or emit duplicate decision telemetry for every recursive iteration.

Backend registration freezes name, description, runner kind, and region
support before publication. The registry mutex protects only that owned
metadata and backend lifetime. Dynamic availability callbacks run after the
mutex is released; because backends are never unregistered, snapshots retain a
stable backend lifetime without executing extension code under a core lock.

Generated code and its owning allocation are published as one lifetime unit.
Readers acquire the published owner before reading a callable. Lazy publication
must use DuckDB's atomic shared ownership or one mutex covering every read and
write; an unlocked function-pointer read paired with locked owner mutation is
invalid.

## Expressions, filters, and projections

Expression support is type-driven. One descriptor defines input type, width,
signedness, nullability, result ABI, overflow semantics, code generation, and
runtime adaptation. Matcher, codegen, runtime, and CBO consume the same facts.

Generated scan-filter ownership is semantic. Moving a static predicate into a
region is valid only when:

- the retained source layout supplies all referenced columns;
- evaluation is exception-free for all admitted values;
- generated and native NULL behavior is identical;
- exactly one owner evaluates the predicate.

If a generated selector is not part of the final recipe, the normal DuckDB
filter remains. A possible fused kernel is not an ownership proof.

Generated conjunctions build the complete packed mask before classifying it.
Intermediate horizontal classification serializes broad scans and is not part
of the packed path. Scalar-terminal hybrids use SIMD only when their predicate
work can amortize mask handling. Packed operation admission uses SLJIT's exact
operation-and-element-width capability test; a generic SIMD feature bit or CPU
family is not sufficient proof.

## Hash joins

Native hash probes require explicit facts for:

- join type and output mode;
- key count, type, NULL semantics, and comparison kind;
- hash-table layout kind;
- salt, collision-chain, and retained-key NULL state;
- physical RHS source representation;
- selected versus identity input.

Observed retained-key NULL state is distinct from nullable schema shape and
from NULLs filtered during build. Specialization consumes the observed storage
fact.

Perfect-hash dynamic filters use one exact membership definition. A source
filter may be adaptive, so a generated probe cannot assume storage evaluated it
for every batch. The generated probe still validates source NULLs, range, and
sparse build membership before emitting a match.

## Aggregates

Aggregate recipes bind primitive state ABI, payload sources, group sources,
nullability, and initialization semantics once. Operator-lifetime descriptors
and lane bindings are cached outside per-chunk loops.

Per-chunk payload binding follows the physical source graph rather than the
aggregate list: one `UnifiedVectorFormat` and validity classification is built
for each referenced input vector, then every `SUM`, `COUNT`, or other primitive
lane that consumes that vector binds to the shared descriptor.

Materialized ungrouped multi-lane reducers use one generated row loop for both
machine-word and exact double-word reference payloads. A signed-128 lane loads
its lower and upper words under the same selection and validity contract,
accumulates into a lane-local exact state, and performs one checked aggregate
state commit per batch. Single-lane exact reducers retain their dedicated
direct ABI, while aggregate-state sources use the separate semantic combine
contract below.

Ungrouped and grouped routes must preserve DuckDB aggregate behavior for empty
input, NULL input, overflow, state initialization, combine, and finalize.
Generated grouped run reducers are admitted from semantic ordering and range
facts, not workload identity.

Multi-lane run codegen specializes from runtime vector identity. When at least
two non-`COUNT(*)` lanes share the same validity-mask pointer, one lazy kernel
artifact tests that mask once per row; `COUNT(*)` lanes remain unconditional,
and each payload still uses its own data pointer. Different masks retain the
independent-lane artifact. On register files with spare saved registers, the
kernel also hoists the lane-array base, compact-output index, row-count output,
and common validity pointer. Smaller targets retain the same semantics through
the base register layout. These are backend runtime facts, not query-name or
benchmark-shape checks.

Group-key min/max facts belong to the executable. Proven signed narrowing can
remove per-chunk range scans. Unknown or incompatible ranges retain checked
materialization. Selected, nullable, multi-key, heterogeneous, and unsupported
state layouts remain explicit boundaries rather than hidden special cases.

Pipeline-local preaggregation may retain a boundary group across scheduler
yields. Publication is failure-atomic: raw equivalence keys and partially
updated aggregate states must not escape through DuckDB's grouped-state API.
Generated code owns transition proof while materializing grouped runs. Core
validates the published flat/all-valid representation, endpoint order, and
cross-batch boundary once. Each producer-proven batch contributes one
conservative endpoint interval without a duplicate row scan. Batch gaps remain
explicit until the bounded summary reaches capacity; only then are intervals
coalesced into one conservative hull.

Pending generated runs flush when a new source invocation breaks the current
key progression. This keeps non-contiguous scheduler ownership from being
hidden inside one output-batch envelope, while contiguous and boundary-merged
runs retain the normal full-vector publication path.

Parallel finalization can skip rehash only when conservative key summaries
prove disjointness; overlapping boundaries reconcile through the normal path.

### Aggregate-state targets

Grouped-state publication uses the semantic sink contract, not the hash-table
row layout. Core records each aggregate's state offset in
`ExecutionRegionAggregateContract.grouped_state_offsets`; the executable copies
the offsets and primitive kinds into its immutable primitive-run plan. These
offsets identify aggregate states within a core-provided state address. They do
not expose tuple rows, hash slots, salts, chains, or ownership.

Before generated publication, runtime lanes must match the planned primitive
kind, semantic state offset, and canonical state layout. The backend-private
call ABI then carries only core-owned state addresses, semantic row/address
selections, and preaggregated primitive deltas. Selector-shape-specific
artifacts cover identity, row-selected, address-selected, and fully selected
publication without adding selector branches to the identity hot loop. The
publisher can initialize appended groups and update selected existing groups
without exposing the hash-table layout. Generated artifacts never specialize
from the first runtime caller.

Core row storage likewise keeps selection ownership explicit. Proven append-only
single-partition publication consumes fresh state addresses in input order and
does not construct a reverse partition map that no consumer can observe.
Lookup-backed and address-returning append routes still request that map.

The common lane budget emits direct constant-offset stores from the immutable
plan. Wider homogeneous primitive lists use one bounded lane loop and read the
already-validated semantic offset from each lane input, so machine-code size
does not grow with lane count. Wider heterogeneous lists retain the generic C++
publisher. Initialization clears the complete canonical nullable-state flag,
updates preserve NULL semantics, and hugeint addition carries exactly.

### Aggregate-state sources

A hash-aggregate source does not publish its sink-state layout back through
source IR. Its state-scan contract contains only semantic primitive lanes:
aggregate index, source output index, logical return type, and primitive update
kind. Physical row addresses, aggregate offsets, state-value offsets, and
validity offsets remain core-owned.

The backend maps those source lanes to target aggregate indices once while
building the recipe. At runtime it asks the core-owned
`ExecutionAggregateStateScanBatch` to combine the mapped primitive states.
Core validates source and target lane compatibility before mutation and keeps
the row traversal behind that semantic API. Unsupported, destructor-bearing,
or incompatible aggregate states use the normal finalized-vector source path.

Direct state consumption is an economics decision, not a workload exception.
The current recipe admits it only when unique mapped aggregate states span at
least 128 semantic bytes; synthetic row-count lanes do not contribute to that
threshold. Narrow reductions are cheaper through DuckDB's existing finalized
vectors. Wide reductions scan each state row once. Their common signed-int64
to hugeint route accumulates exactly in 128 bits inside core and performs one
checked target-state update per lane per batch, avoiding a checked hugeint call
for every state cell. The primitive source scan uses an empty tuple projection:
it pins and advances row-address batches without gathering grouping columns
that the semantic combine cannot consume.

Built-in primitive SUM states define zero as the value of an unset state, so
the homogeneous reducer accumulates values without a per-cell NULL branch and
tracks only whether each lane observed a value. When core-owned signed-int64
SUM states form the canonical adjacent layout, core walks that run with compact
exact stack accumulators. This physical classification never crosses the
source contract; non-canonical layouts and schemas wider than the bounded
scratch retain the branchless semantic-lane loop.

Runtime proof distinguishes
`source.hash_aggregate.primitive_state_combine` from the finalized scan stage.
Tests cover both sides of the economics boundary, including NULL state,
negative values, and accumulation beyond signed 64-bit range.

## Runtime accounting

Accounting distinguishes:

- selected accelerated runner;
- compiled region;
- executed compiled region;
- native-only backend result;
- explicit native-tail delegation;
- materialization elision;
- runtime blocker or compile failure.

Selection is not compilation, and compilation is not execution. Performance
evidence is valid only when the expected physical recipe and executed-stage
receipts are present. A missing proof is a failure for every query selected by
the accelerated-runner CBO, including queries that later fail compilation or
fall back.

Traced proof runs are separate from production timing. Tracing, verification,
and event logging must not be enabled in a performance sample.

## Performance contract

Correctness and performance are both required:

- compiled routes must beat native execution by their workload-specific floor;
- native-selected routes must remain within an independent raw slowdown limit;
- a regression cannot pass because `jit_policy=off` slowed by the same amount;
- paired or normalized ratios are secondary noise evidence only;
- one-thread and parallel floors remain separate when scheduling changes the
  demonstrated margin.

Candidate measurements use five alternating policy pairs. Promotion and ship
qualification use ten. Failed candidates are not silently rerun with more
samples. Generic speedup floors use the median within-repeat `off/auto` ratio,
while raw JIT-auto ceilings remain independent. Diagnosis is separate from the
gate and cannot change its verdict.

A verified performance improvement and its regression baseline are one change.
Tighten the checked-in workload floor or promote the accepted comparison
artifact in the same increment; otherwise the gate still accepts the slower
performance that the implementation replaced.

## Baselines and artifacts

Disposable benchmark output lives under ignored `tmp/` directories and may be
deleted at any time. Accepted local TPC-H evidence lives under ignored
`benchmark/tpch/jit/local_baselines/`; accepted state stores paths relative to
that directory. Promotion copies a fully verified artifact into durable local
baseline storage before atomically publishing state and removes the previous
local accepted artifact afterward.

An accepted TPC-H baseline requires:

- all 22 queries;
- production timing with tracing and verification disabled;
- ten alternating repetitions;
- result correctness;
- artifact-schema verification;
- independent raw JIT-auto runtime ceilings;
- preserved speedup and component contracts;
- a separate traced runtime-proof pass for every compiled or CBO-selected
  accelerated query.

SF1 and SF10 use separate state files because scale factor is part of the
baseline contract. State also binds thread count, timing mode, query set, and
relevant JIT configuration.

A regression-gate invocation clones one immutable, scale-factor-keyed template
and reuses the private working database for the untraced candidate, traced
runtime proof, and full promotion pass. The default template cache is local and
ignored, carries an explicit role/format/scale manifest, is
atomically created under an exclusive creation lock, and is rebuilt when
validation fails. The lock is released before measurement. Filesystem
copy-on-write avoids a physical SF10 copy where supported; a normal copy is the
portable fallback. Disabling the cache explicitly exercises dbgen. Database
generation is setup, not repeated proof.

The combined production guard rejects a sustained busy host before setup. The
TPC-H gate checks again after its private database clone is ready, and generic
measurements recheck at their own boundary. macOS security scanners have an
independent single-core ceiling because normalized machine-wide utilization can
hide their effect on a single-thread measurement. The same principle applies
generically to any sustained competing process: aggregate host utilization and
the busiest process have independent ceilings. Admission may wait through a
bounded transient scan, but measurement never starts on a rejected sample.
An immediate post-measurement sample invalidates a run if load appeared after
admission. Host-load admission is measurement hygiene only; it cannot change
raw ceilings, speedup floors, query coverage, or runtime-proof requirements.

Both gates share one measurement lifecycle: one ordered SQL script in one
shell process, each workload or query's preparation placed immediately before
its samples, and every preparation and attempt closing the database back to
`:memory:` so the next attempt reopens the same checkpointed file with fresh
connection and buffer-manager state. This preserves workload chronology while
avoiding a macOS provenance assessment per sample. Query timers, alternating
policy order, counters, correctness artifacts, and regression contracts remain
attempt-local, and traced counter collection is an untimed tail after all
candidate timers.

Pre-commit correctness and pre-push performance receipts are bound to the exact
Git tree. A push can reuse completed production verification without measuring
inside the `git push` process, which can itself activate macOS security work.
Any tree change invalidates both receipts.

## Verification

Commands for every gate live in `benchmark/jit/README.md`. Every
implementation change must pass proportionate focused tests first, then
static architecture checks, complete JIT correctness, generic one-thread and
parallel workloads, and affected TPC-H scale factors.

## Current boundaries

- Variable-width expression coverage is narrower than fixed-width coverage.
- Unsupported aggregate layouts, DISTINCT shapes, and join modes remain native.
- Generated run reducers unroll bounded, proven primitive lane layouts; wider
  homogeneous layouts use a bounded lane loop, while wider heterogeneous
  layouts retain exact generic execution.
- Multi-key, selected, and non-flat grouped inputs require an explicit proven
  route before generated run ownership can expand.
- Native source and sink protocols remain native unless an exported execution
  contract makes composition safe.
- Native-only selection is valid capability analysis, not a failed recipe and
  not permission to loosen CBO accounting.

These are capability boundaries, not workload exceptions. New support should
generalize the semantic descriptor or runtime ABI, remove a boundary, add
correctness and runtime-proof coverage, and ratchet performance evidence.
