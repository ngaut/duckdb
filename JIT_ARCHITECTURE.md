# DuckDB JIT Architecture

This document is the implementation contract for making DuckDB natively
JIT-friendly. The target is not an add-on expression compiler. DuckDB core owns a
compiled execution substrate beside the vectorized interpreter, and SLJIT is only
the first backend that lowers that substrate to machine code.

All implementation work is tracked on git branch `codex/jit-native-duckdb-core`.
The current upstream comparison base is
`ff62f2bc89a2f899fc18652f4d7a4be2258f8fbd` from `origin/main`.

## Goal

Make DuckDB's execution design expose stable compiled contracts:

```text
SQL / logical plan
        |
        v
optimized logical plan
        |
        v
physical plan
        |
        v
compiled pipeline descriptor
        |
        v
compiled region planner
        |
        v
backend-neutral operator IR
        |
        v
native runtime protocols
        |
        v
backend lowering
        |
        v
executable fused regions
```

The compiled layer must be generic, not benchmark-specific. TPC-H is a
verification workload, not a design input.

## Goals

- Keep normal DuckDB execution as the semantic source of truth.
- Make compiled execution a DuckDB core execution mode with explicit operator
  contracts, not a backend-specific physical-operator recognizer.
- Lower DuckDB expressions and physical pipeline graphs into stable typed
  expression IR and fused region IR before any backend-specific code generation.
- Model stateful operators as resumable producer/consumer protocols, not as
  one-input-chunk to one-output-chunk transforms.
- Make execution mode observable and truthful: native code, generated
  typed-helper code, executor fallback, and unsupported paths must not be
  conflated.
- Scope compiled code to query/executor lifetime unless a catalog-aware
  invalidation model exists.
- Support multiple backends behind one database-owned registry.

## Architecture Verdict

This is the best long-term DuckDB JIT architecture if the implementation follows
the contracts in this document. The key choice is the unit of compilation:
cost-admitted fused regions with native operator protocols, not isolated
expressions or backend-specific shortcuts. That choice matches DuckDB's
vectorized execution model and fixes the observed performance failure mode where
forced JIT creates many small generated regions but removes little executor work.

The design is intentionally generic. TPC-H may expose weaknesses, but no
benchmark name appears in eligibility, IR, region selection, backend capability,
or admission. Region shape keys are derived from semantics: operator sequence,
types, vector formats, selections, helper/state protocols, and boundary costs.

## Rejected Architectures

These alternatives are intentionally rejected:

- **Expression-only JIT**: too small a unit. It can produce native code while
  leaving scan, selection, projection, join, aggregate, and sink overhead in the
  normal executor. That is the observed forced-JIT slowdown pattern.
- **Pipeline-wrapper JIT**: rejected even if it looks like the shortest path
  from filter/projection code generation. It cannot cleanly model stateful
  operator phases, region selection, or backend-independent fused shapes.
- **Backend-owned physical-plan lowering**: creates reverse dependencies and
  forces every backend to rediscover DuckDB semantics. Core DuckDB must lower
  physical plans into neutral IR once.
- **Whole-executor helper calls**: dishonest and usually slow. A generated
  helper is valid only when generated code owns iteration, selection, and output
  placement.
- **Compile-everything default JIT**: wrong performance policy. Correctness
  capability and profitability are separate decisions; `force` exists only for
  diagnostics.
- **Cross-query code cache in v1**: not worth the invalidation risk. Query-scoped
  RAII code is the correct base until catalog, function, settings, backend ABI,
  and helper/state protocol invalidation are designed.

The accepted architecture is therefore a core DuckDB redesign: core-owned
semantic IR, core-owned operator contracts, backend-owned code generation, and
manager-owned admission over fused regions.

## Non-Negotiable Invariants

The implementation is correct only if all of these stay true:

1. DuckDB's normal execution path remains the semantic source of truth.
2. DuckDB core owns physical-plan inspection and lowers it to backend-neutral IR.
3. Physical operators expose compiled contracts through DuckDB core APIs; SLJIT,
   LLVM, and future backends do not depend on physical operator internals.
4. Backends consume only core JIT IR and runtime contracts.
5. Fused regions are first-class compile targets, not hidden pipeline metadata.
6. Stateful operators expose resumable producer/consumer protocols. Joins,
   aggregates, sorts, materialization, sources, and sinks are not modeled as
   simple one-chunk transforms.
7. Helper calls are typed ABI calls from generated loops, never whole executors.
8. Unsupported or unproven stateful operators become explicit region boundaries.
9. `auto` admits only non-overlapping regions with positive cost proof.
10. `force` is diagnostic coverage, not performance evidence.
11. Events and counters must separate eligibility, capability, admission, compile,
   runtime execution, decline, and fallback.
12. Compiled code is query/executor scoped until a catalog-aware invalidation
    model exists.
13. Migration is replace-and-delete. Temporary scaffolding must not survive as a
    second production path.

## DuckDB Core Redesign

DuckDB should expose a compiled execution contract layer that is independent of
any backend. A physical operator may implement normal vectorized execution,
compiled-contract extraction, or both. The compiled-contract API is the only
thing the JIT planner and backends consume.

Core contracts:

- `ScanCursor`: reads storage/table-function/column-data morsels and exposes
  column vector sources, filters, and table filter facts.
- `FilterStage`: consumes vector inputs and emits a selection stream.
- `ProjectionStage`: consumes vector inputs and writes typed vector outputs.
- `HashJoinBuild`: appends build rows into a documented hash table contract.
- `HashJoinProbeCursor`: probes a hash table and drains zero, one, or many
  output chunks from one input morsel with explicit resume state.
- `AggregateLookup`: maps group keys to aggregate state addresses.
- `AggregateUpdate`: applies typed aggregate transitions to state addresses.
- `SinkCursor`: consumes compiled-region output and preserves DuckDB sink
  results, blocking, and finish semantics.
- `StateScanCursor`: drains aggregate, join, materialization, sort, and top-n
  state into output chunks.

The region planner fuses contracts, not classes. A backend sees typed IR nodes
such as `hash_join_probe_cursor`, `aggregate_lookup`, and `aggregate_update`; it
does not see `PhysicalHashJoin` or `GroupedAggregateHashTable` internals.

Pipeline analysis starts with `JitPipelineDescriptor`. DuckDB walks a physical
`Pipeline` once, asks each source/operator/sink for its `JitOperatorDescriptor`,
and records stable operator facts: role, pipeline index, type, name, output
types, estimated cardinality, and role-sliced compiled contracts. It also
records native-role flags derived from those slices. Admission inventory and
region IR lowering both consume this descriptor. They must not independently
re-probe the raw `Pipeline`, call `GetJitOperatorDescriptor()` as a second
discovery path, or re-slice compiled contracts. This is the high-to-low
boundary: DuckDB physical operators publish contracts first, the pipeline
descriptor normalizes role views, then region planning lowers those role views
into IR, then backends lower IR into executable code.

The canonical in-core object is `JitCompiledOperatorContract`. Legacy source,
operator, and sink descriptor views are compatibility payloads derived from that
contract while migration is in progress. The pipeline descriptor prepares a
role-sliced compiled contract for every source/operator/sink node:

- a source node sees only scan/state-scan stages;
- an operator node sees only transform/probe stages;
- a sink node sees only sink/update/build stages.

This role slicing is required because one DuckDB physical operator can expose
multiple contracts in different pipelines. For example, a hash join exposes a
build sink contract, a probe transform contract, and sometimes a state-scan
source contract. Region planning must not accidentally treat all three as owned
by one node.

## Native Region Protocol

Compiled regions use a resumable protocol. They are not constrained to consume
one input chunk and return one output chunk.

```text
region.open(runtime_state)
while source has morsels:
    region.push_or_fetch_morsel()
    while region.can_drain():
        output = region.drain_next_chunk()
        downstream.consume(output)
region.drain_final()
region.close()
```

The protocol result is one of:

- `NEED_MORE_INPUT`: the region consumed its current input and has no buffered
  output.
- `HAVE_MORE_OUTPUT`: the region produced an output chunk and must be called
  again before new input is consumed.
- `BLOCKED`: the region is waiting on DuckDB's normal scheduler/resource
  protocol.
- `FINISHED`: the region and all downstream state are complete.

Stateful contracts own explicit resume records. The records are backend-neutral
IR/runtime facts, not backend-local inventions. Examples:

- Hash join probe resume state:
  - current input row offset;
  - current hash slot when open-address probing is not exhausted;
  - current build row pointer inside a duplicate chain;
  - output row count already materialized into the current output vector;
  - finished flag for the current input morsel.
- Aggregate state scan resume state:
  - partition/table index;
  - state block pointer;
  - row offset inside the state block;
  - finalized-state flag.
- Sort/top-n state scan resume state:
  - run index;
  - vector offset;
  - heap/materialized payload cursor.

This protocol is the root fix for multi-match joins, grouped aggregate output,
sort/top-n output, materialized CTE scans, and any future operator that can
produce more output than fits in one vector from a single input chunk.

## Ownership

`DatabaseInstance` owns `JitManager`. Backends register with that manager, not with a process-global singleton. This
is required because loadable extensions may link their own static DuckDB image.

`JitManager` owns:

- backend registration and selection;
- compile policy;
- bounded event retention;
- cumulative counters;
- compile-event recording.

`ClientContext` supplies settings and suppression state. It does not own backend state.

## Architecture

```text
DuckDB physical plan / expression tree
        |
        v
Core JIT eligibility + semantic inventory
  - operator roles
  - side effects
  - state protocols
  - row/cardinality estimates
        |
        v
Core JIT IR lowering
  - typed scalar ops
  - vector formats
  - validity/null propagation
  - selection semantics
  - state protocol nodes
  - helper-call nodes
  - fallback nodes
        |
        v
Region formation
  - maximal contiguous physical regions
  - explicit operator/state boundaries
  - fallback crossings counted as cost
        |
        v
Backend capability plan
  - native nodes
  - typed helper blockers
  - fallback boundaries
  - backend-private lowering plan
        |
        v
JIT admission
  - policy
  - estimated work
  - compile-cost model
  - benchmark/profiling proof
        |
        v
Backend code generation
  - SLJIT
  - LLVM later
  - WASM / Cranelift later if useful
        |
        v
Executable kernel
  - fused operator region kernel
  - scalar expression primitives embedded inside a region kernel
```

The architecture has four separate decisions:

- **IR eligibility**: DuckDB can lower the fragment into a complete semantic IR.
- **Region selection**: DuckDB can form a non-overlapping executable dataflow
  region from that IR.
- **Backend capability**: a backend can generate correct native/helper code for that IR.
- **Admission**: the selected policy should pay compilation cost for this execution.

These decisions must not be collapsed. Capability is about correctness.
Admission is about performance and policy. A supported backend plan is not
automatically worth compiling.

## Execution Lifecycle

JIT is a query/executor-lifetime optimization. The runtime lifecycle is:

```text
Physical plan and pipeline graph are built
  -> pipeline reset performs cheap JIT inventory and selected-region preparation
  -> core lowering creates expression and region IR only for admissible pipelines
  -> region builder creates deterministic candidates
  -> backend analyzes capability for each candidate
  -> JitManager scores and admits non-overlapping candidates for auto
  -> pipeline executor instantiates prepared kernels through the selected backend
  -> backend compiles admitted candidates into RAII code handles
  -> core executor boundary installs query-scoped kernels
  -> runtime invokes kernels, records events/counters, and handles declines
  -> jit_verify compares generated results with reference boundaries when set
  -> query/executor teardown releases executable code and temporary state
```

The lifecycle has one ownership direction. DuckDB core may inspect physical
operators and bind state handles because it owns those concepts. Backend code
must not inspect planner, optimizer, physical-operator, or executor internals.
The backend receives normalized IR, helper signatures, opaque state protocols,
and runtime contracts.

There is also one compilation entry point. `Pipeline::PrepareJitRegions`
prepares `JitPreparedPipeline` before source global state captures JIT scan
contracts, and `PipelineExecutor` may only call `CompilePreparedRegions` for
that prepared object. Late executor-side prepare-and-compile fallbacks are not
part of the architecture because they bypass the single traceable admission
boundary and can desynchronize source-state contracts from selected kernels.

No cross-query executable-code cache exists in v1. A future cache needs a
separate invalidation design covering catalog changes, function definitions,
settings that affect semantics, backend version, platform ABI, and helper/state
protocol versions.

## Core Lowering

Every expression exposed to a backend must pass through:

```text
DuckDB Expression
  -> JitExpressionIR
  -> core normalization
  -> backend capability analysis
  -> JIT admission
  -> backend lowering
  -> executable kernel
```

Core normalization belongs in DuckDB, not in an individual backend. Examples:

- fold non-throwing casts of constants into typed constants;
- preserve `constant_or_null` as an explicit typed IR node;
- canonicalize `IS DISTINCT FROM NULL` and `IS NOT DISTINCT FROM NULL` into null checks;
- keep SQL three-valued boolean semantics explicit in the IR.

Backends may reject unsupported IR. They must not reinterpret planner trees directly to recover semantics that the
core IR failed to expose.

Each expression IR node must carry semantic metadata before backend analysis:

- logical type and physical type;
- validity/null-propagation rule;
- value source (`constant`, `vector#N`, or `derived`);
- exception behavior.

Backends must treat logical type as the semantic contract and physical type as
the representation contract. For example, `DATE` is physically `INT32` but is
not a native `INTEGER` operation unless the IR/backend contract explicitly adds
DATE support. Deterministic IR printing is a serialization of these fields, not
an independent recomputation path.

Core region lowering owns all DuckDB physical-operator inspection. A backend
must consume `JitRegionIR` / `JitRegionIRNode`, classify those IR nodes, and
lower from that classification. Backend modules should not include
filter/projection physical operator headers or expose convenience paths that
lower directly from DuckDB `Expression` trees.

Each region IR node must carry dataflow metadata before backend analysis:

- output logical and physical types;
- input and output vector format;
- vector source;
- selection source;
- fallback boundary kind.

For example, a filter node consumes unified vectors and input selections and produces a selection vector; a projection
node consumes unified vectors and produces flat vectors; scan/sink/operator fallback nodes are explicit executor
boundaries.

Source boundaries are semantic IR records, not backend-side interpretations of
DuckDB physical operators. A source node that still runs through DuckDB
`GetData` must expose a `JitRegionSourceInfo` descriptor with:

- source kind: DuckDB table scan, table-function scan, generic scan, stateful
  operator, or none;
- execution kind: DuckDB `GetData` helper, executor fallback, native source, or none;
- native-source contract: status (`ready`, `blocked`, or `none`), required
  backend-neutral capability, protocol version, and blocker reason;
- scan/table function name;
- output and returned column counts;
- column IDs and projection IDs after DuckDB pushdown decisions;
- projection, filter-prune, filter-pushdown, dynamic-filter, and in/out-function flags;
- lowered static scan filters when expressible as typed expression IR;
- typed table-scan protocol records for scan function, output shape, projection
  bindings, filter pushdown, dynamic filters, and in/out functions;
- typed native stateful source protocol records for materialized column-data
  sources such as CTE scans;
- typed stateful state-scan protocol records for join probe and aggregate scan
  phases, including deterministic text only as a rendering of those records;
- hash-join probe key bindings, aggregate descriptors, and group bindings when
  those phases are represented as source nodes;
- fallback reasons for filters or source protocols that are not expressible yet;
- deterministic `source<...>` text surfaced through `jit_dump_ir=true`.

Source-pushed scan work must also be visible in candidate traits and shape keys.
A table scan with pushed filters is not merely a `projection` or
`projection-sink` region just because DuckDB stores the filters inside the scan
operator. Core region formation must count source filters, source filter
expression/fallback support, source comparison-filter type families, pruned scan
projection columns, and returned scan columns. Candidate shapes include
`scan-filter` and `scan-project` segments when those semantics are present. This
is required for honest operator-aware admission: the backend and cost model must
see the actual scan/filter/projection/update region that dominates runtime.

The native-source contract is not the same as current execution. A table scan
can honestly report `execution=duckdb-getdata-helper` while also exposing
`native_source_contract<status=blocked,required_capability=duckdb-table-scan-native-source,protocol=v1,blocker=duckdb-getdata-helper-boundary>`.
That record is the architecture handoff from physical-operator inspection to
backend lowering. A backend may only lower a source as fused/native when the IR
source execution is `native-source` and the native-source contract is `ready`.
Blocked contracts are still valuable: they identify the exact source ABI work
needed before source-prefix candidates can be compiled as fused regions.

Backend lowering also has a selected source execution. Candidate traits describe
what the physical source can do; selected source execution describes what this
lowered kernel will actually call at runtime. These are intentionally different
fields in JIT events:

- `candidate_source_execution`: source capability from core physical-operator
  inventory;
- `selected_source_execution`: backend lowering/runtime ABI selected for this
  kernel.

A native-capable table scan with pushed scan filters can select `native-source`
and still expose a source-prefix raw input contract. When the backend can lower
all pushed filter expressions through the typed scalar IR, the lowering plan
sets `owns-source-filters=true`, records
`generated source-prefix table scan filters`, and asks DuckDB scan for
`PRUNE_ONLY` prepared source input. The generated region then owns the row-level
filter loop, source output projection, downstream projections, joins, or
aggregate updates over the unfiltered scan chunk. If the filters are not
representable by the typed IR, the native-source fallback remains honest:
DuckDB scan owns filtering and the reason records
`source-filters-owned-by-duckdb-scan`. A lowered region that selects
`duckdb-getdata-helper` is an explicit non-fused source boundary: DuckDB owns source
filter execution and pruning, while JIT can only own the downstream body over
surviving rows. That path must be skipped by
compiled-region admission, including `force`; it is not native-source ownership and must not be used as proof that source-filter fusion is complete. A
`fusion-blocker:source-fusion-gap:requires-native-source` entry is valid for
non-fused or unsupported regions that need a native source protocol before
they can become fused execution regions.

SLJIT, LLVM, WASM, or any future backend must consume this descriptor from
`JitRegionIRNode`. A backend must not rediscover these facts outside typed JIT
IR:

- scan function names, projection IDs, filter counts, and dynamic-filter presence;
- native-source capability, protocol version, and blocker status;
- hash-join probe keys and join protocol facts;
- aggregate protocol facts and aggregate source payload/group bindings.

Backends must not inspect DuckDB physical operators, parse `reason`, or perform
scraping `source<...>` / `sink<...>` text.

Fused regions are first-class compile targets. They must not be represented as
"pipeline plus extra metadata". The core API must grow these public contracts
before any region family is treated as architecture-complete:

- `JitCompileTarget::REGION`;
- `JitRegionIR` and `JitRegionIRNode`;
- `JitRegionCandidate`;
- `JitRegionLoweringPlan`;
- `JitRegionCompilationInput`;
- `JitRegionCompileResult`;
- `JitRegionKernel`.

Expression IR is an internal scalar lowering primitive consumed by region
analysis and backend lowering. It is not an executable compile target. Pipeline
kernels are not a long-term production JIT target either. Production query
execution is region-first: `jit_policy=auto` admits region candidates, not
independent scalar or pipeline targets.

## Backend Contract

A backend receives immutable compilation input:

- `JitRegionCompilationInput` for fused operator regions.

Backends must analyze capability before codegen:

- `AnalyzeRegion` returns a `JitRegionLoweringPlan` over region nodes and state
  protocols.

Native/helper/fallback/pass-through is represented by the neutral
`JitLoweringKind`. Region capability analysis uses that vocabulary for scalar
expression nodes, operator nodes, and boundary nodes without exposing DuckDB
executor objects to the backend. Pass-through means a backend can preserve
dataflow for a semantic node, such as a reference-only projection, but the node
does not itself generate executable code.

The interface returns a single backend plan object and must not rebuild the same
decision during compile. The plan has two parts:

- a core-visible lowering summary: target, nodes, execution mode, reason, shape key;
- an opaque backend-owned plan: normalized backend IR, helper stubs, register/loop strategy, and generated-region
  metadata.

`CompileRegion` must consume the exact analyzed plan. Rebuilding capability
inside compile is not the source of truth; at most, a backend may assert
internal invariants derived from the already analyzed plan. The analyzed plan is
immutable during compile. If code generation needs to attach executable code
handles or move data into a kernel, it must first copy the analyzed semantic
plan into a local compile plan and mutate only that local copy.

Backends return `JitRegionCompileResult` only after admission allows code
generation.

`JitManager` enforces capability honesty. A compile result must use the
execution mode returned by its analysis result. A region compile result must also
carry the explicit region execution form declared by the lowering plan. Execution
mode answers "what kind of compiled execution exists"; region execution form
answers "did this compiled region remove DuckDB operator/materialization
boundaries from the hot loop?" Core must reject any compiled region whose
lowering plan leaves the execution form as `none`. Node counts, helper counts,
and native counts are trace evidence only; they must not be used to infer
compiled execution mode or fused-region form. Pass-through-only and
fallback-only plans are not compilable. Region kernels use two separate facts:
`code_size` is emitted machine-code bytes, while `HasExecutableBody()` proves
the runtime kernel can execute the compiled region. Native protocol-stage
kernels can have `code_size=0` when they own an operator loop through a narrow
runtime protocol instead of an emitted machine-code stub. Helper-only sink
regions are not fused executable regions; they must be reported as non-fused
unsupported or fallback work until a backend owns the operator loop.

Auto-policy admission is not backend capability. `jit_policy=auto` must reject
kernels without enough expected work or performance proof before backend code
generation. `jit_policy=force` is the compile-every-supported-fragment mode for
testing, diagnostics, and explicit user experiments. `enable_jit=true` means
JIT is available; it does not by itself mean compile everything.
For region targets, `auto` is fused-only: after backend analysis, core must
reject any region whose `region_execution_form` is not `fused`, even if a
backend provides an admission rule and the estimated cardinality clears that
rule. `force` is fused-only for compiled regions as well: it bypasses
profitability gates, but it must not compile non-fused helper or unsupported
regions. Non-fused helper rows remain valid diagnostics only as skipped or
unsupported events.

Admission inputs belong in `JitManager`, because they are cross-backend policy:

- selected policy (`auto`, `force`, `off`);
- estimated source and region cardinality from DuckDB physical operators;
- number and shape of native/helper nodes;
- backend plan shape key;
- backend-owned admission rules keyed by target and shape key, with the measured
  proof that justifies auto compilation;
- measured compile cost by backend and shape;
- optional measured execution benefit by backend and shape;
- current debug settings such as `jit_verify` and `jit_dump_ir`;
- whether `auto` stopped at lightweight region inventory or full typed IR;
- cache hit/miss information once a catalog-safe cache exists.

Admission outputs must be observable and distinct from unsupported lowering:

- `compiled`: code generation was allowed and produced a kernel;
- `skipped`: capability exists, but auto policy declined to compile;
- `unsupported`: the IR/backend cannot generate correct code;
- `unavailable`, `disabled`, or `error`.

Using `unsupported` for a profitability/policy rejection is wrong because it
conflates correctness with performance. Counters and events should preserve
that distinction.

Core must not hard-code backend-specific shape keys or profitability thresholds.
`JitManager` owns the policy decision, but it asks the selected backend for the
admission rule that corresponds to a shape key. This keeps the cross-backend
policy centralized while keeping backend-specific performance proof with the
backend that generated the shape.

Backends may also provide an auto-admission precheck over
`JitRegionPipelineInventory`. This inventory is a cheap, core-owned summary of
the physical pipeline: source/sink roles, source execution, operator counts,
stateful boundary features, projected-column and source-filter counts,
estimated cardinality, and deterministic pipeline/feature shape text. It is not
typed region IR and it cannot compile anything.

Inventory has two explicit modes. `ADMISSION` mode may inspect physical
operators and compute backend-neutral facts, but it must not render
`feature_shape`, `pipeline_shape`, inventory `ir`, or backend reason text.
`DIAGNOSTIC` mode may render those strings for retained events and decision
counters. This keeps observability out of the production negative path while
preserving deterministic trace output when `jit_trace_decisions`,
`jit_dump_ir`, or `jit_trace_runtime` asks for it.

`jit_policy=auto skips pipeline before typed IR lowering` only when the selected
backend proves that the pipeline inventory cannot map to any admitted measured
shape, or that the inventory cardinality is already below the only measured
threshold it could reach. The rule is simple: inventory false positives are
allowed. Inventory false positives are cheap because they merely cause full typed
IR lowering and normal backend analysis. inventory false negatives are
architecture bugs, because they hide a potentially profitable region before
semantic traits and backend capability can be checked.

The semantic rule is equally strict: full typed IR remains the semantic source
of truth for candidates that survive inventory admission. If inventory says a
pipeline might be admitted, the manager must lower full typed region IR before
backend analysis so diagnostics, capability, selected execution mode, candidate
traits, and deterministic IR text remain complete. If inventory proves the
pipeline cannot map to an admitted family, `jit_dump_ir=true` surfaces
deterministic admission-inventory IR instead of forcing full typed IR.
`jit_policy=force` must bypass the inventory precheck and still analyze supported/unsupported
candidates so coverage diagnostics remain complete. Observability has its own
rule: bounded event retention must not force full typed IR lowering for every
impossible auto pipeline; retained events and cumulative decision counters are
observability products, not a reason to pay semantic-lowering cost on known
misses. A precheck rejection is reported as `skipped` with
`execution_mode=executor_fallback`, admission metadata, and zero
backend-analysis time, not as an unsupported backend capability failure.

Current verification must be generated from the trace and benchmark harnesses,
not copied into this document as a static report. The architecture verifier
rejects checked-in run directories, generated `report.md` snapshots, Python
bytecode caches, forbidden labels, and reverse dependencies between core JIT,
backend code, and DuckDB executor internals.

The current contract is deliberately stricter than capability. Native
source-prefix filter/projection, source-owned full-pipeline aggregate update,
and terminal typed-sink helper paths may be correct force-mode capabilities, but
they are not `auto` policy until the exact normalized region family has repeated
production benchmark proof. Forced compilation proves coverage and backend
honesty; it is not profitability evidence by itself.

The root architectural correction is operator-context admission. A region
candidate must carry a backend admission key derived from the executable region
shape, source ownership, expression traits, and downstream operator inventory.
No source-prefix table-scan family is globally admitted just because its body
can be generated. Admission keys must include every expression and operator
boundary that materially changes the cost model, and missing proof must surface
as an explicit skipped decision counter instead of an implicit fallback.

## Fused Region JIT Design

The long-term JIT unit is a fused operator region, not an isolated expression
or projection. Scalar expression lowering remains a useful internal primitive,
but executable JIT kernels must remove meaningful DuckDB executor work across a
contiguous physical region.

The generic flow is:

```text
Physical pipeline graph
  -> core operator-aware IR
  -> region candidates
  -> backend region capability
  -> cost/admission
  -> executable region kernel
```

This design is deliberately benchmark-independent. TPC-H is only a workload
that exposes the problem; the architecture targets generic columnar analytical
pipelines.

### Region IR

Core lowering introduces `JitRegionIR` as the only production operator-aware JIT
IR. A region is a contiguous dataflow segment with explicit source and sink
boundaries. It may cover one or more physical operators, but the backend sees
only normalized JIT IR, not DuckDB physical operator classes.

Region IR nodes should use semantic operator roles:

- `source_boundary`: DuckDB-owned source feeding a generated region;
- `scan`: generated vector/table scan access when the source protocol is
  supported;
- `filter`: selection-producing predicates;
- `projection`: value-producing expressions;
- `join_probe`: hash/range join probe against DuckDB-owned or generated join
  state;
- `join_build`: join-state materialization when a backend supports the build
  protocol;
- `aggregate_update`: local aggregate state update;
- `aggregate_combine`: merge of local states when supported;
- `topn_update` or `sort_input`: ordered-state update when supported;
- `sink_boundary`: DuckDB-owned sink consuming generated output;
- `helper_call`: generated loop calls a typed helper stub;
- `fallback_boundary`: normal DuckDB executor resumes control.

Each node must carry enough semantic data for backend lowering:

- stable region-local node ID;
- semantic role and lowering kind;
- logical and physical types;
- input and output vector formats;
- selected-row and row-id mapping;
- validity/null propagation;
- state handles for joins, aggregates, sorts, and sinks;
- ownership of output vectors and temporary vectors;
- exception behavior;
- helper-call signatures;
- estimated cardinality and selectivity;
- whether the node is streaming, blocking, or stateful.

A `JitRegionIRNode` is not a pointer to a DuckDB physical operator. It is a
semantic record. The minimum core-visible payload is:

- node kind and stable node ID;
- input node IDs and output layout;
- expression fragments, when the node evaluates expressions;
- boundary ID, when the node starts or ends generated execution;
- state protocol ID, when the node touches stateful DuckDB data;
- helper signature ID, when the node calls a runtime helper;
- estimated input rows, output rows, vectors, and selectivity;
- deterministic printable text for `jit_dump_ir=true`.

Stateful operators are not special cases. They are region nodes with explicit
state protocols. A join probe region can consume a DuckDB-built hash table
through a typed helper API. A future generated join build region can own build
loops only after the IR exposes the build-state contract. Aggregates follow the
same rule: generated code may update local aggregate state through typed helper
stubs before a backend owns full native aggregate update logic.

The state protocol is a core ABI, not a backend shortcut into private executor
objects. A `JitStateProtocol` must declare:

- protocol kind: `join_probe`, `join_build`, `aggregate_update`,
  `aggregate_combine`, `aggregate_finalize`, `sort_input`, `topn_update`, or
  `sink_update`;
- query-scoped opaque state handle lifetime;
- typed helper entrypoints, if generated code does not own the full operation;
- input and output vector formats;
- selection-vector semantics;
- validity/null semantics;
- exception behavior and error propagation;
- memory/allocation ownership;
- verification hook at a semantic boundary.

Until a state protocol exists for a blocking or stateful operator phase, region
formation must split there. This is not a special case; it is the normal result
of missing semantic state ABI.

### Region Formation

Core DuckDB code owns all inspection of physical operators. It lowers the
physical pipeline graph into a backend-neutral operator IR, then constructs
region candidates.

Region formation should be deterministic:

1. Lower the physical pipeline graph into semantic region nodes and pipeline
   dependencies.
2. Split the graph into streaming chains separated by pipeline breakers and by
   stateful phases without an exposed state protocol.
3. Mark every semantic node as native-capable, helper-capable, or fallback-only
   at the core semantic level.
4. Emit the maximal native prefix before the first hard boundary when that
   prefix contains real generated work. This is the only source-prefix planner
   product; it exists for scan/filter/projection work that can resume cleanly at
   the next DuckDB operator boundary.
5. Emit the full source-to-sink pipeline candidate. The full candidate is the
   canonical place to carry operator, sink, state, materialization, join, and
   aggregate protocol blockers.
6. Ask the backend to analyze whole candidates. Node support alone is not
   enough; the backend must accept the combined control flow and state protocol.
7. Score supported candidates through admission.
8. Install a deterministic non-overlapping selected set. With the current
   maximal-prefix/full-pipeline planner this normally means either the full
   pipeline or the prefix, never a lattice of interior fragments.
9. Record skipped candidates with their reason. Lack of selection is observable;
   it must not disappear as a missing event.

`force` bypasses the profitability gate, not the executable-region safety
contract. It admits supported candidates for diagnostics, but the executor still
installs a deterministic non-overlapping region set. The planner deliberately no
longer emits arbitrary post-source intervals, sink-only suffixes, or every
subspan of projection/filter chains. Those shapes created compile overhead and
trace noise without removing a meaningful executor boundary. A future graph
region planner may add more region families only when the entry ABI and
operator-state protocol make them executable, not just diagnosable.

This turns fallback into a normal region-boundary concept. Fallback boundaries
must be coarse and visible. They should not appear between every expression and
projection inside a query plan.

### Backend Capability

Backends analyze whole regions, not just individual nodes. Capability has two
levels:

- node capability: whether the backend can lower each semantic node as native,
  pass-through, protocol-blocked, or fallback;
- region capability: whether the backend can generate a single executable
  control-flow loop for the chosen node sequence and state protocol.

A backend may reject a region even when it supports all individual nodes if it
cannot generate the combined control flow correctly. A backend may call
primitive helper stubs for narrow operations such as decimal arithmetic, string
comparison, date extraction, hashing, allocation, or exact exception behavior.
It must not treat whole operator helper paths such as hash-join build/probe,
generic hash aggregate lookup/update, source fetch, or sink bodies as fused
operator-region execution. Protocol-only sink regions without generated
operator work are blockers, not success states.
`JitRegionLoweringPlan` exposes the region-level compiled execution mode
explicitly, so manager admission and non-overlap selection use whole-region
capability instead of inferring executability from native subnode counts. Node
counts are trace evidence only; they must not be used as a fallback capability
model. The same plan also exposes the region execution form explicitly:

- `fused`: generated backend-owned code owns the executable operator interval and
  removes intermediate DuckDB operator/materialization/source/sink helper
  boundaries inside that interval;
- `none`: valid for non-region, disabled, unavailable, unsupported, and
  non-compiled candidate rows, but invalid for a compiled region.

`execution_mode` and `region_execution_form` are intentionally separate. A
compiled region can be `fused` only when generated code owns the executable
interval and the interval has no executor boundary, no typed operator/sink
helper ownership boundary, and no missing native protocol. Primitive stubs for hashing,
comparison, allocation, exceptions, and low-level value primitives are allowed
because they do not own operator protocol. Hash-join build/probe and generic
hash aggregate lookup/update are native contracts for supported layouts and
join/aggregate shapes; unsupported variants must stay missing-protocol, not
typed-helper success states. If generated code would be separated by source,
sink, materialization, typed-helper, or whole-executor boundaries, the candidate
is `none` and must be skipped or reported unsupported rather than compiled.

Core stage ownership is a hard admission contract, not backend advice. Backend
analysis may explain why a candidate is blocked, but it cannot override
`JitRegionContract::native_fusion_ready` or the canonical
`JitRegionStagePlan`. If a backend advertises `region_execution_form=fused`
while the core stage plan still contains a typed-helper, executor-fallback, or
missing-protocol stage, `JitManager` records the candidate as unsupported and
does not call backend code generation. This keeps fake fusion, non-fused helper
source prefixes, and stale shape-specific backends from reaching runtime.

Pipeline shape semantic boundary labels are part of the contract. Plain `scan`,
`sink`, `operator-helper`, `operator-fallback`, and `expression-fallback`
describe DuckDB-owned execution boundaries or unsupported regions. By contrast,
`source-native`, `operator-native`, and `sink-native` mean core lowering has
found a ready native protocol contract for that source, operator, or sink. Those
native protocol labels must not be counted as capability gaps; if a backend
cannot lower the surrounding candidate, the missing fact belongs in the
candidate contract, stage plan, or backend blocker, not by relabeling a native
protocol boundary as helper/fallback.

Backends must not call whole DuckDB executors from generated code.
Typed helper stubs are acceptable only when their signatures are part of the
JIT runtime contract and when generated code owns iteration, selection, and
output placement.

A `JitRegionLoweringPlan` should expose:

- target `region`;
- normalized region shape key;
- ordered node lowering decisions;
- boundary conversion count and cost class;
- state protocols used;
- helper signatures used;
- expected compiled execution mode;
- expected region execution form;
- backend-private immutable plan.

Helper calls are grouped by ABI class:

- pure typed scalar helper;
- vector-format helper;
- state probe/update/combine/finalize helper;
- allocation helper;
- exception helper.

No helper class may call a whole `ExpressionExecutor`, `PipelineExecutor`, or
physical operator executor. If generated code does not own iteration and output
placement, the node is fallback or unsupported, not a compiled success mode.

Region shape keys must be normalized from IR semantics, not benchmark names.
A shape key should include:

- operator sequence;
- type families;
- vector and selection formats;
- native/helper/pass-through/fallback node counts;
- state protocols used;
- boundary crossings removed;
- approximate expression complexity.

For example, `scan-filter-project`, `scan-filter-join_probe`,
`join_probe-project-aggregate_update`, and
`scan-project-aggregate_update` are generic region shapes. They are useful for
many analytical workloads, not just TPC-H.

Candidate shape has two layers. `candidate_pipeline_shape` is the executable
operator interval that a generated kernel may run. It must not include source or
sink context nodes unless those nodes are actually generated by the backend.
`candidate_context_pipeline_shape` is the full DuckDB pipeline around that
candidate. Unsupported analysis may use the context shape to explain scan,
join, aggregate, sort, materialization, and sink boundaries; compiled/native
attribution must use the executable shape. This keeps small generated
filter/projection regions from being reported as compiled scans, joins, or
aggregates.

Core also derives a `JitRegionSignature` for every candidate. The signature is
the backend-neutral contract for region context, structural shape, candidate
feature shape, and full-pipeline context feature shape. Backends may prefix or
map that signature into backend-specific admission keys, but they must not parse
`candidate_pipeline_shape` or `candidate_context_pipeline_shape` text to recover
semantic features such as `table-scan-source`, `hash-join-build`,
`hash-aggregate-update`, or `ungrouped-aggregate-update`. Printable pipeline
shape is observability; `JitRegionSignature` is the semantic input to backend
capability and admission.

Auto admission has two levels. The first level is the production-only pipeline
inventory gate described above. It may skip a pipeline before full typed IR only
when the backend inventory rule proves no admitted measured shape can be
reached. Query roots, CTAS, profiling wrappers, materialization sinks, and
stateful source boundaries can change the outer pipeline shape while leaving an
inner fused candidate valid, so the inventory gate must be conservative: false
positives are cheap, false negatives are correctness and performance bugs.

The second level is candidate admission after full typed IR lowering. Its
long-term invariant is:

- build deterministic region candidates from the DuckDB physical pipeline;
- attach typed candidate traits, candidate scope, executable pipeline shape, and
  surrounding context pipeline shape;
- ask the backend whether that candidate can map to an admitted measured shape;
- record candidate-level skip/admit counters, bounded events, and admission
  proof.

This keeps default-on auto JIT honest without making diagnostics part of the
hot path. A missing admission rule is a silent negative return in production
unless `jit_trace_decisions`, `jit_dump_ir`, or `jit_trace_runtime` asks for
diagnostic rows; the backend inventory precheck receives an explicit `explain`
flag so it can avoid formatting skip reasons unless a diagnostic row will be
retained. A below-threshold admitted rule is recorded on the candidate
with `admission_shape_key`, `admission_rule_present=true`,
`admission_min_cardinality`, `admission_score`, and `admission_proof`.
`duckdb_jit_decision_counters()` retains those cumulative rule-backed rows even
when `jit_event_log_size=0`, so production diagnostics still answer "why did
auto skip or admit this region?" without requiring retained event rows. The
inventory gate is an optimization of the negative path; it is not a replacement
for candidate traits, backend capability, non-overlap selection, or admission
proof.

Candidate traits are the backend-neutral contract between coarse candidate shape
and backend capability. Core region IR computes deterministic
`JitRegionCandidateTraits` for every candidate: source/sink presence, source
kind, source execution kind, sink kind, table-scan source boundary, stateful
source boundary, filter/projection/operator counts, expression-trait
availability, source filter count, source filter expression count, source filter
fallback count, source comparison filter count, source integer comparison filter
count, source non-integer comparison filter count, source conjunction filter
count, source projected column count, source returned column count, arithmetic projection count, integer
arithmetic projection count, non-integer arithmetic projection count, reference
projection count, comparison filter count, integer comparison filter count,
non-integer comparison filter count, conjunction filter count, expression fallback count, operator
fallback count, scan boundaries, and sink boundaries. These traits are printed inside the
candidate IR when `jit_dump_ir=true`, and they are also exposed as first-class
columns through `duckdb_jit_events()`, `duckdb_jit_decision_counters()`, and
`duckdb_jit_kernel_counters()`. Production diagnostics must group by these
columns, including `candidate_source_filter_count`,
`candidate_source_filter_expression_count`,
`candidate_source_filter_fallback_count`,
`candidate_source_comparison_filter_count`,
`candidate_source_integer_comparison_filter_count`,
`candidate_source_non_integer_comparison_filter_count`,
`candidate_source_conjunction_filter_count`,
`candidate_source_projected_column_count`,
`candidate_source_returned_column_count`,
`candidate_arithmetic_projection_count` and
`candidate_integer_arithmetic_projection_count`,
`candidate_non_integer_arithmetic_projection_count`,
`candidate_reference_projection_count`,
`candidate_integer_comparison_filter_count`, and
`candidate_non_integer_comparison_filter_count`, and
`candidate_conjunction_filter_count`, instead of parsing `reason` or
backend IR text.

Backends must consume those traits for eligibility and pre-admission instead of
parsing `candidate_pipeline_shape` or rediscovering DuckDB physical operator
details. Shape strings remain diagnostics and stable grouping keys; they are not
the semantic contract. For example, SLJIT source-prefix auto admission may use a
coarse inventory candidate to decide that full IR lowering is worth doing, but
once full typed IR exists, `projection-chain` admission must require the
candidate traits to show a table-scan source boundary, integer arithmetic
projection work, and no non-integer arithmetic work for the currently measured
SLJIT family. `filter-projection` admission similarly requires integer
comparison filters and rejects non-integer comparison filters until that family
has a separate lowering path and measured proof. Identity/pass-through
projection stacks may still be elided by backend lowering, but they must not
masquerade as an admitted measured projection-chain family.

Backend auto admission is table-driven. Each admitted family declares its
backend shape key, measured minimum cardinality, proof artifact, candidate fact
predicate, and conservative inventory precheck predicate in one backend-owned
rule table. The candidate predicate must consume `JitRegionSignature`,
`JitRegionCandidateTraits`, and `JitRegionContract`; the inventory predicate may
only use `JitRegionPipelineInventory`. Neither predicate may parse rendered
pipeline-shape text. This keeps policy additions local and prevents candidate
admission, inventory admission, observability text, and benchmark proof from
drifting into four separate copies of the same rule.

Backend admission shape keys add a third layer: a backend-owned normalized key
derived from the executable core IR shape. The key must preserve the structural
candidate shape but append operator-aware boundary features when the executable
candidate includes a boundary. For SLJIT, examples are
`sljit:source-prefix:filter-projection:table-scan-source` for a maximal native
prefix and
`sljit:full-pipeline:filter-projection-sink:table-scan-source+ungrouped-aggregate-update`
for source-to-sink ownership. The current planner does not emit post-source or
sink-suffix admission families. Aggregate sinks must distinguish
`hash-aggregate-update`, `perfect-hash-aggregate-update`, and
`ungrouped-aggregate-update`. This keeps `auto` admission misses cost-rankable by
state protocol instead of collapsing every sink to a generic sink label.

`candidate_scope` is the ownership boundary for that executable shape. Current
planner output is intentionally narrow: `source_pipeline` for one maximal
generated prefix and `full_pipeline` for source-to-sink ownership. Source JIT
has a core-owned source-prefix runtime ABI: `FetchFromSource` owns DuckDB source
state, calls the normal source helper when the source is not native, closes
source profiling on the raw source chunk, and then invokes a generated chunk
kernel for the admitted prefix. Source-pipeline kernels must advertise
`CanExecuteSourcePipeline()`, and the core executor returns the next operator
index where normal pipeline execution should resume. The generated prefix
output is written into the same operator-boundary chunk DuckDB would have used
after the skipped prefix operators, not into the raw source chunk. That keeps
source-prefix regions valid when filters or projections change column count,
logical type, physical type, vector format, or selection semantics before the
remaining DuckDB suffix resumes. A backend that compiles a `source_pipeline`
candidate without that source-prefix executable ABI is an architecture error.
Sink-owned suffixes are not planner products in the current architecture.
Existing sink ABI enum support is a reserved runtime contract, not permission to
compile sink-only regions. Aggregate and join sinks should become native only as
part of a full-pipeline native operator protocol.

`JitRegionContract::abi` is the canonical runtime entry contract. It is one of
`chunk_transform`, `source_prefix`, `sink_suffix`, `full_pipeline`, or reserved
`state_scan`. Runtime dispatch, backend ABI validation, source preparation, and
backend lowering decisions must consume this ABI instead of recomputing entry
kind from `owns_source`/`owns_sink` boolean pairs. The ownership booleans remain
for compatibility, but they are not the semantic runtime contract. SQL
observability exposes this directly as `candidate_contract_abi` in
`duckdb_jit_events()`, `duckdb_jit_decision_counters()`, and
`duckdb_jit_kernel_counters()`, so tests and production trace tooling do not
need to parse rendered pipeline shapes or contract IR to recover the ABI. This
removes the old edge case where source-prefix, full-pipeline, sink-suffix, and
chunk-transform decisions were reimplemented independently in core execution,
backend lowering, tests, and trace helpers.

ABI category checks are also centralized in the core JIT common layer through
`JitRegionABIIsChunkTransform`, `JitRegionABIIsSourcePipeline`,
`JitRegionABIIsSinkPipeline`, and `JitRegionABIIsFullPipeline`. Runtime dispatch,
backend admission, and backend planning must use these helpers instead of
defining backend-local predicates or storing parallel source/sink/full booleans.
Compiled kernels may carry the ABI, but they must derive executable capability
from that ABI rather than from duplicated state.

The SQL-visible candidate contract columns are declared and appended through one
internal helper shared by `duckdb_jit_events()`,
`duckdb_jit_decision_counters()`, and `duckdb_jit_kernel_counters()`. The helper
is the schema owner for these columns; individual table functions may choose
where the candidate block appears, but they must not hand-roll the candidate
contract column list or append order.

Full source-to-sink candidates must report `full_pipeline`. They are visible for
capability, admission, and TPC-H/root-cause attribution. A compiled
`full_pipeline` kernel must advertise `CanExecuteFullPipeline()` and enter
through the core-owned full-pipeline runtime ABI. That ABI is exposed as
`JitRegionKernel::TryExecuteFullPipeline(JitFullPipelineRuntime &,
JitFullPipelineResult &)`: the backend owns generated source-to-sink work, while
core provides source fetch, native state binding, profiling, suppression,
finalization, and runtime trace boundaries through the runtime facade. Source
fetch through the facade advances the same source-exhaustion state as the
reference executor.

Full-pipeline kernels may enter only native sink/update/state protocols exposed
by the core IR and runtime contract. There is intentionally no generic sink
facade, no retryable sink chunk callback, and no typed sink callback. Hash join
build/probe and generic hash aggregate lookup/update enter through their
backend-neutral native contracts only for supported protocol shapes. Result
collectors, materialization, sort/top-n build, and any other sink/operator
without a native protocol must be reported as unsupported or fallback with an
explicit missing protocol reason. Calling a whole DuckDB sink or whole DuckDB
operator from a full-pipeline kernel is not a native-fused region.

Stateful operator protocol ownership is unsplit in v1. Core candidate formation
must not create source-prefix, post-source, or sink-suffix candidates that cross
a hash join probe, operator protocol boundary, fallback operator, or upstream
stateful operator span without a runtime protocol that can resume that state.
Those shapes belong either in one `full_pipeline` candidate or in the reference
executor. This keeps backend analysis focused on executable ownership instead of
recording stale "resume protocol missing" candidates that can never run.

The canonical full-pipeline missing-sink reason is
`full pipeline sink requires native sink or operator update protocol`; backend
events may attach typed IR and operator protocol facts after that reason, but
they must not compile the region.

### Native Sink-Update ABI

The native sink-update ABI is narrower than a typed sink helper. Core DuckDB may
bind private sink state because it owns aggregate and sink internals; backends
must receive only backend-neutral handles. For ungrouped aggregate updates that
contract is `JitNativeUngroupedAggregateState`, bound by
`JitBindNativeUngroupedAggregateStates` in core and exposed to a full-pipeline
backend through `JitFullPipelineRuntime::BindNativeUngroupedAggregateStates`.
SLJIT consumes only state pointers, count pointers, physical input vectors,
validity masks, selection vectors, and the update kind declared by IR. It must
not include aggregate physical-operator headers or cast DuckDB local sink state.

Native aggregate sink updates are all-or-nothing at the sink-node level. A sink
with three aggregates is native only if every aggregate update in that sink has
an executable native/generated contract. If any aggregate is unsupported, the
whole sink remains a typed helper or fallback boundary. A partial native aggregate update is an architecture bug because it mutates DuckDB local state before the reference path can safely replay the same rows.
The native implementations cover `count(*)` and nullable `count(x)` for
non-optional BIGINT state, plus nullable `sum(x)` when core IR proves one
physical INT64 payload child and an optional BIGINT or HUGEINT state. The
HUGEINT variant uses the same lower-half carry/borrow rule as DuckDB's
`AddToHugeint` aggregate helper, so BIGINT and decimal64 payload bits feed the
normal 128-bit sum state without calling the whole aggregate executor. Hash and
perfect-hash aggregate sinks expose native grouped state binding for those
updates. Exception-rich aggregate functions, non-INT64 payload sum variants, and
aggregate source-state scans remain explicit helper/fallback work until their
protocols and generated loops are designed and verified.

Grouped hash and perfect-hash aggregate have separate contracts. The
aggregate source-scan contract says whether generated code can iterate aggregate
output state as a source. The aggregate function contract says whether each
payload can map to native update metadata such as `sum`, `count`, state type,
and state offsets. The grouped state layout contract exposes backend-neutral
per-aggregate row offsets and payload sizes as
`grouped_state_layout_ready`, `grouped_state_offsets`, and
`grouped_state_payload_sizes`; this is necessary for generated grouped updates,
but it is not sufficient for native ownership. The grouped state contract says
whether core can bind backend-neutral aggregate state addresses after the row's
group has been found or created. The native hash lookup contract says whether
generated code can own the expensive group lookup/creation protocol itself.
Core exposes the grouped state-address contract through flat
`native_grouped_state_*` fields inside `JitRegionAggregateProtocol`, and it
exposes hash-table lookup ownership through flat
`native_hash_aggregate_lookup_*` fields inside the same protocol. It does not
expose an operator-specific typed lookup helper contract in core IR. Backends
must consume those neutral contracts instead of inspecting grouped aggregate
internals or calling whole aggregate sink helpers. The fields are intentionally
not nested angle-bracket IR records because TPC-H trace extraction treats each
protocol as one deterministic record.
Function metadata alone is not native grouped aggregation. Native grouped state
binding alone is also not native hash aggregation; it is unsupported until the
lookup contract is ready.
When the grouped state contract is missing, grouped
aggregate sinks must report `native-aggregate-function-contract=ready`,
`native-grouped-state-contract=missing`,
`native-grouped-state-layout-contract=ready`,
`native-grouped-state-required-capability=...-native-grouped-state`,
`native-grouped-state-blocker=grouped-state-protocol-boundary`, and
`requires-native-grouped-state-abi=true`, and they must remain non-fused
fallback work. Reporting such a sink as native fused work is an architecture
bug because the generated region still delegates the
expensive hash-table and grouped state mutation loop to DuckDB.
Distinct grouped aggregates are a separate native protocol, not a variant of
plain grouped count. A `count(DISTINCT x)` sink must expose
`distinct_aggregate_count`, `distinct_table_count`, `distinct_child_count`, and
`distinct_filter_count`, keep the grouped-state and lookup contracts missing,
and report `hash-aggregate-distinct-grouped-state-protocol-boundary` plus
`hash-aggregate-distinct-lookup-protocol-boundary` until generated code owns
the distinct-table sink and finalize protocol. Treating it as ordinary
`count(x)` would double-count duplicates and is forbidden.
For generic hash aggregate, grouped-state layout, generated state update, and
hash-table lookup/creation are separate contracts. Core now exposes generic
hash aggregate lookup through the backend-neutral
`JitBindNativeHashAggregateStates` ABI, which binds JIT group-column bindings to
the radix hash table's local group chunk and calls
`RadixPartitionedHashTable::FindOrCreateAggregateStatesFromBoundGroups`. The
backend receives only aggregate state addresses plus grouped-state offsets; it
does not inspect `PhysicalHashAggregate`, `RadixHTLocalSinkState`, or any
DuckDB executor helper. For supported aggregate shapes, SLJIT lowers this as
`SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE` and reports `generated native
hash aggregate lookup and state update`,
`native_hash_aggregate_lookup_contract_status=ready`,
`native_hash_aggregate_lookup_required_capability=hash-aggregate-native-lookup`,
`native_hash_aggregate_lookup_blocker=none`,
`native-hash-aggregate-lookup-contract=ready`, and
`native-grouped-aggregate-update-executable=ready`. Those facts make the hash
aggregate node a typed native protocol node. SLJIT now admits these native
aggregate sink updates through the same generic fused runtime loop used by
other native sink updates: `region_execution_form=fused`,
`kernel=generic-runtime-loop`, and no
`operator-fusion-gap:native-operator-codegen-missing`. The event must not retain
typed lookup helper contracts; it is a native protocol success, not a
helper-backed success. It also must not report the stale "lookup ready but SLJIT
lowering missing" blocker. Unsupported grouped hash shapes should name the
actual missing piece, such as no aggregate payload bindings or an unsupported
aggregate update function.

Perfect-hash aggregate now has a native grouped-state update protocol, not a typed sink helper payload callback.
`JitBindNativePerfectHashAggregateStates`
binds group keys to backend-neutral aggregate state addresses through
`PerfectAggregateHashTable::FindOrCreateAggregateStates`, and SLJIT consumes
only `PERFECT_HASH_AGGREGATE_UPDATE`, `JitGroupedAggregateGroupBinding`,
`JitGroupedAggregatePayloadBinding`, `JitNativeGroupedAggregateStateSet`, and
generated grouped update kernels. For supported aggregate shapes the event must
report `native-grouped-state-contract=ready`,
`native-grouped-state-blocker=none`,
`native_grouped_state_contract_status=ready`, and
`native_grouped_state_blocker=none`. `perfect_hash_aggregate_update` in
`jit_dump_ir` is native sink-state update only when those fields are ready. The
full fused perfect-hash aggregate path additionally reports
`native_hash_aggregate_lookup_contract_status=ready`,
`native_hash_aggregate_lookup_required_capability=
perfect-hash-aggregate-native-lookup`, and
`native_hash_aggregate_lookup_blocker=none`; the
perfect-hash aggregate source scan is a separate native state-scan protocol
reported as `perfect-hash-aggregate-native-state-scan` with
`native_state_scan_contract_status=ready` and
`native_state_scan_blocker=none` when core can scan the finalized
perfect-hash table through the operator source protocol.

Perfect-hash aggregate performance also needs an explicit update-strategy
contract, not just state addresses. The current native strategy is
`direct_state_update`: generated code owns source/filter/projection/group-id
evaluation and updates DuckDB's native perfect-hash aggregate state in the same
row loop. This is the simplest correct fused shape, and it is the baseline that
future strategies must beat.

Rejected strategies are part of the architecture contract too. Per-group scratch
accumulation and generated linked row lists both measured worse on TPC-H Q01:
they added extra memory traffic or a second pass without matching DuckDB's
clustered update locality. They must not remain as hidden alternate code paths.

Long-term aggregate JIT should add strategies only behind the same region
contract and admission machinery:

```text
direct_state_update
generated_local_accumulate
typed_duckdb_clustered_helper
executor_fallback
```

`generated_local_accumulate` is acceptable only if it proves a real locality win
without row-list overhead. `typed_duckdb_clustered_helper` may use DuckDB's
clustered aggregate machinery, but it must report helper execution, not native
codegen. Until a strategy beats the reference path with trace evidence,
perfect-hash full-pipeline JIT remains diagnostic coverage rather than proof
that native aggregate JIT can outperform DuckDB on low-cardinality grouped
workloads.

A native aggregate update alone is not proof of a fused region. Separate
filter/projection and aggregate-update loops over a temporary chunk are not a
fused full-pipeline region. A full-pipeline region may report
`region_execution_form=fused` only when the backend emits one source-to-state
loop for the admitted operator interval. The first fused aggregate shape is
`FILTER -> PROJECTION -> UNGROUPED_AGGREGATE_UPDATE(sum)`: generated code reads
unified source vectors, evaluates the typed predicate IR, computes a direct
reference or binary-reference integer/decimal projection, increments the
aggregate input-row count for rows that pass the filter, skips NULL projected
values for SUM, and commits the bound aggregate state once per vector. Runtime
telemetry must count those rows through the fused native sink/body stages, not
through a helper-sink bucket.

A sink result of `FINISHED` marks processing finished through core. A kernel
result of `FINISHED` is finalized by core before the scheduler sees
`PipelineExecuteResult::FINISHED`, so sink combine, source profiling,
operator-state finalization, and profiler flushes cannot be skipped. A backend
that cannot use the declared source and sink runtime ABI must reject the
candidate. A backend that
declines at runtime must do so before calling source or sink side-effect APIs;
declining after runtime side effects is an architecture error because the normal
DuckDB pipeline can no longer safely replay the same input.
Full-pipeline eligibility is also an executor-entry contract, not just a backend
codegen contract. Core must skip `full_pipeline` candidates before backend
analysis when the executor cannot legally enter the full-pipeline ABI from the
first source-to-sink call. The current hard boundary is any sink that requires
partition/batch protocol (`RequiredPartitionInfo().AnyRequired()`): those
pipelines may still select the maximal `source_pipeline` prefix when it can
resume cleanly, but they must not let a non-enterable full-pipeline candidate
own scan filters or switch table scans to prune-only residual execution. They
also must not manufacture post-source or sink-suffix candidates to hide the
missing executor-entry protocol.
Full region lowering also attaches a backend-neutral `JitRegionSinkInfo`
protocol inventory to sink nodes. The deterministic `sink<...>` text must
expose the sink kind and protocol fields for hash-join build, hash aggregate
update, perfect-hash aggregate update, ungrouped aggregate update, sort,
materialization, and generic operator sinks. Backends consume that sink IR or
reject it; they must not inspect physical sink operators directly. A
DuckDB table-scan source descriptor must also expose the source-prefix raw input
layout as backend-neutral IR: `source_prefix_input_columns`,
`source_prefix_input_types`, `source_prefix_output_projection_map`,
`source_prefix_filter_column_map`,
`source_prefix_requires_unfiltered_input`, and
`source_prefix_filter_prune_required`, and
`source_prefix_filter_split_supported`. These fields describe the raw layout a
backend-neutral source-prefix filter ABI needs. In the current architecture,
native-source table scans use `PRUNE_ONLY` prepared source input when a selected
region owns source filters, and generated code consumes the unfiltered source
chunk through typed vector IR. The generated source-prefix output must project
back to the normal DuckDB operator boundary.
Every candidate has one effective source execution derived from the physical
source contract. Native-capable table scans produce `native-source` candidates;
helper-backed table functions or unsupported scans produce
`duckdb-getdata-helper` candidates. Core must not manufacture a helper-source
alternative for an already native-capable source. Candidate IR must render deterministic `source<...>` text with that effective execution and native-source contract.
Dynamic table filters do not block native-source ownership. They are represented
in the backend-neutral table-scan protocol with `dynamic_filters=true` and are
merged into the storage scan state by DuckDB's table-scan source initialization.
When JIT owns represented static source predicates, storage may still use the
combined static/dynamic filter set for pruning while generated code evaluates the
lowered static predicates. Backends must treat dynamic filters as source
protocol metadata, not as backend-specific DuckDB objects to inspect.
Backends consume this layout from core IR; they must not recover it by
inspecting `PhysicalTableScan`, `TableScanState`, or storage internals. A
source-prefix filter/projection/projection-chain candidate that starts with
DuckDB `GetData` is non-fused: the generated body may be expressible, but the
selected region is still split by a DuckDB-owned source boundary. A
source-prefix candidate whose source execution is `native-source` may report
`fused` only when the selected runtime ABI can fetch the source directly and
the backend either lowers pushed filters into generated source-prefix filter
ops or honestly records that DuckDB scan still owns them.

Full-pipeline selective scan regions have one source-execution candidate. The
generated native variant selects `native-source`, records
`generated source-prefix table scan filters`, and increments native-source
runtime counters for raw scan chunks plus generated-body counters for the
filter/projection/update loop. A candidate that starts with
`duckdb-getdata-helper` is an explicit non-fused source boundary: it must report
`region_execution_form=none`, record
`source-fusion-gap:requires-native-source`, and stay in executor fallback until
source fetch is represented by a native source protocol. Helper-source candidates are diagnostic proof-gap evidence, not measured admission families,
not native regions, and not runtime kernels.

Architecture support is not production admission. A fused shape may enter
`auto` only when it has repeated benchmark proof. The SLJIT native-source
`sljit:full-pipeline:fused-filter-projection-ungrouped-sum` path is the generic
force/debug proof point for scan + pushed filters + projection + native
ungrouped aggregate update. Production `auto` does not advertise this as an
admission family; it rejects at the lightweight pipeline-inventory gate until
repeated full-suite timing proves stable suite-level speedup. A single positive
query-local proof gap is not an admission rule. The proof-gap shape remains
generic and must not inspect TPCH query text.

Helper-backed source-prefix regions that cannot be represented as fused must
emit an explicit source-fusion blocker so the next root fix is visible as
source protocol work, not as an admission threshold tweak.
The valid blocker is
`fusion-blocker:source-fusion-gap:requires-native-source`. A helper-backed
source-prefix candidate must name the non-fused source boundary directly instead of
pretending to be native source fusion. Full-pipeline regions do not have a
generic helper sink success path; a missing native sink/operator protocol is an
unsupported region with the exact missing protocol named in the event reason and
IR.

Source-pushed table filters are normalized in core IR and remain visible in
candidate traits. SLJIT lowers supported pushed filters as backend-owned
source-prefix predicate nodes, requests `PRUNE_ONLY` source input, emits a
native `FILTER` stage over the raw scan chunk, and then emits the source output
projection as a normal native projection. Unsupported pushed filters remain
honest DuckDB-scan-owned filters instead of being relabeled as native generated
work.

Projection chains are also normalized at backend planning time. A projection
whose inputs are a pure reference projection must remap its expression source
indices through that reference projection and emit one executable projection
node. The backend IR marks this as `compose-reference-projection(...)`. This
removes scan column-map projection chunks before arithmetic, casts, null checks,
predicates, and similar typed expressions.

Within a generated fused region, adjacent native `FILTER` +
`PROJECTION` operators are a single runtime dataflow step. The backend first
produces the filter selection vector, then passes that selection as the
projection `execute_sel`, writing only the projected output columns. It must not
materialize a sliced full-width intermediate chunk between those two generated
operators. Compile events expose this with
`runtime-fused:filter-projection=N` so traces can distinguish operator-runtime
fusion from merely adjacent native nodes.

For full-pipeline aggregate regions, runtime filter/projection fusion is still
not enough. The sink update must be part of the same generated loop or the region
is not fused. SLJIT exposes source-filter-owned aggregate regions as
`execution:native-sljit-region-filter-projection-ungrouped-aggregate-update`.
Regions where DuckDB already applied the scan filter before the generated body
runs use
`execution:native-sljit-region-projection-ungrouped-aggregate-update`. The
unsupported non-fused helper-source shape remains visible as a skipped
`filter-projection-ungrouped-aggregate-update` candidate without executable code.

Generated fused aggregate kernels specialize vector input format before entering
the generic row path. Identity selection is normalized to no selection at the
SLJIT source ABI boundary. The kernel then selects a flat/all-valid loop, a
shared-selection/all-valid loop for filtered scan chunks, or the generic
selection-plus-validity loop. These are vector-format contracts, not query
special cases, and the generic path remains the semantic fallback for dictionary
or nullable chunks that do not meet the stronger invariants.

Source-prefix runtime tracing must preserve that ownership split. `GetData`
helper fetch time is measured in `PipelineExecutor::FetchFromSource` and
recorded as a `source_helper` runtime event with `source_helper_*` rows,
invocations, and time. Generated-prefix execution records normal runtime rows
and elapsed generated-kernel time. This prevents a DuckDB source boundary from
being counted as native/generated row execution while still making the remaining
scan-helper cost visible in `duckdb_jit_events()`, `duckdb_jit_counters()`, and
`duckdb_jit_kernel_counters()`.

Candidate estimated cardinality is the estimated work entering the executable
interval: it may use the input boundary estimate even when that boundary is not
part of the executable shape.

### Cost-Admitted Regions

Admission must move from "can compile" to "should compile this region now".
The cost model is region-aware:

```text
compile if:
  estimated_saved_executor_cost
    - estimated_generated_runtime_cost
    - estimated_boundary_conversion_cost
    - amortized_compile_cost
  > safety_margin
```

Admission inputs should include:

- estimated rows entering and leaving the region;
- estimated selectivity;
- expected vectors and invocations;
- number of executor fallback crossings removed;
- vector format conversions required at region boundaries;
- expression complexity;
- type families and helper-call costs;
- state protocol cost for joins, aggregates, sorts, and sinks;
- backend compile-cost history by shape;
- backend runtime history by shape;
- benchmark proof attached to the normalized shape key.

`JitManager` owns the admission calculation. Backends provide
`JitShapeAdmissionRule` metadata for shapes they can prove:

- shape key and backend name;
- minimum estimated rows and vectors;
- minimum native/helper work removed;
- boundary conversion cost class;
- compile-cost estimate;
- runtime-cost estimate or conservative speedup class;
- proof identifier pointing to a benchmark or measured profile.

The v1 auto policy must be deterministic for a given plan and settings.
Database-local runtime history may be recorded and surfaced, but it must not
silently change future admission decisions unless an explicit adaptive policy is
introduced. Cold-start candidates with no shape proof are skipped in `auto`.
Production `auto` is a positive-admission path: by default, candidates with no
backend admission rule are skipped without durable per-candidate decision
events. Candidates with a rule still record proof/threshold skip events, because
those are measured policy decisions. Full missing-rule inventory remains
available when `jit_dump_ir=true`; that trace mode is for diagnosis and
capability planning, not the online hot path.

Architecture support is not the same as production admission. The SLJIT
full-pipeline source-filter/projection/ungrouped-SUM kernel is currently a
force/debug capability. It must not be admitted in `auto` until repeated
production timing proves positive benefit for the exact fused native-source
shape. Similar projection-only or helper-backed shapes still need their own
benchmark proof and must fail closed until then.

The policy result must explain the decision:

- admitted because shape proof and estimated benefit exceed threshold;
- skipped because shape has no proof;
- skipped because estimated rows are too low;
- skipped because boundary conversions dominate;
- skipped because helper-call cost dominates;
- skipped because runtime history shows no benefit.

`jit_policy=force` remains a diagnostic mode. It may compile supported
executable regions that pass the executable-set selector so coverage can be
tested.
Overlapping supported candidates are explicit skips. `jit_policy=auto` must
compile only non-overlapping regions that pass the cost model.

### Execution Contract

The executable abstraction is `JitRegionKernel`. A region kernel owns the
generated loop for its region and has a DuckDB-facing protocol that exactly
mirrors the physical pipeline segment it replaces.

Core execution should route region kernels through `JitRegionExecutor`, not
through backend code embedded in `PipelineExecutor`. `PipelineExecutor` remains
the owner of normal DuckDB execution state and calls the core JIT boundary. The
JIT boundary owns admission trace, runtime trace, kernel decline handling, and
`jit_verify` comparison.

Every region has a `JitRegionBoundaryContract`:

- input boundary: chunk input, generated scan source, or stateful operator
  output;
- output boundary: chunk output, sink update, state update, or selection;
- state references: join tables, aggregate states, sort/top-n states, local
  temporary vectors;
- supported DuckDB operator-result protocol;
- cardinality contract: one input chunk, many output chunks, state-only update,
  or source-driven production;
- vector ownership: borrowed input, generated output, DuckDB-owned sink/state,
  and temporary vector lifetime;
- decline preconditions: runtime vector/state/protocol cases that are valid IR
  but not supported by the compiled kernel;
- conditions under which it declines and lets the executor run the reference
  path.

The region kernel API should make these outcomes explicit:

- `need_more_input`;
- `have_more_output`;
- `state_updated`;
- `sink_updated`;
- `finished`;
- `declined`;
- `error`.

Those outcomes are mapped by core DuckDB code back to the existing physical
operator protocol. A backend must not invent an operator protocol or return a
DuckDB result that the equivalent reference segment could not return.

Verification should compare at region boundaries. For pure chunk-producing
regions, compare output chunks and selections. For state-updating regions,
compare final state or finalized output against the reference path. Verification
must remain semantic, not byte-for-byte over private state layouts unless the
state contract explicitly requires that.

Selection-vector ownership is part of the runtime ABI. If one generated filter
stage slices a chunk and a later stage consumes that sliced chunk, the first
stage's selection buffer must remain stable until the sliced chunk is dead.
Region runtimes and verification helpers must therefore allocate selection
storage per filter stage or otherwise prove stable ownership; reusing one
mutable selection buffer across chained filters corrupts dictionary vectors and
can make verification compare against the same corrupted reference.

A runtime decline is allowed only when the compiled region declared the
precondition in its boundary contract. Decline is recorded as JIT runtime
behavior, and the reference executor owns the replacement work. A decline path
must never be reported as native runtime.

Region JIT must also respect DuckDB's resumable operator protocol. When
`PipelineExecutor` has pending `in_process_operators`, a prefix region must not
run again on the same source chunk. The JIT boundary must decline the region,
resume the reference executor under JIT suppression, and record the resumed work
as `executor_fallback`. This keeps native runtime counters from counting the same
input rows repeatedly when a downstream operator returns `HAVE_MORE_OUTPUT`.

### Initial Generic Region Roadmap

The first profitable region families should be generic and reusable:

1. `scan -> filter -> projection`
2. `scan -> filter -> join_probe`
3. `join_probe -> projection`
4. `join_probe -> projection -> aggregate_update`
5. `scan -> filter -> aggregate_update`
6. `scan -> projection -> aggregate_update`
7. `filter -> projection -> sink_boundary`

Each family should land with:

- deterministic IR printing;
- capability and admission reasons;
- runtime counters by region kernel;
- correctness tests over flat, constant, dictionary, selected, and nullable
  vectors;
- benchmark proof for the normalized shape;
- an explicit `auto` admission rule only after proof shows benefit.

The first implementation step is to keep region IR, region selection, and region
cost accounting as the only production compilation path, so small scalar
kernels cannot become an accidental performance path.

### Architecture-First Implementation Order

Implementation must land the architecture before widening native coverage:

1. Add first-class region target types and event fields without changing
   execution behavior.
2. Build deterministic `JitRegionIR` from existing pipeline/expression IR.
3. Add candidate formation, backend auto-admission precheck, deterministic v1
   candidate selection, and admission scoring. `auto` prunes candidates that
   cannot map to an admitted backend shape before backend analysis, analyzes the
   remaining supported candidates before codegen, and installs the maximum-score
   non-overlapping admitted set for the pipeline. `force` bypasses the precheck
   for full diagnostics.
4. Add `JitRegionExecutor` and boundary-contract verification.
5. Move the current fused filter/projection path behind the region target.
6. Keep the production path behind the region executor.
7. Enable `auto` only for the existing proven filter/projection shape.
8. Add new region families one at a time, each with state/helper ABI, event
   proof, `jit_verify` coverage, and benchmark proof.

Joining or aggregating from generated code before the state protocol exists is
not an acceptable shortcut. The root fix is to expose the state protocol first,
then lower generic region shapes against that contract.

## Runtime Trace Model

Performance work must begin from runtime architecture facts, not from isolated
timing guesses. The JIT event stream therefore has two phases:

- `decision` / `compile`: lowering, backend capability, admission, codegen
  result, IR, compile time, code size, and the kernel ID assigned to a compiled
  kernel;
- `runtime`: actual kernel invocations, linked back to the compiled kernel ID,
  with input rows, output rows, invocation count, elapsed kernel time, and the
  DuckDB operator result or runtime action. Source-prefix fetch helpers use the
  same runtime identity but populate `source_helper_input_rows`,
  `source_helper_output_rows`, `source_helper_invocation_count`, and
  `source_helper_runtime_time_us` so source-helper work is not mixed into
  generated/native kernel row counters.
  Full-pipeline region runtime also records `source_native_output_rows`,
  `source_native_invocation_count`, `source_native_runtime_time_us`, fused stage
  times, and `generated_body_runtime_time_us`. This separates DuckDB source
  protocol cost from the backend-owned generated body and native sink/update
  stages, which is the required evidence trail for moving from source-helper
  generated regions toward materialization-free fused operator regions.

Full-pipeline kernels are already a JIT-local multi-chunk execution unit. They
consume the executor's `ExecutionBudget` through `JitFullPipelineRuntime::MaxChunks()`
and may fetch/process multiple `STANDARD_VECTOR_SIZE` DuckDB chunks inside one
runtime event. This is the correct batching boundary for JIT profitability:
larger JIT work units must be expressed as fused-region runtime ownership, not as a global `STANDARD_VECTOR_SIZE` change that perturbs unrelated DuckDB
operators, storage, compression, and update paths. The evidence for this
contract is `source_native_invocation_count` or `source_helper_invocation_count`
greater than one on a single full-pipeline runtime event.

Runtime tracing is controlled by `jit_trace_runtime=false` by default. Turning
it on records bounded per-kernel runtime events in the same database-owned ring
as compile events and accumulates monotonic counters. This keeps default JIT
overhead clean while still allowing a query to be traced step by step:

```text
core IR event -> backend capability/admission event -> compile event
      -> runtime region event(s) -> counters
```

The trace identity is the executable kernel, not the benchmark query text. A
compiled event creates a `kernel_id`; every runtime event that claims work for
that compiled region must carry the same `kernel_id`. The compiled event tells
what semantic region was generated; the runtime event tells whether that exact
generated region actually ran. If those two facts cannot be joined, the trace is
not acceptable performance evidence.

Runtime events must include declined compiled-kernel calls as `declined`, not
hide them behind the normal executor path. This matters for region kernels whose
boundary contract is valid only for a subset of the operator protocol.

Systematic performance analysis must follow this order:

1. confirm a candidate exists in `duckdb_jit_events()` with a core IR event;
2. confirm backend capability and execution mode from the lowering reason;
3. confirm admission separately from capability (`compiled` versus `skipped`);
4. confirm generated code was produced with non-zero `code_size` and a stable
   `kernel_id`;
5. confirm the kernel actually ran by joining runtime rows to that `kernel_id`
   and checking `input_rows`, `output_rows`, `invocation_count`,
   `runtime_time_us`, and `runtime_result`;
6. confirm the candidate's `candidate_pipeline_shape` matches the expected
   executable operator interval, and confirm `candidate_context_pipeline_shape`
   matches the surrounding lowered physical pipeline context;
7. when no candidate exists, inspect core region inventory/lowering first; auto
   admission must not hide a possible region behind a pre-candidate pipeline
   shortcut;
8. confirm `candidate_scope` matches the executor ownership boundary; current
   SLJIT compiled source-prefix kernels must enter through `FetchFromSource`,
   close source profiling before generated prefix execution, and report
   `source_pipeline` with the core-owned source-prefix runtime ABI;
9. when the event ring is too small to retain the compile event, use
   `duckdb_jit_kernel_counters()` to recover the compile identity, candidate
   shape, candidate executable pipeline shape, candidate context pipeline
   shape, candidate scope, candidate operator interval, candidate estimated
   cardinality, generated runtime totals, declined-attempt totals, and
   executor-fallback totals for that `kernel_id` while the kernel row remains
   inside the bounded retention window;
10. compare cumulative `duckdb_jit_counters()` before and after a benchmark run.

Compile events alone are never performance evidence. A compiled kernel with no
runtime event is a compilation cost, not a speedup. Runtime rows are likewise
not enough unless they can be linked back to the exact compile event or bounded
kernel counter row and backend shape that produced the kernel.

The minimum acceptable trace for a JIT performance claim is therefore:

```sql
SET enable_jit = true;
SET jit_policy = 'auto';
SET jit_trace_runtime = true;
SET jit_dump_ir = true;
SELECT * FROM duckdb_jit_clear_events();

-- run exactly the candidate query or benchmark once per measurement sample

SELECT phase, status, execution_mode, policy_decision, count(*) AS events,
       min(kernel_id), max(kernel_id),
       sum(decision_time_us), sum(compile_time_us),
       sum(input_rows), sum(output_rows), sum(invocation_count),
       sum(runtime_time_us)
FROM duckdb_jit_events()
WHERE target = 'region'
GROUP BY phase, status, execution_mode, policy_decision
ORDER BY phase, status, execution_mode;

SELECT *
FROM duckdb_jit_kernel_counters()
ORDER BY kernel_id;
```

For TPC-H and other multi-query workload traces, the trace harness must also
emit stable aggregate CSVs that summarize region decisions and runtime kernels
by query, policy, execution mode, candidate shape, and candidate pipeline shape.
Markdown summaries are for humans; aggregate CSVs are the diffable performance
evidence.

The workload trace lifecycle has four distinct counts and they must not be
collapsed:

- compiled kernels: generated code exists and has a retained kernel counter;
- reached kernels: the generated kernel boundary was invoked at least once;
- row-processing kernels: the generated kernel processed at least one input row;
- unreached/zero-input kernels: compile overhead or empty upstream output, not
  speedup evidence.

`kernel_runtime_summary.csv` and `query_gap_summary.csv` must expose these
counts directly. A query with compiled kernels but zero row-processing kernels
is a JIT lifecycle gap, not a small generated-region success. A query where only some
compiled kernels process rows must be labeled separately from queries where all
compiled kernels reached useful input.

TPC-H JIT evidence must come from the all-query trace harness. Single-query
TPC-H JIT benchmark files are not an acceptable production evidence surface
because they duplicate one workload slice, miss query-by-query root-cause
attribution, and drift away from the manifest-backed trace contract.

TPC-H has two separate evidence modes:

- `benchmark/tpch/jit/tpch_trace.py` is the diagnostic trace. It answers why a
  query compiled, skipped, fell back, or ran slowly by retaining events, stage
  summaries, capability gaps, runtime rows, IR when requested, and operator
  profile attribution. It also writes a diagnostic
  `admission_proof_gap_summary.csv` from the single trace run so missing auto
  rules are visible next to force-compiled shapes. Those rows are trace
  hypotheses, not production admission proof.
- `benchmark/tpch/jit/tpch_benchmark.py` is the production timing harness. It
  runs repeated query executions with `jit_event_log_size=0` by default,
  captures JSON profiler timings, keeps cumulative decision counters, verifies
  result equivalence against an `off` baseline, and writes manifest-backed
  `runs.csv`, `summary.csv`, `policy_summary.csv`,
  `correctness_summary.csv`, `operator_profile_summary.csv`, and
  `decision_counter_summary.csv`. It also writes
  `admission_proof_gap_summary.csv`, the production timing view that groups
  force-compiled region shapes by query median speedup and labels whether the
  missing auto rule is a positive, mixed, neutral, or negative admission-proof
  gap.

The benchmark harness has two execution passes for each query/policy/repeat. The
profile/correctness pass owns timing and result comparison. The counter pass runs
with profiling disabled, clears JIT events and counters after setup, runs the
measured query without further setting changes, and then exports
`duckdb_jit_decision_counters()`. Each diagnostic skip must appear as either a
skipped pipeline inventory row or candidate row, depending on whether auto
stopped at the lightweight inventory gate or after full typed region IR lowering.
Setup/control pipelines such as `SET` must not appear in
`decision_counter_summary.csv`; otherwise the benchmark is measuring harness work
instead of query JIT decisions. Pure wrapper pipelines such as
`CREATE_TABLE_AS -> RESULT_COLLECTOR`, standalone `RESULT_COLLECTOR`, and
standalone `EXPLAIN_ANALYZE` are not workload regions and must not create pipeline-inventory decision counters.
They also must not enter typed region lowering in `force` or diagnostic modes.
Workload pipelines that merely end at a
wrapper sink remain valid query evidence when they contain a scan, transform,
join, aggregate, sort, or other stateful workload source.

A performance claim should use the benchmark harness for repeated timing and
the trace harness for root-cause attribution. A single diagnostic trace is not a
speed proof, and a benchmark timing without trace attribution is not enough to
explain an architecture regression.

`verify_tpch_benchmark.py` is also an admission-policy gate. If `jit_policy=auto`
compiles any region in the production benchmark, the compiled auto query must
beat the matching `off` median and the auto policy total must not lose to `off`.
`force` must compile diagnostic regions, but force speedups or slowdowns are
coverage evidence only; they do not by themselves justify auto admission.
`admission_proof_gap_summary.csv` is the bridge artifact: it is generated from
production benchmark medians plus decision counters, so a candidate auto rule
can be discussed from one row containing the shape key, execution mode,
candidate scope, query examples, force region count, query-level speedups,
current auto-rule state, and root-cause label. Positive rows are still proof
gaps, not automatic admissions; they identify where a backend-specific measured
rule or cost model needs to be written next.

The diagnostic trace emits the same schema with trace-level timings. That keeps
root-cause investigation and admission-policy discussion aligned, but the trace
variant must not be used alone to add an `auto` rule.

Workload traces must also capture DuckDB JSON profiler output for the executed
result query and reduce it into `operator_profile_summary.csv`. This aggregate
groups profiled physical operator time by query, policy, and operator type, and
excludes trace/materialization wrappers. JIT event rows explain generated-region
decisions; operator profile rows explain where normal DuckDB executor time still
goes after those decisions. A performance conclusion needs both.

Trace database ownership must be explicit and reproducible. A trace run with no
`--db` owns a temporary database. A trace run with `--db` and no
`--use-existing-db` owns creation of a new caller-selected database path and
must refuse to overwrite an existing file. A trace run with `--use-existing-db`
must require `--db`, validate that the required workload schema exists before
measurement, and never delete the caller-owned database. Reused workload inputs
are part of the evidence contract, not a convenience path.

Workload traces must also include a query-level gap summary. `query_gap_summary`
joins policy timing, correctness diffs, auto-admission misses, force-mode
compiled kernels, reached kernels, row-processing kernels, unreached kernels,
representative blocked pipeline shapes, runtime rows, and profiler attribution
into one row per query. It must include the top force-mode profiled operator and
the scan/join/group-by versus projection profile shares so each query row can
explain whether native kernels target the runtime-dominant work. This is the
first file to inspect when answering whether a query is slow because JIT did not
compile, compiled kernels were not reached, compiled too little, skipped by
policy, targeted the wrong operator class, or hit unsupported
operator/expression boundaries. Unsupported/skipped representative pipeline
fields must be workload-relevant; trace-wrapper pipelines such as
`EXPLAIN_ANALYZE` and `RESULT_COLLECTOR` must not be reported as query root
causes.

Workload traces must also include an operator-level gap summary.
`operator_gap_summary` consumes only deterministic pipeline-shape text from the
event stream and aggregates compiled, skipped, and unsupported workload boundary
nodes by policy, status, execution mode, operator name, IR node kind, and
boundary kind. This is the file to inspect when deciding the next architecture
investment: scan/source support, join probe regions, aggregate state protocols,
projection expression coverage, sort/top-N/order boundaries,
CTE/materialization support, or admission proof.

Workload traces must include a capability-level gap summary.
`capability_gap_summary` consumes the same workload-relevant compiled, skipped,
and unsupported boundary nodes as `operator_gap_summary`, but groups them by
architecture capability class: scan/source boundary, join operator boundary,
aggregate state or sink boundary, expression fallback, sort/top-N boundary,
materialization boundary, generic sink, and generic operator fallback. The
verifier cross-checks its occurrence totals against `operator_gap_summary` so
the architecture view cannot drift from the operator-level evidence.

Workload traces must include a capability-priority summary.
`capability_priority_summary` uses the same capability keys as
`capability_gap_summary`, joins them with `operator_profile_summary` by query,
policy, and operator, and joins compiled keys with retained
`kernel_runtime_summary` rows by the executable region identity. Profile time is
counted once per query/operator/capability key so repeated region events cannot
inflate priority. Generated runtime and source-helper runtime are counted once
per compiled kernel/capability key, so a compiled region with multiple nodes in
the same gap class cannot double-count its runtime. This is the no-guessing
investment-order artifact: occurrence counts explain how widely a capability is
missing; priority rows explain how much profiled runtime that capability covers,
how much generated code actually ran, and how much source-helper work remained
around compiled partial regions.

Workload traces must include a query-level capability-priority summary.
`query_capability_priority_summary` uses the same capability keys as
`capability_priority_summary`, but keeps query identity in the key and computes
profile percentage relative to that query/policy. It must also carry the same
generated runtime and source-helper runtime columns as
`capability_priority_summary` and the verifier must prove that query-level
runtime totals roll up exactly to the workload-level priority rows. This is the
query-by-query root-cause artifact: q06-style cases where native code compiled
correctly but scan/source or aggregate/sink boundaries still dominate must be
visible without manually joining multiple CSVs.

Workload traces must include a source-boundary summary.
`source_boundary_summary` is the next-level breakdown under the
`scan_source_boundary` and source-operator fallback capability keys. It parses
source-boundary reason segments from decision and compile events, never
arbitrary `function=` fields from later expression fallback text. It groups by
policy, status, execution mode,
source boundary kind, source operator, scan function, projected column count,
projection pushdown, pushed filter count, dynamic-filter presence, and
in/out-function presence. This is the source protocol investment artifact: it
answers whether the next architecture work should expose a native table scan
state protocol, a DuckDB `GetData` non-fused source-boundary policy, dynamic-filter-aware
scan lowering, non-table source support, stateful native source protocols such
as CTE/materialization scan, or stateful native state-scan protocols such as
hash-join and aggregate scan.
Compiled source-prefix rows must be present when the backend compiles a native
source pipeline. Helper-backed source candidates remain decision rows until a
native source protocol exists, but their decision rows must still expose the
candidate identity: query, policy, candidate id, candidate shape, candidate
pipeline shape, candidate context pipeline shape, and candidate scope. Candidate
IDs are only unique inside a local compilation context and must never be used
alone. The joined source protocol inventory shows both compile-time boundary
shape and actual native-source rows/time. Skipped
or unsupported rows may have zero source-helper runtime because no generated
source-prefix kernel was reached.
Stateful source rows for `HASH_JOIN` must expose the join protocol
inventory: join type, condition count, equality/non-equality/null-equal
comparison counts, key logical types, comparison operators, payload columns,
left/right/probe output columns, delim state, residual predicate state, residual
info state, runtime filter-pushdown probe state, and the native
`native_hash_join_probe_*` / `native_hash_join_build_*` contracts. Inner
equality joins do not expose typed build/probe helper contracts in core IR; the
native operator contracts describe only the backend-neutral DuckDB protocol.
Backend executable support is reported separately: SLJIT creates a hash-join
probe native-region op from the equality-key prefix and the typed match-predicate
suffix with an explicit `native_probe_output_mode`. INNER and RIGHT use
`matched_probe_and_build`; SEMI uses `matched_probe_only`, which materializes
only the probe side after the generated probe selects matched rows. RIGHT matched
phase uses the same protocol plus a generated `found_match` store on the build
tuple. RIGHT_SEMI uses `mark_build_only`: the generated probe owns key matching,
marks the matching build-side tuple chain, returns no rows from the probe
operator, and leaves row production to the hash-join native state-scan source.
MARK uses `mark_probe`, so marker output is owned by the native probe protocol
rather than by a whole executor fallback. FULL, LEFT, ANTI, and RIGHT_ANTI are
not folded into that path until they have explicit unmatched-side protocols.
SLJIT also creates a hash-join build native sink op for build-append-ready shapes
through the core `JitNativeHashJoinBuildBinding` contract. Remaining misses are
reported as runtime-binding, native-lowering, protocol, or stricter static shape
blockers such as `build_append_shape_ready=false`.
Hash-join build append is not limited to one primitive key. Once core proves the
build keys are bound references and the join has an equality hash prefix without
a whole-operator boundary, the native build protocol may reference multiple key
columns and any key type that DuckDB's normal hash-table build path already
supports. Typed non-equality match predicates are part of the native probe
protocol: the generated kernel hashes only the equality prefix, then evaluates
the typed predicate suffix against the build layout before accepting a row.
Residual predicates remain blocked until they are represented as typed residual
expression IR. Correlated MARK joins use the same build append protocol: the
narrow `JoinHashTable::Build` contract owns both the tuple append and the
correlated grouped count update, so the region planner does not need a separate
correlated-MARK sink shape. MARK probe output is a native probe output mode, not
a separate executor fallback.
The stateful hash-join source scan is
a separate native state-scan protocol when core can iterate already-built join
state as a source. This keeps hash join work anchored to explicit DuckDB
protocols, rather than a vague "join fallback" bucket or a whole-operator
executor call mislabeled as native.

Hash join runtime state is exposed through core JIT contracts, not through
physical operator internals. `join_runtime.hpp` exports the finalized table
entries, bitmask, row layout offsets, tuple/pointer offsets, dictionary
next-pointer metadata, salt usage, null policy, chain/resume status, and a
`single_match_probe` fact. Layout readiness
means the pointer table is addressable by generated code; it does not mean every
output protocol can consume that layout. SEMI's `matched_probe_only` mode may use
a chained table for existence probing because the generated kernel only needs to
select matching probe rows. INNER and RIGHT `matched_probe_and_build` still
require `single_match_probe=true` until a resumable chained-match output protocol
exists, because those modes must emit every matching build row, not just prove
that one exists. RIGHT_SEMI `mark_build_only` does not emit matched rows and can
therefore consume chained duplicate-key layouts by following the tuple next
pointer, or the exported dictionary next-pointer array when dictionary emission
has repurposed the tuple pointer slot. Dictionary emission is likewise a
materialization fact, not a probe-readiness blocker: generated probes still
receive row pointers and key layout offsets, while core `GatherRHS` owns
dictionary-vector RHS emission behind the native binding contract. Null-equal join keys are part of the generated key
protocol. The backend hashes source NULLs with DuckDB's vector hash NULL value,
keeps NULL probe rows only for null-equal keys, and compares source/RHS validity
bits before reading key values. Normal equality keeps its original NULL-filtering
semantics.
`operator_runtime.hpp` is the execution-time binding surface: a full-pipeline
runtime asks the owning `PhysicalOperator` to bind the already-lowered
`JitRegionOperatorInfo` to a `JitNativeOperatorBinding`. For hash join probe,
the concrete `PhysicalHashJoin` implementation validates the static native
probe contract, the native probe shape, bound-reference key input, output mode,
and the live hash table layout before returning a binding. Runtime binding
declines are surfaced through JIT runtime events with backend-neutral reasons
such as `native-operator-runtime-binding-blocked:...`; they are not hidden behind
anonymous full-pipeline fallback. Runtime
materialization switches only on the backend-neutral output mode: matched
probe+build rows gather RHS data, while matched probe-only rows slice the probe
side and do not touch RHS layout. Mark-build-only rows do not materialize at all;
the generated kernel marks build-side state and the pipeline continues with an
empty transform output, matching DuckDB's right-semi operator protocol. If the protocol propagates the build side, the
backend-neutral plan carries `mark_build_match` and the found-match offset
derived from the DuckDB table layout contract; the generated probe loop performs
that primitive store itself. Backends consume that binding; they do not include `join_hashtable.hpp`,
cast private operator states, or call `JoinHashTable::Probe`, `ScanStructure::Next`, or any whole-operator executor
path.
Stateful source rows for aggregate operators must expose the aggregate state
protocol inventory: aggregate operator kind, group count/types, aggregate
function names, aggregate return types, child counts, distinct/filter/order
counts, payload types, grouping-set metadata, radix table count, distinct table
metadata, and perfect-hash required-bit metadata where applicable. Hash,
perfect-hash, and ungrouped aggregate source scans are reported as
`stateful_native_state_scan` when their ready native state-scan contract is
selected; fallback rows are valid only for missing or blocked protocols. This
keeps aggregate source and sink work grounded in the state protocol that a fused
JIT region must eventually consume or produce, rather than a blank aggregate
fallback row.

Stateful materialization sources such as CTE and column-data scans are native
sources, not state scans: they expose `execution=native-source` and are reported
as `stateful_native_source` when the generated region owns the source protocol.
They must not populate `native_state_scan_*` fields, because no already-built
join or aggregate state is being iterated.

Stateful source protocol inventory has its own native state-scan contract. The
plain native source contract answers whether a source can run without the
executor boundary. The native state-scan contract answers whether generated code
can iterate an already-built stateful operator result, such as a hash join probe
table, grouped aggregate hash table, perfect-hash aggregate table, or ungrouped
aggregate state. This is exposed through flat `native_state_scan_*` fields on
`JitRegionSourceInfo`. Missing stateful sources must name their missing
capability (`hash-join-native-state-scan`,
`hash-aggregate-native-state-scan`, or
`ungrouped-aggregate-native-state-scan`) and blocker. Perfect-hash aggregate
state scan is ready when core exposes
`perfect-hash-aggregate-native-state-scan`; backends consume that contract
through the native source runtime and must not inspect DuckDB operator state
directly to fill any remaining gap.

Hash join source/state-scan readiness additionally requires a row-producing
source phase. Inner hash joins and other non-build-propagating in-memory joins
can expose native probe/build operator contracts, but their source descriptor
must report `source_produces_rows=false`,
`execution=executor-fallback`, and
`native_state_scan_blocker=hash-join-source-does-not-produce-rows-for-join-type`.
They must not be counted as `source-native` regions, because the normal DuckDB
source call immediately finishes for those join types. The region inventory
also excludes these non-row-producing source pipelines before candidate
admission, so TPCH source-boundary summaries only rank source phases that can
actually emit rows. Inner/left join work remains visible through the hash-join
probe/build operator protocol rows and fusion-gap summaries. Right/full/right-semi
and right-anti join source phases can expose
`hash-join-native-state-scan` when the state-scan protocol is otherwise ready.

Sort result sources follow the same state-scan rule. `ORDER_BY` and `TOP_N` as
pipeline sources must not be reported as a generic stateful native-source gap;
they expose ready `order-by-native-state-scan` or `top-n-native-state-scan`
contracts through their operator descriptors. Sort/top-n construction remains a
sink protocol boundary until a backend-neutral sort-build/update contract
exists.

Workload traces must include a source-boundary priority summary.
`source_boundary_priority_summary` uses the same source-boundary keys as
`source_boundary_summary`, then joins them with `operator_profile_summary` by
query, policy, and source operator. It is the cost-ranked version of the source
protocol inventory: count rows explain how often a source boundary appears;
priority rows explain whether table scans, join-as-source state, aggregate
state, CTE/materialization sources, or non-table scan sources own the measured
runtime. It carries the same `source_helper_*` totals as
`source_boundary_summary`, plus deterministic operator-profiler attribution.
When multiple source-boundary protocol rows reference the same
query/policy/operator profile bucket, profiler time must be allocated
deterministically by boundary occurrence count. It must never copy the full
operator time into each protocol row. The verifier must cross-check its keys and
counts against `source_boundary_summary` and prove that allocated source-boundary
time never exceeds measured policy/operator time, so source protocol
prioritization cannot drift from the event evidence.

Workload traces must include a source-fusion gap summary.
`source_fusion_gap_summary` is narrower than `source_boundary_summary`: it only
records source/full-pipeline region candidates that either already have a
generated body but whose source execution is still a DuckDB helper or executor
fallback, or have source-pushed scan predicates that are rejected until native
source-prefix/full-pipeline fusion exists.
Those rows must carry `source_fusion_gap=requires_native_source`, the source
operator, selected source execution mode, `native_source_status`,
`native_source_required_capability`, `native_source_protocol`,
`native_source_blocker`, admission shape keys, candidate shapes and scopes,
profiler attribution, and measured kernel runtime fields including
`runtime_time_us`, `source_helper_*`, `source_native_*`, fused stage timings, and
`generated_body_runtime_time_us`.
This is the fused-region architecture target. A non-fused source-prefix
filter/projection region is not an admission or threshold problem; it is the
intermediate step before core lowering exposes a backend-neutral native-source
protocol whose contract is `ready`, and the backend lowers that protocol into
the same generated loop as the filter, projection, join, aggregate, or sink
body. The verifier must require source-fusion blocker text on every skipped
non-fused source-prefix region, so a helper-backed region cannot silently
masquerade as fused.

Workload traces must also include a generic fused-region blocker summary.
`fusion_blocker_summary` is the architecture-level priority ledger for every
candidate that carries a blocker to fused execution. It includes skipped
non-fused candidates and unsupported candidates whose core contract names a missing
protocol, typed-helper ownership boundary that is not admitted as fused, or executor
boundary. It parses raw event reasons for `fusion-blocker:*`, groups by blocker
class, current source/sink contracts, candidate shape/scope, and admission shape
key, then joins runtime counters and operator-profiler evidence when a kernel
actually ran. It must include source blockers such as
`source-fusion-gap:requires-native-source`, sink blockers such as
`sink-fusion-gap:requires-native-sink-or-operator-update`, and candidate-level
blockers such as `candidate-fusion-gap:missing-protocol`,
`candidate-fusion-gap:typed-helper-boundary`, and
`candidate-fusion-gap:executor-boundary`. The summary is not a cost model by
itself; it is the lossless trace artifact that proves which native source,
state-scan, grouped-state, sink, or operator ABI must be implemented before a
candidate can become fused. Unsupported source-pushed scan-filter candidates are
tracked in `source_fusion_gap_summary`; candidate-level blockers from
unsupported typed region analysis remain in `fusion_blocker_summary` because
they name the missing architecture contract directly.

The core region IR also carries a candidate-level fused-region ownership
contract. `JitRegionContract` records the owned interval
(`first_node`, `node_count`, `start_operator_index`, `end_operator_index`),
the executable ownership booleans (`owns_source`, `owns_transform`,
`owns_sink`, `owns_state_scan`), and the ownership classes
`source_ownership`, `state_scan_ownership`, `transform_ownership`, and
`sink_ownership` for the candidate being analyzed, not for incidental context
that may appear in the whole-region IR text. Ownership values are
`generated-ir`, `native-protocol`, `typed-helper`, `executor-boundary`, and
`missing-protocol`. A region is `native_fusion_ready=true` only when its
executable candidate has no executor boundary, no typed-helper ownership boundary, and no
missing operator protocol. This makes legacy scope labels reporting-only: core
selection, prepared-source contracts, runtime ABI selection, and SLJIT shape
classification must derive source/full-pipeline behavior from the contract, not
from scope-name folklore. This is the long-term path from non-fused boundary diagnostics to
operator-aware fused regions: table scans, hash-join state scans, grouped
aggregate scans, aggregate updates, joins, and terminal sinks must become
explicit native protocols before they can be counted as fused native work.

Backends consume this core ownership contract from `JitRegionCandidate::contract`.
SLJIT may add backend lowering blockers such as
`candidate-fusion-gap:missing-protocol`,
`candidate-fusion-gap:typed-helper-boundary`, and
`candidate-fusion-gap:executor-boundary`, but it must not rediscover DuckDB
physical operator ownership by reaching back into executor internals. Stateful
sources use `state_scan_ownership` and required capabilities such as
`hash-join-native-state-scan`, `hash-aggregate-native-state-scan`,
`perfect-hash-aggregate-native-state-scan`,
`ungrouped-aggregate-native-state-scan`, plus grouped-state capabilities where
needed. The SQL-visible surfaces `duckdb_jit_events()`,
`duckdb_jit_decision_counters()`, and `duckdb_jit_kernel_counters()` expose the
same candidate fusion fields so runtime traces can rank architecture gaps
without scraping backend-specific reason text.

Workload traces must include an expression-fallback summary.
`expression_fallback_summary` is the next-level breakdown under the
`expression_fallback` capability key. It parses structured core lowering reasons
from region decision events after the `core expression lowering unsupported;`
marker and groups them by policy, status, execution mode, reason, expression
class, expression type, function name, and return type. It must never infer the
blocked expression from unrelated source-boundary fields such as a table-scan
`function=` attribute. This file is the expression coverage investment artifact:
it answers which scalar families, built-in functions, casts, comparisons, CASE
children, or internal compression/decompression expressions prevent native
filter/projection region coverage across the workload.

Integral compressed-materialization expressions are core typed intrinsics, not
generic scalar-function fallbacks. The core IR lowers
`__internal_compress_integral_*` and `__internal_decompress_integral_*` to
deterministic intrinsic nodes with physical source/result widths and constant
minimum values. Backends lower those nodes from IR facts only. String
decompression is different because it allocates string payload storage; it must
use a deliberately designed primitive helper ABI before it can leave expression
fallback honestly.

No-op optional table filters are canonicalized to `constant(true)` by the core
expression lowering. DuckDB's `__internal_tablefilter_optional` runtime function
returns all rows and keeps its child expression only for pruning metadata, so a
JIT backend must see a boolean constant predicate rather than a generic scalar
fallback. Selectivity-optional table filters are not the same thing: they own
runtime state and may execute their child predicate before pausing, so they
remain an explicit stateful filter protocol/fallback until that protocol is
modeled.

Constant-pattern `prefix(varchar, varchar)` is a core typed string predicate.
The core IR records it as `string_prefix` with VARCHAR operands and normal
null propagation; backends may admit narrower capabilities such as
reference-plus-constant prefix. SLJIT lowers that admitted form through the
predicate ABI by reading DuckDB `string_t` vectors directly, preserving
selection vectors and validity masks. Whole scalar-function execution remains a
fallback and must not be reported as native prefix support.

`substring(varchar, 1, N) IN (...)` is a separate core intrinsic and native
predicate contract, not generic substring support. The core IR records
`string_substring` with typed VARCHAR/integer children so backend lowering is
driven by IR facts. SLJIT may admit only the byte-safe constant-list predicate
shape where the start is constant `1`, the length is a non-negative constant,
every list entry is non-NULL ASCII, and every constant has exactly `N` bytes.
That admitted shape can compare DuckDB `string_t` bytes directly without
materializing a substring or depending on Unicode character counting. Other
substring projections, Unicode-sensitive predicates, NULL-list `IN`, and
non-constant bounds remain honest unsupported/fallback cases until the IR grows
a native string-slice value protocol.

Workload traces must include a stage-by-pipeline summary. `stage_pipeline_summary`
aggregates JIT stage costs by policy, status, execution mode, candidate shape,
and deterministic pipeline shape, including IR lowering, backend analysis,
admission, overlap checking, codegen, dominant stage, query examples, and miss
reason. It excludes trace-wrapper pipelines such as `EXPLAIN_ANALYZE` and
`RESULT_COLLECTOR` so compile/planning cost investigation stays tied to
workload-relevant region shapes, using the same shape identity as runtime and
gap attribution.

Workload traces must include a pipeline-runtime summary.
`pipeline_runtime_summary` joins the stage-by-pipeline view with retained
kernel lifecycle counters and profiler attribution by the same executable
pipeline shape, context pipeline shape, and candidate scope. This file is the production
step-by-step view: for each candidate pipeline it shows decision count, stage
cost, compiled/reached/row-processing/unreached kernel counts, runtime rows,
runtime time, source-helper rows/time, profile time, profile operators,
capability gaps, and query examples. It must not treat source context as executable native code; compiled
rows use executable shape attribution, while unsupported rows may use context
shape to explain scan, join, aggregate, sort, materialization, and sink
boundaries.

Workload traces must include an admission-efficiency summary.
`admission_efficiency_summary` joins compiled-region admission metadata with
kernel runtime ownership by the same query, policy, execution mode, region form,
candidate shape, executable pipeline shape, context pipeline shape, and
candidate scope. It classifies each compiled runtime row as helper-dominated,
native-source-dominant, generated-body-dominant, mixed, not reached, or
unmeasured. This is the production-safe answer to "did the admitted JIT region
actually own the hot runtime?" A helper-dominated auto row is recorded as
`auto_admitted_helper_dominated_region`; it is evidence that the current region
shape needs native operator/source ownership or should not be used as speedup
proof. The verifier must require the file and validate it against
`kernel_runtime_summary`, but it must not require helper-dominated auto regions
to keep existing after the architecture is fixed.

The trace directory must be machine-verifiable. `verify_tpch_trace.py` is the
contract gate for TPC-H trace artifacts: it checks exact q01-q22 query identity
and policy coverage, correctness diffs, zero-code native events,
non-region kernel leakage, event CSV cardinality,
query/operator/capability summaries, capability-priority summaries,
admission-efficiency summaries,
source-boundary summaries, and expression-fallback summaries,
aggregate integrity, region/stage/kernel aggregate integrity, pipeline-shape
attribution, runtime-kernel evidence, profiler JSON presence, operator-profile
aggregate integrity, and IR presence when `jit_dump_ir=true`.
Manual inspection of `report.md` is not a substitute for this verifier.

Every trace directory must also contain `trace_manifest.json`. The manifest is
the run-owned artifact contract: schema version, trace kind, generator,
configuration, database ownership mode, artifact list, CSV columns, row counts,
byte sizes, and content hashes. Verifiers must reject traces with a missing
manifest, a mismatched schema version or trace kind, a summary-owned artifact not
listed in the manifest, or artifact metadata that no longer matches the files on
disk. This prevents production trace ingestion from accidentally mixing leftover
files with current query evidence.

Trace output-directory ownership is strict. A trace run must start with a fresh
or empty output directory, and verifiers must reject any file in that directory
that is not listed in `trace_manifest.json`. The harnesses never silently delete
or overwrite an arbitrary directory to make this true; callers choose a new
`--out-dir` or explicitly clean the existing output directory before measurement. That keeps leftover
query/policy artifacts out of production trace ingestion without hiding a
destructive cleanup step inside the benchmark script.

Focused SQL coverage has a separate trace contract. `benchmark/jit/jit_sql_trace.py`
runs representative existing JIT SQL flows for native scalar IR inside regions,
unsupported scalar IR inside regions, native filter/projection regions,
unsupported join fallback, in-process operator resume fallback, and a broader
SQL equivalence matrix covering integer arithmetic/comparison, casts, null
handling, coalesce, constant-or-null, complex scalar fallback types, and
temporal/interval fallback IR.
`verify_jit_sql_trace.py` checks validation results, expected compiled and
unsupported surfaces, runtime counters, nonzero native code, pipeline-shape
attribution for region events, generated IR, monotonic event IDs, required typed
native operation names, complex-type fallback IR names, compiled region shapes,
and that resumed in-process operator work is recorded as executor fallback
rather than repeated native input. This keeps the existing test surface
traceable without conflating it with TPC-H performance evidence.

Focused SQL traces must also include `flow_step_summary.csv`. This file groups
each focused case by target, phase, status, execution mode, policy decision,
candidate shape, and candidate scope, then reconciles event counts, stage
timing, runtime rows, kernel reachability, declined invocations, and fallback
counters against the raw event CSVs and `duckdb_jit_kernel_counters()` exports.
It is the existing-test equivalent of the TPC-H stage/runtime summaries: every
test-facing JIT flow must answer what happened at each step without requiring a
manual join across event and counter files.

The focused trace also emits `test_surface_coverage.csv`. This file inventories
the checked-in JIT sqllogictest files and every `TEST_CASE` in
`test/api/test_jit.cpp`, then maps each surface to its verification route and,
when applicable, the focused trace case that exercises the same flow. The
verifier compares this CSV against the current source tree, so adding a JIT test
without updating trace coverage is a contract failure rather than invisible
drift.

Source boundaries have their own verification gate. `benchmark/jit/verify_jit_architecture.py`
checks database-owned manager wiring, static backend integration, core source
registration, backend ABI contract tests, manifest-backed trace-contract
presence, forbidden labels, and reverse dependencies between core JIT,
backend code, and DuckDB executor internals. This keeps architectural drift
testable instead of relying on code review memory.

The result must answer, in order: what was lowered, what the backend claimed it
could compile, why policy admitted or skipped it, whether code was generated,
which kernel ID was created, whether that kernel ran, how many rows it processed,
and how much kernel time it consumed. If any answer is missing, the performance
investigation is incomplete.

JIT runtime tracing is not a replacement for DuckDB's normal operator profiler.
It explains the generated region. When a generated kernel declines an invocation
or policy skips a supported candidate, the normal DuckDB executor owns the work;
that executor work must be analyzed through DuckDB's profiler or benchmark
runner, while the JIT trace records the admission/decline fact and
`operator_profile_summary.csv` records the remaining physical-operator weight.
When a compiled kernel declines and the executor fallback immediately performs
that invocation, the fallback runtime event must use
`execution_mode=executor_fallback` and stay separate from generated
native/helper kernel totals. Reporting fallback executor time as native JIT time
is forbidden.

`jit_policy=force` is a diagnostic admission-bypass mode for supported
non-overlapping executable regions. It may compile supported region candidates
that `auto` would skip so backend coverage remains testable, but overlapping
supported candidates are recorded as skips unless a future compile-only
diagnostic mode exists. Force compile events must not be used as performance
evidence unless runtime rows show the kernel actually executed.
`jit_policy=auto` is the performance policy and should admit only
non-overlapping shapes with measured proof. A production auto admission is valid
only when repeated benchmark timing shows the auto-compiled query wins against
the corresponding `off` median.

Performance proof means benchmark evidence for the specific kernel shape and
execution policy. Body-only SLJIT native filter/projection and projection-chain
microbenchmarks are diagnostics for the generated transform body, but they are
not proof for a whole source-prefix region while the source node is still a
DuckDB `GetData` helper. `jit_policy=auto` can use those shapes only when the
lowering plan reports a genuinely fused region; current helper-backed
source-prefix instances must stay non-fused. Source-prefix-only table-scan filter/projection regions, scan-pushed
filters, projection-only regions, filter-only regions, and generic unfused
multi-op regions must stay skipped by `jit_policy=auto` until separately
measured and admitted. Backend lowering still assigns deterministic shape keys
such as `sljit:source-prefix:filter-projection`,
`sljit:source-prefix:filter-projection-projection:table-scan-source`, and
`sljit:full-pipeline:filter-projection-sink:table-scan-source+ungrouped-aggregate-update`
so skipped regions remain attributable. Unsupported boundary candidates use
operator-aware full-pipeline keys; those keys are not admission proofs until a
benchmark and backend implementation exist.

Microbenchmark admission proofs have two evidence layers. The benchmark-runner
files remain the speed measurement surface, and
`benchmark/micro/jit/micro_jit_benchmark.py` turns those benchmark-runner
measurements into manifest-backed `runs.csv` and `summary.csv` artifacts. Its
verifier checks repeated-run coverage and requires `auto`/`force` median times
to beat `off` by a real admission margin for each admitted threshold shape.
`benchmark/micro/jit/micro_jit_diagnostic_benchmark.py` is the matching
rejection surface for diagnostic shapes: projection-only, filter-only, generic
filter/projection, and full-pipeline decimal projection/ungrouped-SUM threshold
shapes must stay below the admission margin or be promoted with a real proof.
The full-pipeline projection/ungrouped-SUM diagnostic is intentionally stricter
than a TPC-H-only force-mode observation: it verifies a real native fused
full-pipeline kernel, but repeated benchmark-runner timing currently shows
force slower than `off`, so the shape must remain absent from auto admission.
`benchmark/micro/jit/micro_jit_full_pipeline_selectivity_sweep.py` varies the
surviving row percentage for that same native full-pipeline shape. Its current
evidence shows force remains slower from 1% through 75% surviving rows, so the
q06-style force signal is not explained by simple source-filter selectivity and
must not become a shape-only admission rule.
`benchmark/micro/jit/micro_jit_diagnostic_sweep.py` then maps source-prefix
shapes across cardinalities and writes a `family_summary.csv` with the first
tested row count that reaches the admission margin. That sweep is diagnostic
guidance; promotion still requires an admitted benchmark family and backend
rule.
`benchmark/micro/jit/micro_jit_trace.py` is the trace surface for admitted
shapes: it records compile decisions, admission proof strings, generated code
size, IR, runtime kernel rows, and counters for `off`, `auto`, and `force`.
Trace timings are diagnostic because runtime tracing and `EXPLAIN ANALYZE` add
overhead; they must not replace benchmark-runner speed measurements.
The inventory verifier (`benchmark/micro/jit/verify_micro_jit_inventory.py`)
binds those evidence layers back to backend policy: admitted micro benchmark
file names, operator-region benchmark proof strings, SLJIT shape constants,
thresholds, and harness shape keys must agree; diagnostic families must not grow
`auto` files without a backend admission rule and measured proof.

Benchmarks must verify the physical shape they claim to measure. For example,
the filter/projection benchmark uses a `range()` view rather than a materialized
table because a simple table predicate can be pushed into `TABLE_SCAN`, which
would no longer benchmark a generated filter node.

Compiled results must report the compiled execution mode:

- `native`: generated code owns the loop/control flow and performs the operation itself, optionally calling primitive
  runtime stubs that do not own operator protocol;
- unsupported/error/unavailable/disabled for non-compiled paths.

`executor_fallback` is not a valid compiled result. Whole `ExpressionExecutor` or whole `PipelineExecutor` execution
must be reported as fallback or unsupported, never native.

Compiled region results must also report the fused region execution form:

- `fused`: backend-owned generated code executes the admitted operator interval
  without intermediate DuckDB operator/materialization boundaries, typed
  operator/sink helper boundaries, or missing native protocols;
`none` is never valid for a compiled region. It is reserved for region rows that
did not produce executable code.

The initial SLJIT backend currently exposes only real native compiled paths.
It must not use a generated trampoline into C++ vector-helper execution as
compiled success; unsupported helper-shaped IR is reported as unsupported until a
backend lowering exists where generated code owns the loop/dispatch.

Executable kernels carry their own trace identity after compilation: kernel ID,
compiled execution mode, compile reason, compile time, and code size. Runtime
events use that identity to recreate a bounded per-kernel counter row if retained
detail was cleared or evicted while the kernel is still alive. This keeps trace
attribution tied to the executable kernel instead of relying on a retained event
row as the only source of compile identity.

Core module ownership follows the same trace boundary:

- `src/execution/jit_types.cpp` owns public enum/string helpers, shared
  lowering-plan summaries, and copy semantics for the core IR data structures;
- `src/execution/jit_expression_ir.cpp` owns DuckDB-facing scalar expression
  lowering from planner expressions into backend-neutral typed expression IR;
- `src/execution/jit_pipeline_descriptor.cpp` owns DuckDB-facing physical
  pipeline descriptor construction. It is the single place region preparation
  may walk `Pipeline`, call `GetJitOperatorDescriptor()`, slice compiled
  contracts by role, and derive native-role flags;
- `src/execution/jit_region_ir.cpp` owns descriptor-to-region-IR lowering. It
  consumes expression IR through the core-only `TryLowerJitExpression`
  boundary;
- `src/execution/jit_operator_descriptor.cpp` owns the adapter from selected
  DuckDB physical operators into backend-neutral source/sink protocol
  descriptors. Stateful operator details for table scans, hash joins, and
  aggregate source/sink phases must terminate there instead of leaking into
  region lowering or backend code;
- `src/execution/jit.cpp` owns manager policy, backend selection, admission,
  compile events, runtime event recording, and introspection-pipeline
  suppression;
- `src/execution/jit_region_executor.cpp` owns fused-region JIT execution hooks,
  runtime trace recording, verification against reference region execution, and
  executor-fallback orchestration after a compiled region declines or when
  DuckDB has pending in-process operator state to resume;
- `src/execution/jit_runtime.cpp` owns backend-independent runtime contracts:
  capability results, bounded events, cumulative counters, kernel trace
  identity, compile-result validation, and default backend behavior;
- `extension/jit_sljit/` consumes only the public JIT IR/runtime contracts and
  must not depend on DuckDB physical operator, planner, or executor internals.

`ExpressionExecutor` remains the owner of expression state, bound expression
references, input chunk binding, and the reference interpreter/vectorized path.
It does not call a JIT backend directly. Scalar expression trees are lowered by
core JIT only when they belong to a region candidate, and the lowered IR is
consumed by region analysis.

`PipelineExecutor` follows the same rule. It owns DuckDB operator state, source
and sink protocol, intermediate chunks, and the normal physical-operator path.
It calls `JitRegionExecutor` as a core JIT boundary. The JIT boundary may use
`PipelineExecutor` to verify semantics or execute an explicit fallback, but
runtime trace accounting, JIT decline handling, and JIT verification harnesses
belong in the core JIT module rather than the plain pipeline executor.
`PipelineExecutor`'s `in_process_operators` stack is part of the DuckDB operator
protocol; region JIT must observe it before entering generated code and must not
turn a resume into another prefix-region invocation.

Production `PipelineExecutor` integration must request region kernels only.
There is no standalone `ExpressionExecutor` JIT hook. This prevents forced-JIT
performance traces from mixing a region-first architecture with isolated scalar
kernels.

The public header surface mirrors the same dependency direction:

- `duckdb/execution/jit/common.hpp` exposes shared enums and deterministic
  string helpers;
- `duckdb/execution/jit/ir.hpp` exposes backend-neutral expression IR only. It
  must not expose DuckDB planner `Expression` lowering;
- `duckdb/execution/jit/region.hpp` exposes backend-neutral region IR,
  explicit `JitRegionCandidate` identity, and region boundary contracts only.
  It must not expose DuckDB `Pipeline` lowering;
- `duckdb/execution/jit/operator_descriptor.hpp` exposes the physical-operator
  descriptor contract used to publish per-operator compiled facts. It is not a
  backend header;
- `duckdb/execution/jit/pipeline_descriptor.hpp` exposes the core pipeline
  descriptor that snapshots physical operator facts before admission or region
  IR lowering. Backends must not include it;
- `duckdb/execution/jit/lowering.hpp` exposes the core-only lowering boundary
  from DuckDB planner/executor objects into backend-neutral JIT IR. Backends
  must not include it;
- `duckdb/execution/jit/runtime.hpp` exposes backend/runtime contracts:
  capabilities, executable kernels, compile results, backend plans, events, and
  counters;
- `duckdb/execution/jit/registration.hpp` exposes the narrow backend
  registration facade for in-tree/static extensions. Backend extensions should
  include this instead of `manager.hpp`;
- `duckdb/execution/jit/manager.hpp` exposes the database-owned manager and is
  the only layer that should know about backend registration, settings-driven
  selection, admission, and event retention;
- the removed `duckdb/execution/jit.hpp` umbrella must stay removed. New includes
  should use the narrow header matching the layer they consume.

Concrete DuckDB physical operators expose JIT protocol facts through
`GetJitOperatorDescriptor()`. `BuildJitPipelineDescriptor` is the single pipeline-level discovery point. It snapshots those descriptors and prepares role-sliced contracts before admission inventory or region lowering runs. Region lowering consumes the pipeline descriptor and builds backend-neutral `JitRegionSourceInfo` and
`JitRegionSinkInfo` records. Concrete operator knowledge must terminate at the descriptor adapter before the pipeline descriptor snapshots it.
Backend implementations must consume only backend-neutral JIT IR, candidate
metadata, helper-call nodes, fallback nodes, and deterministic reason/IR text.
Source-boundary diagnostics such as projection pushdown, filter counts, dynamic
filters, returned columns, and table function names therefore belong in
operator-owned descriptors and core-lowered source boundary reason text, not in
backend-visible DuckDB operator objects.

This split is intentional. Performance work must first identify the stage where
the trace changes: core lowering, backend capability, admission, codegen, or
runtime invocation. A timing difference without that stage attribution is not a
root cause.

The current v1 runtime-safe candidate set is deterministic and intentionally
limited:

- candidate 0 spans the full pipeline invocation boundary; if that boundary
  starts at a source and reaches a sink, it is owned by `full_pipeline`, not by
  the source-prefix or sink-suffix ABI;
- candidate 1 exists only when the prefix before the first hard boundary is a
  maximal native source/filter/projection span and contains real generated work.
  It reports `source_pipeline` and resumes the reference executor at the exact
  next operator boundary. It never includes stateful native operator protocols
  such as hash join probe; those require the full-pipeline runtime ABI;
- the planner does not emit interior intervals, post-source transforms,
  sink-only suffixes, or core-suffix-plus-sink candidates. Those shapes need a
  real entry ABI and state-resume protocol before they can become production
  planner products;
- full-lowered sink candidates include deterministic `sink<...>` protocol
  inventory so aggregate/update, hash-join build, sort, and materialization
  gaps are actionable rather than opaque sink labels;
- unsupported full-pipeline candidates keep aggregate, join, materialization,
  sort, and sink work visible to admission and capability analysis without
  relabeling DuckDB execution as native/generated JIT.

This is not the final interval selector. The current manager installs a
deterministic non-overlapping region set per pipeline executor. General
operator-aware region selection must continue to extend the manager/executor
contract through core-owned region descriptors, not ask a backend to inspect
executor state. The core runtime already carries start/end operator indexes,
input types, and output types so generated code can hand control back to DuckDB
at the selected end boundary without backend access to executor internals.

The v1 interval builder may attach only to scan-like source boundaries, sink
boundaries, and simple filter/projection executor boundaries. A pipeline source
that is really a stateful operator source, such as a join or aggregate source,
is not a scan boundary. JIT must not start or stop at a generic operator
fallback such as a join, aggregate, window, sort, or other stateful operator
until that operator exposes an explicit region boundary contract. Without that
contract the core cannot prove `HAVE_MORE_OUTPUT`, blocking, state update, or
cached-output semantics across the generated/reference boundary.
Sink suffix candidates are not executable planner products in the current
architecture. Hash-join build/probe and generic hash aggregate lookup/update use
backend-neutral native build/probe/lookup/update contracts for supported shapes;
unsupported join types, layouts, aggregate states, or aggregate functions are
missing-protocol, not typed-helper success states. Ungrouped aggregate and
perfect-hash aggregate have explicit native update paths for supported shapes.
Other sinks remain visible through full-pipeline analysis and runtime
diagnostics until a native protocol exists, without relabeling DuckDB sink
execution as native/generated JIT.

General middle-of-pipeline regions that both start after operator 0 and resume a
DuckDB suffix require the same explicit boundary contract. Until then they are
not valid v1 candidates, even when every node inside the interval is natively
lowerable.

Split regions that start after a physical operator also require an upstream
operator resume protocol. Hash join probe, aggregate source scan, materialized
scan, sort/top-n source, and other resumable/stateful operators can leave
`PipelineExecutor::in_process_operators` populated. A post-source transform or
sink-suffix kernel behind such an operator is not a valid v1 candidate until
that operator exposes a backend-neutral resume contract. Core candidate
formation does not emit these split regions; backend
`operator-fusion-gap:upstream-operator-resume-protocol-missing` and
`sink-fusion-gap:upstream-operator-resume-protocol-missing` checks are defensive
only. Compiling the suffix and declining at runtime is not a native-fused
success state.

## Pipeline Model

Pipeline lowering is an input to region formation, not the final long-term JIT
target:

- `native`: represented by generated code;
- `helper-call`: represented by a typed helper boundary that is not native-fused;
- `fallback`: executed by the normal DuckDB path or outside the generated region.

Scans, sinks, joins, aggregates, windows, and other operators are fallback or
missing-protocol until a typed JIT node exists for the exact operator phase and
state protocol. Hash-join build/probe and generic hash aggregate lookup/update
are examples of native operator protocol nodes for supported shapes; unsupported
variants must stay missing-protocol rather than falling back to a whole DuckDB
operator helper. Reporting a normal DuckDB physical operator as `helper-call`
without such a node is not allowed.

The initial native scope is filter/projection regions. That path must be moved
behind first-class region IR before new native region families are added. The
production pipeline JIT path must stay behind the region executor, not kept as a parallel
implementation. Fused regions are valid only when the generated kernel preserves
DuckDB operator protocol and `jit_verify=true` can compare it against the
reference path.

Generated filter loops must specialize vector-format checks outside the row
loop when runtime input metadata proves the fast path. The common all-valid,
no-selection path should branch once and then run a tight generated loop; the
generic generated loop remains responsible for selected/dictionary/nullable
vectors. Auto admission must follow measured kernel shape and expected work,
not broad backend availability.

Backend-specific compilers must keep one capability classifier per IR node and
reuse it for event analysis and native lowering. A
backend may build a richer private plan from core IR, but analysis and compile
must agree with that same plan. That avoids separate support matrices drifting
apart.

The SLJIT backend follows the same internal layering:

- `sljit_backend.cpp` owns backend registration and admission proof metadata;
- `sljit_region.hpp` exposes the top-level SLJIT region entrypoints used by
  `sljit_backend.cpp`;
- `sljit_region_plan.hpp/cpp` owns SLJIT region capability analysis, semantic
  region plan types, deterministic region support reasons, native region shape
  descriptions, and private backend region plans from core IR;
- region analysis uses `SljitNativeRegion*Plan` semantic types. These plan
  types must not own executable code handles, generated function pointers, or
  other mutable codegen/runtime state;
- `sljit_region_executable.hpp/cpp` owns executable region state assembly from
  semantic region plans. This is the only layer allowed to attach
  `JitCodeHandle` objects and generated function pointers to native regions;
- `sljit_region_runtime.hpp/cpp` owns region runtime kernel classes. It can touch
  DuckDB vectors, prepared executable state, and generated function pointers,
  but must not perform backend capability analysis or raw codegen;
- `sljit_region.cpp` owns region compile entrypoints. It consumes semantic
  plans, executable region state, and fused region codegen results, but must not
  contain runtime vector execution loops;
- `sljit_native_types.hpp` owns the SLJIT native ABI structs, operation enums,
  and generated function pointer types. It is the shared contract between
  analyzed plans, kernel orchestration, and raw codegen;
- `sljit_native_util.hpp/cpp` owns native operation names and exception message
  text shared by analysis, executable assembly, and runtime-facing kernels, as
  well as the backend-local `jit_dump_ir` gate used by compiled results;
- `sljit_native_plan.cpp` owns native scalar-expression and region reader
  helpers, deterministic support reasons, typed predicate construction, and
  native scalar plan selection from core IR;
- `sljit_native_plan.hpp` exposes the private plan and support classifiers. It
  must depend on core JIT IR/runtime contracts and `sljit_native_types.hpp`;
- `sljit_native_runtime.hpp/cpp` owns DuckDB vector-format runtime adapters
  consumed by scalar primitives inside region kernels, plus predicate runtime
  properties such as whether prepared predicate execution needs an input chunk;
- `sljit_codegen_util.hpp/cpp` owns shared SLJIT executable-code RAII and
  opcode/jump mapping helpers used by backend code generators;
- `sljit_native_codegen.hpp/cpp` owns scalar-expression raw SLJIT emission used
  by region codegen and executable-code finalization;
- `sljit_region_codegen.hpp/cpp` owns fused region raw SLJIT emission and
  executable-code finalization;
- raw `sljit_emit_*` calls should not leak into backend analysis, runtime
  adapters, kernel orchestration, or DuckDB-facing core code;
- no broad SLJIT umbrella header should remain after the region migration. Each
  backend file should include the narrow header matching the layer it consumes.

Region execution reasons must report the actual lowered region shape
(`filter`, `projection`, `filter-projection`, and so on). A generic
filter/projection label is not acceptable for projection-only regions, because
observability is part of the backend honesty contract.

Backends may eliminate full identity projections after core lowering when the
projection preserves column count, order, references, and logical types. The
event reason must report the elision, and `jit_dump_ir=true` must still show the
original core region IR so the semantic boundary remains auditable.

## Admission Model

`JitManager` owns admission because it has the database-owned backend registry,
settings, counters, and access to physical operator estimates. Backends may
provide shape metadata and compile-cost hints, but they must not make global
policy decisions.

The admission decision should be deterministic for a given query plan and
settings. In `auto` mode, a candidate is compiled only when all of these are
true:

- the core IR is complete and semantic;
- the backend plan is supported and has a compiled execution mode;
- the shape is admitted by measured evidence or a conservative hard-coded v1 rule;
- the estimated work exceeds the compile-cost break-even threshold;
- verification/debug settings do not invalidate the performance expectation.

`force` bypasses the performance gate but not correctness gates. `off` bypasses
all JIT lowering and compile work.

The v1 admission table should be deliberately small:

- admit SLJIT filter/projection and composed projection-chain regions only when
  lowering proves the selected region is genuinely fused and the documented
  estimated-row threshold is met;
- admit native table-scan source-prefix filter/projection regions only when the
  backend admission key includes a measured downstream operator context, not
  from the local source-prefix shape alone;
- keep non-fused helper regions visible as operator-aware candidates, but skip
  them in `auto` until a measured operator-aware admission proof exists for that
  exact shape family;
- skip filter-only/projection-only regions in `auto` until separately measured;
- skip unknown-cardinality candidates unless the shape has a cache hit.

The design avoids special-case patches by making "not enough work to amortize
compile cost" a normal admission outcome.

Admission score is a core value, not backend prose:

```text
score =
  estimated_saved_executor_cost
  - estimated_generated_runtime_cost
  - estimated_boundary_conversion_cost
  - amortized_compile_cost
  - safety_margin
```

`auto` admits only candidates with measured shape proof and a score that meets
the shape's break-even rule. It then selects the non-overlapping admitted set
with the highest total score before backend code generation. `force` may ignore
profitability score for diagnostics, but overlap selection still applies and the
score still appears in events. The current v1 event score is the deterministic
admission-rule margin
`candidate_estimated_cardinality - admission_min_cardinality` for shapes with a
measured rule. It is intentionally SQL-visible so the later full cost model can
replace the scalar without changing the trace contract.

## Traceability

Every JIT candidate should have a trace record with stable stage boundaries:

1. physical pipeline and scalar/region summary;
2. core JIT IR;
3. backend capability plan;
4. admission decision;
5. compile result;
6. runtime execution mode.

The trace is database-owned and query-scoped. It must never require a backend to
expose raw internal DuckDB physical objects. Backend-private details may appear
only as deterministic text derived from the backend plan.

`duckdb_jit_events()` remains the compact recent-event view. The event schema
must continue moving toward a complete stage trace with these fields:

- `event_id` / `trace_id`;
- `phase` (`decision`, `compile`, `runtime`);
- target (`region`);
- physical shape;
- normalized shape key;
- candidate ID and selected-region ID;
- candidate start operator index and exclusive end operator index;
- candidate executable pipeline shape, derived from core region IR nodes rather
  than backend or physical-operator pointers;
- candidate context pipeline shape, retaining the full surrounding DuckDB
  pipeline for unsupported-boundary attribution;
- candidate contract ABI, exposed as structured data and used for scope/entry
  validation instead of parsing pipeline text;
- region node count;
- boundary crossings removed;
- backend name;
- core IR text when `jit_dump_ir=true`;
- backend plan summary;
- estimated rows/work;
- admission score;
- admission decision and reason;
- compile status, decision time, compile time, code size;
- stage timing: core IR lowering, backend analysis, admission, overlap
  selection, and backend codegen;
- `kernel_id`;
- runtime execution mode;
- region execution form;
- runtime input rows, output rows, invocation count, elapsed kernel time, and
  operator/runtime result.

This makes "why did JIT compile or not compile?" answerable without benchmarks,
logs, or source-code guessing.
`decision_time_us` measures core lowering, backend capability analysis, and
admission/selection time. `compile_time_us` measures backend code generation
only, and must remain zero for skipped, unsupported, disabled, and unavailable
events.

Stage timing columns decompose the coarse timing fields without changing their
meaning:

- `ir_lowering_time_us`: DuckDB physical/expression tree to core JIT IR;
- `backend_analysis_time_us`: backend capability planning over that IR;
- `admission_time_us`: policy/cost decision;
- `overlap_check_time_us`: non-overlapping region selection and conflict
  checks;
- `codegen_time_us`: backend machine-code generation and finalization.

These stage columns must appear in both `duckdb_jit_events()` and
`duckdb_jit_counters()`. Candidate/admission-stage variants must also appear in
`duckdb_jit_decision_counters()`, aggregated by candidate shape, candidate scope,
admission shape key, admission rule, admission threshold, and admission proof.
They are the minimum architecture-level trace needed to separate "cannot lower",
"backend cannot compile", "policy skipped", and "codegen/runtime is expensive"
without guessing from wall-clock timings.

Node-level timing inside a fused generated loop is a separate diagnostic mode,
not baseline JIT observability. Baseline observability times the generated
kernel boundary and relies on deterministic IR plus backend plan text to show
which filter/projection/helper/fallback nodes are inside that boundary.
Per-node timers inside generated loops would perturb the fast path and must only
be added behind an explicit diagnostic setting when kernel-level evidence has
already isolated the problem to code generated inside that fused region.

## Observability

Events and counters are part of the architecture:

- `duckdb_jit_backends()` shows registered, available, selected backends;
- `duckdb_jit_clear_events()` clears retained event and per-kernel detail for a
  clean measurement window without clearing cumulative counters;
- `duckdb_jit_clear_counters()` explicitly clears coarse cumulative counters and
  decision counters for benchmark/diagnostic measurement windows. This is not
  called by normal query execution and is intentionally separate from event
  clearing;
- `duckdb_jit_events()` shows bounded recent compile/admission events;
- `duckdb_jit_counters()` shows cumulative counts by backend, target, status,
  execution mode, region execution form, and policy, including cumulative
  decision and compile time;
- `duckdb_jit_decision_counters()` shows cumulative candidate/admission counts
  by backend, target, phase, status, execution mode, region execution form,
  policy, candidate shape, candidate scope, candidate pipeline shape,
  candidate context pipeline shape, estimated cardinality, admission shape key,
  admission threshold, proof, and example reason. Below-threshold rows carry
  the measured admission rule and negative admission score. This is the
  production-safe answer to "why did auto skip or admit this region?"
  when `jit_trace_decisions=true` and `jit_event_log_size=0` intentionally
  disables retained events. Production `auto` does not durably record
  missing-proof inventory skips unless `jit_trace_decisions`, `jit_dump_ir`, or
  `jit_trace_runtime` asks for diagnostics; below-threshold rows for admitted
  rules remain durable policy evidence. The nullable `pipeline_shape`,
  `pipeline_estimated_cardinality`, and `example_reason` columns remain
  introspection fields, but candidate-level shape, context, and cardinality are
  the auto-admission source of truth. A retained estimate of `0` is an
  honest unknown estimate, not permission to invent cardinality;
- `duckdb_jit_kernel_counters()` shows runtime totals per retained compiled
  kernel ID, including the compile reason, code size, candidate ID, candidate
  shape, candidate executable pipeline shape, candidate context pipeline shape,
  candidate operator interval, candidate estimated cardinality, and region
  execution form, so long-running runtime traces remain attributable after
  recent events are evicted. Generated-kernel rows/time, declined invocation
  attempts, and executor-fallback rows/time are separate columns; they must not
  be summed into a single native-runtime number.

`jit_event_log_size` bounds retained events and retained per-kernel rows. A
value of `0` disables those retained detail views but keeps aggregate counters.
Event IDs are monotonic.

Clearing the retained event trace also clears retained per-kernel rows. It must
not clear aggregate counters; those remain cumulative so bounded trace retention
does not hide total compile/runtime activity.

`jit_dump_ir=true` exposes deterministic IR text through events. Console dumping is not the testable contract.

JIT system table functions are introspection/control surfaces, not candidate
workload fragments. Binding a `duckdb_jit_*` table function must not set query-global JIT suppression: trace scripts intentionally clear events, run a
workload, and read events in one SQL batch. Query-global suppression would make
pipeline preparation and executor-time compilation observe different JIT state.
Instead, binding sets the statement-local suppression property, prepared execution transfers that property onto the `Executor`, and every pipeline owned
by that executor skips JIT preparation. Pipeline preparation also detects
introspection pipelines directly and leaves their prepared JIT state empty,
while the table-function init path uses `JitSuppressionGuard` only while reading or mutating JIT metadata. This matters for wrappers such as
`COPY (SELECT * FROM duckdb_jit_decision_counters()) TO ...`: the exported
diagnostics pipeline must not create a new JIT event or counter row, but a later analytical statement in the same SQL batch must still be able to compile fused regions.

SET and RESET are control statements, not data-processing workload fragments.
Their binders set the same statement-level suppression property, so toggling JIT
settings cannot itself produce a JIT candidate, event, or counter row.

Admission skips must be visible as skips, not hidden as missing events and not
reported as unsupported. This is required for default-on JIT performance
debugging.

## Verification

`jit_verify=true` runs generated output against the reference DuckDB path for
compiled kernels. Verification failures are correctness bugs.

Required verification dimensions:

- flat, constant, dictionary, and selected vectors;
- NULL propagation and three-valued boolean logic;
- overflow and exception behavior;
- casts and temporal/string/decimal helper boundaries;
- filter/projection region protocol;
- event honesty.

Every new region family must pass these architecture gates before `auto`
admission is enabled:

- core IR is deterministic and printable with `jit_dump_ir=true`;
- backend consumes only core JIT IR/runtime contracts;
- no backend include depends on DuckDB planner, optimizer, physical operator, or
  executor internals;
- capability, admission, compile, and runtime events are separately observable;
- fallback and decline paths are not reported as native runtime;
- `jit_verify=true` compares the generated region against the reference path at
  region boundaries;
- correctness tests cover vector formats, selections, nullability, exceptions,
  and state protocol semantics used by the region;
- benchmarks prove the normalized shape under `auto`, and the benchmark asserts
  the physical shape it claims to measure;
- forced compilation may show coverage, but it is not performance evidence.

The verification order for performance claims is:

1. prove the physical plan contains the intended region shape;
2. prove core lowering produced the expected `JitRegionIR`;
3. prove backend capability accepted the whole region, not only its nodes;
4. prove `auto` admission selected a non-overlapping region;
5. prove a kernel was generated with an executable body and honest `code_size`;
6. prove runtime rows are attributed to that kernel ID;
7. compare against non-JIT with identical plan shape and settings.

## Long-Term Direction

The framework should grow by extending the DuckDB JIT IR and adding backend lowerings. SLJIT-specific pattern matching
must not become the framework. External loadable JIT backends should wait until the host-provided backend registration
ABI is intentionally stable.
