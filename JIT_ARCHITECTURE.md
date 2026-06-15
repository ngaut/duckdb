# DuckDB Native JIT Architecture

This branch treats JIT as part of DuckDB execution architecture, not as an
executor side channel. The JIT owns only regions whose source, operators, sink,
and state scan protocols are explicit in backend-neutral IR. Everything else is
unsupported and runs through the normal DuckDB executor without being described
as native JIT.

## Core Flow

```text
DuckDB physical pipeline
  -> eligibility and capability analysis
  -> maximal fused region planner
  -> backend-neutral region IR
  -> backend lowering
  -> executable full-pipeline or state-scan kernel
```

The IR is the only contract consumed by a backend. SLJIT must not inspect
physical operator internals directly, and core DuckDB must not depend on SLJIT
details.

## Region Model

The region planner emits only first-class execution units:

- `full_pipeline`: owns source entry, every fused operator stage, and the sink.
- `state_scan`: owns a state scan source produced by a prior native protocol.
- `none`: no executable region exists.

Partial source execution that resumes the DuckDB executor mid-pipeline is not a
valid JIT ABI. It creates ownership ambiguity, weak performance signals, and
duplicate runtime protocols. The architecture removes that shape entirely.

## Native Fusion Contract

`native_fusion_ready=true` means all of the following are true:

- no whole executor boundary exists inside the region;
- the source has a native protocol when the region owns the source;
- every fused operator stage has a native operator protocol or generated IR;
- the sink has a native protocol when the region owns the sink;
- no typed helper boundary hides a whole DuckDB operator;
- only primitive stubs are used for low-level hash, compare, allocation,
  exception, and value primitives.

A compiled native event is valid only when the kernel owns the operation it
reports. Executor fallback, whole-expression execution, whole-operator helpers,
or runtime resume paths must be recorded as unsupported or fallback, never as
native.

## Region IR

The core IR records typed operator facts rather than backend objects.

Scalar IR includes:

- physical type and logical type;
- validity and null propagation;
- vector, selection, and constant sources;
- references, constants, casts, comparisons, arithmetic, boolean logic, `CASE`,
  `IN`, `BETWEEN`, `COALESCE`, and `constant_or_null`;
- typed helper-call nodes for primitive runtime stubs;
- unsupported nodes with deterministic blocker text.

Pipeline IR includes:

- native source protocols;
- filter and projection stages;
- hash join build, probe, and state scan protocols;
- hash aggregate lookup, update, and state scan protocols;
- perfect hash and ungrouped aggregate update protocols;
- sort, top-n, materialization, CTE, and result collector protocols;
- sink boundaries and fallback boundaries.

The printable IR is deterministic. `jit_dump_ir=true` exposes the IR through
`duckdb_jit_events()` instead of writing to stdout.

## Native Source Protocol

Table scan and scan-like sources expose a backend-neutral protocol:

- `native_source_input_columns`
- `native_source_input_types`
- `native_source_output_projection_map`
- `native_source_filter_column_map`
- `native_source_requires_unfiltered_input`
- `native_source_filter_prune_required`
- `native_source_filter_takeover_supported`

When scan filters are pushed into the source, the generated full-pipeline kernel
must either own the source filter protocol or reject the region. The selected
event reason for a generated scan filter uses `generated native table scan
filters`.

## Runtime ABI

Full-pipeline kernels enter through:

- `JitRegionKernel::CanExecuteFullPipeline()`
- `JitRegionKernel::TryExecuteFullPipeline(JitFullPipelineRuntime &,
  JitFullPipelineResult &)`

The runtime owns a clean source-to-sink execution unit. It can fetch native
source chunks, push output through native sink protocols, flush caching
operators, finalize sinks, and report metrics. Returning `false` after entry is
an internal error because the kernel accepted ownership of the region.

State-scan kernels are separate kernels with a state scan source contract. They
do not masquerade as full pipelines.

## Backend Lowering

SLJIT receives only region IR and native protocol descriptors. It lowers:

- typed scalar expression IR;
- native filter and projection loops;
- native table scan filter takeover;
- native hash join build and probe protocols;
- native aggregate lookup and update protocols;
- native result collector append;
- native state scan sources where the protocol is available.

If a protocol is missing, SLJIT records a deterministic unsupported event. It
does not call whole DuckDB executors from native-labeled code.

## Admission

`force` may compile any region that is executable and honest.

`auto` requires measured proof for the whole fused region shape. A fast scalar
body is not proof for a full pipeline. Admission keys are shape keys such as
`sljit:full-pipeline:<shape>` plus context details where needed. Missing proof is
reported as a skipped decision with cumulative counters, not hidden by disabling
JIT.

## Observability

`JitManager` is owned by `DatabaseInstance`. Events are bounded by
`jit_event_log_size`, while cumulative counters remain monotonic.

Events and counters expose:

- backend, target, phase, status, and execution mode;
- candidate scope, ABI, shape, operator indexes, and estimated cardinality;
- ownership contract and native fusion readiness;
- selected source execution and region execution form;
- compile, codegen, runtime, native-source, generated-body, and fallback metrics;
- admission key, proof, min cardinality, and policy decision;
- deterministic IR when `jit_dump_ir=true`.

## Verification Rules

The architecture verifier enforces these invariants:

- no deleted partial-source ABI names remain in core, extension, tests, docs, or
  benchmark trace tooling;
- the region ABI enum contains only `NONE`, `FULL_PIPELINE`, and `STATE_SCAN`;
- the candidate scope enum contains only `FULL_PIPELINE`;
- the pipeline executor dispatches full-pipeline kernels only from a clean
  source-to-sink boundary;
- backends cannot compile a full-pipeline candidate without
  `CanExecuteFullPipeline()`;
- native events must be fused and boundary-free;
- native source layout names use `native_source_*`;
- auto admission requires measured whole-region proof.

Correctness gates are focused unit tests, SQLLogic JIT equivalence tests,
architecture verification, TPCH tracing, and full DuckDB test runs when the
change is ready for broad validation.
