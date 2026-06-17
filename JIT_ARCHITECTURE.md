# DuckDB Native Vectorized Compiled Regions

This branch treats compiled execution as part of DuckDB execution architecture,
not as an executor side channel. JIT is one backend family for running first-class
vectorized execution regions. A compiled backend owns only full pipelines whose
source, operators, and sink contracts are explicit in backend-neutral IR.
Stateful operator sources can expose typed source contracts inside a
full-pipeline region, but they are not a separate executable ABI. Everything else
is unsupported and runs through the normal DuckDB executor without being
described as native compiled execution.

## Core Flow

```text
DuckDB physical pipeline
  -> eligibility and capability analysis
  -> maximal fused region planner
  -> backend-neutral region IR
  -> backend lowering
  -> pipeline-owned compiled plan
  -> executable full-pipeline kernel
```

The IR is the only contract consumed by a backend. SLJIT must not inspect
physical operator internals directly, and core DuckDB must not depend on SLJIT
details.

`Pipeline` owns the compiled execution artifact as `ExecutionRegionPlan`.
Planning, admission, backend lowering, and codegen happen before source state
captures scan/source-contract decisions. Source state opens from the small
`ExecutionRegionOpenRequest`, not from the whole selected plan, so physical
sources do not inspect compiled-region selection, kernels, admission proof, or
backend state. `PipelineExecutor` does not compile or own kernels; it only runs
immutable kernels from the compiled plan at a clean source-to-sink boundary.
Backend scratch storage is per execution, not stored in the compiled kernel.

## Region Model

The region planner emits only first-class execution units:

- `full_pipeline`: owns source entry, every fused operator stage, and the sink.
- `none`: no executable region exists.

There is no executable sub-pipeline ABI. A compiled kernel owns the whole
source-to-sink unit. Source-only stateful operators may expose state-source
contracts for a surrounding full pipeline, but those candidates remain `none`
until they are fused with a sink.

## Native Fusion Contract

The core contract admits a fused native region only when all of the following are true:

- no source boundary or missing contract exists inside the region;
- the source has a native contract when the region owns the source;
- every fused operator stage has a native operator contract or generated IR;
- the sink has a native contract when the region owns the sink;
- no delegated boundary hides a whole DuckDB operator;
- only primitive stubs are used for low-level hash, compare, allocation,
  exception, and value primitives.

A compiled native event is valid only when the kernel owns the operation it
reports. Source boundaries, missing native contracts, whole-expression
execution, whole-operator delegates, or runtime continuation through the DuckDB
executor must be recorded as unsupported or boundary, never as native.

Generated/native sink success uses one ownership marker:
`full-pipeline-native-sink`.

## Region IR

The core IR records typed operator facts rather than backend objects.

Scalar IR includes:

- physical type and logical type;
- validity and null propagation;
- vector, selection, and constant sources;
- references, constants, casts, comparisons, arithmetic, boolean logic, `CASE`,
  `IN`, `BETWEEN`, `COALESCE`, and `constant_or_null`;
- primitive runtime stubs with typed ABI descriptors;
- unsupported nodes with deterministic blocker text.

Pipeline IR includes:

- source contracts;
- filter and projection stages;
- hash join build, probe, and state-source contracts;
- hash aggregate lookup, update, and state-source contracts;
- perfect hash and ungrouped aggregate update contracts;
- sort, top-n, materialization, and CTE boundary/state-source descriptors;
- native operator sink contracts for result collectors, sort, and top-n;
- source, sink, and missing-contract boundaries.

The printable IR is deterministic. `jit_dump_ir=true` exposes the IR through
`duckdb_jit_events()` instead of writing to stdout.

## Source Contract

Table scan and scan-like sources expose a backend-neutral contract:

- `source_contract_input_columns`
- `source_contract_input_types`
- `source_contract_output_projection_map`
- `source_contract_filter_column_map`
- `source_contract_requires_unfiltered_input`
- `source_contract_filter_prune_required`
- `source_contract_filter_takeover_supported`

When scan filters are pushed into the source, the generated full-pipeline kernel
must either own the source filter contract or reject the region. The selected
event reason for a generated scan filter uses `generated native table scan
filters`.

## Runtime ABI

Full-pipeline kernels enter through:

- `ExecutionRegionKernel::CanExecuteFullPipeline()`
- `ExecutionRegionKernel::TryExecuteFullPipeline(ExecutionRegionRuntime &,
  ExecutionRegionResult &)`

The runtime owns a clean source-to-sink execution unit. It can fetch native
source chunks, push output through native sink contracts, flush caching
operators, finalize sinks, and report metrics. Returning `false` after entry is
an internal error because the kernel accepted ownership of the region.

Native operator binding is not boolean. A bind returns `READY`, `DEFERRED`, or
`INVALID`. `DEFERRED` is a scheduling/runtime dependency state, for example a
hash join probe whose build table is not finalized yet or whose pointer-table
layout is not available to native code; it records a skipped runtime event and
returns the compiled runner's explicit `VECTORIZED_DEFERRED` dispatch status.
`INVALID` means the compiled contract and runtime state disagree on shape or
semantics, and remains a hard error.

Compiled runner selection is an invariant. `Pipeline` selects
`CompiledVectorizedRunner` only when the immutable `ExecutionRegionPlan` contains
an executable full-pipeline kernel. If that runner observes a missing source,
sink, plan, or kernel, it is an internal architecture error, not a quiet fallback.
The runner receives only the executable kernel through
`ExecutionRegionPipelineAdapter`, not the whole selected plan.
The compiled runner never calls the vectorized executor directly. It returns
`EXECUTED` or one of the named vectorized dispatch statuses:
`VECTORIZED_SUPPRESSED`, `VECTORIZED_CONTINUATION`, and `VECTORIZED_DEFERRED`.
`ExecuteExecutionRunner` is the runner-layer coordinator that maps those
statuses to the vectorized runner.

Executable full-pipeline regions can be owned in two honest modes. `native`
means the backend emitted generated machine code for at least one region body and
must report `code_size > 0`. `native_contract` means the compiled runner owns a
source/operator/sink contract loop, but this region has no generated machine-code
body of its own; `code_size` may be zero and must not be inflated.

Zero-code `native` compile events are always verification failures.

State-source contracts are source contracts for full-pipeline kernels. They do
not create a separate executable kernel ABI and must not masquerade as native
execution on their own.

## Backend Lowering

SLJIT receives only region IR and native contract descriptors. It lowers:

- typed scalar expression IR;
- native filter and projection loops;
- native table scan filter takeover;
- native hash join build and probe contracts;
- native aggregate lookup and update contracts;
- native operator sink contracts;
- state-source contracts where the surrounding full pipeline can own the
  source-to-sink loop.

If a contract is missing, SLJIT records a deterministic unsupported event. It
does not call whole DuckDB executors from native-labeled code.

## Admission

`force` may compile any region that is executable and honest.

`auto` requires measured proof for the whole fused region shape. A fast scalar
body is not proof for a full pipeline. Core execution-region code owns admission
identity and matching: shape keys, context keys, contract-shape fingerprints,
lowered-shape checks, prospect checks, and cardinality thresholds. Backends
provide measured rule data and executable lowering, not private copies of the
matching algorithm.

Admission keys are backend-prefixed shape keys such as
`sljit:full-pipeline:<shape>` plus context details where needed. Missing proof is
reported as a skipped decision with cumulative counters, not hidden by disabling
JIT.

Admission has three ordered gates:

- cheap pipeline prospect matching before graph lowering;
- exact inventory matching before region IR lowering;
- lowered executable-shape matching after backend analysis.

Inventory and candidate signatures use the same feature vocabulary for sources,
sinks, and stateful operators. For example, a table scan feeding the result
collector is keyed with both `table-scan-source` and `result-collector-sink` at
the inventory and candidate levels. This keeps auto admission from drifting
between pre-lowering and lowered-region decisions.

The final gate separates two facts. A candidate rule can admit a region for
backend analysis, but only a fused lowered executable rule can admit auto
execution. If backend analysis proves the region is not fused, the skip event
keeps the candidate proof for observability while still rejecting execution.

## Observability

`ExecutionRegionManager` is owned by `DatabaseInstance` and owns backend
registration, backend selection, execution-region policy, and telemetry for the
current `duckdb_jit_*` SQL surface. Events are bounded by `jit_event_log_size`,
while cumulative counters remain monotonic.

Selected-backend auto-admission exits are observable at every layer. Empty
prospects, minimum-cardinality skips, prospect misses, exact inventory misses,
graph construction failures, and region IR lowering failures all record decision
events instead of disappearing as silent vectorized execution.

Events and counters expose:

- backend, target, phase, status, and execution mode;
- candidate ABI, shape, operator indexes, and estimated cardinality;
- ownership contract, source boundary count, and missing contract count;
- selected source execution and region execution form;
- compile, codegen, runtime, source-contract, generated-body, and boundary metrics;
- admission key, proof, min cardinality, and policy decision;
- deterministic IR when `jit_dump_ir=true`.

## Verification Rules

The architecture verifier enforces these invariants:

- the region ABI enum contains only `NONE` and `FULL_PIPELINE`;
- candidate identity is represented by the region ABI and contract fields, not a
  separate ABI enum;
- the pipeline executor dispatches full-pipeline kernels only from a clean
  source-to-sink boundary;
- the pipeline owns `ExecutionRegionPlan`; executors must not compile or own
  region kernels;
- backends cannot compile a full-pipeline candidate without
  `CanExecuteFullPipeline()`;
- native operator runtime binding exposes `READY`, `DEFERRED`, and `INVALID`,
  not a boolean failure channel;
- native events must have zero source boundaries and zero missing contracts;
- native sink success must use `full-pipeline-native-sink`;
- source contract layout names use `source_contract_*`;
- auto admission requires a backend proof and runtime-bindable native contracts;
  runtime-layout-dependent hash join probes are skipped until their pointer-table
  layout is part of the native contract.

Correctness gates are focused unit tests, SQLLogic JIT equivalence tests,
architecture verification, TPCH tracing, and full DuckDB test runs when the
change is ready for broad validation.
