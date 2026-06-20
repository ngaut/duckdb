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
  -> DuckDB CBO physical-runner decision
  -> pipeline-owned execution-region plan
  -> executable full-pipeline kernel
```

The IR is the only contract consumed by a backend. SLJIT must not inspect
physical operator internals directly, and core DuckDB must not depend on SLJIT
details.

`Pipeline` owns the selected execution artifact as `ExecutionRegionPlan`.
Planning, backend lowering, codegen, and the CBO physical-runner decision happen before source
state captures scan/source-contract decisions.
Source state opens from the small `ExecutionRegionOpenRequest`, not from the
whole selected plan, so physical sources do not inspect runner selection,
kernels, or backend state. `PipelineExecutor` does
not compile, own kernels, or infer policy; it only runs the runner kind selected
by the immutable plan at a clean source-to-sink boundary. Backend scratch storage
is per execution, not stored in the compiled kernel.

## Region Model

The region planner emits only first-class execution units:

- `full_pipeline`: owns source entry, every fused operator stage, and the sink.
- `none`: no executable region exists.

There is no executable sub-pipeline ABI. A compiled kernel owns the whole
source-to-sink unit. Source-only stateful operators may expose state-source
contracts for a surrounding full pipeline, but those candidates remain `none`
until they are fused with a sink.

## Native Fusion Contract

The core contract exposes a fused native region only when all of the following are true:

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
- `source_contract_filter_prune_required`

Scan filters remain owned by DuckDB's scan contract. If a pushed-down table
filter is required to produce the scan chunk, the compiled full-pipeline region
treats that work as source-contract runtime, not generated native filter work.
Generated source-filter execution is not part of the architecture.

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

Runner selection is a plan invariant. `ExecutionRegionPlanner` selects
`CompiledVectorizedRunner` only when the immutable `ExecutionRegionPlan` contains
an executable full-pipeline kernel and DuckDB's core execution plan selected a
compiled runner. `Pipeline` only executes the plan's selected `ExecutionRunner`,
currently `VectorizedRunner` or `CompiledVectorizedRunner`; it does not infer
policy from kernel presence. If the compiled runner observes a
missing source, sink, plan, or kernel, it is an internal architecture error, not
a quiet fallback. The runner receives only the executable kernel through
`ExecutionRegionPipelineAdapter`, not the whole selected plan.
The compiled runner never calls the vectorized executor directly. It returns
`EXECUTED` or one of the named vectorized dispatch statuses:
`VECTORIZED_SUPPRESSED`, `VECTORIZED_CONTINUATION`, and `VECTORIZED_DEFERRED`.
`ExecuteExecutionRunner` is the runner-layer coordinator that maps those
statuses to the vectorized runner.

Executable full-pipeline regions have one honest compiled body:
`generated-machine-code`. Native DuckDB contracts are stage protocols that a
generated region may call through, not compiled bodies. A compiled event must
report `code_size > 0`; zero-code compiled events are verification failures.

State-source contracts are source contracts for full-pipeline kernels. They do
not create a separate executable kernel ABI and must not masquerade as native
execution on their own.

## Backend Lowering

SLJIT receives only region IR and native contract descriptors. It lowers:

- typed scalar expression IR;
- native filter and projection loops;
- native hash join probe contracts when the build layout is exposed through a
  native contract;
- native hash join build contracts through the primitive build protocol when the
  same fused region also emits generated machine code;
- native aggregate lookup and update contracts;
- native operator sink contracts;
- state-source contracts where the surrounding full pipeline can own the
  source-to-sink loop.

If a contract is missing, SLJIT records a deterministic unsupported event. It
does not call whole DuckDB executors from native-labeled code.
Hash join build currently owns the typed primitive build protocol. A build-only
contract loop is normal vectorized DuckDB execution and must not be reported as a
compiled region. When fused with generated filter/projection/probe work, its
trace must show the primitive build stages explicitly.

## Execution Region Policy

`force` may compile any fused generated-code region that is executable and
honest. It is a diagnostic override after DuckDB has built the normal
physical-runner cost profile; it is not an input to the CBO and must not change
the estimated benefit/cost facts. It does not compile protocol-only regions.

`auto` is owned by DuckDB's normal execution planning, not by a private JIT
threshold. In production execution, DuckDB inspects executable region
candidates, lowers backend-neutral IR, asks the selected backend for capability
facts, and then makes one core CBO decision. The decision compares estimated
saved vectorized work against compile/planning cost and selects either
`CompiledVectorizedRunner` or `VectorizedRunner`.

Expression cost and physical-runner decisions are shared DuckDB planner
facts, exposed through `DuckDBCostModel`. Optimizer expression ordering,
adaptive filter ordering, expression IR trait extraction, and execution-region
CBO decisions all consume that one model. Region planning prepares neutral
facts such as operator ownership, stage ownership, backend execution body, and
runner shape; it does not keep a second expression-cost or admission model.

Backends provide capability analysis and executable lowering. They do not own
thresholds, measured-rule tables, or private copies of the execution-policy
algorithm. DuckDB owns the single CBO decision path:

- core region lowering builds a typed candidate with capability facts;
- backend analysis describes the exact executable runner shape and reports how
  many stages emit generated machine code;
- production AUTO enters graph/IR/backend analysis for every executable region
  candidate when the selected backend supports regions. The backend reports
  capability facts; DuckDB CBO alone selects compiled-vectorized or vectorized
  execution;
- a compiled-vectorized runner may be selected only by DuckDB's core execution
  plan, for fused lowered generated-code regions whose compiled runner is
  proven better than the vectorized runner;
- region lowering emits the execution candidates the core plan intends to
  consider. The execution planner does not run a second interval selector,
  native ownership score, stage score, backend score, or admission-table score;
- vectorized selections record the same CBO decision path as compiled selections,
  including estimated saved work, accelerated-runner benefit, and startup cost.

Native operator contracts are not compiled-runner proof by themselves. They are
capability facts until backend lowering reports generated machine-code stages.
`force` may ignore estimated profitability, but it still requires generated
work; protocol-only regions stay vectorized.

Unsupported contracts are not CBO decisions. Core capability analysis may reject
missing source, operator, or sink contracts before backend work, but it must not
estimate private runner costs or select a runner.

Diagnostic modes (`jit_trace_decisions`, `jit_dump_ir`, `jit_trace_runtime`, and
`EXPLAIN ANALYZE`) add retained detail, IR text, and runtime profile data to the
same production CBO path. They must not admit a region that the core CBO rejected,
and they must not act as a second CBO.

## Observability

`ExecutionRegionManager` is owned by `DatabaseInstance` and owns backend
registration, backend selection, execution-region policy, and telemetry for the
current `duckdb_jit_*` SQL surface. Events are bounded by `jit_event_log_size`,
while cumulative counters remain monotonic. Setting `jit_event_log_size=0`
disables retained event rows only; `duckdb_jit_counters()` still records compact
decision and runtime aggregates for production-safe measurement.

Diagnostic inspection exits are observable at every layer. Graph construction
failures, region IR lowering failures, backend capability failures, and AUTO
vectorized selections all record decision events when diagnostics are enabled
instead of disappearing as silent vectorized execution.

Events and counters expose:

- backend, target, phase, status, and execution mode;
- candidate ABI, shape, operator indexes, and estimated cardinality;
- ownership contract, source boundary count, and missing contract count;
- selected source execution and region execution form;
- compile, codegen, runtime, source-contract, generated-body, and boundary metrics;
- candidate estimated cardinality, selected runner, and requested policy;
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
- auto CBO selection requires a lowered executable region and runtime-bindable
  native contracts;
  runtime-layout-dependent hash join probes are skipped until their pointer-table
  layout is part of the native contract.

Correctness gates are focused unit tests, SQLLogic JIT equivalence tests,
architecture verification, TPCH tracing, and full DuckDB test runs when the
change is ready for broad validation.
