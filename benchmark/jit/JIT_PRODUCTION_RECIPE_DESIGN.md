# JIT Production Recipe Architecture

Last updated: 2026-07-17

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
sequence grammar and descriptor ownership; it does not call the capability
predicates again. Runtime kind, partition preservation, scan-filter body
ownership, fused-filter ownership, and the direct-terminal contract are
finalized together before the recipe plan is published.

Prepared expression capability does not change when selector machine code is
emitted. The executable builder therefore binds the plan exactly once, emits
selectors only for filters not owned by a fused terminal, and moves that same
plan into the kernel. Runtime consumes published dispatch, source-chunk, scan
filter, and direct-terminal ownership; it does not scan neighboring primitives
to rediscover a route. A recipe that was accepted but is internally
inconsistent raises an internal error. Unsupported semantics are rejected
before publication and remain native.

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

`ExecutionHashJoinRHSFixedColumnSource` is the core/backend boundary for fixed
build-side columns. It represents either:

- `ROW`: row-layout offset plus row validity metadata; or
- `DICTIONARY`: dictionary-index offset, data, validity, and count.

Core initializes all storage-specific fields before publishing `ready=true`.
Backend helpers depend only on this exported ABI and common validity-mask
types. They do not include `join_hashtable.hpp` or name private
`JoinHashTable` layout types.

Consumers must state their physical requirement. A generic fixed-column loader
may dispatch both storage kinds. Row-layout projection, compressed-row string,
row-pointer grouping, and complementary row-field consumers explicitly reject
dictionary storage. Silent interpretation of dictionary indices as row values
is forbidden.

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
work can amortize mask handling.

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

Ungrouped and grouped routes must preserve DuckDB aggregate behavior for empty
input, NULL input, overflow, state initialization, combine, and finalize.
Generated grouped run reducers are admitted from semantic ordering and range
facts, not workload identity.

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
while raw JIT-auto ceilings remain independent. Focused triage is an explicit
action.

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

A regression-gate invocation provisions one database and reuses it for the
untraced candidate, traced runtime proof, focused recheck, and promotion pass.
The gate removes a privately provisioned database in a `finally` block unless
retention was requested. Database generation is setup, not repeated proof.

## Verification

Static architecture checks enforce layer and ownership invariants:

```sh
python3 benchmark/jit/verify_jit_architecture.py
```

Focused correctness and complete JIT API coverage use the reldebug unit binary:

```sh
build/reldebug/test/unittest "[jit]"
```

Generic production and TPC-H gates are documented in
`benchmark/jit/README.md`. Every implementation change must pass proportionate
focused tests first, then architecture, complete JIT correctness, generic
one-thread and parallel workloads, and affected TPC-H scale factors.

## Current boundaries

- Variable-width expression coverage is narrower than fixed-width coverage.
- Unsupported aggregate layouts, DISTINCT shapes, and join modes remain native.
- Generated run reducers support bounded, proven primitive lane layouts; wider
  or heterogeneous layouts retain exact generic execution.
- Multi-key, selected, and non-flat grouped inputs require an explicit proven
  route before generated run ownership can expand.
- Native source and sink protocols remain native unless an exported execution
  contract makes composition safe.
- Native-only selection is valid capability analysis, not a failed recipe and
  not permission to loosen CBO accounting.

These are capability boundaries, not workload exceptions. New support should
generalize the semantic descriptor or runtime ABI, remove a boundary, add
correctness and runtime-proof coverage, and ratchet performance evidence.
