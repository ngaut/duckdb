# JIT Broad Query Refactor Plan

Last updated: 2026-06-26
Status: active refactor plan before broadening query coverage
Companion docs:

- `benchmark/tpch/jit/JIT_BROAD_QUERY_PLAN.md`
- `benchmark/tpch/jit/JIT_PERFORMANCE_LEARNINGS.md`

## Purpose

This plan defines the refactor required before supporting much broader JIT query
families. The goal is not to add more query-shaped branches. The goal is to make
the backend own value lifetime through shared descriptors so more queries become
natural instances of one dataflow.

Root solution:

```text
load key once
hash once
probe once
keep row pointer live
compute payload while sources are hot
keep aggregate state pointer live
update aggregate
materialize only at a true DuckDB boundary
```

SLJIT wins when it deletes real execution work: final join outputs, projection
source chunks, copied batches, state-address vectors, repeated probes, repeated
selection/validity work, and avoidable sink materialization. It does not win
reliably by replacing one vectorized loop while preserving the old pipeline
shape.

## Verified Plan Snapshot

Current baseline:

- The current full SF10 Q9 shape is backend-fusable and admitted through facts,
  not through a query-number rule.
- The stale first-join materialization boundary is removed for the current full
  Q9 shape. Runtime counters no longer report first-join
  `hash_join_probe.final_output` or `hash_join_probe.projection_source` after the
  shared variable-width direct projection fix.
- The final compressed-key passthrough deleted late recompression but was not the
  root Q9 win. The follow-up early compressed-key skip now proves payload and
  probe-key independence, carries the compressed key before the second join, and
  skips materializing the dead decompressed `nation` column.
- Q9 SF10 production over 5 repeats after the early skip measured
  vectorized/off `1.722s` median versus auto `1.300s` median, speedup
  `1.324615`, correctness diff 0.
- A later no-chain probe aliasing cleanup reduced Q9 traced probe counters
  slightly and kept production medians correct:
  `/private/tmp/duckdb_jit_q9_restrict_prod2` measured off `1.599s` median
  versus auto `1.159s`, speedup `1.379638`, correctness diff 0. The lesson is
  limited: compiler-friendly probe loops help, but they do not delete a
  boundary.
- Q3 and Q20 focused production smokes remain correct and skipped by default
  where CBO facts do not justify compilation.

Root problem still open:

- The runtime still ends some value lifetimes too early around regular hash
  probing and grouped aggregate lookup/update. The current Q9 route now skips the
  dead decompressed `nation` projection, but it still has meaningful work in
  regular hash probe pointer chasing, selected/flat probe variants, grouped
  lookup/update, compressed `year`, selected DECIMAL payload inputs, precomputed
  group hashes, and grouped aggregate state targets.
- The visible symptom is not one bad query branch. It is an incomplete value
  descriptor model across projection chains and grouped aggregate lookup/update.
- The next root deletion is to stop treating compact group-key projection as the
  final shape. The aggregate lookup/update path should consume live descriptors
  and reuse hash/state-target facts until payload update finishes, while the
  regular hash probe path should own the match selection and row-pointer outputs
  without redoing invariant work.
- Helper-level probe optimizations should follow descriptor cleanup, not replace
  it. The profiler still names selected/flat regular hash probe variants,
  `JoinHashTable::InsertHashes`, scan decompression/filtering, and grouped
  lookup/update as the remaining cost centers.

Next implementation target:

```text
regular hash join probe
  -> live row pointers and selected source descriptors
  -> projection chain keeps compressed/fixed-width values live
  -> grouped lookup consumes descriptors and precomputed hashes
  -> state target remains live
  -> generated payload update consumes live DECIMAL inputs
  -> materialize only if the next DuckDB operator requires it
```

Immediate milestone exit criteria:

- Q9 profile still reports no first-join `final_output` or `projection_source`.
- Final projection/grouped aggregate counters prove whether compressed group keys,
  hashes, and state targets are carried or name the exact unsupported descriptor
  fact.
- Post-change CPU sampling shows no `SljitNativeStringDecompress` frames in the
  Q9 fused route; remaining samples are regular hash join probe variants,
  hash-table insertion/probing, scan decompression/filtering, and grouped
  aggregate lookup/update.
- No per-row C++ callback is introduced.
- Aggregate lookup/update calls remain vector-sized unless a measured state-target
  route proves a better batching contract.
- Focused Q9/Q3/Q20 tests pass, architecture verifier passes, and Q9 SF10
  production remains correct with a median improvement beyond noise.

## Verified Constraints

- DuckDB's vector size stays `STANDARD_VECTOR_SIZE=2048`.
- DuckDB runtime code owns hash-table tuple layout, append, duplicate-new
  resolution, null/cast semantics, and aggregate state mutation.
- SLJIT may own generated fixed-width expression evaluation, source selection,
  compact payload update loops, and trait-selected routes.
- No production rule should depend on a TPC-H query number.
- A speedup smaller than noise is not a win unless it also deletes a named
  boundary and survives repeated production runs.
- Trace timings explain stages; production medians decide admission.
- Stale CBO facts can hide a backend-fusable region. Fixing those facts is only
  an observability/admission cleanup until runtime counters prove the admitted
  route deleted the intended boundary.
- The latest full SF10 Q9 shape is backend-fusable and the first-join lifetime
  blocker is removed: the route no longer materializes first-join
  output/projection-source rows for the current projection chain. The specific
  root cause was computed variable-width `STRING_DECOMPRESS` in the between-join
  projection, not the query number.

## Non-Negotiable Refactor Rules

- Delete edge cases by changing the data shape, not by stacking guards.
- Add shared descriptors before adding another route.
- Preserve batching while deleting copies.
- Do not add per-row C++ callbacks in hot paths.
- Do not expose DuckDB hash-table tuple layout to SLJIT.
- Do not broaden CBO admission from forced/profile evidence alone.
- Every fallback must record one specific unsupported fact.
- Every fast path must have a positive runtime counter and a negative boundary
  counter proving what work disappeared.

## Milestone 1: One Value-Lifetime Descriptor Surface

Problem:

The current runtime has too many shape-specific ladders for join probe,
projection, grouped lookup, and payload update. That makes every new query family
tempting to solve with another branch.

Target:

Introduce a shared descriptor vocabulary used by join, projection, aggregate,
and CBO facts.

Descriptor concepts:

- `JitValueRef`: vector slot, selected vector slot, row pointer field, generated
  temporary, hash value, aggregate state pointer, or materialized fallback.
- `JitSourceFacts`: selection kind, validity kind, all-valid proof,
  dictionary/constant/flat facts, row-pointer availability, and source lifetime.
- `JitFixedWidthType`: logical type, physical type, byte width, signedness,
  decimal scale/width, date-compatible INT32 rules, and checked-cast policy.
- `JitStageFacts`: consumed value refs, produced value refs, required boundary,
  and unsupported reason.

Implementation steps:

1. Extract existing fixed-width load/store/validity helpers into one shared
   runtime-facing descriptor helper.
2. Convert direct projection routes to consume descriptors instead of local
   expression-kind checks.
3. Convert grouped aggregate routes to consume group-key and payload descriptors.
4. Keep existing telemetry names stable while moving construction behind the new
   descriptors.

Exit criteria:

- Adding a fixed-width type does not copy a join, projection, or aggregate loop.
- Q1/Q3/Q9/Q14/Q19/Q20/Q22 focused JIT tests still pass.
- Unsupported routes report descriptor facts, not generic miss counters.
- Lines of query-shaped dispatch decrease or move behind trait selection.

## Milestone 2: Join Output To Aggregate Input As One Dataflow

Problem:

Q3 and Q9 proved that deleting full join output is not enough. If row pointers
are immediately gathered into projection-source chunks or aggregate input
batches, the value lifetime still ends too early.

The current Q9 full shape sharpened this: even when the later second-join and
split-payload aggregate routes executed, a stale first-join boundary dominated
until the runtime could carry row pointers through the complete between-join
projection chain, including computed `VARCHAR` output.

Target:

Represent join match rows, projected group keys, payload values, and aggregate
state targets as live values until the aggregate update consumes them.

Implementation steps:

1. Make regular hash join probe produce row-pointer and selection descriptors for
   downstream consumers.
2. Make projection consume row-pointer descriptors for supported reference and
   fixed-width generated outputs.
3. Make grouped aggregate lookup consume group-key descriptors without requiring
   a full group `DataChunk` when fixed-width descriptors are sufficient.
4. Keep aggregate update vector-batched; do not trade copy deletion for tiny
   update calls.
5. Preserve a materialized fallback that records the exact required boundary.

Exit criteria:

- Q3 covered route reports no `hash_join_probe.final_output`, no
  `hash_join_probe.projection_source`, and no copied post-join projection
  boundary.
- Q9 covered route reports no second-join final output and no copied
  post-second-join batch boundary.
- Q9 full current route reports no first-join final output and no first-join
  projection-source boundary. Copied between-join projection rows are replaced by
  direct batch projection; the remaining work is deeper group-key/state-target
  lifetime, not first-join fallback materialization.
- Q9 final projection to grouped aggregate either keeps keys/payload live or
  reports one exact descriptor/state fact blocking it.
- Aggregate update invocation counts remain vector-sized.
- Production Q3/Q9/Q20 focused medians stay correct and improve beyond noise.

## Milestone 3: Grouped Lookup/Update ABI

Problem:

Address vectors and repeated aggregate probes are the next large boundary after
join/projection materialization is removed.

Target:

Let DuckDB grouped aggregate runtime return and reuse state targets while SLJIT
updates fixed-width payloads from compact live sources.

Implementation steps:

1. Split grouped update into visible phases: validate, fill/hash, probe,
   duplicate-target reuse, append-new, payload update, and finish.
2. Keep useful fixed-width key caches when they buy locality.
3. Remove repeated probes when a resolved state target remains live.
4. Reject before mutation when key/null/cast/layout facts are unsupported.
5. Use one DuckDB-owned ABI for append-only, all-existing, and mixed
   find-or-create primitive updates.

Exit criteria:

- Covered grouped routes do not report
  `aggregate_update.address_vector_resolve`.
- Covered routes update payload while state targets are live.
- No partial-mutation fallback exists.
- Stage telemetry separates hash, probe, append, duplicate reuse, and payload
  update.
- Forced/profile stage gains survive production timing.

## Immediate Implementation Plan: Generic Grouped Key Descriptors

Root cause:

Q3's current fast grouped lookup path is still query-shaped. It recognizes a
fixed `[INT32, INT32, INT8]` key pattern and hand-loads those keys from build-side
row pointers. Q20 exposes why that is not the root solution: its grouped keys are
probe-side projected casts, for example `SMALLINT` and `INTEGER` derived from
lineitem input values, and the runtime reports
`aggregate_update.direct_row_pointer_grouped_lookup_update_unsupported.group_key_sources`.
Adding a Q20 branch would preserve the bad shape.

Target:

Replace the Q3-shaped grouped lookup with one fixed-width key descriptor path
that can describe keys loaded from either row pointers or selected input vectors.
DuckDB runtime code still owns hash-table layout, hash/probe/append,
duplicate-new resolution, null/cast correctness, and aggregate state mutation.
SLJIT only carries live value facts and invokes the selected batched route.

Implementation steps:

1. Add a grouped-key source kind to the execution aggregate runtime descriptors:
   `row_pointer_field` and `input_vector`.
2. Extend fixed-width cast descriptors for observed TPC-H shapes, including
   `BIGINT -> INTEGER`, `BIGINT -> SMALLINT`, and `INTEGER -> TINYINT`, with
   checked overflow semantics before mutation.
3. Refactor row-pointer grouped lookup in
   `src/execution/aggregate_hashtable.cpp` into a generic fill/hash/probe path:
   fill a compact fixed-width group-key chunk from mixed key descriptors, compute
   hashes once, then call the centralized grouped find-or-create helper with
   precomputed hashes.
4. Delete the Q3-only key structs and helpers after the generic descriptor path
   covers the same route.
5. Add the Q20 primitive-payload route without building a full aggregate payload
   batch: project only compact grouped keys when needed, keep payload values in
   the live selected input, and update through DuckDB-owned grouped aggregate
   callbacks.
6. Keep unsupported fallbacks pre-mutation and specific:
   `group_key_source_kind`, `cast_kind`, `nullable_key`, `payload_source`, or
   `state_update_shape`, not a generic miss.

Expected deletions:

- Q3-specific key-source validation.
- Q3-specific key value structs.
- Q3-specific row-pointer key hash/equality/load loops.
- Local branch ladders that encode a TPC-H query shape instead of source/type
  descriptors.

Verification gate:

1. `make reldebug -j12`.
2. Focused JIT API tests for Q3 grouped lookup and Q20 grouped lookup.
3. `python3 benchmark/jit/verify_jit_architecture.py`.
4. Full `build/reldebug/test/unittest --print-failing-tests`.
5. Forced/profile Q3/Q9/Q20 runs with runtime counters.
6. Production Q3/Q9/Q20 repeats; require correctness diff 0 and medians beyond
   noise before claiming a win.
7. SF10 smoke before any default CBO broadening.

Exit criteria:

- Q3 still enters the fast grouped lookup/update path without address-vector
  fallback.
- Q20 no longer reports `group_key_sources`; if it still falls back, the counter
  names the next exact descriptor fact.
- The replacement path is descriptor-driven and has no query-number checks.
- Aggregate update calls remain vector-sized.
- The total code shape is simpler: removed Q3-shaped code is larger or more
  brittle than the generic descriptor additions.

Verified result:

- The Q3-only grouped key validation, key struct, hash/equality/load helpers, and
  row-pointer-specific probe/append loop were removed.
- `ExecutionRowPointerGroupKeySource` now describes either a row-pointer field or
  a probe input vector, with checked cast descriptors for the observed TPC-H
  shapes.
- `GroupedAggregateHashTable` fills a compact descriptor group-key chunk, hashes
  once, and delegates probe/append/update to the centralized fast
  find-or-create helper.
- Q20's old `group_key_sources` blocker is gone. The next blocker was primitive
  payload lifetime, and that was removed by projecting only compact group keys
  while keeping the DECIMAL payload in the live pending probe input.
- API coverage now locks both routes:
  - probe-side descriptor grouped lookup/update
  - probe-side compact group-key projection plus primitive split-payload update

Verified forced/profile smoke:

- Q3 reports `aggregate_update.direct_row_pointer_grouped_lookup_update` with
  `find_or_create_descriptor_keys.fill` and `.hash`, and no full join output or
  projection-source boundary for the covered region.
- Q20 reports `aggregate_update.direct_projected_group_payload_update=9741`,
  `projection.direct_remap_post_join_batch_projection=9741`, and
  `aggregate_update.projected_group_payload_update=9741`.
- Covered split-payload routes now compute group hashes at the compact
  projection boundary and pass them into DuckDB grouped aggregate lookup. The
  aggregate-side `find_or_create_fast.hash` stage disappears for Q9/Q20, while
  `projection.post_join_direct_remap_batch_projection_hash` records the hash
  production.
- Q9's split-payload route now takes an existing-only state update after the
  first new-group batch: SF1 forced/profile showed
  `direct_existing_split_payload.update=163` and only one mixed
  `direct_new_split_payload.append` batch.
- Q20 no longer reports `group_key_sources`, `payload_sources`,
  `hash_join_probe.final_output`, `hash_join_probe.projection_source`,
  `projection.copied_post_join_projection`, `projection.copied_post_join_batch`,
  or `aggregate_update.direct_state_update` for the covered route.
- Q3/Q20 SF1 forced/profile smoke stayed correct with correctness diff 0.

## Milestone 4: Fixed-Width Type Unification

Problem:

TPC-H and broader analytical workloads use `BIGINT`, `INTEGER`, `DATE`,
`DECIMAL(15,2)`, and selected `VARCHAR` references. Adding each type as a local
fast path will recreate the current branch sprawl.

Target:

One fixed-width descriptor path covers load, compare, hash, cast, projection,
and aggregate update for supported scalar types.

Priority order:

1. `BIGINT`
2. `INTEGER`
3. `DATE`
4. `DECIMAL(15,2)`
5. all-valid `VARCHAR` references
6. controlled `VARCHAR` predicates
7. `VARCHAR` grouping only when string materialization cost is explicitly owned

Exit criteria:

- DATE reuses INT32-compatible machinery where semantics allow it.
- DECIMAL covers TPC-H arithmetic, comparison, and aggregate payload patterns.
- Nullability, selected vectors, decimal overflow, and mixed distributions have
  focused tests.
- A type is considered complete only when it deletes a measured boundary or hot
  dispatch cost.

## Milestone 5: Runtime Variant Registry

Problem:

Runtime route selection is spread across local conditions. That makes it hard to
prove which shape executed and hard to add broader queries safely.

Target:

Replace ad hoc route ladders with a small trait-selected runtime variant
registry.

Variant axes:

- source lifetime: materialized vector, selected vector, row pointer, generated
  temporary
- key shape: single fixed key, multi fixed key, compressed key, reference key
- payload shape: fixed payload, DECIMAL payload, reference payload, generated
  payload
- aggregate mutation: append-only, all-existing, mixed find-or-create
- validity: all-valid, nullable checked, unsupported
- selection: identity, selected, compacted selected

Exit criteria:

- Route selection is explainable from descriptors and runtime facts.
- New variants add trait entries, not duplicated loop bodies.
- Telemetry records both the selected variant and the fallback reason.
- Existing query-family behavior is unchanged unless a later milestone explicitly
  changes policy.

## Milestone 6: CBO Admission From Proven Facts

Problem:

Forced/profile mode is useful for root-cause work, but default admission must not
expand from optimism.

Target:

Only admit query families when capability facts, runtime counters, boundary
counters, correctness, and production medians agree.

Implementation steps:

1. Keep CBO rules named and surfaced in counters/events/profiler output.
2. Add negative controls for every new rule.
3. Gate rules on descriptor facts that explain prior wins or losses.
4. Keep known losing shapes skipped until their remaining boundary is deleted.

Exit criteria:

- Each new admission rule cites the deleted boundary, replacement runtime path,
  capability facts, production medians, and negative controls.
- SF10 evidence exists before broadening a rule beyond the focused family.
- Correctness diff is 0.
- Full TPC-H does not hide a query regression behind an aggregate geometric mean.

## Milestone 7: SIMD After Scalar Fusion

Problem:

SIMD is worth benchmarking only after scalar fused loops are no longer dominated
by materialization, probing, and boundary work.

Target:

Add SIMD variants for stable fixed-width loops where compute is visible after
fusion.

Candidate loops:

- DECIMAL payload arithmetic
- fixed-width projection payloads
- predicate masks with all-valid facts
- compact selected payload updates after state targets are known

Exit criteria:

- SIMD is compared against scalar fused execution for the same descriptor route.
- Benchmark output separates instruction-level gain from boundary-deletion gain.
- SIMD does not hide unresolved materialization or repeated lookup costs.

## Milestone 8: GPU Only After Residency

Problem:

Per-vector Metal launch/copyback loses to CPU/vectorized execution.

Target:

Use GPU only after multiple DuckDB vectors can stay resident across useful work.

Implementation steps:

1. Batch many DuckDB vectors into one Metal launch.
2. Reuse buffers, command state, and compiled kernels.
3. Avoid immediate host copyback.
4. Keep data resident across fused stages where possible.
5. Charge launch, transfer, synchronization, copyback, and rows-per-launch costs
   in the CBO.

Exit criteria:

- GPU wins on high arithmetic-intensity or resident high-throughput shapes.
- Low-intensity projections stay on CPU/vectorized/SLJIT.
- The CBO has real per-batch GPU cost before default admission.

## Immediate Work Order

Completed current slice:

- final-projection-to-grouped-aggregate split-payload fusion for Q9-like
  fixed-width final projections through shared descriptors
- Q9 verification that the terminal final shape is now compact group-key remap
  plus `aggregate_update.projected_group_payload_update`, not full aggregate
  payload batch construction
- Q3/Q9/Q20 focused production medians and Q9 forced/profile traces
- stale CBO rule cleanup for Q9-like full candidates with four generated stages
  and intermediate reference-`VARCHAR` projections
- shared variable-width expression-to-batch direct projection for the Q9
  between-join `STRING_DECOMPRESS` output; Q9 SF10 now removes first-join
  `final_output` and `projection_source` boundaries for the full shape
- descriptor-gated compressed group-key passthrough for
  `STRING_COMPRESS(STRING_DECOMPRESS(x))`; Q9 SF10 trace now records
  `projection.direct_batch_passthrough_projection=3261613` and removes final
  `op4:projection.direct_batch_expression.string_compress`, but production timing
  did not improve beyond noise
- descriptor-gated early compressed group-key skip; Q9 SF10 trace now records
  `projection.direct_between_join_compressed_group_key_skip_projection=3261613`,
  drops `op1:projection.post_join_direct_computed_projection` from about
  `96-99 ms` to about `0.53 ms`, and moves production auto median from the prior
  `1.359s` checkpoint to `1.300s`

Next work:

1. Optimize the measured regular hash join probe variants and hash-table lookup
   traffic before adding another projection-only cleanup.
2. Carry or reuse grouped hash/state-target facts across the final projection and
   grouped aggregate lookup after the first-join boundary is gone.
3. Refactor fixed-width and variable-width projection descriptors before adding
   another query-family path.
4. Run SF10 smokes before any default CBO broadening.
5. Only then move to SIMD experiments.
6. Keep GPU work behind residency and real cost accounting.

## Verification Gate

For code changes:

1. `make reldebug -j12`
2. Focused JIT API tests for the touched surface.
3. `python3 benchmark/jit/verify_jit_architecture.py`
4. Full `build/reldebug/test/unittest --print-failing-tests` unless the change is
   documentation-only.
5. Targeted forced/profile benchmark with runtime path and materialization
   boundary counters.
6. Targeted production benchmark with enough repeats to beat noise.
7. Adjacent query-family smoke.
8. SF10 smoke before policy broadening.

For documentation-only changes:

1. `git diff --check`
2. Read the rendered markdown for stale wording, duplicate milestones, and
   contradictions with verified benchmark evidence.

## Definition Of Done

A milestone is complete only when:

- the deleted work is named
- the replacement path is counted
- unsupported fallback is specific
- correctness diff is 0
- update/probe calls remain batched
- focused tests lock the route
- production medians beat noise
- adjacent query families do not regress
- the implementation deletes or centralizes code instead of adding another local
  special case

The aggressive path is not "compile more SQL". The aggressive path is to make
more SQL naturally fit the same data-centric execution model.
