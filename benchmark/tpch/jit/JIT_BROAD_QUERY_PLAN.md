# JIT Broad Query Root Plan

Last updated: 2026-06-26
Last verified commit: `48868a4a1d`
Branch: `codex/jit-native-duckdb-core`

Status: active root plan. This file is the execution plan for making more query
families faster with SLJIT. It is intentionally stricter than "make more things
compile": a milestone is not complete until it deletes named work, preserves
correctness, passes the verification gate, and improves production medians beyond
noise.

Companion refactor plan:
`benchmark/tpch/jit/JIT_BROAD_QUERY_REFACTOR_PLAN.md`.

## Root Solution

The root solution is data-centric value-lifetime fusion:

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

SLJIT wins when it removes operator-boundary work. It does not win reliably by
replacing one vectorized expression loop with scalar generated code while the old
pipeline shape, materialization, copied batches, address vectors, and state lookup
remain.

The work to delete, in priority order:

- full regular hash join output materialization
- projection-source chunks that only feed the next fused stage
- copied post-join projection batches
- aggregate state-address vectors and address buffers
- repeated hash-table probes when a resolved state target is still live
- repeated selection, validity, and cast work after runtime facts prove the shape
- tiny per-row or small-chunk update calls that destroy vector-sized batching

The backend should own value lifetime. The CBO should admit a shape only when
runtime facts and production benchmarks prove the backend owns the hot path.

## Milestone Map

| Milestone | Primary deletion | Exit signal |
| --- | --- | --- |
| 1. Observability and policy facts | Unknown or ambiguous JIT decisions | Every query/policy pair has a classified reason and trace/runtime counters name the remaining boundary. |
| 2. Runtime variant refactor | Duplicated branch ladders and query-shaped policy | Value-lifetime facts and fixed-width descriptors route join, projection, and aggregate paths without copied loop trees. |
| 3. Join-to-aggregate dataflow fusion | Join output chunks, projection-source chunks, copied post-join batches | Q3/Q9/Q20-family traces keep row pointers, selections, payloads, and aggregate inputs live until a real boundary. |
| 4. Grouped lookup and update ABI | Address-vector resolution and repeated aggregate probes | DuckDB-owned lookup returns reusable state targets and generated payload updates run while targets are live. |
| 5. Fixed-width type coverage | Type-specific generated loop forks | `BIGINT`, `INTEGER`, `DATE`, `DECIMAL(15,2)`, and safe references share load/hash/compare/update descriptors. |
| 6. Regular hash join and multi-join ownership | Regular-hash final output materialization | Probe, match selection, downstream projection, and aggregate update are owned as one dataflow. |
| 7. CBO broadening from facts | Default admission from hope or forced/profile-only wins | Each new default rule cites counters, deleted boundaries, negative controls, and production medians. |
| 8. SIMD after scalar fusion | Scalar compute loops that remain hot after boundary deletion | SIMD beats scalar fused execution for the same region, separate from materialization wins. |
| 9. GPU after residency | Per-vector launch/copyback overhead | Multi-vector resident GPU batches win after launch/transfer/copyback costs are charged. |

## Current Evidence

These are the facts the plan is built on:

- `STANDARD_VECTOR_SIZE=2048` is the verified execution contract. The 4096-vector
  experiment exposed broad test instability and is not a local JIT tuning knob.
- FLOAT/DOUBLE CTAS direct materialization was the clearest proof of the model:
  `0.0265s` vectorized versus `0.0154s` SLJIT fused direct append on the 5M-row,
  8-expression workload. It won by deleting materialization and sink-boundary
  work, not because FLOAT alone is special.
- TPC-H mostly needs `BIGINT`, `INTEGER`, `DECIMAL(15,2)`, `DATE`, and `VARCHAR`.
  Type support matters only when it deletes dispatch, materialization, validity,
  selection, or boundary work.
- Q1 proved filter-to-perfect-hash aggregate fusion: removing the separate filter
  selection and evaluating the DATE predicate inside the aggregate loop moved SF1
  and SF10 beyond small local cleanups.
- Q3 proved one-join grouped-aggregate boundary deletion:
  - full join output can be replaced by row pointers
  - direct post-join projection can write into the pending aggregate batch
  - grouped lookup/update can consume row-pointer keys and compact selected
    payload sources
  - keeping a useful fixed-width key cache is better than reloading/casting row
    pointer keys repeatedly
  - duplicate consecutive keys should reuse the resolved aggregate target
- Q9 proved multi-join boundaries can be deleted generically, but the current
  full SF10 shape also proved that the route recognition is still too brittle:
  - the older covered shape is
    `hash_join -> projection -> hash_join -> projection -> projection -> aggregate`
  - the current full candidate shape is
    `hash_join -> projection -> projection -> hash_join -> projection -> projection -> projection -> projection -> aggregate`
  - the first blocker was a mixed fixed-width plus `VARCHAR` reference projection
    between joins
  - direct non-fixed reference projection is valid when the reference is
    all-valid
  - the next full-shape blocker was computed variable-width projection:
    `STRING_DECOMPRESS(UHUGEINT nation) -> VARCHAR` in the first between-join
    projection was not eligible for direct batch materialization, so one
    unhandled output forced projection-source and final-output materialization
  - the shared expression-to-batch helper now handles fixed-width outputs with
    in-place writes and `VARCHAR` outputs through a copied result vector, so the
    current eight-transform route can keep the first join row pointers live
  - the second regular join can run selection-only and direct-project from live
    row pointers
  - forced/profile removed `hash_join_probe.final_output`,
    `hash_join_probe.projection_source`,
    `projection.copied_post_join_projection`, and
    `projection.copied_post_join_batch` for the older covered Q9 fused region
  - after stale CBO facts were fixed, the full SF10 Q9 candidate was admitted,
    but profile mode regressed because `hash_join_probe.final_output` and
    `hash_join_probe.projection_source` still appear for 3.26M first-join rows
  - RHS `DATE_YEAR(o_orderdate)` now reads DATE directly from second-join row
    pointers and records `projection.direct_rhs_row_pointer_generated_projection`
  - the muted production gain exposed the next blockers: pair-key regular probe
    cost, between-join reference projection, and final projection into an
    aggregate input batch
  - the no-chain probe aliasing cleanup was retained because it reduced traced
    Q9 probe counters slightly and kept production medians correct, but it did
    not delete a boundary; sample stacks still name selected/flat regular probe
    variants, hash build/finalize, storage/filtering, and grouped lookup/update
  - aggregate state lifetime is still not deeply owned; the pipeline builds an
    aggregate input batch instead of keeping the resolved group/state target live
    through payload update
- Q20 proved that row-pointer group keys and probe-side group keys are the same
  descriptor problem. The old `group_key_sources` blocker is fixed for the
  covered route without a Q20-only branch; the remaining root work is carrying
  hash/state-target facts deeper instead of stopping at compact group-key vector
  construction.
- Runtime path counts and materialization boundary counts now exist and must stay
  row-counted. They are the source of truth for what work remains.

Noise rule: `1.01x` and `1.05x` are not wins by themselves. A claim needs a
deleted boundary, stable counters, correctness diff 0, and repeated production
medians.

## Non-Negotiable Design Rules

- Delete work instead of renaming it behind another helper.
- Preserve batching while deleting copies.
- A lower copy count is not a win if it increases hot-loop invocations.
- Keep DuckDB hash-table tuple layout in DuckDB runtime code.
- Do not add per-row C++ callbacks to the hot path.
- Do not broaden default CBO admission from forced/profile losses or noise wins.
- Forced/profile mode is a profiler and root-cause tool, not production policy.
- Add type support through shared helpers, not copied loop trees.
- Keep query-name rules out of production policy.
- SIMD starts after scalar fused loops are no longer dominated by memory movement.
- GPU starts after multi-vector residency, buffer reuse, delayed copyback, and
  real launch/transfer CBO cost exist.

## Active Slice: Q9 First-Join Lifetime In Current Full Shape

Status: the older Q9 covered route deleted the second-join output,
projection-source, copied-batch, and final aggregate payload-batch boundaries.
The hidden RHS `DATE_YEAR(o_orderdate)` gather is also removed for the covered
regular-hash route: the runtime reads DATE directly from second-join RHS row
pointers, handles SQL NULL and infinite DATE semantics, extracts finite years
inline, and records `projection.direct_rhs_row_pointer_generated_projection`.

The current full SF10 Q9 route exposed the next root blocker. CBO can now admit
the large full candidate. The first admitted profile was slower because the
first join still materialized `hash_join_probe.final_output` and
`hash_join_probe.projection_source` for 3.26M rows. The current shape has two
projections between the first and second join, plus a longer projection chain
after the second join.

That blocker is now removed for the real SF10 shape. The root cause was not the
number of projections alone; it was one computed variable-width output in the
between-join projection chain. The runtime directly handled fixed computed
values and all-valid references, but `STRING_DECOMPRESS(UHUGEINT nation) ->
VARCHAR` could not write into the pending second-join batch. One unhandled output
made the direct helper materialize projection sources and then fall back to full
first-join output. The fix is shared: execute supported computed expressions
directly into the pending batch, writing fixed-width results in place and copying
`VARCHAR` result vectors so string ownership stays with the target batch.

Older covered target:

```text
hash_join -> projection -> hash_join -> projection -> projection -> aggregate
```

Current target:

```text
hash_join -> projection -> projection -> hash_join -> projection -> projection -> projection -> projection -> aggregate
```

Before the second-join slice, the known Q9 blocker after the first join fix was:

- `hash_join_probe.final_output=319404`
- `projection.copied_post_join_projection=638808`
- `projection.copied_post_join_batch=315308`

Implemented root fix:

1. Run the second regular hash join in row-pointer/selection-only mode.
2. Keep the second join row pointers live.
3. Direct-project the post-second-join projection from:
   - selected LHS vectors
   - second join RHS row pointers
   - DuckDB-owned gather/reference helpers
4. Start the remaining projection chain after the direct projection, not from a
   materialized full join output.
5. Materialize the second join output only as a visible fallback.
6. Direct-materialize the final fixed generated projection into the aggregate
   input batch for supported fixed-width generated expressions such as
   `STRING_COMPRESS` and `DATE_YEAR`.
7. Direct-materialize fixed-width RHS generated sources from hash-join row
   pointers when the source descriptor proves the layout, starting with
   `DATE_YEAR` over DATE.
8. Split the final projection/grouped-aggregate boundary so only grouped keys are
   compactly projected and payload columns are read from the live input.
9. Remap fused typed DECIMAL payload sources through final-projection references
   for Q9-like generated payloads.
10. Route split payload updates through DuckDB-owned grouped aggregate helpers
    so state tuple layout and mutation stay out of SLJIT.

Verified forced/profile evidence after the row-pointer RHS generated-source fix:

- `projection.direct_between_join_projection=319404` remains.
- `projection.direct_second_join_projection=319404` appears.
- `projection.direct_rhs_row_pointer_generated_projection=319404` appears.
- `hash_join_probe.direct_row_pointer_reference=638808`, covering both joins.
- `hash_join_probe.final_output` disappears for the Q9 fused region.
- `hash_join_probe.projection_source` disappears for the covered region.
- `projection.copied_post_join_projection` disappears for the covered region.
- `projection.copied_post_join_batch` disappears for the covered region.
- `projection.direct_post_join_batch_projection=958212` records the replacement
  path.
- `projection.direct_remap_post_join_batch_projection=319404` records the final
  compact group-key projection in the split-payload route.
- `aggregate_update.direct_projected_group_payload_update=319404` records the
  final projection to grouped aggregate split route.
- `aggregate_update.projected_group_payload_update=319404` records the rows that
  no longer build an aggregate payload batch.
- `op3:projection.post_join_direct_computed_projection` traced median improved
  from about `1972 us` to about `1849 us` over three forced/profile repeats
  after inlining finite DATE year extraction.
- Final projection expression telemetry now reports, for example,
  `op4:projection.direct_batch_expression.string_compress` and
  `op4:projection.direct_batch_expression.integral_compress`.
- Correctness diff is 0.

Latest full-shape checkpoint after variable-width direct projection:

- The full SF10 Q9 candidate is backend-fusable and fully fused by lowering:
  `operator-projection-projection-operator-projection-projection-projection-projection-sink`.
- Backend native summary is
  `table-scan:1|projection:6|hash-join:2|hash-group-by:1`.
- The CBO rule `scan_filtered_narrow_two_join_grouped_aggregate` was stale: it
  required exactly two generated stages and no reference-`VARCHAR` projection.
  The current Q9 candidate has four generated stages and three intermediate
  reference-`VARCHAR` projections while the final grouped keys are compressed
  fixed-width.
- The rule now admits Q9-like facts through descriptor facts, not query number:
  `generated_stage_count >= 2`, high expression cost for reference-`VARCHAR`
  projection chains, two native joins, one grouped aggregate, no source-filter
  stage, no sort, and no wide string grouped aggregate.
- Verification:
  `make reldebug -j12`, focused Q9/Q3/Q20 API tests, and
  `python3 benchmark/jit/verify_jit_architecture.py` pass.
- Q9 SF10 profile after the stale CBO cleanup was correct but slower:
  vectorized/off `1.635336s`, auto `1.992021s`, speedup `0.820943`,
  correctness diff 0.
- Runtime counters named the regression:
  `hash_join_probe.final_output=3261613`,
  `hash_join_probe.projection_source=3261613`,
  `projection.copied_post_join_projection=3261613`, and
  `projection.direct_post_join_computed_projection=6523226`.
- After the shared variable-width expression-to-batch fix, Q9 SF10 profile is
  correct and faster in the traced run:
  vectorized/off `1.607866s`, auto `1.499500s`, speedup `1.072268`,
  correctness diff 0.
- The fused Q9 runtime counters now prove the deleted boundary:
  `projection.direct_between_join_projection=3261613`,
  `hash_join_probe.direct_row_pointer_reference=6523226`,
  `projection.direct_post_join_reference_projection=6523226`,
  `projection.direct_post_join_computed_projection=6523226`, and no first-join
  `hash_join_probe.final_output` or `hash_join_probe.projection_source`.
- Q9 SF10 production over 5 repeats measured vectorized/off `1.609s` median
  versus auto `1.347s` median, speedup `1.194506`, correctness diff 0.
- The intended later route still runs:
  `projection.direct_rhs_row_pointer_generated_projection=3261613`,
  `projection.direct_second_join_projection=3261613`, and
  `aggregate_update.direct_projected_group_payload_update=3261613`.
- The final compressed group-key passthrough now deletes the final
  `STRING_COMPRESS(STRING_DECOMPRESS(nation))` recomputation for the Q9 fused
  region. Q9 SF10 trace records
  `projection.direct_between_join_compressed_passthrough_projection=3261613` and
  `projection.direct_batch_passthrough_projection=3261613`; the final
  `op4:projection.direct_batch_expression.string_compress` stage disappears and
  `op4:projection.post_join_direct_remap_batch_projection` drops from about
  `19.4 ms` to about `10.3 ms`.
- Q9 SF10 production after this deletion stayed correct but did not improve
  beyond noise: 5 repeats measured vectorized/off `1.606s` median versus auto
  `1.359s` median, about `1.18x`, compared with the prior auto checkpoint around
  `1.347s`. Treat the passthrough as structural groundwork, not the root Q9 win.
- The follow-up early compressed-key skip proved the missing value-lifetime fact:
  the final recompress deletion was too late because the route still
  materialized decompressed `nation` in the between-join batch. The direct route
  now preflights payload and probe-key dependencies, carries the compressed
  group-key source early, skips the decompressed between-join projection when it
  is unused, and records
  `projection.direct_between_join_compressed_group_key_skip_projection=3261613`.
  Q9 SF10 trace dropped `op1:projection.post_join_direct_computed_projection`
  from about `96-99 ms` to about `0.53 ms`.
- Q9 SF10 production after the early skip stayed correct and moved beyond the
  passthrough-only checkpoint: 5 repeats measured vectorized/off `1.722s` median
  versus auto `1.300s` median, speedup `1.324615`, correctness diff 0. Compared
  with the previous auto median around `1.359s`, the JIT route improved by about
  4%.
- Post-change xtrace Time Profiler produced empty CPU-sample bundles on this
  macOS run, but xtrace's automatic `sample` fallback over an eight-query Q9 loop
  produced useful stacks. `SljitNativeStringDecompress` no longer appears there;
  the active DuckDB frames are regular hash join probe variants, hash-table build
  insertion, scan decompression/filtering, and grouped aggregate lookup/update.

Root cause:

- CBO was stale first; after that was fixed, runtime route selection became the
  real blocker.
- The first join direct between-join projection initially handled the older
  one-projection shape, but the current full Q9 shape has a two-projection chain
  between joins.
- The runtime ended first-join value lifetime at a materialized
  output/projection-source chunk because the fixed-only direct computed path
  could not own the `STRING_DECOMPRESS` `VARCHAR` result.
- This is not a query-name problem. The fix is a descriptor/shape route for a
  chain of post-join projections that preserves row pointers and selected values
  until the next join or aggregate consumes them, with variable-width generated
  projection handled by the same expression-to-batch helper.

The synthetic perfect-hash coverage case exposed a separate edge: selection-only
probe mode was being ignored by the perfect-hash fallback path. The fix is to
dispatch fallback materialization by actual hash-table layout and record
selection-only perfect probes as `hash_join_probe.direct_selection_reference`.

Production checkpoint after the split-payload final aggregate fix:

- Focused Q9 SF1 forced production over 7 repeats measured `0.144s`
  to `0.145s` vectorized/off versus `0.115s` auto, or about `1.26x`,
  correctness diff 0.
- Focused Q3/Q9/Q20 SF1 forced production over 3 repeats stayed correct:
  Q3 `0.062s -> 0.051s`, Q9 `0.144s -> 0.115s`, Q20 `0.065s -> 0.047s`.
- The split-payload fix is structurally correct: final aggregate payload batch
  construction is gone for the covered route. The remaining Q9 runtime is now
  dominated by pair-key regular probing, between-join reference projection, and
  grouped aggregate hash/probe work over the compact key batch.
- A generic adjacent-key aggregate state-target reuse experiment showed the
  right principle but not a Q9 root win: Q9 does not expose enough consecutive
  group-key locality under trace to justify an always-on hot-loop check. The
  current route samples per chunk before enabling the reuse and leaves the next
  root deletion as hash/state-target ownership across final projection and
  grouped aggregate lookup.
- The compact group-key split route now carries precomputed group hashes from
  projection into grouped aggregate lookup. The aggregate-side
  `find_or_create_fast.hash` stage is gone for covered Q9/Q20 split-payload
  routes and replaced by `projection.post_join_direct_remap_batch_projection_hash`.
  Q9 also uses an existing-only split-payload update after the first new-group
  batch: forced/profile showed `direct_existing_split_payload.update=163` and
  one mixed `direct_new_split_payload.append` batch at SF1. This is a correct
  value-lifetime cleanup, but not the Q9 root win by itself: SF10 Q9 stayed near
  the noise threshold at about `1.05x`, while Q20 SF10 stayed strong at about
  `1.44x`.

Next root fixes, in order:

1. Delete or carry the remaining regular hash join probe ownership boundary:
   probe once, keep row pointers and match selection live as descriptors, and
   feed downstream projection/grouped lookup without re-reading or re-packing
   facts that are still live.
2. Attack the measured regular hash join probe loop cost where it survives the
   descriptor cleanup:
   `ExecuteSelectedAllValidRegularHashJoinProbeVariant`,
   `ExecuteFlatAllValidRegularHashJoinProbeVariant`, pair-key hash-table lookup,
   bloom checks, and pointer chasing dominate the remaining generated Q9 region.
   Helper-level churn is not enough; the retained aliasing cleanup was useful
   but small.
3. Carry or reuse grouped hash/state-target facts across final projection and
   grouped aggregate lookup so lookup/update does not reload facts that are still
   live.
4. Keep compressed `nation`, compressed year, payload source descriptors, and
   state target live until payload update finishes. The decompressed `nation`
   column is now skipped for the covered Q9 route; the broader descriptor rule
   must generalize without a query-number branch.
5. Separate source-scan floor from JIT-region work. Post-change sample stacks
   still show scan decompression/filtering outside the generated region, so Q9
   cannot be judged only by generated-stage timers.
6. Preserve vector-sized calls unless a deeper state-pointer route demonstrably
   avoids enough memory traffic to pay for a different batching contract.
7. Keep the Q20 lesson generic: probe-side group keys and row-pointer group keys
   are both descriptor sources, and primitive payloads should stay in the live
   pending input when only compact group keys need projection.

Required contract for the next Q9 slice:

- Build descriptors for projection chains between joins and for the final
  projection result, not query-name branches.
- The first join row pointers must remain live through all supported
  between-join projections until the second join consumes them.
- Keep compressed `nation`, compressed year, DECIMAL payload, and selection facts
  as values until grouped aggregate lookup/update consumes them. For covered Q9,
  the decompressed `nation` projection is already skipped when payload/probe-key
  dependency checks prove it is dead.
- Let DuckDB-owned grouped aggregate helpers own hash-table layout, append,
  duplicate-new resolution, null/cast correctness, and state tuple mutation.
- Record a specific unsupported descriptor fact before falling back to aggregate
  batch construction or first-join output materialization.
- Do not broaden production CBO policy from this slice until Q9 production
  repeats and Q3/Q9/Q20 adjacent smokes stay correct with stable counters.

Q20 descriptor checkpoint:

- The old `direct_row_pointer_grouped_lookup_update_unsupported.group_key_sources`
  reason is removed for the covered Q20 route.
- Q20 now uses compact group-key remap plus primitive split-payload update:
  `aggregate_update.direct_projected_group_payload_update=9741`,
  `projection.direct_remap_post_join_batch_projection=9741`, and
  `aggregate_update.projected_group_payload_update=9741` in SF1 forced/profile.
- The covered route no longer builds a full aggregate input payload batch and
  does not report `hash_join_probe.final_output`,
  `hash_join_probe.projection_source`, copied post-join projection/batch, or
  `aggregate_update.direct_state_update`.

## Milestone 1: Observability And Policy Facts

Goal: every performance claim must be explainable from counters, events, profiler
output, benchmark CSVs, and shape inventory.

Status: mostly complete. Keep extending this only when new runtime facts are
added.

Required facts:

- CBO rule names are counter keys, not only output columns.
- CBO selection reasons survive every planner path.
- Backend capability facts are born from lowering/finalized plans, not parsed
  from strings.
- Runtime path counts identify which branch executed.
- Materialization boundary counts identify how many rows crossed the boundary.
- Shape inventory has a first-class `no_jit_decision` class for zero-counter
  query/policy pairs.

Exit criteria:

- Every query/policy pair is classified as `proven_win`, `correctly_skipped`,
  `cbo_limited`, `backend_limited`, `runtime_limited`, or `no_jit_decision`.
- Forced/profile losses name the boundary or runtime path that caused the loss.
- SF10 evidence is not replaced by SF1 guesses.
- Trace timings are used as stage evidence, not final performance claims.

## Milestone 2: Runtime Variant Refactor

Goal: stop multiplying hand-written branch ladders as broader query support is
added.

Status: in progress.

Implementation direction:

- Introduce one shared value-lifetime contract for join probe, projection, and
  grouped aggregate update.
- Keep runtime variant choice trait-selected:
  - regular hash probe
  - post-join projection
  - grouped aggregate lookup
  - grouped aggregate payload update
- Fixed-width type support must extend common helpers, not copy a route.
- Stable telemetry names must remain stable; removed boundaries should disappear
  from counters rather than being renamed.

Core descriptors:

- `JitValueRef`: source vector, selection index, row pointer, generated temporary,
  hash value, aggregate state pointer, or materialized vector.
- `JitStageFacts`: selection shape, validity shape, fixed-width support,
  row-pointer availability, aggregate-state-pointer availability, and required
  materialization.
- `JitFixedWidthDescriptor`: type id, physical width, validity proof, cast policy,
  hash/compare/load/store support.

Exit criteria:

- Adding a fixed-width type does not copy a join probe or aggregate loop.
- Existing API tests still assert intended fast-path events.
- Runtime loop code decreases or replaces duplicated control flow.
- Q1/Q3/Q5/Q9/Q14/Q19/Q20/Q22 focused smokes do not regress.
- No obsolete compatibility branch remains without a counter proving it can fire.

## Milestone 3: Join-To-Aggregate Dataflow Fusion

Goal: make Q3/Q9/Q20-family pipelines keep row references, selections, computed
payloads, and aggregate state pointers live across join output, projection, and
grouped aggregate update.

Completed:

- Direct fixed-width post-join projection into the pending aggregate input batch.
- Full one-join regular hash output materialization removed for the grouped
  aggregate path.
- Q3 fixed-width computed post-join projection removes
  `hash_join_probe.projection_source`.
- Sparse probe-output buffering preserves vector-sized downstream calls.
- New and existing fixed-width payload routes use selected state-address batches.
- Q3 row-pointer grouped lookup/update avoids the old projection-fed group-key
  path and reuses duplicate consecutive state targets.
- Q9 first join output boundary is deleted through direct between-join projection,
  including all-valid `VARCHAR` reference outputs.

Remaining root work:

- Fuse the remaining Q9 final projection and grouped aggregate state/update
  boundary so the aggregate batch is not the value-lifetime endpoint.
- Generalize the Q3 row-pointer grouped lookup lesson only through shared
  fixed-width descriptors.
- Explain and remove Q20's unsupported group-key-source reason without adding a
  Q20-shaped special case.
- Keep aggregate hash-table probing, appending, duplicate-new resolution, and
  tuple layout centralized in DuckDB runtime code.
- Avoid building group-vector representations when fixed-width row-pointer key
  descriptors can feed centralized hash/compare/append helpers.

Exit criteria:

- Q3 forced/profile does not report `hash_join_probe.final_output`,
  `hash_join_probe.projection_source`, or copied post-join projection boundaries
  for the covered region.
- Q9 forced/profile reports no second-join `hash_join_probe.final_output` and
  no second-join projection-source or copied-batch boundaries for the covered
  region.
- Q9 computed RHS projections read supported fixed-width values directly from
  row pointers or report the exact unsupported source that still forces a gather.
- Q9 final projection to grouped aggregate either keeps keys/payload/state target
  live or reports the exact descriptor/state fact that still forces aggregate
  batch construction.
- Q20 either enters the shared row-pointer grouped lookup route or reports a
  specific descriptor fact that is still missing.
- Aggregate update invocation counts stay vector-batched.
- At least one SF10 grouped join query improves beyond noise before CBO
  broadening.

## Milestone 4: Grouped Lookup And Update ABI

Goal: lower grouped aggregate lookup/update enough that generated payload updates
run while the state pointer is live, without exposing tuple layout to SLJIT.

Current rule:

- DuckDB owns lookup, append, duplicate-new resolution, tuple layout, hashing,
  comparison, and null/cast correctness.
- SLJIT can receive compact selected source rows and state-address spans.
- SLJIT must not learn aggregate tuple layout.

Next implementation:

- Split row-pointer grouped lookup/update into visible phases:
  - prevalidate key/null/cast facts
  - fill and hash fixed-width keys
  - probe pointer table
  - reuse duplicate consecutive targets
  - compact new keys
  - append new group tuples
  - update selected payloads
- Keep useful key caches when they improve locality.
- Remove repeated probes after the target is already known.
- Reject or fallback before mutation when a null/cast/layout fact is unsupported.

Exit criteria:

- Covered paths do not require `aggregate_update.address_vector_resolve`.
- Covered fixed-width new/existing paths do not require address-buffer callbacks.
- No per-row C++ callback appears in the hot path.
- Stage telemetry separates fill/hash, probe, duplicate reuse, append, and payload
  update.
- Repeated forced/profile runs show stage-level improvement larger than noise.

## Milestone 5: Fixed-Width Type Coverage

Goal: support TPC-H types through one shared data path.

Priority order:

1. `BIGINT`
2. `INTEGER`
3. `DATE`
4. `DECIMAL(15,2)`
5. all-valid `VARCHAR` references
6. controlled `VARCHAR` predicates
7. `VARCHAR` grouping only after string materialization cost is owned

Shared helper coverage:

- fixed-width load/store
- validity check or all-valid proof
- selection load or identity-selection proof
- checked casts required by TPC-H shapes
- hash and compare
- aggregate state update
- materialization only when unavoidable

Exit criteria:

- DATE reuses INT32-compatible helpers where semantics allow it.
- DECIMAL support covers arithmetic and comparison patterns used by TPC-H.
- Tests cover nullability, selected vectors, decimal overflow boundaries, and
  mixed key distributions.
- Benchmarks show type support deletes real work, not only replaces vectorized
  loops with generated scalar code.

## Milestone 6: Regular Hash Join And Multi-Join Ownership

Goal: make regular-hash-heavy query families faster by owning probe and downstream
dataflow.

Current evidence:

- Perfect-hash chains can win with narrow rules.
- Regular-hash chains need backend ownership of downstream boundaries.
- Selected all-valid one-key/two-key probe helpers are useful groundwork but not
  sufficient by themselves.
- Q7/Q8/Q21 should stay skipped unless production medians prove a real win.

Implementation direction:

- Extend trait-selected regular hash probe helpers without cloning branch ladders.
- Keep row pointers, match selection, and projected payload values live into the
  next owned stage.
- Add local reject paths only when build-side facts fund them, as in Q9 pair-key
  bloom.
- Avoid admitting regular-hash-heavy multi-join families until downstream
  materialization and grouped lookup are owned.

Exit criteria:

- Forced/profile traces show fewer rows in `hash_join_probe.final_output` or
  copied post-join boundaries.
- New wins cite the exact probe, projection, and aggregate boundaries deleted.
- Default policy does not compile Q7/Q8/Q21 until production medians prove a win.

## Milestone 7: CBO Broadening From Facts

Goal: admit broader query shapes only after backend capability facts, runtime
counts, boundary counts, and production medians agree.

Implementation direction:

- Convert materialization-boundary counters into negative controls.
- Add named CBO rules only for measured query families.
- Require negative controls for every new rule.
- Keep forced/profile settings separate from default auto policy.
- Carry sort sink, scan-filter, join layout, grouped lookup, width, validity, and
  type facts into rule predicates when those facts explain prior wins or losses.

Exit criteria:

- Each admission rule cites capability facts, runtime path counts, boundary counts,
  and production benchmark evidence.
- Known losing families stay skipped:
  - string-heavy grouped aggregates
  - wide `VARCHAR` grouping
  - regular-hash chains without owned downstream batching
  - copied post-join batches
  - small stateful projections feeding sort sinks
- Full TPC-H SF10 production medians improve on more than one query family.
- Correctness diff is 0 for every benchmark claim.

## Milestone 8: SIMD After Scalar Fusion

Goal: benchmark SIMD against scalar fused execution, not against an old
materializing pipeline.

Implementation direction:

- Inspect generated scalar loops after Milestones 3-5.
- Add SIMD only for stable fixed-width loops with clear selection and validity
  facts.
- Prioritize aggregate payload loops and projection payload loops where arithmetic
  density is high and memory movement has already been reduced.
- Keep SIMD paths behind the same trait/fact surface as scalar paths.

Exit criteria:

- SIMD beats scalar fused execution for the same region.
- Benchmark output separates instruction-level gains from deleted-boundary gains.
- SIMD does not hide unresolved materialization or state lookup costs.

## Milestone 9: GPU After Residency

Goal: make Metal useful only where the GPU can run at profitable granularity.

Implementation direction:

- Batch many DuckDB vectors into one Metal launch.
- Reuse buffers and command state.
- Avoid immediate host copyback.
- Keep data resident across fused stages where possible.
- Charge launch, transfer, setup, synchronization, copyback, and rows-per-launch
  costs in the CBO.

Exit criteria:

- GPU wins on high arithmetic-intensity or resident high-throughput shapes.
- Low-intensity projections stay on CPU/vectorized/SLJIT.
- The CBO has a real per-batch GPU cost before default admission.

## Immediate Implementation Order

1. Profile the post-skip Q9 route with CPU sampling and runtime counters before
   writing the next optimization. If Time Profiler captures no samples, use the
   xtrace fallback sample report and record that limitation.
2. Attack regular hash join probe ownership first: specialize the selected and
   flat all-valid variants around the measured pair-key and one-key layouts,
   hash-table salt/bloom facts, and row-pointer output needs.
3. Carry grouped hash/state-target facts across final projection and grouped
   aggregate lookup without exposing DuckDB tuple layout to SLJIT.
4. Keep unsupported projection, probe, and aggregate nodes visible through exact
   fallback counters: unsupported source lifetime, type, validity, generated
   expression, probe-key dependency, payload dependency, or required
   materialization.
5. Run `make reldebug -j12`, focused Q9/Q3/Q20 API tests, and
   `python3 benchmark/jit/verify_jit_architecture.py`.
6. Run Q9 forced/profile at SF1 and SF10 with runtime counters, then production
   repeats. Correctness diff must be 0 and medians must improve beyond noise
   before claiming the route as a win.
7. Run adjacent Q3/Q9/Q20 focused production smokes and at least one broader
   SF10 grouped-join smoke before broadening CBO policy.
8. Extend fixed-width type support only when it removes a measured boundary.
9. Revisit SIMD after scalar fusion has made compute visible.
10. Revisit GPU only after residency and cost accounting exist.

## Verification Gate

Before claiming a milestone complete:

1. Build with `make reldebug -j12`.
2. Run focused tests for the touched JIT surface.
3. Run `python3 benchmark/jit/verify_jit_architecture.py`.
4. Run full `build/reldebug/test/unittest --print-failing-tests` unless the change
   is documentation-only.
5. Run targeted production benchmarks with enough repeats for the changed query
   family.
6. Run broader TPC-H SF10 smoke after CBO or runtime-admission changes.
7. Require correctness diff 0.
8. Treat trace timings as stage evidence, not final performance claims.
9. Treat zero-compiled timing movement as planning overhead or noise unless it
   repeats and has a specific root cause.

## Definition Of Done

A JIT performance slice is done only when all of these are true:

- the deleted work is named in boundary counters
- the replacement path is named in runtime path counters
- correctness diff is 0
- update/probe invocation counts remain batched
- focused tests lock the intended route
- production medians improve beyond noise
- adjacent query-family smokes do not regress
- the code path is shared enough that the next type/query does not copy it

The plan is aggressive, but the aggression is aimed at deleting real work:
post-join final output, projection-source chunks, copied batches, address buffers,
state-address vectors, repeated probes, repeated selection/validity work, and
unowned materialization.
