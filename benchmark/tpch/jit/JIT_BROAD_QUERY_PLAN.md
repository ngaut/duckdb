# JIT Broad Query Root Plan

Last updated: 2026-07-04
Last verified base: `7b59f15ed6` plus current working-tree JIT changes
Branch: `codex/jit-native-duckdb-core`

Status: active root plan. This file is the execution plan for making more query
families faster with SLJIT. It is intentionally stricter than "make more things
compile": a milestone is not complete until it deletes named work, preserves
correctness, passes the verification gate, and improves production medians beyond
noise.

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
- The last Q3-shaped grouped find-or-create branch, hard-coded for
  `[INT32, INT32, INT8]`, is removed. Q3 now uses the same descriptor-driven
  grouped lookup loop as the broader fixed-key path, and xtrace did not show the
  generic comparer becoming a new dominant cost.
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
- Backend native shape is
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
- The selected state-address ABI is now consistent across generated and
  primitive grouped updates: when an execute selection is present,
  `address_sel` is a logical-row map. The mixed split-payload fallback now uses
  `TryFindOrCreateGroupsSelectedStateUpdateFast` instead of the row-update
  callback, and the stale unused duplicate-position scratch selection is gone.
  SF1 forced/profile records `find_or_create_fast.selected_state_update` under
  the mixed `direct_new_split_payload.append` path for Q9/Q20 while preserving
  precomputed group hashes and correctness. Production Q3/Q9/Q20 over three
  repeats stayed positive: Q3 `0.062s -> 0.052s`, Q9 `0.144s -> 0.118s`, Q20
  `0.063s -> 0.044s`, all with correctness diff 0.
- The remaining direct primitive grouped find-or-create route now uses the same
  selected state-address ABI, so `TryFindOrCreateGroupsUpdateFast` and the
  row-update arm inside `TryFindOrCreateGroupsFastInternal` are deleted. The
  helper now has two outcomes only: fill an address vector or emit selected
  state-address spans for existing, new, and duplicate-new groups.
- The append-only primitive grouped route now uses the existing state-address
  append callback as well. `TryAppendNewGroupsUpdateFast`, the
  `ExecutionGroupedAggregateStateRowUpdateFunction` typedef, and the row-update
  arm inside `TryAppendNewGroupsFastInternal` are deleted. The append helper now
  either fills an address vector or appends new groups and emits a state-address
  span through DuckDB-owned tuple layout.
- Verification for this ABI deletion:
  `make reldebug -j12`, `[api][jit]` with the append test now requiring
  `find_new.state_address_update`, `python3 benchmark/jit/verify_jit_architecture.py`,
  `build/reldebug/test/unittest --print-failing-tests`, forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_append_state_address_profile`, and production
  Q3/Q9/Q20 in `/private/tmp/duckdb_jit_append_state_address_prod` all passed
  with correctness diff 0. Production medians stayed positive: Q3
  `0.063s -> 0.051s`, Q9 `0.142s -> 0.118s`, Q20 `0.064s -> 0.044s`.
- Direct Time Profiler xtrace over repeated Q9 auto is saved at
  `/tmp/trace_duckdb_20260626_163015.trace` with 5,356 samples. It sampled the
  `duckdb` process directly; top frames still include `ht_entry_t::GetValue`,
  `BloomFilter::GetMask`, `ht_entry_t::ExtractSalt`,
  `ExecuteAllValidInt64PairNoChainProbe`, `JoinHashTable::Finalize`, and scan
  decompression/filtering. This confirms the append ABI cleanup is not the next
  limiting frame; Milestone 6 regular probe/build ownership remains the larger
  root target.
- Milestone 6 probe cleanup has started by deleting duplicated flat/selected
  regular hash probe ladders rather than adding another fast path. The flat and
  selected all-valid no-chain predicates now share
  `SljitHashJoinCanUseAllValidNoChainProbe`; the flat and selected pair-chain
  predicates share `SljitHashJoinCanUseAllValidPairChainProbe`; and the
  flat/selected two-key no-chain and chain wrappers are trait-selected template
  instantiations. Telemetry stage names are unchanged, so old evidence still
  maps to the same runtime path counters.
- Verification for that probe-shape cleanup:
  `make reldebug -j12`, `[api][jit]`,
  `python3 benchmark/jit/verify_jit_architecture.py`, direct verification-setting
  smoke, forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_probe_template_profile`, and production Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_probe_template_prod` passed with correctness diff 0.
  Production medians stayed positive: Q3 `0.062s -> 0.051s`, Q9
  `0.142s -> 0.118s`, Q20 `0.065s -> 0.045s`.
- A direct DuckDB Time Profiler run for the current Milestone 6 checkpoint is
  saved at `/tmp/trace_duckdb_jit_probe_template_q9_file_20260626_164548.trace`
  with 2,647 samples. It shows the remaining root cost is still regular
  DuckDB hash join work: `JoinHashTable::Probe` is about 33.7% total,
  `ProbeForPointers` is about 20.2% self, `JoinHashTable::Hash` about 5.9%,
  scan/filtering about 28%, sink/build about 9.6%, and hash-join finalize about
  8.1%. This cleanup reduces branch sprawl and salt/bloom duplication; it does
  not complete Milestone 6.
- The first core DuckDB probe cleanup now removes the `AddPointerToCompare`
  helper from `ProbeForPointersInternal`, hoists the entry pointer and result
  vector bases, compares stored salt bits directly in the collision walk, and
  folds salted/unsalted candidate writes back into one append block. The follow-up
  direct selection-buffer write removes hot-loop `SelectionVector::set_index`
  calls for the compare and match selections while preserving the same
  `RowMatcher` row-index contract.
  Verification passed for `make reldebug -j12`, `[api][jit]`, `test/sql/join`,
  the architecture verifier, forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_probe_folded_profile`, and production Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_probe_folded_prod`; production medians stayed positive
  at Q3 `0.062s -> 0.052s`, Q9 `0.144s -> 0.119s`, and Q20
  `0.064s -> 0.045s`, all correctness diff 0.
- Direct Time Profiler after the core probe cleanup is saved at
  `/tmp/trace_duckdb_jit_probe_core_q9_20260626_165548.trace` with 2,640
  samples. A trace diff against the prior direct Q9 profile shows the intended
  local movement, not a root win: `IncrementAndWrap` self samples dropped about
  1.0 percentage point and `ht_entry_t::ExtractSalt` dropped about 0.8 points,
  while `ProbeForPointers` remains dominant and sampled at about 22.7% self.
  The next root target is still reducing regular hash probe/match lifetime, not
  adding another local helper.
- Direct Time Profiler after direct selection writes is saved at
  `/tmp/trace_duckdb_jit_probe_selwrite_q9_20260626_170505.trace` with 2,618
  samples. Diffing it against the core-probe trace shows
  `SelectionVector::set_index` disappeared from the top sampled functions, but
  `ProbeForPointers` remained about 22.7% self. This confirms the edit removed
  local setter overhead without changing the Milestone 6 root blocker.
- Direct Time Profiler after folding the candidate append and direct salt-bit
  compare is saved at
  `/tmp/trace_duckdb_jit_probe_folded_q9_20260626_172141.trace` with 2,663
  samples. Diffing it against the direct-selection trace shows the sampled lambda
  frame is gone and `ProbeForPointers` is effectively unchanged at about 22.5%
  self. This is retained as source and instruction-shape cleanup, not a root
  probe/dataflow win.
- The next Milestone 6 cleanup now gives the one-join and two-join pending probe
  paths one implementation for selected probe batch append:
  `SljitAppendSelectedProbeBatch`. The edit deletes the duplicated local copy,
  row-pointer copy, cardinality update, and telemetry blocks while leaving each
  path's capacity and flush decisions local. Final-build verification passed for
  `make reldebug -j12`, `[api][jit]`, `test/sql/join`, the architecture
  verifier, forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_pending_probe_helper_final_profile`, and production
  Q3/Q9/Q20 in `/private/tmp/duckdb_jit_pending_probe_helper_final_prod`.
  Production medians stayed positive and correct at Q3 `0.062s -> 0.052s`, Q9
  `0.141s -> 0.119s`, and Q20 `0.063s -> 0.044s`, all correctness diff 0.
  The Q9 runtime path and boundary counters stayed in the same shape:
  `hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_no_chain=2931`,
  `hash_join_probe.fast_regular_probe_flat_all_valid_single_key_no_chain=164`,
  and the same direct row-pointer/reference/projection/group-payload boundaries.
  Direct Time Profiler for this final build is saved at
  `/tmp/trace_duckdb_jit_pending_probe_helper_final_q9_20260626_173631.trace`
  with 2,638 samples. Diffing against the folded-probe trace keeps
  `ProbeForPointers` in the same dominant band (`22.5% -> 23.4%` self, sampling
  noise), so this is retained as duplicated-boundary implementation cleanup, not
  a root Milestone 6 performance win.
- The current Milestone 6 row-pointer descriptor slice removes the covered Q9
  final compact group-key remap and projected group-payload aggregate update.
  Final projection group-key facts are now described as row-pointer/input-vector
  descriptors, including `STRING_COMPRESS` for `nation` and `INTEGRAL_COMPRESS`
  for `o_year`, and DuckDB-owned grouped aggregate helpers consume those
  descriptors while the primitive DECIMAL payload remains in the live input.
  The replacement path is generic descriptor plumbing, not a Q9 branch.
  Q9 forced/profile now reports
  `aggregate_update.direct_row_pointer_grouped_lookup_update=164` and
  `aggregate_update.row_pointer_grouped_lookup_update=319404`; the old
  `projection.direct_remap_post_join_batch_projection` and
  `aggregate_update.projected_group_payload_update` boundaries are gone for this
  route. The descriptor fill loop is specialized by source/cast kind so the
  large Q9 events keep `find_or_create_descriptor_keys.fill` near
  `0.29-0.33 ms` instead of making the deleted projection boundary reappear as a
  slower generic row loop.
  Verification passed for `make reldebug -j12`, `[api][jit]`, `test/sql/join`,
  the architecture verifier, forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_row_pointer_descriptor_profile`, and comparable
  no-trace production Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_row_pointer_descriptor_prod_notrace`. Production
  medians stayed positive and correct at Q3 `0.063s -> 0.051s`, Q9
  `0.139s -> 0.117s`, and Q20 `0.063s -> 0.044s`, all correctness diff 0.
  Direct Time Profiler over a repeated Q9 file is saved at
  `/tmp/trace_duckdb_20260626_181554.trace` with 29,929 samples. It shows the
  remaining root is still regular hash probe work: `ProbeForPointers` is about
  `27.7%` self, `TemplatedSignHashFunction` about `5.1%`,
  `ht_entry_t::ExtractSalt` about `4.5%`, with scan/filtering and grouped-update
  support frames below that. Therefore this slice is a real boundary deletion,
  but it does not complete Milestone 6.
- The follow-up probe-state cleanup removes stale state from the same regular
  hash join object: `ProbeState` no longer inherits the build-only salt vector,
  `InsertState` owns that vector directly, and the unused `non_empty_sel` probe
  selection is gone. This is retained as state ownership cleanup, not as a root
  probe-loop win. Verification passed for `make reldebug -j12`, `[api][jit]`,
  `test/sql/join`, the architecture verifier, and no-trace production Q3/Q9/Q20
  in `/private/tmp/duckdb_jit_m6_probe_state_cleanup_prod_notrace`; medians
  stayed positive and correct at Q3 `0.059s -> 0.051s`, Q9 `0.134s -> 0.114s`,
  and Q20 `0.061s -> 0.045s`, all correctness diff 0.
	- The next probe continuation cleanup deletes `ProbeState::ht_offsets_and_salts_v`.
	  Continuation tokens now live in `hashes_dense_v`: the first unselected probe
	  pass stores them at the original row slot, and selected continuation passes use
	  a compact compare-row map only where RowMatcher's original-row
	  contract requires it. This removes the extra per-row continuation vector and
  the stale helper wrapper, while keeping RowMatcher indexed by original probe
  rows. Verification passed for `make reldebug -j12`, `[api][jit]`,
  `test/sql/join`, the architecture verifier, and `git diff --check`.
  Forced/profile Q3/Q9/Q20
  in `/private/tmp/duckdb_jit_m6_probe_continuation_split_profile` stayed
  correct; profile medians were Q3 `0.061068s -> 0.060205s`, Q9
  `0.135810s -> 0.135843s`, and Q20 `0.064099s -> 0.061425s`. No-trace
  production Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_probe_continuation_split_prod_notrace` also
  verified with correctness diff 0; medians were neutral at Q3
  `0.060s -> 0.060s`, Q9 `0.133s -> 0.134s`, and Q20 `0.061s -> 0.061s`.
  A short repeated-Q9 Time Profiler trace
  `/tmp/trace_zsh_20260626_184042.trace` has 5,057 samples and keeps
  `ProbeForPointersInternal<true,false>` as the largest C++ self frame at
  `23.9%` (`ht_entry_t::ExtractSalt` `3.4%`). The longer 29s trace
  `/tmp/trace_zsh_20260626_184210.trace` is mostly unsymbolicated generated code,
  so it is not used for C++ frame attribution. This cleanup is worth keeping as
  state deletion, but the neutral production result means it is not a Milestone 6
  root win.
- A follow-up packed-entry-value micro-edit was rejected. Carrying the raw hash-table
  entry value through the salted probe loop lowered the short repeated-Q9 trace frame
  from `23.9%` to `22.4%`, but production Q9 failed the verifier at
  `0.978571x` in
  `/private/tmp/duckdb_jit_m6_probe_entry_value_final_prod_notrace`. The edit was
  reverted. The reverted-state production smoke in
  `/private/tmp/duckdb_jit_m6_probe_entry_value_reverted_prod_notrace` verified with
  correctness diff 0 and medians Q3 `0.063s -> 0.062s`, Q9
  `0.138s -> 0.140s`, and Q20 `0.063s -> 0.064s`. Trace-only source shaping is
  not sufficient Milestone 6 evidence.
- The retained dense-probe-hash cleanup removes one more regular probe ownership
  boundary. The common no-null-filter `JoinHashTable::Probe` path now hashes
  directly into `ProbeState::hashes_dense_v` and probes via
  `GetRowPointersWithDenseHashes`, so it no longer allocates a temporary hash vector
  and then copies it back into probe-owned dense state. Precomputed and
  null-filtered hashes keep the existing densifier because their hash vectors are
  externally owned or sparse by original row id. Verification passed for
  `make reldebug -j12`, `[api][jit]`, `test/sql/join`, the architecture verifier,
  and `git diff --check`. Final-source profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_dense_probe_hash_flat_profile` verified with
  correctness diff 0; medians were Q3 `0.062534s -> 0.062283s`, Q9
  `0.139142s -> 0.141199s`, and Q20 `0.065517s -> 0.064241s`. Final-source
  no-trace production Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_dense_probe_hash_flat_prod_notrace` verified with
  medians Q3 `0.061s -> 0.062s`, Q9 `0.137s -> 0.139s`, and Q20
  `0.063s -> 0.063s`. The final-source repeated-Q9 trace
  `/tmp/trace_zsh_20260626_191125.trace` has 3,826 samples but is mostly
  unsymbolicated generated code, so it is not used for C++ frame attribution.
  Collapsed stack checks still showed the old
  `VectorBuffer::Copy`/`GetRowPointersInternal<true>` probe-copy path in 19 stacks
  before this cleanup and no matches after it. This is retained as boundary deletion,
  not as a Milestone 6 root win.
- The SLJIT fast regular probe helpers now compare salt bits directly instead of
  materializing packed pointer-mask salts with `ht_entry_t::ExtractSalt`.
  `SljitHashJoinFindFirstChainPointer` uses `GetSaltWithNulls`, and the pair-key
  and single-key no-chain helpers compare `entry_value & ht_entry_t::SALT_MASK`
  against `hash & ht_entry_t::SALT_MASK`. Verification passed for
  `make reldebug -j12`, `[api][jit]`, `test/sql/join`, the architecture verifier,
  `git diff --check`, and the removed-pragma smoke. No-trace production Q3/Q9/Q20
  in `/private/tmp/duckdb_jit_m6_sljit_probe_salt_bits_prod_notrace` verified
  with medians Q3 `0.062s -> 0.061s`, Q9 `0.138s -> 0.138s`, and Q20
  `0.063s -> 0.064s`. Profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_sljit_probe_salt_bits_profile` also verified; Q9
  measured `0.139591s -> 0.140836s`. The direct repeated-Q9 trace
  `/tmp/trace_zsh_20260626_192419.trace` has 2,535 samples but is mostly
  unsymbolicated generated code, so it is only used as a negative helper check:
  the collapsed stacks contain no `ExtractSalt` or `GetSaltWithNulls` frames, and
  only two named bloom-filter stacks. Keep this as instruction-shape cleanup, not
  as a Milestone 6 root win.
- The post-join RHS row-pointer gather paths now share one `GatherHashJoinRHSColumn`
  helper. Every supported projection/materialization path tries
  `ExecutionTryDirectGatherHashJoinRHSFixedColumn` first and falls back to
  `JoinHashTable::GatherRHSColumn` in one place, with the fallback hash-table
  dependency asserted there. Verification passed for `make reldebug -j12`,
  `[api][jit]`, `test/sql/join`, the architecture verifier, `git diff --check`,
  and the removed-pragma smoke. No-trace production Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_rhs_gather_helper_prod_notrace` verified with
  medians Q3 `0.061s -> 0.062s`, Q9 `0.136s -> 0.138s`, and Q20
  `0.062s -> 0.063s`. Profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_rhs_gather_helper_profile` also verified; Q9
  measured `0.139239s -> 0.141463s`. The useful repeated-Q9 xtrace is
  `/tmp/trace_zsh_jit_m6_rhs_gather_helper_q9_20260626_193620.trace` with 2,561
  samples over 3.30s; it names `ProbeForPointersInternal<true,false>` as the top
  frame at `23.16%` self and `ht_entry_t::ExtractSalt` at `3.79%`. Keep the helper
  as source deletion and direct-gather consistency only. It does not move the
  Milestone 6 root.
- `JoinHashTable` build/finalize now uses raw salt bits instead of packed
  pointer-mask salts. `ApplyBitmaskAndGetSaltBuild` writes
  `hash & ht_entry_t::SALT_MASK`, the build loop compares
  `entry.GetValue() & ht_entry_t::SALT_MASK`, and
  `src/execution/join_hashtable.cpp` no longer calls `ExtractSalt`, `GetSalt`, or
  `GetSaltWithNulls`. Verification passed for `make reldebug -j12`,
  `[api][jit]`, `test/sql/join`, the architecture verifier, `git diff --check`,
  and the removed-pragma smoke. Final 9-repeat no-trace production Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_join_build_raw_salt_bits_prod9_notrace` verified
  with medians Q3 `0.061s -> 0.062s`, Q9 `0.138s -> 0.139s`, and Q20
  `0.063s -> 0.063s`. Profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_join_build_raw_salt_bits_profile` also verified; Q9
  measured `0.141995s -> 0.143777s`. The final repeated-Q9 trace
  `/tmp/trace_zsh_jit_m6_join_build_raw_salt_bits_q9_20260626_194757.trace` has
  2,575 samples and is mostly unsymbolicated, but collapsed stacks contain zero
  named `ExtractSalt`, `GetSaltWithNulls`, `ProbeForPointers`, or
  `JoinHashTable::Finalize` frames. Keep this as source/instruction-shape cleanup,
  not as a Milestone 6 root win.
- The next probe scratch cleanup deletes `ProbeState::keys_to_compare_row_sel`.
  Selected regular probe continuation still needs original probe row identity for
  `RowMatcher`, but that map is now scratch in the unused tail of `match_sel`
  during each dense probe iteration instead of persistent probe state. This
  keeps previous matches intact, remaps no-match continuation tokens before
  writing new matches, and leaves flat probing indexed directly by row id.
  The cleanup exposed a real SF10 Q9 correctness bug: the row-pointer grouped
  descriptor path could read a second-join projection input column that had been
  intentionally omitted by the compressed/precomputed skip projection. The fix is
  a descriptor fact guard, `group_key_omitted_input`, so the invalid
  input-vector read is rejected and the existing sidecar/batch path consumes the
  live compressed key instead.
  Verification passed for `make reldebug -j12`, `[api][jit]`, `test/sql/join`,
  `python3 benchmark/jit/verify_jit_architecture.py`, `git diff --check`, and
  the isolated SF10 Q9 repro that previously crashed in `AggregateReverseMemCpy`.
  SF1 forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_fixed_profile` verified with
  medians Q3 `0.060700s -> 0.056383s`, Q9 `0.138486s -> 0.127730s`, and Q20
  `0.063615s -> 0.050615s`. SF1 no-trace production in
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_fixed_prod_notrace` verified
  with medians Q3 `0.060s -> 0.051s`, Q9 `0.142s -> 0.121s`, and Q20
  `0.063s -> 0.045s`. SF10 Q9 verified in both profile and production:
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_q9_sf10_fixed_profile`
  measured `1.524604s -> 1.289852s`, and
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_q9_sf10_fixed_prod_notrace`
  measured `1.486s -> 1.140s`, all with correctness diff 0. The final repeated
  Q9 xtrace is
  `/tmp/trace_duckdb_jit_m6_probe_compare_scratch_fixed_q9_sqlarg_20260626_202233.trace`
  with 3,824 samples over 4.54s; `ProbeForPointersInternal<true,false>` is still
  the largest self frame at `22.3%`. Keep this as state deletion plus correctness
  cleanup. It does not complete Milestone 6.
- The follow-up join-state split deletes the misleading `JoinHashTable::SharedState`
  base. Probe state now owns only probe comparison/no-match selections plus dense
  hashes, and insert state now owns only build salt, remaining, compare, match,
  no-match, and RHS row-location scratch. The edit preserves RowMatcher's contract:
  the build path still mutates an owned `key_match_sel`, and the selected probe
  path still keeps original probe row identity where RowMatcher requires it.
  Verification passed for `make reldebug -j12`, `[api][jit]`, `test/sql/join`,
  the architecture verifier, `git diff --check`, and the removed-pragma smoke.
  SF1 forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_join_state_split_profile` verified with medians
  Q3 `0.061857s -> 0.057291s`, Q9 `0.139290s -> 0.130279s`, and Q20
  `0.062637s -> 0.050856s`. SF1 no-trace production in
  `/private/tmp/duckdb_jit_m6_join_state_split_prod_notrace` verified with
  medians Q3 `0.061s -> 0.051s`, Q9 `0.140s -> 0.118s`, and Q20
  `0.065s -> 0.046s`. SF10 Q9 production in
  `/private/tmp/duckdb_jit_m6_join_state_split_q9_sf10_prod_notrace` verified at
  `1.483s -> 1.139s`, correctness diff 0. The direct repeated-Q9 trace
  `/tmp/trace_duckdb_20260626_204054.trace` has 3,894 samples over 4.68s and
  still names `ProbeForPointersInternal<true,false>` as the largest self frame at
  `23.1%`, with `InsertHashesLoop<false>` at `5.6%`. Keep this as ownership
  cleanup only; the Milestone 6 root target is unchanged.
- The build-side selection cleanup now uses direct owned selection buffers inside
  `InsertHashesLoop`, `PerformKeyComparison`, and
  `InsertMatchesAndIncrementMisses` instead of `SelectionVector` accessor calls.
  This covers the build salt-match list, build key-match reset, no-match
  continuation, and build-side null filtering while keeping RowMatcher's mutable
  selection contract unchanged. Verification passed for `make reldebug -j12`,
  `[api][jit]`, `test/sql/join`, the architecture verifier, `git diff --check`,
  and the removed-pragma smoke. SF1 forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_m6_join_build_sel_direct_profile` verified with
  medians Q3 `0.061960s -> 0.057760s`, Q9 `0.138828s -> 0.128560s`, and Q20
  `0.063311s -> 0.050823s`. SF1 no-trace production in
  `/private/tmp/duckdb_jit_m6_join_build_sel_direct_prod_notrace` verified with
  medians Q3 `0.061s -> 0.049s`, Q9 `0.136s -> 0.117s`, and Q20
  `0.062s -> 0.044s`. SF10 Q9 production in
  `/private/tmp/duckdb_jit_m6_join_build_sel_direct_q9_sf10_prod_notrace`
  verified at `1.495s -> 1.140s`, correctness diff 0. The direct repeated-Q9
  xtrace `/tmp/trace_duckdb_20260626_204916.trace` has 3,787 samples over 4.52s:
  `InsertHashesLoop<false>` moved from `5.6%` to `5.0%` against the previous
  trace, but `ProbeForPointersInternal<true,false>` remains the top self frame at
  `23.0%`. Keep this as local instruction cleanup, not a Milestone 6 exit.
- The current regular-probe implementation adds a DuckDB-owned direct path for
  the common all-valid inner two-key 64-bit no-chain equality probe. It is gated
  by join type, equality predicates, no residual, no chain matcher, no build/probe
  NULL handling, no duplicate chains, and matching `INT64`/`UINT64` key physical
  types. The path hashes the two probe keys, linearly probes the existing regular
  hash table, compares both row-layout keys directly, writes row pointers, and
  emits the match selection. It bypasses the separate vector hash, row-pointer
  probe, and RowMatcher compare passes for that shape. Failed follow-up
  micro-specializations for selector/salt dispatch and first-bucket branching
  were removed; the retained loop is intentionally compact. Verification passed
  for `make reldebug -j12`, `[api][jit]`, `test/sql/join`, the architecture
  verifier, `git diff --check`, and the removed-pragma smoke. SF1 profile
  Q3/Q9/Q20 in `/private/tmp/duckdb_jit_m6_pair_probe_compact_profile` verified
  with medians Q3 `0.061931s -> 0.057349s`, Q9 `0.147971s -> 0.129587s`, and
  Q20 `0.064916s -> 0.050722s`. SF1 production in
  `/private/tmp/duckdb_jit_m6_pair_probe_compact_prod_notrace` verified with
  medians Q3 `0.062s -> 0.051s`, Q9 `0.144s -> 0.115s`, and Q20
  `0.064s -> 0.045s`. SF10 Q9 production in
  `/private/tmp/duckdb_jit_m6_pair_probe_compact_q9_sf10_prod_notrace` verified
  at `1.618s -> 1.116s`, speedup `1.449821`, correctness diff 0. The final Q9
  xtrace `/tmp/trace_duckdb_20260626_213759.trace` has 4,055 samples over 4.88s:
  `ProbeForPointersFlatInternal<true>` is down to 7 samples, while the new
  `ProbeInt64PairNoChainLoop<long long, long long>` is the top frame at `32.0%`
  self. This is real probe/match ownership for the no-chain pair shape, but the
  direct loop is now the sampled root and needs deeper hash/probe ownership next.
- The follow-up probe cleanup kept the direct-pair loop compact but made its
  emission contract explicit through a local matched-row consumer. The same
  producer/consumer shape now exists in the SLJIT all-valid pair and single-key
  no-chain fast paths, which preserves `row_pointers` and `match_sel` behavior
  while preparing a real fused descriptor consumer. Build/finalize now writes
  hash/salt vectors directly, carries loaded entry values through occupied,
  salt, and pointer checks, inserts known-empty serial build slots without
  re-reading them, and increments build probe offsets with the true pointer-table
  bitmask now that salt is carried separately. Header-level force-inline hints
  were measured and removed as hint-only churn. Verification passed for
  `make reldebug -j12`, `[api][jit]`, `test/sql/join`, the architecture
  verifier, `git diff --check`, and the removed-pragma smoke. Final SF1
  coverage-CBO production in
  `/private/tmp/duckdb_jit_m6_consumer_build_cleanup_final_coverage_prod_notrace`
  verified Q3 `0.062s -> 0.051s`, Q9 `0.147s -> 0.117s`, and Q20
  `0.063s -> 0.045s`, all correctness diff 0. The current Q9 xtrace is
  `/tmp/trace_duckdb_20260626_223044.trace`; it still names
  `ht_entry_t::GetValue`, `InsertHashesLoopProbe`,
  `TemplatedSignHashFunction`, and `BloomFilter::GetMask` as the next
  instruction-level roots, so the next profitable work is deeper probe/build
  ownership rather than more helper hints or local setter cleanup.

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
- Keep runtime probe-path choice trait-selected:
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
- The first active Milestone 6 slice removes duplicated flat/selected pair-probe
  code. `sljit_region_runtime.cpp` now shares all-valid no-chain/pair-chain
  capability predicates and uses templated flat/selected two-key probe wrappers,
  a net source deletion in the hot probe runtime while preserving runtime stage
  names.
- Direct Time Profiler evidence after this cleanup still points below that
  branch ladder: Q9 spends about one third of samples in DuckDB
  `JoinHashTable::Probe`, with `ProbeForPointers` the largest self frame.
  Therefore the next deletion must own more probe/match/dataflow lifetime, not
  clone another selected or flat helper.
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

## Current Aggressive CBO Checkpoint

- The stale generic `configured_native_operator` admission token is deleted.
  The stale quantitative `native_operator` admission class is also deleted.
  Native operator work is now a score component only after generated code or
  materialization elision has anchored the compiled region. Protocol-only native
  wrappers stay vectorized because they do not prove value-lifetime ownership.
- Graph prefiltering now distinguishes native aggregate sinks from generic
  native operator work. Native stage benefit can make planning worthwhile only
  when primitive aggregate update, generated perfect-hash lookup, generated IR,
  or materialization elision anchors the region. Native aggregate state
  contracts without those generated body facts are protocol-only, the same as
  native-only join graphs.
- Default settings are now the measured aggressive profile:
  `jit_cbo_generated_stage_benefit=4096`,
  `jit_cbo_materialization_elision_benefit=4096`,
  `jit_cbo_native_operator_stage_benefit=1024`,
  `jit_cbo_full_pipeline_benefit=4096`,
  `jit_cbo_startup_base_cost=32000`, and
  `jit_cbo_startup_margin_basis_points=0`.
- Native benefit guards are explicit capability gates: weak native-only join
  plumbing is rejected as protocol-only, while generated/native fusion still
  scores native-stage benefit with costed post-filter rows.
- The mixed find-or-create grouped fused-payload route no longer calls the
  selected state-address payload callback from inside hash-table lookup.
  Append-new and existing-group fast paths remain direct, while mixed new/existing
  batches fall through to direct state-address resolution plus one fused payload
  update over the address vector. The old selected state-address payload
  boundary has been folded into the primitive state-update counter, and the
  explicit `direct_new_grouped_primitive_payload_update` path remains the direct
  typed-payload backend.
- Verification for this checkpoint passed:
  `make reldebug -j12`, `[api][jit]`, `test/sql/jit`,
  `python3 benchmark/jit/verify_jit_architecture.py`, and all-22 SF1 production
  TPC-H in `/private/tmp/duckdb_jit_aggressive_defaults_route_final_all22_r3`.
  Correctness diff was 0. Default-auto wins with compiled regions were Q1
  `1.116667x`, Q7 `1.055556x`, Q9 `1.260870x`, Q13 `1.006803x`, Q14
  `2.312500x`, and Q15 `1.125000x`; Q6 and Q18 were small zero-compiled/noise
  wins. A higher-repeat compiled-regression check in
  `/private/tmp/duckdb_jit_final_compiled_regressions_r9` verified correctness
  and narrowed stable remaining runtime targets to Q4 `0.976190x`, Q10
  `0.984375x`, and Q17 `0.972222x`. Q12 improved to `1.050000x`, while Q13 and
  Q21 were neutral.
- Q10 xtrace/profile evidence shows the current blocker is runtime work, not CBO:
  the selected state-address callback cost was removed, but the region still
  spends time in source-contract hash join output materialization and grouped
  state-address resolution. Regular hash aggregates now report that real
  state-address path directly instead of a stale generated-lookup blocker. The
  next clean runtime target is a broader row-pointer/group-key descriptor lookup
  for variable-width grouped shapes, not another CBO shape predicate.
- The Q10 variable-width grouped lookup slice now keeps the one-join
  hash-join/projection/grouped-aggregate route batched even when projection
  outputs include variable-width keys. The row-pointer descriptor path hashes
  and compares casted/compressed group keys directly instead of rebuilding a
  scratch descriptor value for every hash/probe comparison. When every group key
  is a row-pointer field, adjacent identical row pointers reuse the previous
  descriptor hash and skip full descriptor equality. The hot lookup slice moved
  from roughly `246 ms` to `152 ms` in traced SF10 Q10 runs. The final SF10
  all-query production sweep in
  `/private/tmp/duckdb_jit_tpch_sf10_all_after_q10_direct_descriptor` verified
  correctness for all 22 queries, compiled 19/22, made 17/22 faster, and moved
  Q10 from `0.969s` vectorized to `0.964s` JIT (`1.005187x`). A 15-repeat
  Q10-only production check in
  `/private/tmp/duckdb_jit_q10_sf10_production_after_inline_hash_r15` was
  neutral-positive at `0.968s` to `0.967s`.
- The Q4 string preaggregation slice keeps preaggregation batched and removes the
  expensive `memcmp`-style equality helper for `string_t`; generated string
  equality now uses `string_t::operator==`. The
  `direct_mark_preaggregate_count_star_groups` stage moved from roughly
  `0.46-0.49 ms` to `0.36-0.37 ms` in trace, while SF1 production
  `/private/tmp/duckdb_jit_q4_string_equals_prod_r9` stayed neutral at
  `0.042s -> 0.042s`.
- Q17 now has a direct hash-join/filter/ungrouped-aggregate route. The runtime
  runs the regular hash join probe in selection-only mode, keeps match selections
  and build row pointers live, evaluates the post-join filter from those live
  sources, then remaps aggregate payload references directly to selected
  probe/build vectors. The same route handles residual predicates lowered into
  the hash join and mark-build right/delim joins; it does not materialize a full
  joined chunk before the filter. Focused API coverage asserts
  `hash_join_probe.direct_row_pointer_reference`,
  `aggregate_update.direct_hash_join_filtered_payload_update`, and absence of
  `hash_join_probe.final_output`.
- Q17 SF1 profile in
  `/private/tmp/duckdb_jit_q17_mark_build_direct_profile` confirms the boundary
  deletion: the old hot region changed from
  `hash_join_probe.final_output=6088; aggregate_update.direct_state_update=587`
  to
  `hash_join_probe.direct_row_pointer_reference=6088; hash_join_probe.filtered_expression_input=6088; aggregate_update.direct_hash_join_filtered_state_update=587`.
  It did not produce a Q17 production win: `/private/tmp/duckdb_jit_q17_mark_build_direct_prod_r9`
  measured `0.034s -> 0.035s` (`0.971429x`). Direct xtrace over repeated Q17
  at `/tmp/trace_duckdb_20260627_025444.trace` shows process-level CPU dominated
  outside this route, especially existing perfect-hash join and scan/decompression
  work. The next Q17 root target is broader pipeline coverage or compile-cost
  amortization, not reverting this direct route.
- Current broad SF1 production evidence is
  `/private/tmp/duckdb_jit_tpch_all_direct_prod_r5`, verified with correctness
  diff 0 for all 22 queries. Compiled wins above noise were Q1 `1.116667x`, Q9
  `1.232759x`, Q14 `2.242424x`, and Q15 `1.125000x`; Q7 was a smaller compiled
  win at `1.037037x`, and Q20 was a slight zero-compiled/noise win. Stable
  remaining compiled regressions are still small-region/compile-overhead cases
  such as Q10 and Q17, plus Q4 at noise scale.
- The active milestone remains Milestone 6 with Milestone 7 CBO cleanup running
  in parallel. The CBO is now intentionally more aggressive in the simple
  direction: once a region has real generated work and no native sort, native
  join/aggregate fusion is admitted as `generated_native_fusion` without the
  old hidden high-cardinality and low-expression native-benefit guards. Native
  only join plumbing still does not open a region by itself because admission
  still needs generated, materialization, full-pipeline, or native-aggregate
  basis.
- The Q17 aggressive-admission failure exposed a runtime bug, not a CBO reason
  to retreat. Selection-only perfect-hash probes populated probe/build
  selections without materializing `join_output`; the generic fallback then read
  an unmaterialized chunk as if it held projected columns. The shared fallback
  now materializes perfect-hash selection-only output through
  `ExecutionMaterializePerfectHashJoinProbe`, records
  `hash_join_probe.perfect_selection_reference`, and regular row-pointer pending
  buffering no longer accepts perfect-hash probe batches.
- Perfect-hash post-join projection now uses the same direct probe-side path as
  regular joins for probe/LHS references and computed expressions. Q17 trace no
  longer needs `hash_join_probe.final_output` in the main perfect-hash grouped
  route; it records `hash_join_probe.direct_selection_reference`,
  `projection.direct_post_join_reference_projection`,
  `projection.direct_post_join_computed_projection`, and
  `aggregate_update.direct_state_update`. SF1 production improved from the
  earlier fixed-but-regressed Q17 run (`0.034s -> 0.037s`) to
  `/private/tmp/duckdb_jit_q17_perfect_direct_prod_r5`
  (`0.034s -> 0.036s`), but Q17 is still not a production win.
- Q21 xtrace confirmed the remaining loss is runtime work inside native
  join/probe/build execution, not a debuggability problem in the simplified CBO.
  A sparse selected-source path was tested for hash-probe-to-hash-build sinks.
  The first version was slower because the old full-output path was mostly
  zero-copy slice/dictionary materialization while the sparse projection path
  copied reference projections into flat vectors. The retained design is the
  cleaner view path: direct no-projection build input is used only when it
  removes dead columns, and projection-to-build input uses required-column
  reference/dictionary views through `projection.required_projection_view`.
- Generic full-pipeline native hash-build and delim sinks now use the same
  extended source-fetch budget as the batched specialized routes. Q21 traced
  invocation counts for the large compiled regions dropped from roughly `59/61`
  runtime invocations to `1/3` for the same row volume. Production did not move
  materially, so Q21 remains a Milestone 6 runtime target: the next root work is
  generated/native hash probe and build hot-loop ownership, not another CBO shape
  predicate.
- Fresh broad SF1 production after these changes is
  `/private/tmp/duckdb_jit_broad_after_budget_prod_r7`, correctness diff 0.
  Clear compiled wins are Q3 `0.061s -> 0.050s` (`1.220000x`), Q10
  `0.061s -> 0.058s` (`1.051724x`), and Q20 `0.062s -> 0.043s`
  (`1.441860x`). Q5 was neutral, and remaining compiled regressions are Q4
  (`0.041s -> 0.044s`), Q12 in this noisy run (`0.059s -> 0.063s`), Q17
  (`0.034s -> 0.037s`), and Q21 (`0.111s -> 0.121s`). These are runtime
  optimization targets; weakening admission would hide the problem instead of
  deleting work.
- Q10 SF10 direct row-pointer aggregate now uses an explicit leading-key hash
  strategy for wide descriptors whose first group key is a fixed-width integer.
  This is correct because equal full descriptors must share the leading-key
  hash, while full descriptor equality still resolves collisions. The immediate
  same-row prefetch in the no-chain single-key probe was also removed because it
  could not hide a dependent load; pair no-chain already used the cleaner shape.
  Verification passed for `make release -j12`, the focused JIT join/descriptor
  tests, Q10 production/profile, and all SF10 TPC-H production.
  `/private/tmp/duckdb_jit_q10_sf10_profile_leading_key_hash_t1_r3` shows
  `find_or_create_row_pointer_descriptor.hash` dropping from about `48.8 ms` to
  about `0.45 ms`, with probe flat around `43 ms`; total
  `direct_row_pointer_grouped_lookup_update` dropped from about `160.8 ms` to
  about `120.0 ms`. Production Q10 improved from the old direct descriptor
  checkpoint `/tmp/duckdb_jit_q10_sf10_production_after_inline_hash_r15`
  (`0.968s -> 0.967s`, `1.001x`) to
  `/private/tmp/duckdb_jit_q10_sf10_production_leading_key_hash_t1_r15`
  (`0.947s -> 0.904s`, `1.048x`).
- The leading-key hash strategy is now covered by a focused API test that forces
  four direct row-pointer `BIGINT` group keys with a shared leading key and
  different later descriptor fields. That exposed a root bug in the
  selected-view row-pointer aggregate route: reference-only grouped payloads do
  not have a fused payload function, so the view path fell through to the
  split-primitive updater, which only accepts flat/all-valid payload inputs. The
  route now keeps selected payload views only when the grouped fused updater can
  consume them; otherwise it materializes the compact flat payload batch and uses
  the supported primitive update path. This fixes the crash without weakening
  the direct descriptor lookup or the leading-key hash admission.
- Focused Q10 SF10 production after the selected-view route fix is
  `/private/tmp/duckdb_jit_q10_sf10_production_after_view_gate_t1_r15`,
  verified with correctness diff 0 at `0.960s -> 0.922s` (`1.041x`). The broad
  SF10 production sweep after the same fix is
  `/private/tmp/duckdb_jit_tpch_sf10_all_after_view_gate_r3`, verified with
  correctness diff 0 for all 22 queries. Eighteen queries are faster than
  non-JIT and nineteen are jitted; Q10 is `0.967s -> 0.928s` (`1.042x`) in that
  sweep.
- Broad SF10 production after the Q10 hash fix was
  `/private/tmp/duckdb_jit_tpch_sf10_all_after_leading_key_hash_r3`, verified
  with correctness diff 0 for all 22 queries. It exposed Q7 as the next runtime
  target rather than a CBO problem: Q7 was `0.961x` in that broad run, and
  `/private/tmp/duckdb_jit_q07_sf10_profile_after_leading_key_hash_t1_r3`
  pointed to generated `filter+projection` in the large first region.
- Q10 SF10 row-pointer grouped aggregate now preaggregates consecutive
  descriptor runs before grouped lookup when all group keys come from row
  pointers. The fused-payload case reuses the existing grouped fused updater
  against temporary run-local aggregate states instead of trying to materialize
  aggregate payload expressions as standalone projections. Row-pointer equality
  also has a pointer-identity fast path that still checks nullable descriptor
  validity. Focused production
  `/private/tmp/duckdb_jit_q10_sf10_production_rowptr_preagg_ptrfast_r15`
  verified correctness diff 0 at `0.955s -> 0.910s` (`1.049x`). The useful
  xtrace sample `/tmp/trace_run_duckdb_q10_xtrace.sh_20260629_133805.trace`
  shows the hot CPU after this change dominated by storage decompression and
  bitpacking; `ExecutionRowPointerGroupKeysEqual` is down to roughly `1.1%`
  self time.
- Q7 SF10 generated filter/projection now uses the shared direct selected
  reference projection path for multi-column fixed-width reference projections.
  This deletes the separate filtered-input append and copies selected scan
  columns directly into the pending hash-join input batch. Runtime trace
  `/private/tmp/duckdb_jit_q07_sf10_profile_direct_multiref_r1` records
  `filter.direct_selected_reference_projection=18230325`; the large 60M-row
  generated region dropped from about `380 ms` generated time to about `343 ms`.
  Focused production
  `/private/tmp/duckdb_jit_q07_sf10_production_direct_multiref_r15` verified
  correctness diff 0 at `0.595s -> 0.579s` (`1.028x`).
- The old Q11 native `hash_join_probe -> append_sink` batching route is removed.
  It was useful as profiling evidence that sparse source batches matter, but it
  was still a protocol-only native wrapper and therefore contradicted the
  production compiled-body rule. Future Q11 work must add a generated body or a
  real materialization-elision anchor before native-stage benefit can be scored.
- Current broad SF10 production status after the fixed-width/native codegen
  cleanup and perfect-hash COUNT(reference) lookup cleanup is
  `/private/tmp/duckdb_jit_tpch_sf10_broad_after_count_lookup_cleanup_r5_20260702`,
  verified with correctness diff 0 for all 22 queries. Twenty-one queries are
  jitted and all twenty-one jitted queries are faster than non-JIT. There are no
  measured regressions in that sweep. Top production wins include Q20 `1.524x`,
  Q9 `1.480x`, Q14 `1.209x`, Q15 `1.182x`, Q17 `1.175x`, Q5 `1.174x`, Q1
  `1.154x`, Q3 `1.135x`, Q6 `1.133x`, and Q22 `1.101x`. Q16 is the only
  non-jitted TPC-H query and remains neutral. Focused fixed-width production
  `/private/tmp/duckdb_jit_tpch_sf10_fixed_width_codegen_cleanup_20260702`
  verified Q1/Q6/Q14 at `1.147x`, `1.156x`, and `1.177x` respectively over five
  repeats. The follow-up scalar fixed-width cleanup centralized source-to-result
  copies in the fixed-width helper and moved integer coalesce onto the shared
  invalid-result loop; final focused SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_scalar_fixed_width_cleanup_final_20260702`
  verified Q1/Q6/Q8/Q14 correctness diff 0 at `1.140x`, `1.161x`, `1.010x`,
  and `1.169x` respectively over five repeats.
- Aggregate reducer loop cleanup moved selected-source and two-source
  null-skipping iteration into the aggregate primitive helper layer while
  leaving arithmetic, overflow, and accumulation bodies local to each reducer.
  The same helpers now drive simple reference reducers and binary reducers.
  Focused SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_aggregate_reference_loop_cleanup_20260702`
  verified Q1/Q6/Q14/Q15/Q17 correctness diff 0 at `1.146x`, `1.141x`,
  `1.170x`, `1.142x`, and `1.163x` respectively over five repeats.
- Grouped reference aggregate reducers now use grouped-loop helpers for
  count-star, count-reference, and sum-reference codegen while preserving the
  all-valid count fast path. Focused SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_grouped_reference_loop_cleanup_20260702`
  verified Q1/Q10/Q13/Q18 correctness diff 0 at `1.146x`, `1.012x`, `1.054x`,
  and `1.019x` respectively over five repeats.
- Perfect-hash local aggregate initialization now uses one shared group-index
  loop helper for sparse payload zeroing, sparse sentinel zeroing, dense
  payload zeroing, and deferred flag zeroing. Focused SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_perfect_hash_zero_loop_cleanup_20260702`
  verified Q1/Q8/Q14 correctness diff 0 at `1.165x`, `1.018x`, and `1.165x`
  respectively over five repeats.
- Perfect-hash local aggregate commit now uses one dense seen-group commit loop
  helper for dense payload commits and deferred flag commits. The helper owns
  the group scan, seen check, output group marker, and state-pointer
  materialization while each caller keeps only payload-specific work. The
  follow-up shared perfect-hash helper cleanup also centralizes output group
  marking and state-pointer materialization for commit, fused update lookup,
  and the primitive update path. The payload updater now also uses shared local
  helpers for state-pointer reloads, nullable-payload continuations, and shared
  expression-tree value loading across predicate guards and payload updates.
  Focused SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_perfect_hash_expression_value_cleanup_20260702`
  verified Q1/Q8/Q14 correctness diff 0 at `1.173x`, `1.018x`, and `1.188x`
  respectively over five repeats.
- Perfect-hash primitive aggregate updates now admit COUNT(reference) payloads
  to the generated perfect-hash lookup route. The primitive update emitter
  treats nullable COUNT as its own validity-gated increment path instead of
  sharing SUM payload loading. Focused SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_perfect_hash_count_lookup_cleanup_20260702`
  verified Q1/Q8/Q14 correctness diff 0 at `1.150x`, `1.005x`, and `1.186x`
  respectively over five repeats.
- Follow-up flat codegen refactoring keeps the same hot-path evidence while
  deleting duplicated loop scaffolding. `sljit_native_flat_loop_codegen.hpp`
  now owns counted scalar, decrement-to-zero scalar, unrolled scalar, and
  SIMD-then-scalar-tail loop shapes. Flat integer, flat floating, and flat
  integer projection codegen keep only their operation-specific row bodies. The
  floating stats loop now lives with the floating stats emitters, and shared
  flat projection source-pointer loading is owned by the flat projection helper.
  Native predicate select codegen now uses the same fixed-width source loader as
  non-select predicates, with select row-loop scaffolding owned by the select
  helper layer. String compression/decompression now uses the same selected-source
  invalid-result loop as fixed-width native paths instead of carrying a local
  transform loop.
  Verification passed for `make release -j12`,
  `build/release/test/unittest "*jit*"`,
  `python3 benchmark/jit/verify_jit_architecture.py`, stale-code scan,
  and `git diff --check`. Focused SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_predicate_select_cleanup_20260702`
  verified correctness diff 0 for Q1/Q6/Q14 at `1.149x`, `1.169x`, and
  `1.219x` respectively over five repeats. Focused string-route production
  `/private/tmp/duckdb_jit_tpch_sf10_string_loop_cleanup_20260702` verified
  Q5/Q9/Q20 correctness diff 0 at `1.240x`, `1.446x`, and `1.536x`. The broad
  all-query artifact above is the current post-refactor validation.
- Q16 is not a CBO-conservatism miss. Forced zero-startup admission in
  `/private/tmp/duckdb_jit_q16_force_startup0` regressed production from
  `0.158s` to `0.166s` median while compiling seven regions. The root fix is in
  DuckDB's distinct aggregate path and in cost-fact ownership, not in CBO
  leniency: grouped row pointers feed a compact integer distinct-count backend,
  and the backend increments the count state only on first insert. The physical
  distinct count-pointer strategy is now telemetry-independent; production and
  traced planning expose the same `distinct_ptr=1` candidate instead of making
  the backend trace-only. Focused SF10 Q16 production
  `/private/tmp/duckdb_jit_tpch_sf10_q16_trace_invariant_r5_20260704` verified
  correctness diff 0 over five repeats at `0.167s` off median and `0.144s`
  auto median, speedup `1.160x`, with `compiled_regions=0`. The rejected
  combined candidate is the real architecture shape:
  `gen=7,gen_backend=3,mat=1,join=3,join_build_sink=1,distinct_ptr=1`, rejected
  by negative saved work after charging distinct count-pointer stateful backend
  cost and blocking generated backend-stage credit through a native hash-join
  build sink. Future Q16 work should optimize the distinct backend itself; do
  not admit the old generated fragments or reintroduce CBO leniency.
- Q8 source-filter ownership is now explicit instead of inferred from source
  traits. Generated source-filter plans request source-contract input layout;
  only the single-filter identity-layout shape may also keep DuckDB scan filters
  enabled. This preserves storage pruning for the Q8-style orders scan without
  reintroducing the multi-filter source-layout crash. Focused SF10 Q8 production
  `/private/tmp/duckdb_jit_tpch_sf10_q8_hybrid_input_layout_r15_20260702`
  verified correctness diff 0 at `0.401s -> 0.360s`, speedup `1.114x`. Full
  SF10 production
  `/private/tmp/duckdb_jit_tpch_sf10_arch_budget_20260704`
  verified correctness diff 0 for all 22 queries: 20 queries jitted, 17 queries
  faster overall, 16 jitted queries faster, and Q16 remains non-jitted but is
  faster from the core distinct count-pointer backend.

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
