# JIT Broad Query Refactor Plan

Last updated: 2026-06-30
Status: active refactor plan after broad SF10 verification
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

- The current broad SF10 production sweep
  `/private/tmp/duckdb_jit_cleanup_all_sf10_r3` is verified with correctness
  diff 0 for all 22 TPC-H queries. Twenty-one queries are jitted, all twenty-one
  jitted queries are faster than non-JIT, and Q16 is the only non-jitted query.
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
- Current Q9 SF10 production in the all-query sweep measured vectorized/off
  `1.696s` median versus auto `1.166s` median, speedup `1.454545`,
  correctness diff 0.
- The follow-up chain-probe cleanup centralizes first chain-head lookup in one
  helper and moves salt derivation behind the bloom gate for those variants. It
  is retained as code-shape cleanup, not as a new Q9 root win.
- The active Milestone 6 cleanup now removes duplicated flat/selected regular
  pair-probe wrappers instead of adding another branch. All-valid no-chain and
  pair-chain capability checks are shared, and the two-key no-chain/chain probe
  wrappers are templated on selected versus flat input. Stage telemetry remains
  stable, so benchmark counters still compare directly across checkpoints.
- The stale `[INT32, INT32, INT8]` grouped find-or-create branch is now removed.
  Q3 uses the same descriptor-driven grouped lookup loop as the broader fixed
  key path, and direct xtrace shows the generic comparer is not a new dominant
  cost.
- Q3 and Q20 are no longer default-skipped examples: current SF10 auto compiles
  both, with Q3 at `1.142x` and Q20 at `1.480x` in the all-query sweep.

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
- Q16 is the distinct-aggregate exception. Forced admission of its old
  generated/native fragment regressed production, and xtrace showed the root was
  `count(DISTINCT)` grouped aggregation. The current backend fixes the first
  ownership boundary in DuckDB core: address-only grouped lookup feeds a compact
  pointer-key integer distinct set, the count state is incremented only on first
  insert, and the temporary distinct chunk/valid-selection scratch is gone. Treat
  any further Q16 work as backend dataflow ownership, not CBO threshold tuning.
- The latest direct DuckDB Time Profiler checkpoint
  (`/tmp/trace_duckdb_jit_probe_template_q9_file_20260626_164548.trace`) confirms
  the same direction after the probe-template cleanup: Q9 still spends about
  33.7% total in `JoinHashTable::Probe`, about 20.2% self in
  `ProbeForPointers`, about 28% in table scan filtering/decompression, about
  9.6% in sink/build work, and about 8.1% in hash-join finalize.
- The follow-up core probe cleanup hoists `ProbeForPointersInternal` invariants
  and deletes the tiny candidate helper. Direct xtrace
  (`/tmp/trace_duckdb_jit_probe_core_q9_20260626_165548.trace`) shows lower
  sampled `ExtractSalt` and `IncrementAndWrap`, but `ProbeForPointers` remains
  the dominant self frame. Treat this as a retained simplification, not the
  ownership fix.
- The direct selection-buffer write follow-up removes the sampled
  `SelectionVector::set_index` frame from the same Q9 xtrace loop
  (`/tmp/trace_duckdb_jit_probe_selwrite_q9_20260626_170505.trace`), but leaves
  `ProbeForPointers` at about 22.7% self. This is another local instruction
  cleanup; the larger dataflow ownership target is unchanged.
- The final local probe cleanup folds salted/unsalted candidate writes back into
  one append block and compares salt bits directly inside the collision walk.
  The final trace
  (`/tmp/trace_duckdb_jit_probe_folded_q9_20260626_172141.trace`) removes the
  sampled lambda frame, but `ProbeForPointers` remains about 22.5% self. Keep
  this as source cleanup only; the next useful change must still delete or carry
  the regular probe ownership boundary.
- The pending-probe cleanup removes another duplicated Milestone 6 implementation
  copy: one-join and two-join pending probe paths now share
  `SljitAppendSelectedProbeBatch` for selected-column copy, row-pointer copy,
  cardinality repair, and boundary/stage telemetry. The final-build trace
  (`/tmp/trace_duckdb_jit_pending_probe_helper_final_q9_20260626_173631.trace`)
  keeps `ProbeForPointers` in the same dominant sampling band (`22.5% -> 23.4%`
  self against the folded trace), so this is ownership-boundary cleanup only.
- The current row-pointer descriptor cleanup deletes the covered Q9 final
  `direct_remap_post_join_batch_projection` plus
  `projected_group_payload_update` boundary. Final projection group keys are now
  descriptor sources, including string-compressed `nation` and integral-compressed
  year, and the primitive DECIMAL payload remains in the live payload input.
  Runtime evidence moved Q9 to
  `aggregate_update.direct_row_pointer_grouped_lookup_update=164` and
  `aggregate_update.row_pointer_grouped_lookup_update=319404` in the SF1
  forced/profile run. Comparable production medians stayed positive at Q3
  `0.063s -> 0.051s`, Q9 `0.139s -> 0.117s`, and Q20 `0.063s -> 0.044s`.
  The direct repeated-Q9 trace
  (`/tmp/trace_duckdb_20260626_181554.trace`, 29,929 samples) still has
  `ProbeForPointers` as the largest self frame at about `27.7%`, so the next
  root fix remains regular probe/match ownership or the probe loop itself.
- The probe state cleanup after that trace removes stale state, not a new route:
  `salt_v` is build-only and now lives in `InsertState`, while the unused
  `ProbeState::non_empty_sel` member is deleted. The cleanup keeps the regular
  probe object smaller and clearer, but it does not change the xtrace conclusion:
  `ProbeForPointers` is still the root frame to attack or bypass.
- The follow-up continuation cleanup deletes `ProbeState::ht_offsets_and_salts_v`.
  Probe continuation tokens are now carried in `hashes_dense_v`; only selected
  continuation passes need the compact dense-position-to-original-row map because
  RowMatcher still compares original probe rows. The result is verified and
  source-clean, but production Q3/Q9/Q20 medians are neutral, so this remains
  state deletion rather than evidence for broader CBO admission. A short Q9 trace
  (`/tmp/trace_zsh_20260626_184042.trace`) still names
  `ProbeForPointersInternal<true,false>` as the largest C++ frame at about
  `23.9%` self.
- The raw packed-entry-value probe micro-edit after that was intentionally reverted:
  it improved the short trace frame to `22.4%`, but production Q9 failed the
  verifier at `0.978571x` in
  `/private/tmp/duckdb_jit_m6_probe_entry_value_final_prod_notrace`. The current
  tree is the reverted state, verified in
  `/private/tmp/duckdb_jit_m6_probe_entry_value_reverted_prod_notrace`.
- The dense-probe-hash cleanup is retained after that revert. The no-null-filter
  regular probe path hashes directly into `ProbeState::hashes_dense_v` and enters
  `GetRowPointersWithDenseHashes`, while precomputed and null-filtered hashes keep
  the old densifier. This removes a temporary hash vector plus dense copy from the
  hot regular probe ownership path. Final-source profile and no-trace production
  Q3/Q9/Q20 both verified in
  `/private/tmp/duckdb_jit_m6_dense_probe_hash_flat_profile` and
  `/private/tmp/duckdb_jit_m6_dense_probe_hash_flat_prod_notrace`; Q9 production
  stayed inside the verifier floor at `0.137s -> 0.139s`. The final-source Q9
  trace `/tmp/trace_zsh_20260626_191125.trace` was mostly unsymbolicated generated
  code, but the collapsed-stack copy-path check still went from 19 matching stacks
  before the cleanup to 0 after it.
- The SLJIT fast probe salt cleanup applies the same salt-bit compare shape to
  runtime helpers: chain lookup uses `GetSaltWithNulls`, and no-chain pair/single
  helpers compare high salt bits on the raw packed entry value. Production
  Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_sljit_probe_salt_bits_prod_notrace`; Q9 was neutral
  at `0.138s -> 0.138s`. Profile verified in
  `/private/tmp/duckdb_jit_m6_sljit_probe_salt_bits_profile`. The direct Q9 trace
  `/tmp/trace_zsh_20260626_192419.trace` is mostly unsymbolicated generated code,
  with no named `ExtractSalt` frames in collapsed stacks, so this is retained as
  instruction-shape cleanup only.
- The RHS row-pointer gather cleanup is also retained as source cleanup only.
  `GatherHashJoinRHSColumn` now owns the direct fixed-column gather plus generic
  `JoinHashTable::GatherRHSColumn` fallback, so five projection/materialization
  call sites no longer carry duplicated fallback code. Production
  Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_rhs_gather_helper_prod_notrace`; Q9 stayed neutral
  to negative at `0.136s -> 0.138s`. Profile verified in
  `/private/tmp/duckdb_jit_m6_rhs_gather_helper_profile`; Q9 measured
  `0.139239s -> 0.141463s`. The useful Q9 trace
  `/tmp/trace_zsh_jit_m6_rhs_gather_helper_q9_20260626_193620.trace` still names
  DuckDB `ProbeForPointersInternal<true,false>` as the largest frame at `23.16%`
  self, so the next target remains the regular probe loop and downstream
  descriptor lifetime.
- The generic `JoinHashTable` build/finalize path now matches the raw salt-bit
  convention too. Build hashes write `hash & ht_entry_t::SALT_MASK`, and the
  insertion loop compares the raw entry value's high salt bits directly. This
  removes all `ExtractSalt`/`GetSalt`/`GetSaltWithNulls` references from
  `src/execution/join_hashtable.cpp` without changing aggregate hash table's
  packed-salt tentative-entry contract. Final 9-repeat production Q3/Q9/Q20
  verified in
  `/private/tmp/duckdb_jit_m6_join_build_raw_salt_bits_prod9_notrace`; Q9 stayed
  inside the floor at `0.138s -> 0.139s` and Q20 was neutral. Profile verified in
  `/private/tmp/duckdb_jit_m6_join_build_raw_salt_bits_profile`; Q9 measured
  `0.141995s -> 0.143777s`. The final Q9 trace
  `/tmp/trace_zsh_jit_m6_join_build_raw_salt_bits_q9_20260626_194757.trace` is
  mostly unsymbolicated, but the collapsed stacks contain zero named salt-helper
  or `JoinHashTable::Finalize` frames.
- The latest probe scratch cleanup deletes the remaining persistent selected
  compare-row map from `ProbeState`. Selected dense probe iterations borrow the
  unused tail of `match_sel` to map `RowMatcher` no-match rows back to dense
  continuation tokens, so the probe object no longer owns a vector that only
  exists for one compare pass. The SF10 Q9 repro also found a descriptor lifetime
  bug: final row-pointer grouped update must not read a second-join projection
  input column that was intentionally omitted by compressed-key or precomputed
  payload skip projection. The backend now records
  `group_key_omitted_input` and uses the existing sidecar/batch path for that
  shape instead of reading stale vector data. Verification passed for
  `make reldebug -j12`, `[api][jit]`, `test/sql/join`, the architecture verifier,
  `git diff --check`, the removed-pragma smoke, SF1 Q3/Q9/Q20 profile and
  production, and SF10 Q9 profile and production. SF10 Q9 production measured
  `1.486s -> 1.140s` with correctness diff 0. The final useful Q9 xtrace
  `/tmp/trace_duckdb_jit_m6_probe_compare_scratch_fixed_q9_sqlarg_20260626_202233.trace`
  still names `ProbeForPointersInternal<true,false>` as the top self frame at
  `22.3%`, so Milestone 6 remains focused on regular probe ownership and
  grouped hash/state lifetime.
- The latest source cleanup removes `JoinHashTable::SharedState`. Probe and build
  scratch lifetimes are no longer modeled as inheritance: probe state owns probe
  compare/no-match selections and dense hashes, while insert state owns build salt,
  remaining, compare, match, no-match, and RHS row-location scratch. This prevents
  future probe/build state from drifting through a shared base that does not
  represent a real lifetime. SF1 Q3/Q9/Q20 profile and production verified, and
  SF10 Q9 production verified at `1.483s -> 1.139s` with correctness diff 0.
  The direct Q9 xtrace `/tmp/trace_duckdb_20260626_204054.trace` still has
  `ProbeForPointersInternal<true,false>` as the top self frame at `23.1%`, so the
  next implementation target remains regular probe/match ownership or grouped
  hash/state lifetime rather than more state-shape cleanup.
- The follow-up build-side direct selection cleanup removes accessor calls from
  the owned build scratch selections in `InsertHashesLoop`,
  `PerformKeyComparison`, and `InsertMatchesAndIncrementMisses`. SF1 Q3/Q9/Q20
  profile and production verified, and SF10 Q9 production verified at
  `1.495s -> 1.140s` with correctness diff 0. The final Q9 xtrace
  `/tmp/trace_duckdb_20260626_204916.trace` moved `InsertHashesLoop<false>` from
  `5.6%` to `5.0%` self versus the previous trace, but
  `ProbeForPointersInternal<true,false>` remains at `23.0%` self. This confirms
  the cleanup is worth keeping but is not the root milestone fix.
- The current regular-probe cleanup adds a compact DuckDB-owned direct path for
  inner two-key 64-bit no-chain equality probes. The path is deliberately gated
  to no residual, no chain matcher, no build/probe NULL handling, no duplicate
  chains, equality predicates, and matching `INT64`/`UINT64` physical key types.
  It hashes the two probe keys, probes the existing regular table, compares both
  row-layout keys directly, and writes row pointers plus match selection, so the
  no-chain pair shape bypasses vector hash, `GetRowPointers`, and RowMatcher.
  Selector/salt dispatch and first-bucket micro-specializations were measured and
  removed because they made profile mode worse. Verification passed for
  `make reldebug -j12`, `[api][jit]`, `test/sql/join`, the architecture verifier,
  `git diff --check`, and the removed-pragma smoke. SF1 production verified in
  `/private/tmp/duckdb_jit_m6_pair_probe_compact_prod_notrace` with Q9
  `0.144s -> 0.115s`; SF10 Q9 production verified in
  `/private/tmp/duckdb_jit_m6_pair_probe_compact_q9_sf10_prod_notrace` at
  `1.618s -> 1.116s`, speedup `1.449821`, correctness diff 0. The final trace
  `/tmp/trace_duckdb_20260626_213759.trace` shows
  `ProbeForPointersFlatInternal<true>` at only 7 samples and the new direct pair
  loop as the remaining top frame at `32.0%` self.
- Follow-up cleanup kept the direct pair loop compact and introduced a local
  matched-row consumer in core pair probing plus SLJIT pair and single-key
  no-chain probe paths. It also removed generic writer/accessor churn from
  build/finalize by writing hash/salt vectors directly, carrying the loaded
  entry value through occupied/salt/pointer extraction, inserting known-empty
  serial build slots without a second slot load, and deleting the stale
  `bitmask | SALT_MASK` build-probe increment now that salt is carried
  separately. Header force-inline hints were tried and removed after
  trace/benchmark evidence did not justify them. Final SF1 coverage-CBO
  production in
  `/private/tmp/duckdb_jit_m6_consumer_build_cleanup_final_coverage_prod_notrace`
  verified Q3 `0.062s -> 0.051s`, Q9 `0.147s -> 0.117s`, Q20
  `0.063s -> 0.045s`, all correctness diff 0. The next useful trace is
  `/tmp/trace_duckdb_20260626_223044.trace`; helper hints are exhausted, and the
  remaining work is deeper build/probe ownership.

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
- Probe cleanup removes duplicated selected/flat branches without changing stage
  counter names, and post-change sampling still names the same deeper DuckDB
  probe/build/finalize work.
- Core DuckDB probe cleanup is acceptable only while it removes repeated
  invariant work. It does not replace the broader requirement to keep probe,
  match selection, projection, and grouped lookup facts live across the region.
- Direct selection-buffer writes are acceptable in this path because the
  surrounding state owns fixed-size selection buffers and row indices remain
  `STANDARD_VECTOR_SIZE` bounded. They are not a substitute for deleting the
  downstream probe/match materialization boundary.
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
- A remaining typed `[INT32, INT32, INT8]` probe fork in
  `TryFindOrCreateGroupsFastInternal` was also deleted; the generic
  `AggregateFastGroupSourceRowsMatch` and `AggregateFastExistingRowMatches`
  loop now covers that shape.
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
- The mixed split-payload append now routes through the selected state-address
  ABI as well. `RadixUpdatePrimitiveGroupSelected` follows the same contract as
  generated selected updates: with `execute_sel`, `address_sel` is indexed by the
  logical row. The obsolete duplicate-position scratch selection in
  `AggregateHTAppendState` was removed.
- The direct primitive grouped find-or-create route now uses the same selected
  state-address callback, so the public `TryFindOrCreateGroupsUpdateFast`
  wrapper and the row-update arm of `TryFindOrCreateGroupsFastInternal` are
  removed. Find-or-create fast lookup no longer has a separate row-callback
  mutation mode.
- The append-only primitive grouped route now uses
  `TryAppendNewGroupsWithStateAddressesFast`; `TryAppendNewGroupsUpdateFast`,
  `ExecutionGroupedAggregateStateRowUpdateFunction`, and the row-update arm of
  `TryAppendNewGroupsFastInternal` are removed. The append helper has one
  mutation contract: append through DuckDB tuple layout and emit state-address
  spans for the update callback.
- The append coverage test now requires `find_new.state_address_update`, so the
  old row-update append path cannot silently return.
- SF1 forced/profile after the ABI cleanup records
  `find_or_create_fast.selected_state_update` below
  `direct_new_split_payload.append` for Q9/Q20, still with no aggregate-side
  `find_or_create_fast.hash` because compact projection passes precomputed group
  hashes into lookup.
- After the append ABI cleanup, forced/profile Q3/Q9/Q20 in
  `/private/tmp/duckdb_jit_append_state_address_profile` stayed correct with
  Q9/Q20 still reporting selected state updates under split payload. Production
  Q3/Q9/Q20 in `/private/tmp/duckdb_jit_append_state_address_prod` stayed
  positive: Q3 `0.063s -> 0.051s`, Q9 `0.142s -> 0.118s`, Q20
  `0.064s -> 0.044s`, all correctness diff 0. The full
  `build/reldebug/test/unittest --print-failing-tests` suite also passed.
- Direct Time Profiler xtrace over repeated Q9 auto
  (`/tmp/trace_duckdb_20260626_163015.trace`) collected 5,356 `duckdb` samples.
  The hot frames are still regular hash probe/build/finalize and scan work, not
  the append update callback.
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

- benchmark harness query parsing is centralized; `--queries all` now works in
  both the benchmark runner and verifier, and out-of-range query ids fail during
  argument validation
- deprecated verification pragma references are gone from the repo
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

1. Extend the new Q16 distinct-count backend only from measured ownership facts:
   keep grouped row pointers live, preserve the direct first-insert count update,
   and do not reintroduce tuple-backed duplicate scans or correction selections.
2. Optimize the measured regular hash join probe variants and hash-table lookup
   traffic before adding another projection-only cleanup.
3. Carry or reuse grouped hash/state-target facts across the final projection and
   grouped aggregate lookup after the first-join boundary is gone.
4. Refactor fixed-width and variable-width projection descriptors before adding
   another query-family path.
5. Run SF10 smokes before any default CBO broadening.
6. Only then move to SIMD experiments.
7. Keep GPU work behind residency and real cost accounting.

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
