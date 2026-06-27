# JIT Performance Learnings

This note records what we learned while optimizing DuckDB's experimental SLJIT
and Metal execution paths. It is intentionally evidence-driven: measurements
that were verified are separated from principles and next directions.

## Verified State

- The repo is back on DuckDB's default vector size of 2048.
- `make reldebug -j12` passed after the latest cleanup.
- `build/reldebug/test/unittest "[api][jit]"` passed.
- Full `build/reldebug/test/unittest` passed:
  - 977381 assertions
  - 4537 test cases
  - 356 skipped tests

Latest cleaned TPC-H Q1 SF1, single-thread checkpoint:

| Mode | Median |
| --- | ---: |
| Vectorized/off | 0.072s |
| JIT/auto | 0.069s |

The measured end-to-end speedup was about 1.04x. That is real, but too small to
treat as a root solution. Generated-region runtime was 465681 us over 15 runs.

## What Actually Worked

### FLOAT/DOUBLE CTAS Direct Materialization

The strongest proven win was FLOAT/DOUBLE CTAS direct materialization into the
insert path:

| Workload | Time |
| --- | ---: |
| Vectorized 5M-row, 8-FLOAT-expression CTAS | 0.0265s |
| SLJIT fused direct append | 0.0154s |

This worked because JIT removed whole layers of work:

- no intermediate result vectors for the fused expression path
- less materialization
- fewer copies
- direct append into the sink path

The lesson is not "FLOAT is special". The lesson is that JIT wins when it deletes
operator-boundary and materialization work, not when it merely generates scalar
code for one expression inside the same vectorized pipeline shape.

### Native FLOAT Support

Native FLOAT/DOUBLE code generation helped because it avoided generic conversion
and widened arithmetic paths. It only became compelling when combined with fused
direct materialization. As a standalone expression replacement, JIT overhead can
still erase the benefit.

### Focused Source/Selection Cleanup

Several low-level cleanups were worth keeping:

- common source selection canonicalization
- direct filter-selection handoff into aggregate update kernels
- selected-present fast paths for aggregate source loading
- hoisting group-selection base pointers
- hoisting group-data base pointers where safe
- avoiding null checks when runtime facts prove all payloads are present

These improved generated-region runtime, but did not create a large end-to-end
TPC-H win by themselves.

The direct filter-selection handoff was still a real Q1 improvement because it
deleted a `FILTER -> sliced DataChunk -> aggregate` wrapper from the generated
path. After the handoff, the grouped aggregate kernel consumes the filter
selection directly:

| Workload | Before | After |
| --- | ---: | ---: |
| TPC-H Q1 SF1, single-thread | 1.044x | 1.077x |
| TPC-H Q1 SF10, single-thread | 1.072x | 1.111x |

The lesson is important: if a direction removes a real data-path boundary but
the first result is only moderate, keep going. The handoff proved the boundary
was real; it also exposed the next root cost, which is the remaining separate
predicate pass before the perfect-hash aggregate update.

The next step fused that predicate into the perfect-hash aggregate loop. Q1 no
longer writes a filter selection and then rereads it in the aggregate update;
the generated loop evaluates `l_shipdate <= DATE '1998-09-02'` as a guard before
group lookup and payload updates.

| Workload | Direct handoff | Predicate-gated loop |
| --- | ---: | ---: |
| TPC-H Q1 SF1, single-thread | 1.077x | 1.224x |
| TPC-H Q1 SF10, single-thread | 1.111x | 1.251x |

Runtime tracing on SF1 confirms the hot update stage is now
`aggregate_update.filtered_perfect_hash_update`; `filter.selection` is gone.

The root cause was not only "missing predicate fusion". The source filter was
lowered as a native DATE compare, while the fused aggregate builder needed a
typed boolean expression tree. Deferred filter preparation then copied the
native filter with auxiliary trees stripped, so the aggregate builder never saw
the predicate tree. The fix was to treat DATE as an INT32-compatible typed-tree
value and preserve auxiliary trees only for deferred filters. That eliminated
the edge case instead of adding a runtime fallback.

### Q3 Join-To-Aggregate Boundary Removal

The Q3 one-join grouped-aggregate forced/profile path now proves two more
data-centric lessons.

First, removing `hash_join_probe.final_output` was necessary but not sufficient.
The probe can keep row pointers live, but a projection-source chunk still costs
work unless downstream stages consume references directly.

Second, grouped aggregate state pointers must stay owned by DuckDB's hash-table
runtime. The useful ABI is not a per-row C++ callback and not a full source-order
address vector. The new selected state-address updater gives generated code a
compact state-address span plus the source-row selection while DuckDB still owns
probing, appending, duplicate-new resolution, and tuple layout.

Verified forced/profile Q3 evidence:

| Removed | Replacement |
| --- | --- |
| `aggregate_update.address_buffer_callback_new_update=30519` | `aggregate_update.state_address_selection_new_update=30519` |
| full regular hash join output | `hash_join_probe.direct_row_pointer_reference=30519` |
| simple RHS reference projection through the source chunk | `projection.direct_post_join_reference_projection=30519` |

The same selected state-address ABI now covers existing-group fused typed
payload updates. The all-existing grouped aggregate telemetry test requires
`aggregate_update.state_address_selection_existing_update` and forbids
`aggregate_update.address_buffer_callback_existing_update`. That matters because
the ABI is now symmetric for new and existing fixed-width typed payload routes:
the generated updater receives state-address spans, while DuckDB owns lookup and
tuple layout.

The direct reference projection slice reduced traced
`hash_join_probe.materialize_projection_sources` runtime from about `2541 us` to
about `170 us` in the Q3 forced/profile run. End-to-end forced timing was still
below vectorized in a single-repeat trace run, so this is not a CBO admission
result. It is structural evidence that the next root work is to fuse computed
post-join payload expressions from join input and row pointers without forming a
projection-source chunk.

## What Failed And Why

### Per-Vector GPU Launch

The early Metal path was slower on simple tests because it paid GPU costs at the
wrong granularity:

- one launch per DuckDB vector is too expensive
- buffer and command state reuse was insufficient
- immediate host copyback destroyed GPU residency
- the cost model did not charge a realistic per-batch GPU cost

Correct direction:

- batch many DuckDB vectors into one Metal launch
- reuse buffers and command state
- avoid immediate host copyback
- keep data resident across fused stages
- charge real per-batch startup and transfer costs in the CBO

Until that exists, GPU should only win on workloads with enough arithmetic per
byte and enough rows per launch.

### Vector Size 4096

Changing DuckDB's vector size to 4096 exposed broad test instability. The vector
size is a global execution contract, not a local JIT tuning knob. We restored
2048 before full verification.

### SLJIT Without A JIT Projection Event

One simple FLOAT result showed:

| Mode | Time |
| --- | ---: |
| Vectorized | 29 ms |
| SLJIT setting, no SLJIT projection event | 32 ms |
| Metal | 57 ms |

This is expected: enabling a JIT setting without actually fusing the hot path
adds planning/runtime overhead but does not delete execution work.

### One-Entry Aggregate Run Cache

The common-selected one-entry run cache was correct but bad for TPC-H Q1.
Measured Q1 group locality:

- row count: 5916591
- run count: 2131269
- average run length: 2.776 rows

Runs are too short. Frequent flush and branch overhead beat any benefit from
keeping one group in a local accumulator.

### Dense Active Slots

Dense active-slot remapping was also correct but not profitable for Q1. The
extra per-row slot-map load cost more than it saved. The existing count-sentinel
sparse-local path remained lower overhead for this shape.

### Extra Common Group Selection Specialization

Group common-selection specialization did not show a reliable win. It added more
code and branching without removing enough per-row work. The cleaned code keeps
the simpler accepted fast paths instead.

### Sparse Eager Zero

Sparse eager-zero only makes sense for very small sparse domains. For Q1-sized
domains, setup and commit scans over many groups outweighed row-loop branch
removal. The useful rule is to cost-gate it to small domains and strong runtime
facts.

## Data Type Lessons

TPC-H does not exercise FLOAT/DOUBLE-heavy schemas. It mostly uses:

- BIGINT
- INTEGER
- DECIMAL(15,2)
- DATE
- VARCHAR

That means the FLOAT/DOUBLE CTAS win does not translate to TPC-H unless the same
fusion principle is applied to the TPC-H types.

The missing work is not just "add more native types". Native integer, decimal,
date, and varchar paths help only when they also remove generic dispatch,
materialization, validity handling, selection indirection, or sink-boundary work.

## CBO Lessons

The CBO must distinguish between three very different cases:

1. JIT setting enabled but no generated hot path.
2. Generated expression code inside an otherwise vectorized pipeline.
3. Fused region that deletes materialization, copies, or operator-boundary work.

Only the third case deserves aggressive benefit assumptions.

For GPU, the CBO must charge:

- launch cost per batch
- transfer/copyback cost
- buffer setup or reuse state
- synchronization cost
- expected rows per launch
- arithmetic intensity

For SLJIT, the CBO must charge:

- compilation/startup cost
- generated region call overhead
- selection/vector indirection that remains
- sink/source work outside the generated region
- whether a fused path removes materialization

## Core Principles

### Delete Work, Do Not Move It Around

JIT wins when it removes whole categories of work:

- intermediate vectors
- copies
- sliced chunk wrappers
- validity checks proven unnecessary
- selection loads proven redundant
- generic function dispatch
- operator-boundary materialization

Small instruction-level changes are secondary until the larger pipeline shape is
right.

### Keep Data Hot

The right fusion model is data-centric:

- load payload once
- compute while it is still in registers
- keep group id and aggregate state pointer hot
- write final state only when necessary
- avoid reloading selection/group metadata per expression

The goal is not "more JIT". The goal is fewer trips through memory and fewer
generic engine layers.

### Do Not Trust 1.01x Or 1.05x

Small speedups are often noise. Treat them as leads, not conclusions. A change is
meaningful only when it survives repeated runs, has a plausible root cause, and
does not regress other verified workloads.

### Be Aggressive Once The Direction Is Proven

A temporary bad checkpoint is not a verdict against a structurally correct
optimization. If the direction deletes a real boundary, removes a pass, keeps
data resident, or turns an edge case into a stronger fact, keep pushing until the
root cause of the disappointing result is understood.

The operating rule is:

- do not rollback a correct direction just because the first number is weak
- profile the generated loop and the surrounding pipeline before judging it
- identify whether the loss is setup cost, materialization, memory traffic,
  selection indirection, state lookup, branch shape, or CBO selection
- optimize the actual limiter boldly, then measure again
- only narrow or abandon the direction after the root cause proves it cannot win
  for the target workload

This is how the Q1 path moved from modest filter-selection handoff gains to a
larger predicate-gated aggregate win. The first checkpoint showed the boundary
was real; the deeper pass found the next cost and removed it.

### Failed Experiments Are Profiling Data

Do not reverse a correct direction just because the first implementation is bad.
First locate the cause:

- short group runs killed one-entry caching
- per-row slot-map loads killed dense remapping
- launch/copyback overhead killed per-vector GPU
- setup/commit scans killed sparse eager-zero on larger domains

After the cause is known, either narrow the optimization to the shape where it
wins or redesign it around the real bottleneck.

The predicate-gated Q1 work is the positive version of this rule. The direct
handoff was only a moderate win, but it deleted a real boundary and exposed the
next cost. Continuing deeper produced a larger, repeatable win because the next
optimization removed another pass, not because it tuned a few instructions.

### Eliminate Edge Cases By Strengthening Facts

The best fast paths come from stronger runtime/planner facts, not scattered
conditionals:

- all payloads present
- source selection is common
- group selection is selected-present
- group domain is small enough for eager-zero
- sink can accept direct materialization

When those facts are explicit, special cases become normal cases.

## Next Aggressive Directions

### 1. TPC-H Type Coverage With Fusion

Add native BIGINT, INTEGER, DECIMAL(15,2), DATE, and selected VARCHAR paths where
they remove real work. Do not add isolated type support that still materializes
through the same generic vectorized boundaries.

Exit criteria:

- correctness tests for each type family
- CTAS/direct materialization benchmark per type
- TPC-H query evidence, not only synthetic microbenchmarks

### 2. Grouped Aggregate State Locality

The current Q1 bottleneck is per-row grouped aggregate state lookup/update around
sparse perfect-hash state. The next root-level optimization should reduce that
traffic.

Promising directions:

- packed group-state layout when runtime stats prove a small active domain
- remapping without a per-row slot-map load on the hot path
- multi-group local accumulation only when flush frequency is controlled
- better dictionary/statistics knowledge for compressed string grouping keys

Exit criteria:

- prove fewer loads/stores in the generated loop
- show TPC-H Q1 generated-region improvement beyond noise
- preserve full unit-test correctness

### 2a. Predicate-Gated Perfect-Hash Aggregate Loop

After source filters became generated and the filter-selection handoff removed
the sliced chunk boundary, Q1 still runs two generated passes per input batch:

1. evaluate `l_shipdate <= constant` into a selection
2. loop over selected rows to do perfect-hash group lookup and payload updates

The root fix is to make the filter predicate a guard inside the perfect-hash
aggregate loop. That keeps the row in the same generated loop, avoids writing
and rereading a selection vector, and lets the codegen skip group lookup for
rejected rows.

Exit criteria:

- done: runtime stage breakdown no longer contains `filter.selection` for
  Q1-shaped generated source filters
- done: correctness matches vectorized Q1
- done: SF10 Q1 improved from the direct-handoff checkpoint of 1.111x to 1.251x

### 2b. CBO Must Not Let Expression Cost Pay For Unfunded Native Joins

The all-query TPC-H verification exposed a default-auto regression on Q14. The
region was correct, but the cost model admitted a full-pipeline table-scan,
native hash-join, and native ungrouped-aggregate region using generated
expression cost alone. Trace timing disproved that estimate: the generated body
and source-contract overhead were slower than DuckDB's vectorized path.

Root principle:

- generated expression cost can justify standalone generated work
- native hash-join or stateful protocol work needs explicit native-stage or
  full-pipeline benefit evidence
- correctness-clean compiled regions still need admission control until their
  runtime path is proven faster
- strict all-query benchmarks belong in the verification loop for CBO changes;
  a focused query win can hide a different default-auto regression

Exit criteria:

- done: strict all-query TPC-H SF1 verifier passes after blocking unfunded
  native-join plus ungrouped-aggregate admission
- done: Q14 no longer compiles under default auto and returns to vectorized
  timing
- still open: optimize the Q14-shaped region enough to earn explicit CBO
  admission instead of relying on expression-cost optimism

### 2c. Q14 Source Filters Exposed Source-Contract Cost

Forced Q14 compilation showed the same pattern as Q1: a correct fused direction
can still lose until the real boundary cost is removed. Runtime tracing showed
that the generated `filter+projection` stage dominated the compiled region, not
the decimal CASE aggregate payload. Routing conjunctive table-scan filters
through DuckDB's filtered scan contract removed that generated stage.

The result changed forced Q14 from a clear regression to near break-even in
production timing, but it is still noise-level and not strict-verifier clean as
a forced optimization:

| Workload | Before | After |
| --- | ---: | ---: |
| TPC-H Q14 SF1, forced native-stage admission | 0.927x | 0.97-1.00x |

This did not prove Q14 should be admitted by default. It proved the next root
cost: source-contract and region-transition overhead became the dominant time
after generated source filter/projection disappeared. The next Q14-class work
should reduce or eliminate that contract overhead before spending effort on
payload arithmetic.

Exit criteria:

- done: generated runtime trace no longer shows a `filter+projection` stage for
  the Q14-shaped compiled region
- done: default-auto all-query TPC-H SF1 remains strict-verifier clean
- still open: make source-contract execution cheap enough that Q14 earns native
  join plus ungrouped-aggregate admission without relaxed CBO settings

### 2d. Q1 Needs Narrow Grouped-Aggregate Admission

TPC-H Q1 exposed a CBO calibration bug, not an execution bug. The main
scan/filter/projection/perfect-hash aggregate region was already fully fused and
profitable, but default CBO rejected it because grouped aggregate native protocol
had zero explicit native-stage benefit. Setting a global native-stage benefit to
`1` proved Q1 could win, but it also compiled Q4 and Q21 join-probe regions and
caused large regressions.

The root rule is narrower: generated compute may pay for standalone grouped
aggregate protocol when there are no native joins or sorts. It must not unlock
generic join-probe pipelines. With that rule, Q1 compiles by default while Q4
and Q21 stay vectorized.

Verified result:

| Workload | Default auto before | Default auto after |
| --- | ---: | ---: |
| TPC-H Q1 SF1 | 0.99-1.00x | 1.19-1.21x |
| TPC-H Q4 SF1 | vectorized | vectorized |
| TPC-H Q21 SF1 | vectorized | vectorized |

Exit criteria:

- done: Q1 default auto compiles the standalone grouped aggregate region
- done: all-query TPC-H SF1 production verifier passes with default settings
- done: Q4 and Q21 do not inherit the admission rule through join-only spans

### 2e. Scan-Filtered Join Aggregates Need Downstream-Batch Budgeting

Q14's remaining loss was not the aggregate arithmetic. A forced trace showed the
compiled body was cheap, but the source contract resumed after every 50 raw scan
chunks. Because the date filter is selective, those 50 source invocations often
produced only a partial downstream vector. The runtime then alternated between
scan-only events and scan-plus-generated-work events.

The fix was to budget batched source-contract execution by full downstream
batches, while retaining a larger raw-source fetch cap for cooperative
scheduling. That collapsed Q14 forced trace runtime events from 177 events over
three repeats to 3 events over three repeats. With tracing off, Q14 moved from a
forced regression to a default-auto win once CBO admitted the exact
scan-filtered join plus ungrouped aggregate shape.

Verified result:

| Workload | Default auto result |
| --- | ---: |
| TPC-H Q14 SF1, 9 repeats | 1.05x |
| TPC-H Q19 SF1, 15 repeats after follow-up batching | 1.00x |
| TPC-H all queries SF1, 5 repeats | strict verifier passed |

Q19 exposed the next edge of the same principle. It has scan filters plus an
extra generated filter/projection before the join. The first Q14 CBO admission
made Q19 compile, but Q19 initially stayed unbatched and processed tiny
post-scan chunks through `filter+projection` and regular hash-probe work. Extending
the generated-filter/projection batcher to scan-filtered sources reduced Q19
trace runtime events from 93 to 3 over three repeats and removed the measured
production regression.

Root principle:

- schedule fused pipelines by the batch granularity consumed by the downstream
  generated/native work, not by arbitrary raw source invocation count
- when CBO admits a new shape, immediately search for sibling shapes that now
  compile but do not use the same optimized runtime path
- do not globally fund native joins; admit only shapes whose runtime path has
  been measured and whose negative controls stay vectorized

Exit criteria:

- done: Q14 compiles by default under auto and uses DuckDB scan filters
- done: Q14's default-auto SF1 benchmark is a measured win
- done: Q19's scan-filter plus generated-filter/projection variant is batched
- done: Q4 and Q21 remain vectorized under default auto
- done: full TPC-H SF1 verifier passes after Q14/Q19 admission and batching

### 2f. Regular Hash-Aggregate Admission Must Separate Deep Compute From Glue

Q9 proved that regular hash aggregates can win without generated hash lookup
ownership when the backend resolves grouped state addresses natively and keeps
the generated payload work fused. The first default admission was still too
broad: it also compiled Q5, Q7, and Q18 shapes where the generated body was too
shallow or where two regular join probes plus generated source filtering spent
more time in protocol and state lookup than the vectorized plan.

The root cause was not the native state-address direction. The root cause was
CBO shape blindness:

- `source_filter_pushdown` was a capability flag, not proof that the candidate
  actually used scan filters; treating it as actual filter use blocked Q9
  incorrectly.
- a grouped aggregate with one generated stage is mostly state-address setup,
  not enough generated work to fund compilation
- one join plus grouped aggregate can win with generated source filters (Q3)
- two joins plus grouped aggregate need deeper generated compute and no generated
  source-filter stage until that path is optimized (Q9 wins, Q5/Q7 lose)

The current admission rule follows the measured shapes:

- standalone grouped aggregate requires at least two generated stages
- join plus grouped aggregate requires at least three generated stages
- one join may still use generated source filters
- two joins require zero source filters
- generated lookup remains a blocker annotation, but native grouped-state
  address resolution is a valid execution path

Verified result:

| Workload | Default auto result |
| --- | ---: |
| TPC-H Q1 SF1, 5 repeats | 1.20x |
| TPC-H Q3 SF1, 5 repeats | 1.23x |
| TPC-H Q9 SF1, 5 repeats | 1.11x |
| TPC-H Q5/Q7/Q18 SF1 | vectorized/neutral |
| TPC-H all queries SF1, 5 repeats | strict verifier passed |

The next Q9 hotspot was still the generated regular hash join probe. Instruments
showed roughly half the CPU samples in anonymous JIT pages, with DuckDB hash
table salt extraction and hash-table work next. The SQL runtime trace pointed to
the same place: generated regular hash-probe stages dominate the compiled Q9
region. The profitable low-risk fix was to specialize the flat/all-valid probe
by hash-table salt mode and hoist `pointer_mask`/`bitmask` into saved registers.
That removes repeated invariant loads and the `use_salt` branch from the hot
specialized probe loop.

Exit criteria:

- done: Q9 default auto compiles the regular hash-aggregate state-address region
- done: Q5/Q7/Q18 losing grouped-aggregate shapes do not compile by default
- done: Q3 remains admitted despite generated source filters
- done: grouped hash-aggregate tests assert native state-address execution
- done: flat/all-valid regular hash probe has salted/unsalted specializations
- done: `[api][jit]`, architecture verifier, focused TPC-H, and all-query TPC-H
  verification pass

### 2g. Scan-Filter-Owned Pipelines Must Cost Post-Filter Rows

Q19 looked like a small remaining compiled loss after batching and regular probe
specialization. A traced run showed the generated/probe/aggregate body was not
the bottleneck: one runtime event consumed only 2,268 post-filter rows and spent
about 2.4 ms in generated/native work under trace, while the source contract
spent about 37.7 ms, mostly in `table_scan.storage_scan_all_columns`.

The root cause was CBO row accounting. The scan-filter-owned candidate was
costed with the pre-filter scan cardinality, 1,500,304 rows, even though DuckDB
owned the pushed scan filters and the accelerated body only saw the much smaller
post-filter stream. That multiplied a shallow generated body by too many
batches and admitted a region whose real wall time was source dominated.

The fix is to discount the runner-benefit row count when `uses_scan_filters`
is true. Generated source filters are different: SLJIT evaluates those rows
inside the accelerated body, so they keep the full input cardinality. This keeps
Q3/Q9-style generated work funded while Q19-style selective DuckDB scan filters
stay vectorized until the source-side predicate itself becomes generated or the
scan path becomes natively owned.

Verified result:

| Workload | Default auto result |
| --- | ---: |
| TPC-H Q19 SF1, 9 repeats | vectorized, 1.00x |
| TPC-H Q1 SF1, 5 repeats | 1.20x |
| TPC-H Q3 SF1, 5 repeats | 1.23x |
| TPC-H Q9 SF1, 5 repeats | 1.11x |
| TPC-H all queries SF1, 5 repeats | strict verifier passed |

Exit criteria:

- done: Q19 no longer compiles by default after scan-filter row discounting
- done: Q1/Q3/Q9 compiled wins remain after the discount
- done: all-query TPC-H SF1 verifier passes after the CBO correction

### 2h. Regular Hash-Probe Wins Come From Removing Scalar Loop Invariants

The next Q9 pass showed the compiled region was not CBO-limited. The hot runtime
stage was still `op0:hash_join_probe.generated_regular_probe_flat_all_valid_function`
inside the first regular hash probe over the 6M-row `lineitem` stream. Source
contract and materialization were secondary; the generated scalar probe loop was
the root.

Useful fixes were all data-centric invariant removals inside the generated loop:

- cache flat/all-valid probe code by salt, chain, and dictionary-emission layout
  flags so runtime layout checks disappear from specialized code
- hoist the Murmur hash multiplier instead of rematerializing the same 64-bit
  immediate for every hash multiply
- hoist flat key data pointers when saved registers are available
- keep the hash-table slot offset in a register for flat/all-valid comparison
  instead of saving/restoring it through the stack for every candidate
- compare output capacity against `STANDARD_VECTOR_SIZE` directly instead of
  loading `input->output_capacity` for every match
- specialize the no-chain variant, but treat branch-layout wins as empirical:
  smaller code is not automatically faster on Apple cores

Measured shape on Q9 SF1 single-thread tracing:

| Stage | Before this pass | After invariant pass |
| --- | ---: | ---: |
| op0 generated flat/all-valid probe | ~54-55 ms | ~48-49 ms |
| whole generated body | ~107-110 ms | ~98-102 ms |

Production timing remains noisy at this scale, but the query-level shape stayed
positive: Q9 auto stayed around 0.137-0.141s versus vectorized/off around
0.159-0.168s in the local 9-11 repeat runs.

Exit criteria:

- done: generated hash-probe stage moves down under trace
- done: Q9 remains a production win over vectorized/off
- done: focused JIT tests and architecture verifier pass after the invariant
  reductions

### 2i. Typed Probe Helpers Validate The Real Hash-Probe Bottleneck

After the scalar generated loop was stripped of obvious invariants, Q9 was still
dominated by regular hash-table probe work. The useful next step was not SIMD or
more expression fusion. It was to make the hot probe shape explicit: flat,
all-valid, no duplicate chains, matched output, and fixed-width equality keys.

The typed helper path proved the root cause. A compiler-optimized C++ helper for
the exact probe contract beat the generic generated SLJIT loop and removed lazy
probe codegen from the runtime path. This is still a JIT-owned fast path because
it is selected only from the native region contract; it is not a benchmark-only
shortcut.

The winning shape was deliberately narrow:

- two-key `INT64/UINT64` composite probe for Q9 op0
- one-key fixed-width integer probe for Q9 op2 and simple hash-join coverage
- all probe inputs flat and all-valid
- build keys proven all-valid by the same dispatch facts used by the generated
  all-valid probe
- no duplicate chains, no residual predicate, no build-match marking, and output
  capacity large enough for the input batch

The measured lesson was that prefetch and branch layout are not generic wins.
One-row bucket lookahead helped the two-key Q9 probe because the loop has enough
work to overlap the next hash-table entry load. The same idea hurt the small
single-key probe by adding register pressure and extra arithmetic. Immediate row
prefetch helped the single-key probe but was redundant for the two-key lookahead
path. Branch-likelihood hints also regressed the trace. Keep only the facts that
survive stage-level timing.

Measured Q9 SF1 single-thread result:

| Checkpoint | Q9 auto median | Q9 vectorized/off median | Speedup |
| --- | ---: | ---: | ---: |
| pre-typed-helper baseline | 0.139s | 0.162s | 1.17x |
| typed helper + pair lookahead | 0.129-0.131s | 0.162-0.168s | 1.26-1.28x |

Trace shape after the helper pass:

| Stage | Before helper pass | After helper pass |
| --- | ---: | ---: |
| Q9 op0 regular probe | ~50 ms generated | ~39 ms typed helper |
| Q9 op2 regular probe | ~3.7 ms generated | ~2.9 ms typed helper |

The current remaining Q9 costs are now more balanced:

- op0 random hash-table lookup is still the largest cost
- two hash-join materialization steps are about 3 ms each
- projection glue is about 5-6 ms combined
- grouped aggregate state-address resolution is about 6 ms

Exit criteria:

- done: Q9 op0 and op2 use typed fast probe stages under trace
- done: Q9 production median improved beyond noise
- done: Q3 stayed stable in focused Q3/Q9 verification
- done: all-query TPC-H SF1 verifier passed with correctness diff 0
- done: `[api][jit]`, architecture verifier, and build passed after formatting

### 2j. Pre-Backend CBO Admission Needs Runtime-Proven Shape Guards

Q14 exposed a two-phase CBO blind spot. The profitable backend path is found
only after lowering: source-contract table scan, one native hash join probe, one
typed projection, and one native ungrouped aggregate update. Default auto first
runs a cost-only gate before backend analysis, and that pre-backend gate does
not yet know that lowering will select DuckDB scan filters. The old
scan-filter-specific admission rule therefore could not fire, even though a
forced trace proved the lowered runtime path was already cheap enough.

The fix was not to fund generic native joins. The cost-only gate now admits only
the runtime-proven direct materialization shape:

- full pipeline
- generated compute, not projection glue
- materialization elision into the aggregate sink
- exactly one native join stage
- exactly one native ungrouped aggregate stage
- no grouped aggregate and no sort stage
- high generated expression work

The last guard matters. The first version admitted Q17 too because it has the
same one-join/one-ungrouped-aggregate skeleton, but only 164 expression-cost
units per batch. It was correctness-clean and roughly neutral, not a real win.
Adding a high-expression-work floor keeps Q14 (`1110`) admitted and blocks Q17.

Verified result:

| Workload | Result |
| --- | ---: |
| TPC-H Q14 SF1, 20 repeats | off 0.039s, auto 0.037s, 1.054x |
| TPC-H all queries SF1, 5 repeats | strict verifier passed |

All-query negative controls after the guard:

- Q14 compiles by default and remains correctness-clean
- Q17 stays vectorized after the shallow-work guard
- Q6, Q7, Q8, Q20, Q21, and Q22 do not inherit this admission rule

Exit criteria:

- done: default-auto Q14 reaches backend lowering and compiles the intended
  one-join/one-ungrouped-aggregate region
- done: Q14 20-repeat production median remains a small repeatable win
- done: Q17 shallow-work shape is rejected by the pre-backend CBO
- done: all-query TPC-H SF1 verifier passes with correctness diff 0

### 2k. Regular Join Output Copies And Selected Probes Are Real, But Aggregate Lookup Is The Next Root

Q3 and Q9 both had avoidable regular hash-join overhead after the first native
join helpers landed. The important distinction was not "join helper exists";
it was which data shape reached the probe and materialization boundary.

Fixes that survived measurement:

- regular hash join probe output now references probe columns directly when the
  match selection is an identity selection over the full input chunk, matching
  the existing perfect-hash shortcut
- selected all-valid, single-key, no-chain regular probes now use a native fast
  helper instead of the generic generated selected probe
- the single-key flat and selected helpers prefetch the next hash-table bucket,
  matching the pair-key helper's lookahead style
- coverage for the selected helper must use a grouped aggregate shape; an
  ungrouped aggregate triggers the generated-filter batching path, compacts the
  filtered rows, and correctly uses the flat helper instead

Measured effects:

| Workload | Result |
| --- | ---: |
| Q9 op2 materialization in trace | ~8.9 ms -> ~0.6 ms over 3 traced repeats |
| Q3 20-repeat production after selected helper + prefetch | off 0.073s, auto 0.055s, 1.327x |
| Q9 20-repeat production after regular reference + prefetch | off 0.165s, auto 0.129s, 1.279x |
| TPC-H all queries SF1, 5 repeats | strict verifier passed |

All-query SF1 after this pass:

- Q1: 1.190x, compiled
- Q3: 1.263x, compiled
- Q9: 1.273x, compiled
- Q14: 1.056x, compiled
- all other queries: vectorized, with only noise-level movement

Root cause that remains:

- Q3 grouped aggregation creates many new groups, so the hot cost is real hash
  aggregate insertion and state-address resolution, not an obsolete JIT wrapper
  branch
- Q9 still spends meaningful time in the pair-key random hash-table probe and
  grouped aggregate state lookup
- the existing grouped aggregate fast-existing resolver only helps when all
  groups are already present and vectors are flat/all-valid; it does not solve
  high-new-group Q3

Next root solution:

- lower grouped aggregate lookup itself into data-centric generated/native code
  that owns group hashing, probing, append, and state-address production
- preserve or pass through hash values when a preceding join already computed a
  compatible group key hash
- only benchmark SIMD after this lookup/materialization work is fused enough
  that arithmetic, not state lookup, is the bottleneck

Exit criteria:

- done: selected all-valid helper fires in focused API coverage
- done: `[api][jit]`, `*hash join*`, architecture verifier, and TPC-H verifier
  all pass
- done: Q3/Q9 focused production medians remain better than the prior compiled
  baseline
- next: grouped aggregate lookup ownership must move from wrapper calls into
  fused generated/native execution

### 3. SIMD After Fusion

SIMD support is worth benchmarking seriously only after the fused loop is regular
enough to feed lanes:

- predictable payload loads
- predictable group/state access
- minimized selection indirection
- no per-row generic dispatch

SIMD before that point risks optimizing arithmetic that is not the bottleneck.

Exit criteria:

- inspect generated loop shape before adding SIMD
- benchmark scalar fused vs SIMD fused
- measure both generated-region time and end-to-end query time

### 4. GPU Batch Fusion

The GPU path needs a different granularity:

- multi-vector batches
- fused expressions per launch
- reused command/buffer state
- delayed copyback
- CBO startup/transfer cost that matches reality

Exit criteria:

- GPU wins on high arithmetic-intensity queries
- GPU loses correctly on simple low-intensity projections
- cost model decisions match measured break-even points

### 5. Fused Filter Metadata Must Move As One Unit

Q6 exposed a correctness root cause in the filtered aggregate fusion path:

- table-scan source filters are planned against source-contract input columns
- simple filter plans can also carry a typed expression tree for later fusion
- the projection remapper updated `source_index`, but left
  `expression_tree_source_indices` out of date
- normal filter execution used the remapped `source_index` and looked correct
- fused filtered aggregate execution used the outdated expression-tree source list
  and read the wrong column after scan filter/projection pruning

The fix is structural: source remapping must update every parallel source
representation carried by an expression plan. This is better than adding a
special Q6 guard because it eliminates the edge case for all fused expression
plans that carry both a compact operator form and an expression-tree form.

Performance lesson:

- fixing the remap makes forced Q6 correct, but not fast
- selected all-valid filtered aggregate fast path is useful groundwork, but Q6
  remains dominated by repeated source-filter passes before the aggregate
- the next root performance move is to fuse generated source filters into one
  data-centric predicate pass, then feed the filtered aggregate

Exit criteria:

- done: focused API regression covers scan-filter projection remap plus filtered
  primitive aggregate update
- done: Q6 predicate matrix matches vectorized results for date, discount,
  quantity, and their combinations
- next: combine generated source filters so Q6 stops scanning/slicing the same
  vector through multiple filter operators

### 6. Latest Cleaned-State Correction: Source Fusion Helped, Lookup Ownership Still Blocks Coverage

The latest cleaned-state verification changed two important facts.

First, Q6's source-filter correctness fix was necessary but not sufficient for
performance. Fusing multiple generated table-scan filters into one predicate
removed repeated filter operators and improved forced Q6 from the earlier
~46 ms range to ~37-38 ms. Keeping large complex scan filters in DuckDB's scan
filter path improved the forced shape again to ~35 ms, but vectorized/off
remained about 30 ms. The root cause is now clear: after source-filter work is
made correct and less fragmented, Q6 is still source-scan/filter dominated. It
should stay vectorized by default until the source path itself is natively owned
or the aggregate work becomes large enough to pay for the extra boundary.

Second, regular hash aggregate lookup must be treated as an unfunded native
protocol until generated/native lookup ownership exists. In the current cleaned
state, Q3-shaped regions with blocked hash aggregate lookup can be correctness
clean but slower than vectorized because they still call DuckDB to resolve
grouped state addresses. The CBO now charges blocked regular hash aggregate
lookup, which keeps Q3 neutral/vectorized while preserving high-work wins such
as Q1 and Q9. This is an honesty fix, not the root performance fix.

Current root solution for more TPC-H coverage:

- implement regular hash aggregate lookup ownership: hash, probe, append, and
  produce aggregate state addresses inside the fused native/JIT path
- keep source filters generated only when they feed a fused loop that deletes a
  pass; otherwise let DuckDB's scan filter path own selective scans
- use forced compilation as a profiler, not as a CBO policy; if forced is still
  slower after fusion, locate the dominant stage before funding the shape

Latest measured cleaned-state checkpoints:

| Workload | Result |
| --- | ---: |
| Q1 SF1 default auto subset | ~1.11x |
| Q9 SF1 default auto subset | ~1.08x |
| Q3 SF1 after blocked-lookup charge | vectorized/neutral |
| Q6 SF1 forced generated source filters | ~0.80x |
| Q6 SF1 forced large scan-filter strategy | ~0.86x |

### 7. Q12 String CASE Support Exposed Sparse Join Probe as the Real Blocker

Adding native typed-tree support for `VARCHAR = constant`, `VARCHAR <> constant`,
and `CASE ... THEN 1 ELSE 0` made the Q12 aggregate payload lower cleanly. The
forced Q12 trace then showed the important performance fact: the CASE projection
is cheap. On SF1 it was about 0.6 ms, while the native regular hash join probe
over the 1.5M-row orders side was about 13 ms. Grouped aggregate state update
work was also sub-millisecond.

The root cause was not string codegen. Q12 builds a small filtered lineitem hash
table, but its `l_orderkey` range is sparse: about 31K build rows spread across
nearly 6M key values. DuckDB's perfect hash join correctly rejects that shape,
so the native path falls back to regular hash probing. Until sparse regular
probe and grouped aggregate lookup are owned better, auto-funding this shape is
wrong.

The CBO rule is now narrower: single-join grouped aggregates with only two
generated stages do not pay for a blocked grouped hash aggregate lookup. This
keeps the new string CASE support available for forced/profiling paths and
future fused lookup work, but prevents Q12 from regressing in auto mode.

Measured checkpoint:

| Workload | Result |
| --- | ---: |
| Q12 forced string CASE/native join probe | ~0.82-0.88x |
| Q12 auto after guard | neutral, compiled regions 0 |
| Q1/Q9/Q14 subset after guard | Q1 ~1.19x, Q9 ~1.29x, Q14 small/noisy-to-~1.08x |

Q6 was also force-admitted with zero startup cost after the source-filter fixes.
It compiled cleanly but stayed flat at about 29 ms. That confirms Q6 is not a
CBO-threshold problem; the remaining work is still source/filter-path ownership,
not cheaper admission.

Metal lesson: batching many vectors per launch is correct, but the batch budget
must be finite. Using `runtime.MaxChunks()` directly can request an impossible
Metal buffer before reading the source, even with `STANDARD_VECTOR_SIZE=2048`.
Cap the per-launch batch to a practical number of vectors and reuse buffers.

### 8. Q3 Was a Startup-Accounting Miss, Not a Backend Regression

The earlier blocked-lookup lesson was too conservative for Q3. Re-running the
full forced-admission sweep showed that Q3 has one profitable compiled region:
a generated compute region that fuses a single hash join into a grouped
aggregate with materialization elision and three generated stages. With default
startup it missed admission because the modeled benefit was about 26K, just
below the 32K startup plus 50% margin requirement. With a 16K startup boundary
the same region compiled and Q3 improved from about 69 ms to 53-54 ms on SF1.

The important distinction is shape, not a global threshold. Lowering startup
globally also admits Q17's standalone ungrouped aggregate region, which forced
neutral/slower. The safe rule is narrower: discount startup only for one-join
grouped aggregate fusion with materialization elision and at least three
generated stages. This admits Q3 while keeping Q4, Q15, and Q17 at zero compiled
regions.

Measured checkpoint after the startup discount:

| Workload | Result |
| --- | ---: |
| Q3 SF1 default auto, 20 repeats | 0.069s -> 0.054s, ~1.28x, compiled regions 20 |
| Q4 SF1 default auto, 20 repeats | neutral, compiled regions 0 |
| Q15 SF1 default auto, 20 repeats | compiled regions 0; residual timing noise only |
| Q17 SF1 default auto, 20 repeats | compiled regions 0 |
| Full TPC-H SF1 default auto, 9 repeats | Q1 ~1.19x, Q3 ~1.28x, Q9 ~1.29x, Q14 ~1.06x |

Principle: do not let one blocked native protocol become a blanket veto. If
forced execution shows a clean, repeated win, find the narrower shape that pays
and fund that shape only. Keep the larger unfused sparse-join and lookup-heavy
families unfunded until their backend path is genuinely faster.

### 9. Chained Hash Probe Fast Path Is Groundwork, Not Q7/Q12 Admission Yet

Regular hash join probing had an avoidable gap: single-key all-valid duplicate
key chains still fell back to generated probe code, while no-chain tables had
C++ fast helpers. A native helper can follow DuckDB's chain contract directly:
probe the pointer table, walk duplicate-key chains, pause with
`input_offset` plus `resume_row_pointer` when output fills, and resume the same
probe row on the next drain. The helper is safe only for the simple matched
output cases: one equality key, all-valid sources, no residual predicate, no
build marking, and no chain matcher.

The focused API test now verifies a real chained hash table with duplicate
build keys and requires the runtime stage
`fast_regular_probe_flat_all_valid_single_key_chain`, with no generated regular
probe fallback. This removes one backend gap and is the right direction for
sparse/chained joins.

It does not by itself make Q7/Q12 profitable. Forced native-operator funding
after the helper still left Q7 slightly slower and Q12 uncompiled/neutral in the
current shape. The remaining root blocker is broader: sparse join-heavy
pipelines still need better data-centric ownership of the join + grouped
aggregate lookup path, not just a faster duplicate-chain loop.

### 10. Q19 Is a Narrow One-Vector Admission, Not a General Startup Cut

Q19 has a small but repeatable shape: DuckDB scan filters prune the source down
to roughly one vector, then SLJIT fuses a single join into an ungrouped aggregate
with materialization elision. Forcing zero startup cost showed a stable small
median shift over 50 repeats:

| Workload | Result |
| --- | ---: |
| Q19 SF1 forced startup waiver, 50 repeats | 0.047s -> 0.045s, ~1.04x |

The safe admission rule is deliberately narrow: full-pipeline, scan filters
owned by DuckDB, at least three source filters, one join, one ungrouped
aggregate, no grouped aggregate, no sort, materialization elision, and
post-filter rows no larger than one vector. The default all-query check after
the waiver kept Q4/Q15/Q17 at zero compiled regions and admitted Q19. However,
the win is small enough to treat as a boundary-policy improvement, not a major
performance result.

### 11. Q12 Proved Bloom-Aware Probe Helps a Stage, But Not Enough to Admit

Q12 was the right deeper probe case because the CBO was not the root blocker.
With a tiny native-operator protocol cost, Q12 lowered and compiled, but the
initial runtime was much slower than DuckDB vectorized execution:

| Forced Q12 variant | Result |
| --- | ---: |
| Before chained-probe tuning | 0.056s -> 0.064s, ~0.88x |
| Pipelined chained probe | 0.055s -> 0.061s, ~0.90x |
| Bloom-aware chained probe, fixed | 0.057s -> 0.055s, ~1.04x |

The stage-level root cause was clear. In the forced traced run, Q12 probes 1.5M
orders rows into a small filtered lineitem build and returns only 30,988 matches.
The all-valid single-key chain probe dominated generated runtime:

| Probe path | Hash-probe stage |
| --- | ---: |
| Original chain helper | ~13.3 ms |
| Pipelined next-row prefetch | ~10.7 ms |
| Bloom-aware precheck | ~2.9 ms |

The first bloom implementation was fast but wrong: it returned tiny counts
because the pipelined loop advanced `key`, `salt`, and `ht_offset` without
advancing the full `hash` used for the bloom lookup. The hash table still worked
without bloom because it used the advanced offset/salt, but bloom saw the prior
row's hash and introduced false negatives. The fix is a useful principle: when a
pipelined loop carries derived state, every consumer must advance from the same
row, or a new optimization can silently corrupt filtering.

Even after the fix, Q12 is not a default-admission candidate. A 50-repeat run
with minimal native protocol cost showed only `0.056s -> 0.055s`; that is
noise-level for our bar. The backend improvement is still worth keeping because
it removes a real miss-heavy probe bottleneck and can help future fused join
shapes, but the CBO should continue skipping Q12 by default until the whole
pipeline is clearly faster.

### 12. Q9 Pair-Key Bloom Is a Real Probe-Side Win

Q9's remaining dominant stage was the first regular hash join probe:
lineitem probes the partsupp build with two `BIGINT` equality keys and returns
only about 319K matches from 6.0M probe rows. The existing typed pair-key helper
already removed generic probe overhead, but it still paid a random pointer-table
probe for nearly every miss.

The important root-cause detail is that build-side hash generation already
computes the exact combined two-key hash. Preparing a bloom filter for sparse
two-key JIT probes reuses that existing hash stream; the native pair probe can
then reject most misses before touching the pointer table. This is not a generic
"compile more joins" change. It is gated to JIT-enabled inner joins with exactly
two plain equality keys, no residual predicate, a large probe side, and a build
side no more than one quarter of the probe estimate.

| Q9 SF1 checkpoint | Result |
| --- | ---: |
| Previous default all-query run | ~1.29x |
| Pair-key bloom, 20 repeats | `0.1555s -> 0.1040s`, ~1.50x |
| Full TPC-H SF1 default, 9 repeats | Q9 `0.155s -> 0.105s`, ~1.48x |

The stage trace explains the gain:

| Q9 op0 pair-key probe stage | Runtime |
| --- | ---: |
| Before pair-key bloom | ~36.4 ms |
| After pair-key bloom | ~20.3 ms |

Principle: sparse native joins should spend cycles on deterministic, local
rejects before random hash-table probes. If the build path already owns a hash
stream, reuse it. Do not create a second data structure or loosen CBO admission
until the miss path itself is cheaper.

### 13. Q20 Needs the Two-Stage Blocked Grouped-Aggregate Exception

Q20 exposed a too-conservative edge in the CBO. The profitable region has only
two generated stages, one native hash join, one grouped hash aggregate, and a
blocked grouped-aggregate lookup. The old rule intentionally refused that shape
because similar blocked lookup paths had regressed in Q12/Q17. The forced run
showed Q20 is different: it is long enough that even a small modeled per-batch
win is real.

Measured checkpoint:

| Workload | Result |
| --- | ---: |
| Q20 default before rule | compiled regions 0; ~0.986x/noise |
| Q20 default after rule, 20 repeats | `0.071s -> 0.060s`, ~1.18x |
| Full TPC-H SF1 after rule, 10 repeats | Q20 `0.071s -> 0.061s`, ~1.16x |

The admission rule is narrow: full pipeline, exactly two generated stages,
materialization elision, one native join, grouped aggregate only, blocked hash
aggregate lookup, no sort, no DuckDB-owned scan-filter discount, and at least
512 vectors of modeled work. This admits Q20 but keeps the known bad small
blocked grouped-aggregate shapes skipped.

The next tempting forced win was Q3's extra two-join/no-aggregate region, but
the same surface traits appear in Q5, Q7, and Q8. Broad native-join funding made
Q7 and Q8 much slower, so the right next step there is backend/root-path work,
not a CBO rule. We need a better discriminator or a faster native join pipeline
before funding that family.

### 14. Q3 Join-Build Admission Needs Data-Centric Width and Payload Facts

Q3's next real win was not generic "more joins". The useful extra region is an
upstream source-filtered hash-join build chain that keeps the filtered scan rows
inside one generated/native pipeline before building the next hash table. The
first broad startup waiver admitted every long one-stage/two-join build chain
and immediately proved too loose:

| Broad join-build waiver | Result |
| --- | ---: |
| Q3 | faster, `~1.15x` |
| Q4 | regressed, `~0.61x` |
| Q8 | regressed, `~0.89x` |
| Q21 | regressed, `~0.75x` |

The root cause was missing cost facts. The bad Q4/Q21 regions had no generated
source filter. Q8 did have a generated source filter, but carried a wider
hash-build payload (`payload_columns=4`) and used a regular/chain probe at
runtime. Q5/Q7 had narrow payloads but only projected two source columns and
were noise-level at best. Q3 had all the profitable data-centric traits: a
generated source filter, a perfect-hash probe shape, a narrow build payload
(`<= 2` columns), and at least four projected source columns worth keeping in
the fused pipeline.

The final rule therefore threads those facts into the CBO and waives startup
only for long generated join-build chains with:

- one generated stage
- exactly two native join stages and no aggregate/sort
- generated source filters
- perfect-hash probe shape present
- hash-build payload width `1..2`
- at least four projected source columns
- at least 128 vectors

Measured checkpoint:

| Workload | Result |
| --- | ---: |
| Targeted Q3/Q5/Q7/Q8/Q21, 20 repeats | Q3 `0.066s -> 0.0585s`, ~1.13x; Q5/Q7/Q8/Q21 compile zero regions |
| Full TPC-H SF1 default, 10 repeats | Q3 `0.065s -> 0.059s`, ~1.10x; only Q1/Q3/Q9/Q14/Q19/Q20 compile |

Principle: a CBO exception needs the actual data path facts, not only operator
counts. For join-build fusion, width and payload decide whether keeping rows hot
is useful or whether JIT just adds probe/build bookkeeping around DuckDB's
already-good vectorized path.

### 15. Perfect-Hash Chains Are Admissible; Regular-Hash Chains Need Backend Ownership

Q22 showed a real no-generated-work win: a long native two-join chain with
perfect-hash probes and no aggregate, sort, materialization elision, or scan
filter ownership. The important discriminator was the hash-table layout. The
same broad two-join startup waiver also admitted Q21, but Q21 used regular hash
tables and regressed. The rule therefore requires a perfect-hash probe for this
long native two-join shape.

Measured checkpoint:

| Workload | Result |
| --- | ---: |
| Q21/Q22 after perfect-hash gate | Q21 compiled regions 0; Q22 `0.0255s -> 0.0230s`, ~1.11x |
| Full TPC-H SF1 after gate | Q22 stays admitted; Q21 stays skipped |

The deeper Q21 root cause was not "two keys". Its hot regular-hash probe was
one equality key plus one `<>` predicate key, which forces DuckDB's chain
matcher contract. SLJIT now has flat and selected all-valid single-key
notequal-chain helpers, so forced Q21 no longer falls back to generated regular
probe code for that exact stage. Q21 still is not production-admissible because
the forced query remains dominated by source filter/projection work and multiple
compiled regions around regular hash tables.

The next broad sweep reinforced the same rule. Forcing the skipped TPC-H
queries with zero startup did not reveal a clean hidden win: Q4, Q7, Q8, and
Q17 compiled but were slower; Q6 was break-even; most other skipped queries
still had no executable region. That is a backend/root-path signal, not a CBO
threshold signal.

The useful backend cleanup from that sweep was another data-centric probe
specialization: selected all-valid two-key regular hash probes now have native
helpers for both no-chain and chained layouts. This removes lazy generated probe
code for filtered two-key join batches while preserving the selection-vector
contract (`match_sel` remains the selected-row index). Focused API tests require
the new stages:

- `fast_regular_probe_selected_all_valid_int64_pair_no_chain`
- `fast_regular_probe_selected_all_valid_int64_pair_chain`

Full TPC-H SF1 after this helper preserved the production admitted set:
Q1/Q3/Q9/Q14/Q19/Q20/Q22 compile and win, Q21 remains skipped. The selected
pair helper is groundwork for more filtered two-key joins; it is not enough by
itself to admit the regular-hash-heavy Q21 family.

### 16. Grouped Aggregate Fast Paths Must Own Mutation Policy

The first regular hash aggregate shortcut only handled all-existing groups. It
was correct for low-cardinality aggregates, but TPC-H Q20 and synthetic
high-cardinality grouped aggregates spend real time creating new groups. The
next data-centric step was a separate fixed-width all-new fast path:

- probe the pointer table for the batch
- claim empty slots with normal linear probing
- append group rows and initialize aggregate states once
- update primitive aggregate state directly
- fall back before semantic mutation on same-salt matches, duplicates, NULL
  semantics, VARCHAR keys, or unsupported payloads

This kept the state-address contract centralized in DuckDB's aggregate hash
table instead of teaching SLJIT how to own tuple-data layout. The follow-on
cleanup removed the primitive existing-group address-vector route: append-only
primitive update is tried first for all-new batches, then find-or-create
primitive update owns mixed existing/new batches and updates aggregate state
while the row's state pointer is live. That removes the partial-update trap
where an existing-only speculative primitive route could mutate some rows before
discovering a normal miss and falling through.

Measured checkpoint:

| Workload | Result |
| --- | ---: |
| Focused API coverage | append-only primitive update and find-or-create primitive update both verified |
| Forced Q5/Q10/Q18/Q20, 7 repeats | Q20 `0.069s -> 0.056s`, ~1.23x; Q18 forced improves modestly; Q5/Q10 remain non-admissible |
| Full TPC-H SF1 default, 5 repeats | Q20 `0.071s -> 0.057s`, ~1.25x; admitted set remains Q1/Q3/Q9/Q14/Q19/Q20/Q22 |

The important root-cause split: Q20 has fixed-width group keys where new-group
creation can be made cheaper without changing query shape. Q10/Q5 have
string-heavy or wide grouped aggregates where a blind direct-new attempt adds
overhead and still cannot own the row-match semantics. Those need real
generated/native lookup ownership or upstream data-flow fusion, not a wider
runtime retry budget.

### 17. Post-Join Batching Must Preserve the Fast Aggregate Lookup Path

TPC-H Q10 exposed a sharper version of the data-centric rule. The final join
emits many tiny chunks into a projection plus regular hash aggregate. A copied
batch reduced aggregate invocations from thousands to dozens, but it copied
VARCHAR group keys into a temporary batch before the aggregate copied them again.
That violated the real data path and regressed production timing.

A safer streamed variant deferred `FinishStateUpdates()` across the tiny chunks
and avoided the copied batch. That removed the double string materialization and
kept correctness, but Q10 still remained below vectorized execution because the
hot work was still thousands of standalone `FindOrCreateGroupAddressesFast`
calls plus projection dispatch. The remaining root solution is not a CBO change:
the join-to-aggregate handoff needs to keep row selections, build row pointers,
group keys, and payload inputs in one data-centric path.

An attempted regular-hash payload partial fusion also found an important guard:
if payload fusion forces `ResolveStateAddresses()` through the generic grouped
aggregate lookup for those tiny chunks, the query becomes dramatically slower.
Regular hash aggregate payload fusion is only admissible when it can prove the
direct fast group-address path is used, or when it owns generated hash lookup
semantics outright. The reusable lower-level hook is an address-only fast path;
the planner must not select it blindly.

### 18. Regular Hash Payload Fusion Must Remap Copied Payloads Directly

The grouped regular-hash DECIMAL payload path exposed a source-index bug that
looked like a planner miss but was really a double-composition problem. Once an
aggregate payload is copied out of a projection expression, its expression-tree
source indices are already in the original region-input coordinate space. The
partial-fusion helper must remap those direct input sources into the shortened
projection; it must not compose them through the projection again as if they
were projection output indices. The failed shape was:

- group key kept in the shortened projection
- payload copied as `p * (1.00 - d)`
- copied payload source indices still refer to input columns `p` and `d`
- old rewrite tried to interpret source `#2` as a projection output slot

The root fix is direct-source remapping for partial aggregate fusion. This also
keeps the data-centric goal intact: retain only the grouping projection needed
for DuckDB's state-address path, while evaluating the aggregate payload directly
inside the fused SLJIT aggregate update.

The same investigation found a runtime ABI rule for generic typed payload
codegen: pointer arrays for selection and validity must be non-null when the JIT
dereferences the array base. Individual entries may be null to represent flat or
all-valid input, but the array object itself must exist on the generic path.
Passing a null array base caused raw JIT code to crash before the payload math
ran.

The first correct version was still slower on simple grouped payloads because it
was not data-centric enough: regular-hash grouped typed payloads resolved or
created state addresses in one pass, then ran a separate generated payload
update pass over the address array. Dense payload arithmetic needs a flat
all-valid fast loop inside the grouped typed aggregate kernel; otherwise the
generic typed tree evaluator burns cycles on per-reference validity checks and
stack slots. After adding the grouped flat/all-valid fast loop, the synthetic
regular-hash DECIMAL break-even moved from slower-than-vectorized to parity at
four payload expressions and a small win at eight payload expressions. Simple
one-payload grouped aggregates remain vectorized territory until regular hash
lookup and payload update are fused into one generated data-centric loop.

### 19. Callback State-Address Updates Are a Useful Half-Step, Not the Root

The next regular-hash grouped aggregate improvement was to let DuckDB's hash
table own the existing/new group lookup fast paths while SLJIT owns the typed
payload update. The clean boundary is a state-address callback: the hash table
finds or creates group state addresses, then invokes the generated grouped
payload updater immediately on that address vector. This keeps typed payloads
off the old materialized-reference-only primitive update path without teaching
the hash table about SLJIT expression trees.

Measured on a 1M-row, single-thread synthetic DECIMAL grouped aggregate with
regular hash forced and coverage CBO settings:

| Shape | Vectorized median | SLJIT median | Speedup |
| --- | ---: | ---: | ---: |
| high cardinality, 1 expression | 0.0335s | 0.0320s | 1.05x |
| high cardinality, 4 expressions | 0.0655s | 0.0610s | 1.07x |
| high cardinality, 8 expressions | 0.1170s | 0.1055s | 1.11x |
| low cardinality, 1 expression | 0.0060s | 0.0080s | 0.75x |
| low cardinality, 4 expressions | 0.0110s | 0.0110s | 1.00x |
| low cardinality, 8 expressions | 0.0220s | 0.0160s | 1.38x |

The result is directionally useful but not a license to lower production CBO
thresholds. The simple low-cardinality case regressed because vectorized/native
direct update is already extremely cheap when there is little arithmetic. The
callback path wins when expression density is high enough to amortize grouped
state-address glue, especially after existing groups are established. The root
solution is still a generated data-centric regular-hash aggregate loop that
keeps the group state pointer hot while evaluating payload expressions, rather
than a callback over a produced address vector.

### 20. Large Scale Needs Shape-Specific CBO, Not SF1 Noise Chasing

SF10 exposed a useful missed win in TPC-H Q5 that SF1-style testing would not
have justified confidently. The profitable region is narrow and data-centric:
two native hash-join probes, two generated projections, and one grouped hash
aggregate over about 60M estimated rows. It has only modest per-batch modeled
work, but the row count is high enough to pay startup. A narrow CBO protocol for
this shape made Q5 compile one region and moved SF10 targeted timing to roughly
1.09x-1.10x.

The same run also showed why the rule must stay narrow. Q7's previously bad
multi-join grouped aggregate remains the wrong target because wide/string group
materialization and native join/group lookup dominate. A separate false positive
appeared in Q7 after the aggregate: a 46-batch stateful hash-aggregate scan into
ORDER BY with high-cost projections. The model saw enough expression work, but
the fixed stateful source/order sink protocol overhead dominated at that batch
count. The fix was not to ban high-cost projections; it was to carry `sort_sink`
into the cost input and reject only small stateful standalone projections feeding
sort sinks.

The two-join batched runtime path for the Q5 shape was correct but did not
materially improve beyond the CBO admission. That is a root-cause signal: Q5's
remaining time is mostly native join and grouped aggregate work, not final
projected batch packing. The next root solution for more TPC-H coverage is not
more blind batching; it is generated data-centric join/grouped-aggregate loops
that keep row pointers, projected values, and aggregate state addresses hot
across the join-to-aggregate boundary.

Full-suite SF10 verification must distinguish compiled-region regressions from
zero-compile auto-policy timing noise. Q15/Q22 can fail a strict 0.98 gate in a
short all-query run even with zero compiled regions, while targeted r9 runs are
neutral. Treat those as planning overhead/noise to reduce, but do not confuse
them with runtime JIT regressions.

### 21. Q5 Needed The Scan-Filtered Narrow Two-Join Rule

The Q5 large-scale root cause was not missing code generation. The profitable
region was already executable, but the production CBO rejected it before backend
analysis because `uses_scan_filters=true` excluded it from the existing narrow
two-join grouped-aggregate protocol. The observed SF10 shape is narrow:

- scan-filtered source with no generated source filters
- exactly two native hash-join probe stages
- exactly two generated projection stages
- exactly one grouped aggregate update stage
- one grouped key and no reference VARCHAR projection payload
- enough post-scan-filter batches to amortize startup

Adding a separate scan-filtered narrow two-join grouped-aggregate protocol made
Q5 compile without weakening the wide/string guards that protect Q7-like shapes.
Targeted SF10 production timing after the change:

| Query | Repeats | OFF median | AUTO median | Speedup |
| --- | ---: | ---: | ---: | ---: |
| TPC-H Q5 SF10 | 9 | 0.603s | 0.539s | 1.119x |

Runtime tracing made the same path look bad because traced native join and
grouped aggregate stages pay large instrumentation overhead. Use traced runs for
admission and stage evidence, but use production-timing runs for performance
claims.

### 22. Shape Inventory Needs A First-Class No-Decision Class

Milestone 1 added `shape_inventory.csv` and `shape_inventory.md` to the TPC-H JIT
benchmark output. The first real smoke exposed an important measurement edge:
small valid runs can produce `summary.csv` and `runs.csv` rows with zero
`duckdb_jit_counters()` rows. That is not a compiled-region regression and not a
backend blocker. It means no execution-region counter was recorded for that
query/policy pair.

The inventory now emits those rows as `no_jit_decision`. This keeps the benchmark
audit honest:

- compiled-region regressions remain tied to real compiled/runtime counter rows
- zero-counter movement is separated as planning/no-decision noise
- every requested query/policy pair still appears in the artifact
- later CBO work cannot hide missing evidence behind an empty inventory

This matters for the broader plan because CBO, backend, and runtime-limited
classes should be proven from explicit telemetry. If the telemetry is absent, the
right classification is absence of a JIT decision, not an inferred root cause.

### 23. Refactor CBO Policy Before Changing CBO Policy

The first Milestone 2 step moved funded-protocol admission and startup policy into
named rule tables without changing thresholds or predicates. That sequencing is
important. When the cost model already has many similar shape gates, mixing
structural cleanup with policy changes makes regressions hard to attribute.

The useful pattern is:

- keep existing predicates intact first
- introduce named rules and one shared evaluator
- verify the admitted/skipped behavior with the existing unit suite
- only then expose matched rule names and change policy

This turns future CBO changes into auditable decisions instead of another layer of
anonymous booleans.

### 24. CBO Rule Names Must Be Counter Keys, Not Just Columns

Milestone 2 exposed the matched funded-protocol rule and startup rules through
cost-profile telemetry, `duckdb_jit_events()`, `duckdb_jit_counters()`, profiler
JSON, and `shape_inventory.csv`.

The important detail is that rule names are part of the counter aggregation key.
If they are only accumulated as output columns, two different admitted shapes can
collapse into one counter row and show up as `multiple`. That is acceptable as a
defensive fallback, but it is too weak for the shape inventory. The inventory needs
stable evidence for which CBO rule funded each shape.

Verified smoke:

- TPC-H Q1 SF1 production mode produced a compiled native
  `compiled_vectorized` row with
  `runner_cost_funded_protocol_rule=standalone_grouped_aggregate`.
- The generated shape signature now includes
  `rule=standalone_grouped_aggregate,startup=none`.
- TPC-H Q1 SF1 profile mode produced the same rule in profiler JSON event data.

This keeps the next CBO changes honest. Policy changes can now be reviewed as
named admission changes instead of inferred from anonymous cost totals.

### 25. Shape Facts Should Be Derived Once Per Decision

The next Milestone 2 step introduced `PhysicalRunnerShapeFacts` in the cost model.
The first facts are deliberately boring: effective rows, batches, native operator
stage count, generated compute work, no native join, no native sort, grouped-only
aggregate, and wide string grouped aggregate.

That is the right level for the first refactor because it preserves policy while
removing repeated predicate fragments. The rule table still calls named predicate
functions, but those predicates now read shared facts instead of recomputing the
same shape concepts in slightly different ways.

Verified after the refactor:

- `make reldebug -j12` passed.
- Full `build/reldebug/test/unittest --print-failing-tests` passed.
- TPC-H Q1 SF1 production and profile smokes still produced the
  `standalone_grouped_aggregate` funded rule.

The principle is sequencing: derive facts first, preserve behavior, then make
policy changes from those facts. That keeps future aggressive admission changes
reviewable instead of blending them into a structural rewrite.

### 26. Admission Reasons Must Survive Every Planner Path

Milestone 2 now emits one stable CBO selection token:

- `runner_cost_selection_reason`

The value starts with `admitted_...` for accelerated-runner selection and
`rejected_...` for vectorized selection.

Those fields are carried through cost-profile telemetry, `duckdb_jit_events()`,
`duckdb_jit_counters()`, profiler JSON, decision reason strings, and
`shape_inventory.csv`.

The bug found during verification was not in the cost model. The cost profile had
the right `admitted_funded_protocol_rule:standalone_grouped_aggregate` token, but
the cost-only planner path rebuilt the final selected-runner reason after backend
lowering and dropped the token from the human-readable event reason. The fix was
to make CBO cost-reason token formatting a shared helper and use it on that path
too.

Verified after the fix:

- TPC-H Q1 SF1 production inventory contains
  `runner_cost_selection_reason=admitted_funded_protocol_rule:standalone_grouped_aggregate|generated_stage_benefit`.
- TPC-H Q1 SF1 profile JSON contains the same structured field.
- The profile event reason string also contains
  `cbo_selection_reason=admitted_funded_protocol_rule:standalone_grouped_aggregate...`.
- Full `build/reldebug/test/unittest --print-failing-tests` passed.

The lesson is that stable reasons are only useful if every planner path preserves
them. A token present in a cost object but missing from the final event reason is
still an observability bug.

### 27. Backend Capability Facts Start At Lowering, Not In String Parsers

Milestone 3 started with a compact structured capability surface: native and
boundary operator-kind counts accumulated during lowering, data-shape facts from
native SLJIT nodes, plus join and aggregate capability facts read from the
finalized SLJIT native-region plan. The important design choice is where the
facts are born. They are recorded from structured `ExecutionRegionOperatorKind`,
`ExecutionRegionLoweringKind`, vector/selection metadata, and finalized SLJIT
native op values, not inferred later by parsing human-readable event reasons.

That keeps the data path clean:

- detailed and compact lowering share one count/update helper
- compact lowering must pass the operator kind, so it cannot silently lose backend
  facts
- lowering capability counters live under one `capability_facts` member, keeping
  `ExecutionRegionLoweringPlan` from growing a flat list of unrelated fields
- `CompactEventReason()` emits stable `backend_native=...` and
  `backend_boundary=...` tokens from the structured counters
- native SLJIT nodes add `backend_input_format=...`,
  `backend_output_format=...`, `backend_vector_source=...`, and
  `backend_selection_source=...`
- finalized SLJIT native ops add `backend_join=...` and `backend_aggregate=...`
  facts after projection and primitive aggregate fusion have made the aggregate
  lookup path known
- production CBO admission remains unchanged until inventory proves a capability
  fact should become a guard

Verified examples:

- TPC-H Q1 SF1 profile mode emitted
  `backend_native=table-scan:1|projection:3|perfect-hash-group-by:1`.
- The same profile emitted
  `backend_input_format=unified-vector:3|boundary:2` and
  `backend_selection_source=none:2|input-selection:1|boundary:2`.
- The same profile emitted
  `backend_aggregate=perfect_hash_update:1|primitive_payload_update:1|grouped_state_address_lookup:1|generated_perfect_hash_lookup:1`.
- The fused-contract boundary diagnostic emitted
  `backend_native=projection:1|ungrouped-aggregate:1` and
  `backend_boundary=table-scan:1`.
- API coverage now also asserts regular hash aggregate native state-address lookup
  with
  `backend_aggregate=hash_update:1|primitive_payload_update:1|grouped_state_address_lookup:1|native_state_address_lookup:1`.
- `make reldebug -j12`, focused `[api][jit]`, architecture verification, and full
  `build/reldebug/test/unittest --print-failing-tests` passed.

The principle is the same as the CBO refactor: introduce structured facts first,
preserve policy, then use the facts to make aggressive changes. Backend capability
facts should eventually drive runtime variant selection and admission guards, but
they should not be reverse-engineered from trace text.

### 28. Type And Validity Facts Must Preserve The Backend's Real View

The next Milestone 3 pass extended backend capability facts with type, validity,
and hash-probe layout information, still without changing production admission.
The facts are recorded from finalized SLJIT native plans:

- hash join probe/build keys add `backend_join_key_type=...`
- aggregate group inputs add `backend_group_key_type=...`
- finalized aggregate payload expressions add `backend_payload_type=...`
- native source not-null facts add `backend_source_validity=...`
- hash probes now distinguish regular, perfect-hash, residual, equality-key, and
  non-equality-key facts under `backend_join=...`

The important design point is that these facts describe what the backend actually
executes, not the user's original SQL spelling. A Q1-shaped perfect-hash
aggregate compresses VARCHAR group keys before generated group lookup, so the
compiled fact is `backend_group_key_type=uint8:2`, while the payload facts are
`backend_payload_type=int64:1|decimal64:5`. That distinction is useful: it tells
the CBO that this path is not generic VARCHAR grouping; it is compressed fixed
width grouping with DECIMAL payloads.

Verified examples:

- The Q1-shaped aggregate smoke emitted
  `backend_source_validity=may-have-null:6`,
  `backend_group_key_type=uint8:2`, and
  `backend_payload_type=int64:1|decimal64:5`.
- The focused hash-join probe API test now asserts `backend_join_key_type=int64:1`,
  `backend_join=hash_probe:1`, and `equality_key:1`.
- `make reldebug -j12`, focused `[api][jit]`, architecture verification, full
  `build/reldebug/test/unittest --print-failing-tests`, and a Q1-shaped telemetry
  smoke passed.

What remains is intentionally not hidden in lowering. Chain/no-chain probe layout,
flat versus selected all-valid execution, and existing/new grouped aggregate
outcomes are runtime facts. They should be recorded from the executed runtime
stage and then used to select traits. Guessing them during lowering would create
the same kind of optimistic CBO fiction this plan is trying to remove.

### 29. Runtime Facts Need Counted Telemetry, Not Stage-Name Archaeology

The next Milestone 3 pass moved chain/no-chain, flat/selected all-valid, and
existing/new aggregate outcomes out of generated-stage strings and into a counted
runtime metric: `jit_runtime_path_counts`. The key design decision is that
runtime facts are recorded at the actual branch that executed, then carried as
typed counters through events, counters, profiler JSON/text, benchmark CSVs, and
shape inventory signatures.

This avoids two bad outcomes:

- lowering does not guess runtime-only outcomes such as selected versus flat
  input, chain versus no-chain probe, or existing versus new grouped aggregate
  batches
- benchmark tooling no longer needs to parse timed stage names to know which
  runtime data path actually ran

Verified runtime facts now include:

- `hash_join_probe.fast_regular_probe_selected_all_valid_single_key_no_chain=32`
  from a hash-join CTAS smoke
- `aggregate_update.primitive_payload_update=32` from the aggregate update path
  in the same smoke
- `aggregate_update.direct_append_new_grouped_primitive_update=...` and
  `aggregate_update.direct_new_grouped_primitive_update=...` in focused API tests

The next performance move should consume these facts instead of adding another
manual branch ladder. If a path repeatedly reports
`aggregate_update.resolve_grouped_state_addresses` or
`aggregate_update.fused_payload_update_with_grouped_state_addresses`, the root
problem is still materialized address vectors. If it reports a fast hash-probe
path but the query is still slow, the root problem is likely downstream
materialization or aggregate lookup, not probe codegen.

Verification for this pass:

- `make reldebug -j12`
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- SQL runtime-path smoke against `duckdb_jit_events()`
- full `build/reldebug/test/unittest --print-failing-tests`

### 30. Boundary Facts Should Count Rows Crossing The Boundary

The runtime-path facts say which branch ran. That is not enough for performance.
The next pass added `jit_materialization_boundary_counts`, a row-counted boundary
metric that records how much data crossed each runtime boundary.

The important distinction:

- `jit_runtime_path_counts` answers "which path executed?"
- `jit_materialization_boundary_counts` answers "how many rows crossed a
  materialization, address-vector, row-pointer, or copied-batch boundary?"

Recorded boundary examples now include:

- `hash_join_probe.row_pointer_reference=...`
- `hash_join_probe.final_output=...`
- `hash_join_probe.residual_source_chunk=...`
- `aggregate_update.direct_state_update=...`
- `aggregate_update.address_vector_direct_new=...`
- `aggregate_update.address_vector_resolve=...`
- `aggregate_update.address_vector_payload_update=...`
- `aggregate_update.address_buffer_callback_existing_update=...`
- `aggregate_update.address_buffer_callback_new_update=...`
- `aggregate_update.state_address_selection_append_new_update=...`
- `projection.copied_post_join_projection=...`
- `projection.copied_post_join_batch=...`
- `projection.reference_post_join_projection=...`

The smoke result made the distinction concrete:

- hash join runtime path:
  `hash_join_probe.fast_regular_probe_selected_all_valid_single_key_no_chain=32`
- materialization rows:
  `hash_join_probe.row_pointer_reference=65536;hash_join_probe.final_output=65536`
- aggregate runtime path:
  `aggregate_update.primitive_payload_update=32`
- aggregate boundary rows:
  `aggregate_update.direct_state_update=65536`

This is the right signal for aggressive work. A query can be on a fast probe path
and still lose if the boundary rows show large final-output materialization,
copied post-join batches, or address-vector payload updates. The next refactor
should consume these facts to choose runtime variants and to prove when CBO
negative controls are about real data movement rather than branch names.

Verification for this pass:

- `make reldebug -j12`
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- SQL smoke against `duckdb_jit_events()` for boundary counts

### 31. Runtime Variants Should Be Trait-Selected, Not Branch-Ladder Cloned

The first Milestone 4 refactor moved regular hash-join probe input selection
behind `SljitRegularHashJoinProbeRuntimeTraits`. The executed code path is still
the same: flat all-valid probes, selected all-valid probes, and generic probes
keep their previous fast-path order and stage tokens. The difference is that the
choice is now named once and executed by shared variant helpers.

This matters because broader query support will otherwise copy the same branch
ladder for every new key type, validity shape, selection shape, and chain shape.
The root design is:

- derive the input/table traits once from vector format and hash-table layout
- choose one runtime variant from those traits
- preserve counted runtime-path and materialization-boundary telemetry
- add future fixed-width type support inside common variant helpers, not as a
  parallel probe loop tree

This slice is structural, not a standalone performance claim. Its value is that
the next aggressive work can add real data-path ownership with less duplicated
control flow.

Verification for this pass:

- `make reldebug -j12`
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- SQL smoke preserving
  `hash_join_probe.fast_regular_probe_selected_all_valid_single_key_no_chain=32`
  and boundary counts
- full `build/reldebug/test/unittest --print-failing-tests`

### 32. Aggregate Direct Routes Need One Owner Before Deeper Fusion

The second Milestone 4 slice did the same cleanup for grouped aggregate routing.
Before address-vector resolution, the runtime tries a fixed sequence of direct
routes:

- direct append-new primitive update
- direct find-or-create primitive update
- direct existing-group fused payload update
- direct append-new fused payload update
- direct new-group fused payload update
- fallback to grouped state-address resolution

That order is now centralized in one helper and guarded by
`SljitGroupedAggregateRuntimeTraits`. Behavior and telemetry are intentionally
unchanged; the refactor only gives the direct-route policy one owner.

This matters because the next root performance fix is not another side path
around `ResolveStateAddresses`. It is making fixed-width grouped aggregate loops
keep the state pointer live and update payloads before an address vector exists.
Having one route owner makes it clear which cases still fall through to
`aggregate_update.address_vector_resolve` and
`aggregate_update.address_vector_payload_update`.

Follow-on cleanup removed the primitive existing-group address-vector route from
that owner. The route was redundant after find-or-create primitive update could
handle existing and new rows while the state pointer was live, and it carried an
unhelpful partial-update risk on normal misses. Primitive grouped update now has
one mutation policy: append-only when the whole batch is provably new, otherwise
find-or-create.

The next cleanup removed the append-new fused payload address-vector copy. The
hash table already has row locations in append order and a reverse partition
selection that maps source rows to those row locations. Passing both to generated
batch code lets SLJIT load `addresses[address_sel[i]]` and keeps the update
vectorized without exposing tuple-data layout or making a per-row C++ call.

Verification for this pass:

- `make reldebug -j12`
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- SQL smoke preserving aggregate runtime path and boundary counts
- full `build/reldebug/test/unittest --print-failing-tests`

### 33. Address-Callback Routes Are Not The Root Endpoint

The grouped fused payload callback route is better than falling back to full
`ResolveStateAddresses` plus a separate payload pass, but it still produces an
address buffer before SLJIT updates payloads. The callback API now passes a raw
state-address span instead of a `Vector`, and the telemetry split makes the
remaining buffer explicit:

- `aggregate_update.address_buffer_callback_existing_update=...`
- `aggregate_update.address_buffer_callback_new_update=...`
- `aggregate_update.state_address_selection_append_new_update=...`
- `aggregate_update.address_vector_resolve=...`
- `aggregate_update.address_vector_payload_update=...`

This distinction matters. Existing/new callback routes are direct routes through
the aggregate hash table, but they are still address-buffer based. Append-new can
use row locations plus a reverse partition selection instead. The span callback
removes the callback `Vector` boundary; it does not eliminate address buffering
for existing and mixed new-group paths. The root solution is a lower
`GroupedAggregateHashTable`
updater API that calls a payload updater while the lookup path still has each
row's state pointer. SLJIT should consume that API rather than duplicating tuple
layout or adding another wrapper around the address buffer.

Verification for this pass:

- `make reldebug -j12`
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- full `build/reldebug/test/unittest --print-failing-tests`

### 34. In-Lookup Updates Must Respect Miss Semantics

The next grouped aggregate slices added a lower
`GroupedAggregateHashTable` row-state updater for fast fixed-width paths. The
first consumers are the direct-new and append-new primitive aggregate routes.
They now update payloads while each row's aggregate state pointer is live,
instead of first materializing a state-address vector and then walking a second
update pass.

The important correctness detail is where this is safe:

- direct-new find-or-create validates the shape up front and then resolves every
  row, so existing rows can be updated during probe and newly appended or
  duplicate rows can be updated after the append step
- append-new validates duplicate absence before state mutation, then appends and
  updates each row from the live state pointer
- existing-group lookup can return a normal miss after seeing earlier matching
  rows, so it must not mutate state in the lookup loop until there is a stronger
  no-miss proof or a two-phase design that does not reintroduce the address
  vector

This is why existing-group mutation has not moved into the lookup loop yet. It is
not a retreat from data-centric fusion; it is the edge case that has to disappear
before broader in-loop mutation is correct. The next step is to give
existing-group routes a fact that proves all rows exist, or to restructure the
route so miss detection happens before mutation without producing an address
vector.

Verified smoke:

- runtime path contained
  `aggregate_update.direct_append_new_grouped_primitive_update=1`
- materialization boundary contained `aggregate_update.direct_state_update=64`
- no `address_vector_*` boundary was emitted for the append-new primitive route

Verification for this pass:

- `make reldebug -j12`
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- SQL smoke against `duckdb_jit_events()` for the append-new route
- full `build/reldebug/test/unittest --print-failing-tests`

### 35. Delete The Post-Join Copy Without Deleting Batching

The first attempt to remove `projection.copied_post_join_batch` fed each tiny
post-join projected chunk directly into grouped aggregate update. That was the
wrong deletion. It removed one copy, but it also destroyed full-vector aggregate
batching and increased hot update invocations. Forced/profile timing regressed
to Q3 `0.760x` and Q20 `0.947x`.

The correct boundary deletion keeps the pending aggregate input batch and writes
fixed-width post-join projection output directly into that batch. This removes
the scratch projected chunk plus append copy while preserving vector-sized
aggregate updates.

The important root cause was not only expression support. Post-join output often
arrives as selected or dictionary-backed vectors, so a direct path that only
accepts flat identity sources will silently miss the hot shape. The direct batch
adapter now falls back to the normal SLJIT expression executor for selected or
unified-format sources, but points the result vector at the final aggregate
batch slice. Reference outputs copy directly into their final columns; generated
fixed-width outputs write into the batch memory.

Verified forced/profile evidence:

- Q3 moved from `projection.copied_post_join_projection=30519` and
  `projection.copied_post_join_batch=30519` to
  `projection.direct_post_join_batch_projection=30519`
- Q20 moved from `projection.copied_post_join_projection=9741` and
  `projection.copied_post_join_batch=9741` to
  `projection.direct_post_join_batch_projection=9741`

The aggregate update invocation count stayed batched:

- Q3 kept `aggregate_update.direct_new_grouped_fused_payload_update=15`
- Q20 kept `aggregate_update.direct_new_grouped_primitive_update=5`

This is structural progress, not a CBO admission change. Production SF1 with
default policy still skipped the Q3/Q20 regions in the focused run, and the
verifier passed with correctness diff 0. The next root blocker is still below
this boundary: Q3 has `aggregate_update.address_buffer_callback_new_update`, and
both Q3/Q20 still materialize `hash_join_probe.final_output`.

Verification for this pass:

- `make reldebug -j12`
- new API telemetry test for direct post-join batch projection
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- forced/profile TPC-H Q3/Q20 with runtime and boundary counters
- production TPC-H Q3/Q20 focused verifier

### 36. Delete Full Join Output, Then Measure The Replacement Boundary

The next one-join grouped-aggregate slice removed
`hash_join_probe.final_output` for regular hash joins without teaching SLJIT
hash-table tuple layout. The probe runs in row-pointer mode, and a DuckDB-owned
projection-source materializer fills only the join output columns required by
the downstream fixed-width projection. The existing direct-to-batch projection
then writes into the pending aggregate input batch, preserving vector-sized
aggregate update calls.

Verified forced/profile boundary movement:

| Query | Before | After |
| --- | --- | --- |
| Q3 | `hash_join_probe.final_output=30519` | `hash_join_probe.direct_row_pointer_reference=30519`; `hash_join_probe.projection_source=30519` |
| Q20 | `hash_join_probe.final_output=9741` | `hash_join_probe.direct_row_pointer_reference=9741`; `hash_join_probe.projection_source=9741` |

The aggregate update count stayed batched:

- Q3 kept `aggregate_update.direct_new_grouped_fused_payload_update=15`
- Q20 kept `aggregate_update.direct_new_grouped_primitive_update=5`

The performance interpretation is deliberately conservative. Forced/profile
timing was Q3 `0.899x` and Q20 `1.116x`. Q20 remains useful because the primitive
aggregate update path is direct-state. Q3 is still not a production win because
the old full-output boundary has become a narrower projection-source gather, and
Q3 still reports `aggregate_update.address_buffer_callback_new_update=30519`.
Production default Q3/Q20 compiled zero regions after this pass; focused SF1
medians were Q3 `1.000x` and Q20 `0.985x`, so the Q20 movement is zero-compiled
policy noise rather than a runtime JIT regression.

The root solution is therefore still one level deeper:

- fuse projection-source gathering into the grouped aggregate update path where
  the row pointer and match selection are already live
- extend the grouped aggregate row-state updater to fixed-width fused typed
  payloads so Q3 does not build an address buffer before payload update
- keep the current projection-source boundary as a measured intermediate state,
  not a CBO admission reason

Verification for this pass:

- `make reldebug -j12`
- focused API telemetry test requiring `hash_join_probe.projection_source` and
  forbidding `hash_join_probe.final_output`
- `build/reldebug/test/unittest "[api][jit]" --print-failing-tests`
- `python3 benchmark/jit/verify_jit_architecture.py`
- forced/profile TPC-H Q3/Q20 with runtime and boundary counters
- production TPC-H Q3/Q20 focused verifier
- `python3 benchmark/tpch/jit/verify_tpch_benchmark.py`

### 37. Row-Pointer Grouped Lookup Deletes The Boundary, But Probe Cost Still Dominates

The direct row-pointer grouped lookup/update route now fires for Q3's fixed key
family without copying hash-table tuple layout into SLJIT. The route loads RHS
group keys from DuckDB row pointers, checks row-level validity, hashes the
`[INTEGER, DATE, TINYINT]` key family, asks DuckDB to find/create aggregate
groups, and calls the generated selected state-address payload updater with a
compact LHS payload source override.

Verified Q3 SF1 forced/profile evidence:

- `aggregate_update.direct_row_pointer_grouped_lookup_update=15`
- `aggregate_update.row_pointer_grouped_lookup_update=30519`
- no `projection.direct_post_join_reference_projection`
- no `projection.direct_post_join_computed_projection`
- no `projection.direct_post_join_batch_projection`
- no `aggregate_update.state_address_selection_new_update`
- median `0.063062s` vectorized/off versus `0.061738s` forced JIT/auto,
  or `1.021x`, with correctness diff 0

The route also exposed two implementation rules:

- Payload source remapping must use the hash-join binding's LHS output indices.
  The pending probe batch is the join input, not a payload-only chunk.
- Nullable join tuple layout is not a route blocker. It is a row-level validity
  fact that must be checked before mutation.

The remaining hot work is not projection anymore. Trace timing is now dominated
by DuckDB grouped aggregate lookup/update internals:

- `find_or_create_row_pointer_keys.fill_and_hash` is about `0.22 ms`
- `find_or_create_fast.probe` is about `2.5-2.7 ms`
- total `direct_row_pointer_grouped_lookup_update` is about `3.2 ms`

This is not enough for CBO broadening. The next root solution is to reduce the
grouped lookup/update work itself: keep the row-pointer key descriptor live
through compare/append, avoid group-vector representation when possible, and
centralize fixed-width key compare/append in DuckDB-owned runtime helpers.

### 38. Keep Useful Key Caches, Delete Repeated Duplicate Probes

The first deeper Q3 row-pointer grouped lookup attempt tried to avoid building
the full key cache and probe directly from row pointers. That regressed the hot
probe stage: repeated row-pointer loads and checked-cast recovery pushed the
probe from the prior `2.5-2.7 ms` range to about `2.85-2.90 ms`. The key cache
was not stale materialization; it was buying locality.

The corrected fix kept the DuckDB-owned fixed-width key cache, still avoided the
generic projection-fed grouped update path, compacted append-only new keys, and
kept the previous resolved group target live across consecutive duplicate keys.
For Q3's lineitem-driven order, duplicate group rows are adjacent often enough
that a repeated key should not re-enter the pointer-table probe at all.

Verified Q3 SF1 forced/profile evidence after the fix:

- `aggregate_update.direct_row_pointer_grouped_lookup_update=15`
- `aggregate_update.row_pointer_grouped_lookup_update=30519`
- no `hash_join_probe.final_output`
- no `hash_join_probe.projection_source`
- no copied post-join projection boundaries
- `find_or_create_row_pointer_keys.fill_and_hash` about `0.22-0.23 ms`
- `find_or_create_row_pointer_keys.probe` about `0.17-0.18 ms`
- total `direct_row_pointer_grouped_lookup_update` about `0.78-0.81 ms`
- median `0.062720s` vectorized/off versus `0.060045s` forced JIT/auto,
  or `1.045x`, with correctness diff 0

Production-timing Q3 SF1 with the same forced CBO settings over 7 repeats was
`0.062s` vectorized/off median versus `0.051s` forced JIT/auto median, or
`1.216x`, with correctness diff 0.

A focused Q3/Q9/Q20 SF1 production smoke with the same forced CBO settings over
3 repeats stayed correct and showed Q9/Q20 still have useful adjacent wins:

| Query | Vectorized/off median | Forced JIT/auto median | Speedup |
| --- | ---: | ---: | ---: |
| Q3 | 0.062s | 0.050s | 1.240x |
| Q9 | 0.141s | 0.116s | 1.216x |
| Q20 | 0.066s | 0.046s | 1.435x |

The next blockers differ by query. Q9 profile still reports
`hash_join_probe.final_output=638808`, so the next Q9 work is join-output
boundary deletion, not grouped aggregate micro-tuning. Q20 profile reports
`aggregate_update.direct_row_pointer_grouped_lookup_update_unsupported.group_key_sources=5`
and remains on the primitive direct-state aggregate path, so its row-pointer
group-key support should only be extended through shared fixed-width descriptors.

The principle is sharper now: deleting work does not mean deleting every
temporary. Delete the temporary only if it is actually the stale boundary. In
this case, the stale work was repeated hash-table probing for consecutive
duplicate keys after the state target was already known.

### 39. Mixed VARCHAR References Should Not Block Direct Join-To-Join Projection

Q9's next blocker was `hash_join_probe.final_output=638808`. The first attempted
fix added a six-op direct first-join-to-second-join path, but it did not fire.
The root cause was not the operator count. Lowered IR showed the hot region as:

```text
hash_join -> projection -> hash_join -> projection -> projection -> aggregate
```

with the between-join projection carrying five fixed-width values plus
`n_name` as a `VARCHAR` reference. The old direct projection helper rejected the
whole route because it required every projection output to be fixed-width.

The correct fix was to relax only the reference path:

- direct reference projection can copy/slice/gather non-fixed values such as
  `VARCHAR` when validity is proven all-valid
- computed direct projection remains fixed-width unless a specific generated
  path owns the variable-width result
- unsupported mixed projections still fall back with visible materialization
  counters

Verified Q9 SF1 forced/profile evidence after the fix:

- `projection.direct_between_join_projection=319404`
- `hash_join_probe.direct_row_pointer_reference=319404`
- `hash_join_probe.final_output` fell from `638808` to `319404`
- first join no longer reports `op0:hash_join_probe.materialize_output`
- the remaining `hash_join_probe.final_output=319404` belongs to the second
  join
- correctness diff 0

The focused Q9 SF1 forced production run over 7 repeats measured
`0.141s` vectorized/off median versus `0.112s` forced JIT/auto median, or
`1.259x`, with correctness diff 0.

A focused Q3/Q9/Q20 SF1 forced production smoke after the shared helper change
showed no adjacent regression over 3 repeats:

| Query | Vectorized/off median | Forced JIT/auto median | Speedup |
| --- | ---: | ---: | ---: |
| Q3 | 0.062s | 0.051s | 1.216x |
| Q9 | 0.144s | 0.115s | 1.252x |
| Q20 | 0.066s | 0.046s | 1.435x |

The next Q9 blocker is now the second join and final projection chain:
`hash_join_probe.final_output=319404`,
`projection.copied_post_join_projection=638808`, and
`projection.copied_post_join_batch=315308`. The next root step is to keep the
second join row pointers live into the post-second-join projection and grouped
aggregate update, not to tune the first join further.

### 40. Q9 Second Join Can Stay Selection-Only, But The Source Gather Remains

The next Q9 slice removed the second regular hash join's full output boundary.
The root fix was data-centric: run the second join in selection-only mode, keep
the selected LHS rows and RHS row pointers live, and direct-project the
post-second-join projection from those references. The second join should only
materialize output as a visible fallback.

Verified Q9 SF1 forced/profile evidence after the fix:

- `projection.direct_between_join_projection=319404`
- `projection.direct_second_join_projection=319404`
- `hash_join_probe.direct_row_pointer_reference=638808`
- no `hash_join_probe.final_output` for the Q9 fused region
- `projection.copied_post_join_projection` dropped from `638808` to `319404`
- `projection.copied_post_join_batch=315308` remains
- `hash_join_probe.projection_source=319404` remains
- correctness diff 0

The important root cause is now narrower. The second join itself is no longer the
materialization boundary. The remaining work is the source-gather and copied
projection chain between the second join's live row pointers and aggregate
update. The next fix should feed the post-second-join projection directly into
the aggregate batch or row-pointer grouped update path while preserving
vector-sized aggregate calls.

A synthetic two-join coverage query also exposed a real edge in fallback logic:
perfect-hash join probe ignored selection-only mode. The runtime now dispatches
selection-only fallback materialization by actual hash-table layout, and the
perfect-hash selection-only path records
`hash_join_probe.direct_selection_reference`. This keeps the fallback honest
without pretending the synthetic perfect-hash route is the real Q9 regular-hash
route.

### 41. Q9 RHS DATE_YEAR Can Read Row Pointers Directly

The next Q9 slice removed the hidden RHS generated-source gather for
`year(o_orderdate)`. The previous structurally clean path still gathered the
second join's RHS DATE column into a temporary vector before running the native
`DATE_YEAR` projection. That was better than full join-output materialization,
but it was not value-lifetime fusion.

The fix is shared and descriptor-based:

- use `ExecutionHashJoinRHSFixedColumnSource` to prove the RHS layout, type,
  offset, nullability, and column count
- read DATE directly from second-join RHS row pointers
- preserve SQL NULL and infinite DATE semantics by marking the result invalid
- extract finite years inline instead of calling `Date::ExtractYear` once per
  row
- record the route as
  `projection.direct_rhs_row_pointer_generated_projection`

Verified Q9 SF1 forced/profile evidence:

- `projection.direct_rhs_row_pointer_generated_projection=319404`
- `hash_join_probe.direct_row_pointer_reference=638808`
- no `hash_join_probe.final_output`
- no `hash_join_probe.projection_source`
- no `projection.copied_post_join_projection`
- no `projection.copied_post_join_batch`
- `projection.direct_post_join_batch_projection=958212`
- correctness diff 0

The trace showed a real but small local improvement after inlining finite DATE
year extraction:

| Q9 forced/profile stage | Before inline DATE | After inline DATE |
| --- | ---: | ---: |
| `op3:projection.post_join_direct_computed_projection` median | ~1972 us | ~1849 us |
| `op4:projection.post_join_direct_batch_projection` median | ~1382 us | ~1365 us |
| `op5:aggregate_update.direct_new_grouped_primitive_update` median | ~4342 us | ~4284 us |

Focused Q9 SF1 forced production over 7 repeats measured `0.144s` vectorized/off
median versus `0.115s` forced JIT/auto median, or `1.252x`, with correctness
diff 0. A focused Q3/Q9/Q20 SF1 forced production smoke stayed correct:

| Query | Vectorized/off median | Forced JIT/auto median | Speedup |
| --- | ---: | ---: | ---: |
| Q3 | 0.061s | 0.052s | 1.173x |
| Q9 | 0.145s | 0.118s | 1.229x |
| Q20 | 0.066s | 0.046s | 1.435x |

Interpretation: this was the right cleanup, but it was not the next large Q9
speedup by itself. The row-pointer DATE path removed a hidden gather and made the
stage cleaner, but the Q9 trace is now dominated by pair-key regular probing,
between-join reference projection, final projection into an aggregate batch, and
grouped aggregate state/update work. The next root solution is to stop treating
`projection.direct_post_join_batch_projection=958212` as the endpoint: compressed
group keys, amount payload, and the aggregate state target need to stay live
through grouped lookup/update.

### 42. Q9 Final Projection Telemetry Shows Aggregate Lookup Is The Next Boundary

The next slice split direct fixed projection-to-batch timing by expression kind.
This is trace-only observability: it does not change production execution, but it
prevents guessing about whether the final projection or grouped aggregate update
is the next root problem.

Verified Q9 SF1 forced/profile medians over three traced repeats:

| Stage | Median |
| --- | ---: |
| `op4:projection.post_join_direct_batch_projection` | ~1431 us |
| `op4:projection.direct_batch_expression.string_compress` | ~955 us |
| `op4:projection.direct_batch_expression.integral_compress` | ~161 us |
| `op5:aggregate_update.direct_new_grouped_primitive_update` | ~4278 us |
| `op5:aggregate_update.direct_new_grouped_primitive_update.find_or_create_fast.probe` | ~3381 us |
| `op5:aggregate_update.direct_new_grouped_primitive_update.find_or_create_fast.hash` | ~321 us |

The useful lesson is that Q9's final projection is not the largest remaining
cost. The grouped regular hash aggregate still hashes and probes projected group
vectors after the projection boundary. A generic adjacent-key state-target reuse
experiment was structurally aligned with the root plan, but traced Q9 did not
show enough consecutive group-key locality for an always-on hot-loop check to
pay. The implementation was tightened to sample per chunk before enabling
consecutive reuse, and production Q9 remained stable:

| Check | Result |
| --- | ---: |
| Focused Q9 SF1 forced production, 7 repeats | `0.144s -> 0.114s`, ~1.263x |
| Focused Q3/Q9/Q20 SF1 forced production, 3 repeats | Q3 ~1.192x, Q9 ~1.261x, Q20 ~1.435x |

Do not treat the adjacent-key cleanup as the Q9 root win. The next real deletion
is descriptor/hash ownership across final projection and grouped aggregate
lookup: compute compressed keys and amount payload once, keep the hash or state
target live, and fall back with a named unsupported descriptor fact when that is
not possible.

### 43. Q9 Final Projection Can Feed Grouped Aggregate Without Payload Batch

The next Q9 slice stopped treating the final projection output batch as the
aggregate boundary. The first implementation missed with
`direct_projected_group_payload_update_unsupported.payload_not_final_reference`:
Q9-like generated payloads are not always plain references from the final
projection. The second miss was `group_key_projection`: the old direct batch
projection helper preflighted the whole projection and rejected a compact
group-key-only batch.

The root fix was to split the boundary by value lifetime:

- direct-project only the grouped keys into a compact group-key batch
- keep payload sources in the live pre-final-projection input
- remap fused typed payload expression sources through final-projection
  references when the aggregate owns a generated DECIMAL payload
- let DuckDB grouped aggregate lookup/update own hash-table layout, append,
  duplicate-new resolution, and state mutation
- update payloads through the selected state-address callback while state targets
  are live
- record unsupported descriptor facts before falling back

Verified Q9-like API coverage now requires the new route:

- `aggregate_update.direct_projected_group_payload_update`
- `projection.direct_remap_post_join_batch_projection`
- `aggregate_update.projected_group_payload_update`
- no `aggregate_update.address_vector_payload_update`
- no `aggregate_update.state_address_selection_new_update`
- no `aggregate_update.state_address_selection_existing_update`
- no `direct_projected_group_payload_update_unsupported.*`

Verified TPC-H Q9 SF1 forced/profile evidence over three traced repeats:

- `aggregate_update.direct_projected_group_payload_update=319404`
- `projection.direct_remap_post_join_batch_projection=319404`
- `aggregate_update.projected_group_payload_update=319404`
- no `hash_join_probe.final_output`
- no `hash_join_probe.projection_source`
- no `projection.copied_post_join_projection`
- no `projection.copied_post_join_batch`
- correctness diff 0

The final projection is now a compact key remap, not a full aggregate input
payload batch. Profile timing was `0.1496s -> 0.1310s`, about `1.14x`; traced
runs are useful for stage evidence but not admission claims.

Production timing stayed stable:

| Check | Result |
| --- | ---: |
| Focused Q9 SF1 forced production, 7 repeats | `0.145s -> 0.115s`, ~1.261x |
| Focused Q3/Q9/Q20 SF1 forced production, 3 repeats | Q3 ~1.216x, Q9 ~1.252x, Q20 ~1.383x |

The next root blocker is lower: Q9 still builds a compact group-key vector and
then hashes/probes it in the regular grouped aggregate. The trace still shows
`find_or_create_fast.probe` around `3.6-3.7 ms` for this stage, while pair-key
regular join probing remains a larger cost. The next deletion is to carry or
reuse hash/state-target facts across the final projection and grouped aggregate
lookup, not to add another query-shaped branch.

### 44. Q3 Row-Pointer Keys And Q20 Probe-Side Keys Are One Descriptor Problem

Q20 proved that the Q3 row-pointer grouped lookup was still too query-shaped.
The old path only recognized `[INT32, INT32, INT8]` keys loaded from build-side
row pointers. Q20's grouped keys are probe-side casts such as `BIGINT ->
SMALLINT` and `BIGINT -> INTEGER`, so the runtime reported
`direct_row_pointer_grouped_lookup_update_unsupported.group_key_sources`.

The root fix was to delete the Q3-shaped key path and make key lifetime
descriptor-driven:

- a grouped key source is now either a row-pointer field or an input vector
- casts are explicit descriptor facts, including `BIGINT -> SMALLINT`
- key fill rejects nulls and cast overflow before hash-table mutation
- the aggregate hash table fills a compact descriptor key chunk, hashes once, and
  reuses the centralized fast find-or-create helper
- the Q3-specific key struct, hash/equality/load helpers, and local append/probe
  loop are gone

That cleanup exposed the next Q20 blocker: primitive DECIMAL payload lifetime.
The fix followed the same value-lifetime rule used for Q9, but for the one-join
path:

- direct-project only compact group keys from the final projection
- remap the payload reference through the join LHS output mapping back to the
  live pending probe input
- accept expression-tree wrappers around references as reference-like payloads
- call DuckDB's primitive split-payload grouped update helper
- avoid building the full aggregate payload batch

Verified API coverage now requires the probe-side primitive split route:

- `aggregate_update.direct_projected_group_payload_update`
- `projection.direct_remap_post_join_batch_projection`
- `aggregate_update.projected_group_payload_update`
- no `direct_projected_group_payload_update_unsupported.*`
- no `hash_join_probe.final_output`
- no `hash_join_probe.projection_source`
- no copied post-join projection or batch
- no `aggregate_update.direct_state_update`

Verified Q3/Q20 SF1 forced/profile smoke:

| Query | Runtime evidence |
| --- | --- |
| Q3 | `aggregate_update.direct_row_pointer_grouped_lookup_update=15`, descriptor key fill/hash stages present |
| Q20 | `aggregate_update.direct_projected_group_payload_update=9741`, `projected_group_payload_update=9741` |

Correctness diff was 0 for both. The Q20 profile speedup stayed around `1.23x`
for a single traced run; that is route evidence, not a production admission
claim. The remaining grouped-aggregate cost is still hash/probe/append over the
compact group-key chunk. The next root deletion is to carry hash/state-target
facts deeper, not to add another TPC-H-specific branch.

### 45. Carry Hashes With Compact Keys, Then Prove Whether State Targets Matter

The split-payload route was still stopping one step too early. It projected
compact group keys and then asked the aggregate hash table to hash those vectors
again. The clean fix was not an SLJIT hash-table shortcut: projection now emits a
precomputed hash vector for the compact key batch, and DuckDB grouped aggregate
lookup consumes that hash through the normal state-owned API.

This removed a real stage for both Q9/Q20 split-payload routes:

- `projection.post_join_direct_remap_batch_projection_hash` records hash
  production while compact keys are hot
- `find_or_create_fast.hash` disappears under
  `aggregate_update.direct_projected_group_payload_update`
- the grouped aggregate still owns probing, append, duplicate handling, and
  state mutation

Q9 then exposed a second safe deletion. After the first batch creates the groups,
most split-payload batches are existing-only. The new existing-only split update
resolves every state target first and only then calls the primitive updater, so a
miss falls back before mutation. SF1 forced/profile showed the intended shape:

| Stage | Count |
| --- | ---: |
| `direct_existing_split_payload.update` | 163 |
| `direct_new_split_payload.append` | 1 |
| `find_or_create_fast.hash` under split payload | 0 |

Production timing stayed honest:

| Check | Result |
| --- | ---: |
| Q3/Q9/Q20 SF1 forced production, 7 repeats | Q3 `0.062s -> 0.052s`, Q9 `0.145s -> 0.117s`, Q20 `0.066s -> 0.047s` |
| Q9/Q20 SF10 forced production, 3 repeats | Q9 `1.499s -> 1.427s`, ~1.05x; Q20 `0.586s -> 0.406s`, ~1.44x |

The lesson is that carrying hashes was the right value-lifetime cleanup, and
existing-only state updates are the right mutation policy. But Q9 at larger
scale is still not solved by this layer. The next Q9 root work must carry or
reuse the resolved state target deeper, and it must reduce pair-key regular join
probe or grouped probe cost enough to move SF10 beyond noise.

### 46. Q9 Full Shape Needed Variable-Width Direct Projection, Not A Q9 Branch

The stale CBO cleanup admitted the real SF10 Q9 full shape, but the first traced
run regressed badly: vectorized/off was `1.635336s`, auto was `1.992021s`, and
the runtime counters showed the real boundary:

- `hash_join_probe.final_output=3261613`
- `hash_join_probe.projection_source=3261613`
- `projection.copied_post_join_projection=3261613`

The synthetic two-projection coverage test was already green, so the root cause
was not simply "two projections between joins". The real Q9 `op1` IR had one
unhandled computed variable-width output:

```text
STRING_DECOMPRESS(UHUGEINT nation) -> VARCHAR
```

Direct reference projection handled all-valid `VARCHAR` references, and direct
computed projection handled fixed-width casts/DECIMAL references. But the
computed `VARCHAR` output could not write into the pending second-join batch, so
one unhandled output forced projection-source materialization and then full
first-join output fallback.

The fix was shared, not Q9-shaped:

- keep fixed-width direct projection writing in place
- let computed `VARCHAR` projection write a temporary result vector
- copy that result into the pending batch so string ownership stays with the
  target vector
- route LHS, RHS, and mixed hash-join computed projection through the same
  expression-to-batch helper

Verified Q9 SF10 profile after the fix:

| Check | Result |
| --- | ---: |
| Profile, 1 repeat | off `1.607866s`, auto `1.499500s`, ~1.07x |
| Production, 5 repeats | off `1.609s`, auto `1.347s`, ~1.19x |

Correctness diff was 0. Runtime counters for the full Q9 fused region now show:

- `projection.direct_between_join_projection=3261613`
- `hash_join_probe.direct_row_pointer_reference=6523226`
- `projection.direct_post_join_reference_projection=6523226`
- `projection.direct_post_join_computed_projection=6523226`
- no first-join `hash_join_probe.final_output`
- no first-join `hash_join_probe.projection_source`

This is a structural win because it deletes the first-join boundary in the real
SF10 shape. It is not the final Q9 root solution: the trace still has
`projection.direct_post_join_batch_projection` and compact group-key remap before
grouped aggregate lookup. The next deletion is to carry grouped hash/state-target
facts deeper, not to add another query-specific projection branch.

### 47. Q9 Compressed Group-Key Passthrough Deletes Recompression, But Not The Root Cost

The next Q9 slice followed the value-lifetime chain one step further. The final
group-key projection was recomputing:

```text
STRING_COMPRESS(STRING_DECOMPRESS(compressed_nation))
```

The fix is descriptor-gated, not query-name gated:

- detect `STRING_DECOMPRESS(compressed fixed-width string)` in the between-join
  projection
- carry the compressed fixed-width value in a sidecar aligned with the
  second-join input batch
- prove the final grouped key is `STRING_COMPRESS` of the same projected value
- fill that grouped key from the compressed sidecar and leave the normal
  expression path as fallback

The grouped aggregate existing-only path also now shares fast group-key source
validation with mixed find-or-create and can reuse the last resolved existing
state target for consecutive duplicate keys. That keeps the state-target rule
generic, but Q9 still does not show enough adjacent-key locality for this to be
the main win.

Verified Q9 SF10 trace after the compressed passthrough:

| Stage | Before | After |
| --- | ---: | ---: |
| `op4:projection.direct_batch_expression.string_compress` | ~10.1 ms | 0 |
| `op4:projection.direct_batch_expression.compressed_passthrough` | 0 | ~1.3 ms |
| `op4:projection.post_join_direct_remap_batch_projection` | ~19.4 ms | ~10.3 ms |

Runtime counters now show:

- `projection.direct_between_join_compressed_passthrough_projection=3261613`
- `projection.direct_batch_passthrough_projection=3261613`
- no `op4:projection.direct_batch_expression.string_compress` in the Q9 fused
  region
- first-join `final_output` and `projection_source` remain absent

Production timing stayed honest. Q9 SF10 production over 5 repeats measured
off `1.606s`, auto `1.359s`, or ~`1.18x`. That is still correct and faster than
vectorized/off, but it is not a clean improvement over the prior `1.347s` auto
checkpoint. The structural deletion is useful groundwork, but the root Q9 cost
is still earlier: pair-key regular probing and the between-join projection still
dominate. The next deletion should carry compressed nation earlier, so the
pipeline avoids the `STRING_DECOMPRESS` side of the round trip, or it should
remove more regular probe work.

### 48. Q9 Needed Early Compressed-Key Lifetime, Not Just Late Passthrough

The compressed passthrough in section 47 deleted the final recompress, but the
wall-clock result showed that was not enough. A deeper profiler pass found the
actual leak: the route still materialized the decompressed `nation` value in the
between-join batch and only recovered the compressed value later.

Pre-fix xtrace evidence pointed inside the generated Q9 region at
`SljitNativeStringDecompress`, regular hash probe work, bloom checks, and
aggregate lookup/update. The fix was to carry the compressed group-key fact
earlier and skip the normal decompressed `nation` projection when the final
grouped key is proven to consume the same value as a compressed passthrough.

The guard is descriptor-driven:

- the skipped between-join projection must be a compressed key passthrough source
- the skipped output must not be a second-join probe key
- the final grouped key must map back to the same compressed source
- payload expressions must not read the skipped decompressed output
- any failure records a specific unsupported fact before fallback

That last condition found the root cause during implementation. The first
preflight rejected real Q9 as `payload_dependency` because it conservatively
treated complex payload expressions as unknown. Using the existing expression
source collector proved the DECIMAL amount expression does not read `nation`, so
the skip became legal without a Q9-specific branch.

Verified Q9 SF10 trace after the early compressed-key skip:

| Stage or counter | Result |
| --- | ---: |
| `op1:projection.post_join_direct_computed_projection` | about `96-99 ms` before, `0.53 ms` after |
| `projection.direct_between_join_compressed_group_key_skip_projection` | `3261613` |
| `projection.direct_between_join_compressed_passthrough_projection` | `3261613` |
| `projection.direct_batch_passthrough_projection` | `3261613` |
| `aggregate_update.direct_projected_group_payload_update` | `3261613` |

Q9 SF10 production over 5 repeats after this deletion measured vectorized/off
`1.722s` median versus auto `1.300s` median, speedup `1.324615`, correctness
diff 0. Compared with the prior auto checkpoint around `1.359s`, the JIT path
itself improved by roughly 4%.

Post-change xtrace Time Profiler bundles were empty for this binary on this
macOS run, so the useful post-change stack evidence came from xtrace's automatic
`sample` fallback over an eight-query Q9 loop. That sample no longer contained
`SljitNativeStringDecompress`. The active DuckDB frames moved to:

- `ExecuteSelectedAllValidRegularHashJoinProbeVariant`
- `ExecuteFlatAllValidRegularHashJoinProbeVariant`
- `JoinHashTable::InsertHashes`
- scan decompression and filter code such as `BitpackingScanPartial`,
  `duckdb_fsst_decompress`, and numeric range selection
- `GroupedAggregateHashTable::TryResolveExistingGroupsFastInternal`
- `RadixUpdatePrimitiveGroup`

This changes the next root target. Q9 is no longer primarily paying for the
decompress/recompress round trip in the fused route. The measured remaining
work is regular hash join probe ownership, build/probe hash-table traffic,
source scan decompression outside the generated region, and grouped aggregate
state lookup/update.

### 49. Q9 Pair Probe Needed Salt Lifetime Behind Bloom, Not Bloom Helper Churn

The next Q9 SF10 pass used xtrace-style CPU profiling again. Direct Time
Profiler traces were empty on this macOS run, so the useful profile came from an
attach-style `sample` run over a long Q9 loop. The stack was consistent with the
runtime counters: the main generated-region CPU was still in regular hash join
pair probing.

The failed experiment was instructive. Splitting and unrolling the bloom helper
looked locally cleaner, but it increased code shape/register pressure and made
the pair-probe counters worse. The root fix was not a new helper. It was value
lifetime: the no-chain fast probes were computing salt for every source row
before the bloom check, even when bloom rejected the row and no hash-table entry
would be inspected.

The retained fix moves salt extraction behind the bloom gate for the fast
no-chain pair probe, no-chain single-key probes, and pair chain probes. It also
removes the carried `next_salt` state from the lookahead path. `ht_entry_t`
`ExtractSalt` now takes the hash scalar by value, which matches the operation
and avoids passing a hot 64-bit value through a reference.

Verified Q9 SF10 profile counters:

| Stage | Before pair-probe work | After scalar salt | After salt-after-bloom |
| --- | ---: | ---: | ---: |
| total runtime | `1023.8 ms` | `973.0 ms` | `954.3 ms` |
| generated runtime | `719.3 ms` | `681.9 ms` | `663.0 ms` |
| `op0` selected pair no-chain probe | `178.2 ms` | `163.5 ms` | `149.4 ms` |
| `op0` flat pair no-chain probe | `138.4 ms` | `132.3 ms` | `119.1 ms` |
| `op0` regular hash probe total | `367.4 ms` | `343.8 ms` | `316.9 ms` |

Production Q9 SF10 over 5 repeats moved from the scalar-salt-only checkpoint
auto median `1.307s` to salt-after-bloom auto median `1.257s`; vectorized/off
median was `1.643s`, so auto speedup was `1.307x`, correctness diff 0.

Post-fix sample top stacks moved in the right direction:

- selected regular probe top samples: `1384 -> 992`
- flat regular probe top samples: `1082 -> 816`
- `ExtractSalt` no longer appears in the top-stack list
- remaining major work is pair probing, `JoinHashTable::InsertHashes`, scan
  decompression/filtering, and aggregate state lookup/update

Principle: in data-centric fusion, do not compute derived metadata until the
next consumer is proven live. Bloom-rejected rows have no salt consumer.

### 50. Q9 Bloom Is Selective; The Remaining Win Is Amount Value Lifetime

The next deep dive used a real xtrace Time Profiler run over an eight-query Q9
SF10 loop:

```text
/tmp/trace_duckdb_20260626_122041.trace
```

This trace produced useful symbolized samples. It also corrected an easy
misread: `ht_entry_t::ExtractSalt` was hot, but the collapsed stacks showed it
was owned by `JoinHashTable::Finalize`, not by the generated probe path. The
generated probe hotspot was the bloom check and entry probing:

- `BloomFilter::GetMask` under
  `SljitNativeRegionKernel::ExecuteRegularHashJoinProbeVariant`
- `ht_entry_t::GetValue`
- selected and flat `ExecuteAllValidInt64PairNoChainProbe`

Three experiments were measured and rejected:

| Experiment | Result | Root Cause |
| --- | ---: | --- |
| Disable no-chain bloom in generated probes | generated runtime `659 ms -> 1000 ms`, op0 probe `320 ms -> 654 ms` | Bloom is expensive but highly selective; removing it floods the hash table with misses. |
| Replace four variable shifts with a 4096-entry two-bit-pair mask table | generated runtime `659 ms -> 701 ms`, op0 probe `320 ms -> 361 ms` | On this CPU, two table loads are worse than the variable shifts and increase probe pressure. |
| Skip selected all-valid proof before direct reference copy | op1 reference projection `74.4 ms -> 69.5 ms`, total generated runtime flat/noisy | The proof is not the root cost, and the generalized version needs careful nullable partial-projection invariants. |

The current Q9 SF10 baseline after restoring the losing experiments is still:

| Check | Result |
| --- | ---: |
| Q9 production, 5 repeats | off `1.648s`, auto `1.261s`, speedup `1.306899`, correctness diff 0 |
| Q9 profile, 1 repeat | generated runtime about `659 ms` |
| op0 regular pair probe | about `320 ms` |
| op1 between-join direct reference projection | about `74 ms` |
| op3 final amount/date projection | about `18 ms` |
| aggregate direct projected update | about `45 ms` |

The xtrace lesson is that bloom/filter/probe micro-ops are now near the local
limit. The next root solution is value lifetime across the two joins: Q9 copies
four DECIMAL inputs through the second-join batch only to compute one `amount`
payload later. A data-centric path should compose the amount expression across
the between-join projection, compute or carry one payload value, and stop
materializing the four amount inputs. That is broader and safer than tuning
`Vector::Copy` or weakening the bloom filter.

### 51. Q9 Amount Sidecar Is Real; Generic Sidecars And Probe Accessor Rewrites Are Not

The next Q9 slice implemented the value-lifetime plan from lesson 50. The fused
two-join aggregate now detects final aggregate payloads whose inputs come only
from the second join's LHS, remaps those inputs through reference-like
between-join projections, computes the Q9 DECIMAL64 amount while the first join
row pointers and source vectors are still live, and carries a one-column
sidecar into the second-join projection. The second projection then copies the
precomputed payload instead of materializing the four amount inputs only to
recompute the payload later.

This only becomes a win when the sidecar has a direct kernel. The generic
expression-adapter sidecar and a linear interpreted DECIMAL program were both
measured and rejected: they moved work earlier but paid too much selected-input
adapter and per-node dispatch overhead. The retained descriptor requires a
direct DECIMAL64 discounted-amount program before it can skip the original
projection columns.

Verified traced profile checkpoints:

| Check | Result |
| --- | ---: |
| Baseline profile before amount sidecar | Q9 `1.406252s`, generated `659952 us`, source `288843 us` |
| Baseline amount materialization | op1 reference projection `73793 us`, op3 amount projection `18425 us` |
| Generic sidecar profile | Q9 `1.438068s`, generated `681405 us`, op1 sidecar `77951 us` |
| Direct DECIMAL64 sidecar profile | Q9 `1.371368s`, generated `623287 us`, source `292242 us` |
| Direct sidecar amount stages | op1 sidecar `29125 us`, op3 payload passthrough `2055 us`, op3 remaining computed projection `6690 us` |

Final production verification on SF10 single-thread, 7 repeats:

```text
/private/tmp/duckdb_jit_q9_sf10_final_payload_prod
off  median 1.649s
auto median 1.209s
speedup 1.363937x
correctness_diff 0
```

A real xtrace Time Profiler run over a repeated Q9 loop was also captured:

```text
/tmp/trace_duckdb_20260626_130538.trace
duration 7.15395s
samples 4767
```

The global top samples after the payload fix were in hash-table/probe work:
`ht_entry_t::ExtractSalt`, `BloomFilter::GetMask`, `ht_entry_t::GetValue`, and
the selected/flat int64-pair no-chain probe variants. That evidence was useful,
but it also showed a trap. A probe micro-optimization that replaced entry
accessors with raw `Load<hash_t>`, local bloom-mask code, and salt-bit compares
regressed the traced run:

| Probe Experiment | Result |
| --- | ---: |
| Direct amount sidecar profile | Q9 `1.371368s`, generated `623287 us`, selected pair probe `148131 us` |
| Raw entry/salt-bit probe rewrite | Q9 `1.392368s`, generated `632898 us`, selected pair probe `152539 us` |

Root cause: Instruments attributed samples to tiny inline helpers, but that did
not mean the fix was to bypass the helpers. The raw-load rewrite increased hot
loop pressure and did not remove meaningful work. The change was removed after
measurement. The next root target remains the data path around probe/build and
aggregate state lifetime, not accessor churn.

### 52. Q5 Needs Compressed String Group Keys In The Row-Pointer Aggregate Path

Q5 exposed a different two-join aggregate boundary from Q9. The first attempt
removed second-join output materialization by direct-projecting the selected
second probe rows. That proved the route but did not improve production timing
enough, because the projection still compressed and copied the string group key
before the aggregate consumed it.

The root cause was visible in the lowered IR:

```text
op2=projection(
  native:string-compress:16[reference<VARCHAR source=vector#2>(#2)],
  native:reference:region-input[...],
  native:reference:region-input[...])
op3=aggregate_update(kind=hash;groups=1;aggregates=1;payload_update=generated-primitive)
```

The group key is not raw `VARCHAR`; it is a `UHUGEINT` compressed string derived
from an input-vector `VARCHAR` carried through the second join. The existing
row-pointer aggregate descriptor only knew plain references and numeric casts,
so it reported `direct_row_pointer_grouped_lookup_update_unsupported.group_key_sources`
and fell back to projection/materialization.

The retained fix makes `STRING_COMPRESS` a normal row-pointer group-key cast:
`VARCHAR -> UINT8/UINT16/UINT32/UINT64/UINT128`. The aggregate descriptor now
fills the temporary group chunk with the same compressed string encoding used by
compressed materialization, and the two-join Q5 route tries the row-pointer
aggregate path before the direct-projection fallback.

Runtime shape check after the fix:

```text
aggregate_update.direct_row_pointer_grouped_lookup_update=37
hash_join_probe.direct_row_pointer_reference=72985
hash_join_probe.pending_probe_batch=72985
aggregate_update.row_pointer_grouped_lookup_update=72985
hash_join_probe.final_output=1825856
```

The important result is that the second join no longer produces a
`hash_join_probe.final_output` or `projection.direct_second_join_projection`
boundary. The remaining `final_output=1825856` is the first join boundary, which
is the next data-lifetime target.

Production verification on SF10 single-thread, 9 repeats:

```text
/private/tmp/duckdb_jit_q5_rowptr_string_compress_prod
off  median 0.568s
auto median 0.497s
speedup 1.142857x
correctness_diff 0
```

The xtrace Time Profiler run for the fixed path is:

```text
/tmp/trace_duckdb_20260626_134048.trace
duration 3.20s
samples 573
```

Top samples were dominated by scan and storage work: `pread`,
`BitpackingSelect`, `TryBitpackingPrefixRangeFilter`, `madvise`, and selection
vector operations. JIT code appeared as unsymbolicated executable memory, but
the counters explain the relevant region boundary more clearly than the global
sample view. The next Q5 root solution is to remove the first-join batch
materialization and feed the second join from the first join's selected rows and
row pointers directly, keeping `n_name`, `extendedprice`, and `discount` live
until the aggregate update consumes them.

### 53. Q9 No-Chain Probe Aliasing Cleanup Helps, But It Is Not The Root Fix

After the Q9 first-join materialization and late payload boundaries were removed,
the measured hot path moved to regular hash join probing. A pre-change xtrace
run over repeated Q9 showed top samples in `ht_entry_t::ExtractSalt`,
`BloomFilter::GetMask`, `ht_entry_t::GetValue`, the selected/flat int64-pair
no-chain probe variants, `JoinHashTable::InsertHashes`, storage decompression,
and string predicate work.

The retained cleanup is deliberately small and generic: the no-chain fast probe
variants now keep key arrays, hash-table entries, row-pointer output, and match
selection output in restricted local pointers. This gives the compiler a clearer
aliasing contract inside the vector-sized scalar loops without changing DuckDB's
hash-table ABI, adding query-shaped rules, or removing bloom/salt checks.

Trace counters before versus after the cleanup on SF10 Q9:

| Counter | Before | After |
| --- | ---: | ---: |
| `runtime_time_us` | 918113 | 909710 |
| `source_contract_runtime_time_us` | 291586 | 286475 |
| `generated_body_runtime_time_us` | 626525 | 623220 |
| `op0:hash_join_probe` | 328543 | 325010 |
| `op0:selected int64-pair no-chain` | 157046 | 154438 |
| `op0:flat int64-pair no-chain` | 122692 | 122105 |

Production verification was stable but must be read honestly. Two independent
7-repeat SF10 Q9 runs measured:

```text
/private/tmp/duckdb_jit_q9_restrict_prod
off  median 1.595s
auto median 1.147s
speedup 1.390584x
correctness_diff 0

/private/tmp/duckdb_jit_q9_restrict_prod2
off  median 1.599s
auto median 1.159s
speedup 1.379638x
correctness_diff 0
```

The post-change Instruments xtrace attempt attached but captured only one useful
sample, so the deep-dive evidence came from live macOS `sample` against the same
DuckDB process running a 20-query Q9 loop:

```text
/private/tmp/sample_duckdb_q9_post_restrict.txt
selected regular hash join probe: 1707 stack-top samples
flat regular hash join probe:     1287 stack-top samples
JoinHashTable::InsertHashes:      1608 stack-top samples
Bitpacking scan/filter/decode, FSST, memchr, and grouped lookup/update remain visible.
```

The root cause did not change: this cleanup improves compiler ownership of the
current loops, but it does not delete a boundary. Q9 still spends real time in
regular probe output ownership, hash build/finalize, storage/filtering, and
grouped lookup/update. The next root step is not another accessor rewrite. It is
to keep match rows, row pointers, group hashes, and aggregate state targets live
across probe -> projection -> grouped lookup -> payload update, materializing
only when a real DuckDB boundary requires it.

### 54. Chain Probe Salt Lifetime Is Now Shared, Not Another Branch

The single-key and pair-key chain fast probes still had copied linear-probe
loops after the no-chain cleanup. Some of those loops computed hash salt before
the bloom gate and carried `next_salt` for the next row, even though a
bloom-rejected row has no entry or salt consumer. The fix was to delete the
copied chain-head lookup and route all chain helpers through one
`SljitHashJoinFindFirstChainPointer` helper.

This is intentionally a code-shape and value-lifetime cleanup, not a new Q9 root
win. It removes the stale pre-bloom salt derivation from chain probes, preserves
the existing vector-sized probe contract, and keeps DuckDB hash-table layout
behind the same runtime ABI.

Verified evidence:

- `make reldebug -j12`, `[api][jit]`, explicit chain/bloom probe tests,
  `python3 benchmark/jit/verify_jit_architecture.py`, and the full
  `build/reldebug/test/unittest --print-failing-tests` passed.
- Forced/profile Q3/Q9/Q20 SF1 stayed correct with correctness diff 0 and kept
  the intended fast routes: Q9 still reports selected int64-pair no-chain plus
  flat single-key no-chain probes, no `hash_join_probe.final_output`, and
  `aggregate_update.direct_projected_group_payload_update`.
- Forced production Q3/Q9/Q20 SF1 over three repeats stayed positive:
  Q3 `0.062s -> 0.050s`, Q9 `0.142s -> 0.120s`, Q20 `0.063s -> 0.045s`,
  all with correctness diff 0.
- Direct `xtrace` over repeated Q9 captured useful DuckDB samples in
  `/private/tmp/duckdb_jit_chain_probe_q9_direct.trace`. Top samples still name
  `ht_entry_t::GetValue`, `BloomFilter::GetMask`, `ht_entry_t::ExtractSalt`,
  `ExecuteAllValidInt64PairNoChainProbe`, `JoinHashTable::Finalize`, scan
  decompression, and FSST work. That confirms the next root target remains
  regular probe/state-target ownership, not chain helper churn.

### 55. The Last Q3-Shaped Grouped Lookup Branch Was Stale

The generic grouped-key descriptor work had removed the old Q3-only validation
and descriptor construction, but `GroupedAggregateHashTable` still kept one
hard-coded `[INT32, INT32, INT8]` find-or-create loop. That duplicated the
generic loop's probe, tentative-new duplicate handling, existing-row update,
and consecutive-key reuse logic just to spell three fixed-width comparisons by
hand.

That branch is now deleted. `TryFindOrCreateGroupsFastInternal` has one
descriptor-driven fast probe path for all supported fixed-width grouped keys,
including the Q3 shape. This is the better endpoint: the special case became
the normal case, the aggregate hash table owns the same batched state-target
lookup/update contract, and there is no query-shaped branch left to maintain.

Verified evidence:

- `make reldebug -j12`, focused grouped JIT tests, `[api][jit]`,
  `python3 benchmark/jit/verify_jit_architecture.py`, and the full
  `build/reldebug/test/unittest --print-failing-tests` passed.
- Final forced/profile Q3/Q9/Q20 SF1 stayed correct with correctness diff 0.
  Q3 still reports `aggregate_update.direct_row_pointer_grouped_lookup_update`
  with `find_or_create_descriptor_keys.fill`, `.hash`, and
  `find_or_create_fast.probe`. Q9/Q20 keep
  `aggregate_update.direct_projected_group_payload_update`, and Q9/Q20 still
  avoid aggregate-side `find_or_create_fast.hash` by passing precomputed group
  hashes from the compact projection boundary.
- Final forced production Q3/Q9/Q20 SF1 over three repeats stayed positive:
  Q3 `0.064s -> 0.053s`, Q9 `0.147s -> 0.121s`, Q20 `0.066s -> 0.045s`,
  all with correctness diff 0.
- Direct `xtrace` over repeated Q3 is saved at
  `/private/tmp/duckdb_jit_generic_group_probe_q3.trace`, with compact summary
  `/private/tmp/duckdb_jit_generic_group_probe_q3_summary.json`. The top samples
  remain perfect-hash join, generated filter code, storage decompression, and
  scan/filter work. `_platform_memcmp` is `0.48%`, and
  `AggregateFastGroupSourceRowsMatch` is `0.28%`, so the generic comparer did
  not become the new bottleneck.

This is a code-shape cleanup, not a new admission rule. It removes a stale
Q3-shaped fork and keeps the remaining root target unchanged: carry group
hashes and aggregate state targets deeper through grouped lookup and payload
update, while regular hash probe ownership remains a visible cost center.

### 56. Selected State-Address Updates Need One ABI

The split-payload grouped aggregate route had two different update contracts:
the all-existing fast path used a selected state-address update, while the mixed
find-or-create fallback still used the row-update callback. That kept the mixed
path on the older callback shape even though `TryFindOrCreateGroupsFastInternal`
already has a selected state-address mode for existing, new, and duplicate-new
groups.

The cleanup is structural:

- `RadixUpdatePrimitiveGroupSelected` now follows the same ABI as generated
  selected grouped updates. If `execute_sel` is present, `address_sel` is indexed
  by the logical row selected for execution; if `address_sel` is absent, the
  state-address span is already compact by loop index.
- `TryUpdateNewPrimitiveGroupsWithPayloadInput` now calls
  `TryFindOrCreateGroupsSelectedStateUpdateFast` for the mixed fallback instead
  of `TryFindOrCreateGroupsUpdateFast`.
- The unused `duplicate_group_positions` scratch selection in
  `AggregateHTAppendState` is removed instead of renamed.
- The direct primitive grouped find-or-create route now uses the selected
  state-address callback as well. That made `TryFindOrCreateGroupsUpdateFast`
  and the row-update arm inside `TryFindOrCreateGroupsFastInternal` obsolete, so
  both were deleted. The fast helper now either fills an address vector or emits
  selected state-address spans.
- The append-only primitive grouped route now uses the state-address append
  callback too. `TryAppendNewGroupsUpdateFast`,
  `ExecutionGroupedAggregateStateRowUpdateFunction`, and the row-update arm
  inside `TryAppendNewGroupsFastInternal` are deleted. The append helper now has
  one mutation shape: append new groups through DuckDB tuple layout, then emit a
  state-address span for the update callback.

One failed intermediate edit proved why the ABI matters: composing a compact
new-group address selection broke the Q9-like selected generated update because
SLJIT codegen already treats `address_sel` as a logical-row map when
`execute_sel` is present. The correct fix was to align the primitive selected
wrapper with that ABI, not to change the shared hash-table helper's selection
contract.

Verified evidence:

- `make reldebug -j12`, focused Q9/split-payload grouped JIT tests,
  `[api][jit]`, `python3 benchmark/jit/verify_jit_architecture.py`, and the full
  `build/reldebug/test/unittest --print-failing-tests` passed.
- Forced/profile Q3/Q9/Q20 SF1 stayed correct in
  `/private/tmp/duckdb_jit_selected_split_profile`. Q9 records one mixed
  `direct_new_split_payload.append` batch with
  `find_or_create_fast.selected_state_update`, then 163 existing-only selected
  updates. Q20 records the same selected-state update stage for its five mixed
  split-payload batches. Q9/Q20 still avoid aggregate-side
  `find_or_create_fast.hash` by reusing compact projection hashes.
- After deleting the direct row-update find-or-create wrapper, forced/profile
  Q3/Q9/Q20 SF1 stayed correct in
  `/private/tmp/duckdb_jit_selected_find_or_create_profile`; Q9/Q20 still record
  selected-state updates under split payload, and Q3 row-pointer grouped lookup
  continues through selected state-address updates.
- Forced production Q3/Q9/Q20 SF1 over three repeats stayed positive in
  `/private/tmp/duckdb_jit_selected_split_prod`: Q3 `0.062s -> 0.052s`, Q9
  `0.144s -> 0.118s`, Q20 `0.063s -> 0.044s`, all with correctness diff 0.
- The follow-up production smoke in
  `/private/tmp/duckdb_jit_selected_find_or_create_prod` was also correct:
  Q3 `0.063s -> 0.052s`, Q9 `0.144s -> 0.120s`, Q20 `0.064s -> 0.045s`.
- After deleting the append row-update callback, `[api][jit]` passed with the
  direct append test requiring `find_new.state_address_update`.
  Forced/profile Q3/Q9/Q20 SF1 stayed correct in
  `/private/tmp/duckdb_jit_append_state_address_profile`; Q9/Q20 still record
  selected-state updates under split payload. Production Q3/Q9/Q20 SF1 over
  three repeats in `/private/tmp/duckdb_jit_append_state_address_prod` stayed
  positive: Q3 `0.063s -> 0.051s`, Q9 `0.142s -> 0.118s`, Q20
  `0.064s -> 0.044s`, all with correctness diff 0. The full
  `build/reldebug/test/unittest --print-failing-tests` suite passed after this
  append cleanup.
- Direct `xtrace` over repeated Q9 auto is saved at
  `/tmp/trace_duckdb_20260626_163015.trace`. Unlike the earlier Python-wrapper
  trace, this one launched `duckdb` directly and collected 5,356 samples. The
  top frames are still regular hash probe/build/finalize and scan work:
  `ht_entry_t::GetValue`, `BloomFilter::GetMask`, `ht_entry_t::ExtractSalt`,
  `ExecuteAllValidInt64PairNoChainProbe`, `JoinHashTable::Finalize`, and scan
  decompression/filtering. The next root target therefore remains regular probe
  ownership and build/probe cost, not append callback mechanics.
- Earlier Time Profiler runs around the Python benchmark wrapper were
  startup-heavy (`/private/tmp/duckdb_jit_selected_q9.trace` and
  `/private/tmp/duckdb_jit_selected_q9_long.trace`), and the useful fallback at
  that point was `/private/tmp/duckdb_jit_selected_q9.sample.txt`. The direct
  `duckdb` xtrace above supersedes those wrapper traces while preserving the
  same conclusion: regular probe/build work remains the CPU target.

This is another ABI cleanup, not a new broadening rule. It removes the row-update
callbacks from the covered mixed split-payload, direct find-or-create, and
append-only primitive grouped paths, and makes state-address updates mean one
thing everywhere. The next root deletion remains larger: carry or reuse state
targets across final projection and grouped lookup, and keep regular hash probe
ownership from re-reading facts that are already live.

### 56. Pair Probe Template Cleanup Deletes Branch Sprawl, Not The Root Cost

The active Milestone 6 slice removed duplicated regular hash pair-probe code
without changing the runtime behavior model. The flat and selected all-valid
no-chain predicates now share one capability check, the flat and selected
pair-chain predicates share one capability check, and the two-key no-chain and
chain probe wrappers are selected by template parameter instead of copied
functions. The dispatch still returns the same telemetry stages, so old and new
profile counters compare directly.

This is the right kind of cleanup because it deletes branch-ladder code while
preserving the measured fast paths:

- source delta for `extension/jit_sljit/sljit_region_runtime.cpp`: 93 inserted,
  288 deleted
- `make reldebug -j12` passed
- `[api][jit]` passed, including selected/flat pair-probe and chain-probe tests
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- direct verification-setting smoke passed
- forced/profile Q3/Q9/Q20 SF1 stayed correct in
  `/private/tmp/duckdb_jit_probe_template_profile`
- production Q3/Q9/Q20 SF1 over three repeats stayed positive in
  `/private/tmp/duckdb_jit_probe_template_prod`: Q3 `0.062s -> 0.051s`, Q9
  `0.142s -> 0.118s`, Q20 `0.065s -> 0.045s`, all correctness diff 0

The CPU evidence still points below this cleanup. A direct DuckDB Time Profiler
run is saved at
`/tmp/trace_duckdb_jit_probe_template_q9_file_20260626_164548.trace` with 2,647
samples. The relevant call tree is:

- `JoinHashTable::Probe`: about 33.7% total
- `ProbeForPointers`: about 20.2% self
- `JoinHashTable::Hash`: about 5.9%
- table scan filtering/decompression: about 28%
- sink/build work: about 9.6%
- hash-join finalize: about 8.1%

Conclusion: the cleanup is retained because it simplifies the hot runtime and
removes stale helper forks, but it is not a new admission rule and not the
Milestone 6 finish. The next useful deletion must carry probe/match row-pointer
facts farther through downstream projection and grouped lookup, or materially
change the regular hash-table probe/build work that the Time Profiler still
names.

### 57. Core Probe Invariant Hoisting Helps Locally, But ProbeForPointers Still Dominates

The next Milestone 6 slice moved one level lower, into DuckDB's regular
`JoinHashTable::Probe` implementation. `ProbeForPointersInternal` was doing
small repeated work in the collision walk: it recomputed full salt values inside
the loop, called a one-use helper that reloaded result-vector base pointers for
each candidate, and wrote hot compare/match selections through `SelectionVector`
setters that showed up in the Time Profiler sample.

The cleanup keeps hash-table tuple layout fully inside DuckDB:

- deleted `AddPointerToCompare`
- hoisted `entries.get()`, pointer-result data, and offset/salt scratch data
- compares stored salt bits directly in the collision walk
- folds salted and unsalted candidate writes into one append block
- writes the owned compare/match selection buffers directly in the hot probe
  loop
- kept the existing compare/continuation semantics and selection layout

Verified evidence:

- source delta for `src/execution/join_hashtable.cpp`: 33 inserted, 36 deleted
- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` passed
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- forced/profile Q3/Q9/Q20 SF1 stayed correct in
  `/private/tmp/duckdb_jit_probe_folded_profile`
- production Q3/Q9/Q20 SF1 over three repeats stayed positive in
  `/private/tmp/duckdb_jit_probe_folded_prod`: Q3 `0.062s -> 0.052s`, Q9
  `0.144s -> 0.119s`, Q20 `0.064s -> 0.045s`, all correctness diff 0

The direct Time Profiler trace is
`/tmp/trace_duckdb_jit_probe_core_q9_20260626_165548.trace` with 2,640 samples.
Diffing it against the previous direct Q9 trace shows the expected local movement:

- `IncrementAndWrap`: about `2.1% -> 1.2%` self
- `ht_entry_t::ExtractSalt`: about `3.7% -> 2.9%` self
- `TemplatedSignHashFunction`: about `4.4% -> 3.8%` self
- `ProbeForPointers`: still dominant, about `20.2% -> 22.7%` self in this
  sample pair

The follow-up direct-selection trace is
`/tmp/trace_duckdb_jit_probe_selwrite_q9_20260626_170505.trace` with 2,618
samples. Against the core-probe trace:

- `SelectionVector::set_index`: gone from the top sampled functions
- `ProbeForPointers`: unchanged at about `22.7%` self
- production medians stayed positive and correct

The final folded-candidate trace is
`/tmp/trace_duckdb_jit_probe_folded_q9_20260626_172141.trace` with 2,663
samples. Against the direct-selection trace:

- the sampled `ProbeForPointersInternal` lambda frame is gone
- `ProbeForPointers`: effectively unchanged at about `22.5%` self
- `ExtractSalt`: unchanged within sampling noise at about `3.8%` self because the
  remaining samples come from build/finalize paths, not the folded probe compare
- production medians stayed positive and correct

Do not overread the last line as a precise regression claim; the trace is a short
sampling run and some unsymbolicated/inline attribution shifted. The production
medians stayed correct and positive. The important lesson is stricter:
micro-cleanups can remove repeated instructions, but they do not remove the
regular probe ownership boundary. The next useful work still needs to reduce the
number of probe/match facts that are materialized, re-read, or lost before
projection and grouped lookup consume them.

### 58. Shared Pending Probe Append Removes A Duplicate Boundary Implementation, Not The Boundary

The next Milestone 6 cleanup stayed in `sljit_region_runtime.cpp` and removed a
duplicated pending-probe append body from the one-join and two-join grouped
aggregate routes. Both paths now use `SljitAppendSelectedProbeBatch` for the
same selected source copy, row-pointer copy, vector-size repair, chunk
cardinality repair, and `pending_probe_batch_append`/`pending_probe_batch`
telemetry.

What changed:

- one helper owns the selected pending-probe batch append mechanics
- each caller still owns its path-specific capacity, flush, and full-vector
  bypass decisions
- stage and materialization counter names are preserved
- `source_selection` is const because the append path only reads the match
  selection

Final-build verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` passed
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- forced/profile Q3/Q9/Q20 SF1 stayed correct in
  `/private/tmp/duckdb_jit_pending_probe_helper_final_profile`
- production Q3/Q9/Q20 SF1 over three repeats stayed positive in
  `/private/tmp/duckdb_jit_pending_probe_helper_final_prod`: Q3
  `0.062s -> 0.052s`, Q9 `0.141s -> 0.119s`, Q20 `0.063s -> 0.044s`, all
  correctness diff 0

The final Q9 profile still uses the same Milestone 6 shape:

- `hash_join_probe.fast_regular_probe_selected_all_valid_int64_pair_no_chain=2931`
- `hash_join_probe.fast_regular_probe_flat_all_valid_single_key_no_chain=164`
- `aggregate_update.direct_projected_group_payload_update=319404`
- materialization boundaries remain direct row-pointer/reference/projection/group
  payload boundaries

The direct Time Profiler trace is
`/tmp/trace_duckdb_jit_pending_probe_helper_final_q9_20260626_173631.trace` with
2,638 samples. Diffing it against
`/tmp/trace_duckdb_jit_probe_folded_q9_20260626_172141.trace` shows
`ProbeForPointers` stays in the same dominant band (`22.5% -> 23.4%` self).
That shift is sampling noise, not a structural regression, because the edit does
not touch the probe loop and production medians stayed positive.

The useful takeaway is that the remaining boundary is now represented by less
code, but it is still present. The next Milestone 6 change needs to carry the
probe row pointers/match selection and downstream descriptors farther, or change
the regular probe loop itself; another helper extraction will not move Q9.

### 59. Q9 Final Group-Key Descriptors Remove The Aggregate Remap Boundary, But Probe Still Dominates

The next Milestone 6 slice moved the final Q9 projection-to-aggregate boundary
into DuckDB-owned grouped aggregate helpers. The covered route no longer builds a
compact group-key projection chunk and then feeds
`aggregate_update.projected_group_payload_update`. Instead, the final projection
describes its group keys as descriptor sources:

- `STRING_COMPRESS` over the live `nation` input vector
- `INTEGRAL_COMPRESS` over the extracted year input vector
- primitive DECIMAL payload lanes kept in the existing payload input

The implementation is deliberately generic: `ExecutionRowPointerGroupKeySource`
now supports integral-compress descriptors, the aggregate hash table fills a
compact descriptor group chunk from row-pointer or input-vector sources, and the
radix hash aggregate exposes a DuckDB-owned primitive payload update for
row-pointer descriptor keys. The Q9 route uses those descriptors; it does not
branch on query number.

Important counter movement from the SF1 forced/profile run in
`/private/tmp/duckdb_jit_m6_row_pointer_descriptor_profile`:

- Q9 runtime path now includes
  `aggregate_update.direct_row_pointer_grouped_lookup_update=164`
- Q9 materialization boundaries now include
  `aggregate_update.row_pointer_grouped_lookup_update=319404`
- `projection.direct_remap_post_join_batch_projection` is gone for this route
- `aggregate_update.projected_group_payload_update` is gone for this route
- descriptor fill/hash stages are visible under
  `find_or_create_descriptor_keys.fill` and `.hash`

The descriptor fill loop initially risked recreating the deleted projection cost
as a generic row loop, so the retained implementation dispatches once by
source/cast kind and uses typed loops for no-cast, string-compress, and
integral-compress input-vector descriptors. In the large Q9 runtime events the
descriptor fill stage stays around `0.29-0.33 ms` per about `99K` rows, with the
remaining grouped update work dominated by the lookup/update itself.

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- direct smoke confirms the removed verification pragma now errors as unknown,
  confirming the deprecated no-op registration is gone
- forced/profile Q3/Q9/Q20 SF1 verified in
  `/private/tmp/duckdb_jit_m6_row_pointer_descriptor_profile`
- no-trace production Q3/Q9/Q20 SF1 verified in
  `/private/tmp/duckdb_jit_m6_row_pointer_descriptor_prod_notrace`

Profile medians:

- Q3: `0.063626s -> 0.058437s`, speedup `1.088796`, correctness diff 0
- Q9: `0.142669s -> 0.132642s`, speedup `1.075594`, correctness diff 0
- Q20: `0.064875s -> 0.051069s`, speedup `1.270340`, correctness diff 0

Comparable production medians without runtime tracing:

- Q3: `0.063s -> 0.051s`, speedup `1.235294`, correctness diff 0
- Q9: `0.139s -> 0.117s`, speedup `1.188034`, correctness diff 0
- Q20: `0.063s -> 0.044s`, speedup `1.431818`, correctness diff 0

Direct Time Profiler evidence:

- trace: `/tmp/trace_duckdb_20260626_181554.trace`
- samples: 29,929
- `ProbeForPointers`: about `27.7%` self
- `TemplatedSignHashFunction`: about `5.1%` self
- `ht_entry_t::ExtractSalt`: about `4.5%` self
- scan/filtering and grouped update support remain below the regular probe frame

Conclusion: this is a real boundary deletion and should stay, but it is not the
Milestone 6 exit. The next useful work must either carry/reuse grouped
hash/state-target facts deeper or reduce regular hash probe/match work directly.
More projection/aggregate helper churn without changing those facts is not a
root fix.

### 60. ProbeState Should Not Own Build-Only Salt Storage

The repeated-Q9 xtrace still names `ProbeForPointers` as the largest self frame,
so the next cleanup stayed around regular hash join state ownership. While
reading the probe path, one stale split was visible: `SharedState` owned
`salt_v`, but only `InsertHashesLoop` used it for build insertion. `ProbeState`
therefore constructed build-only salt storage for every probe state. It also
carried `non_empty_sel`, which had no remaining users.

Change:

- `salt_v` moved from `SharedState` to `InsertState`
- `ProbeState::non_empty_sel` deleted
- probe and insert selection state remain otherwise unchanged

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- no-trace production Q3/Q9/Q20 SF1 verified in
  `/private/tmp/duckdb_jit_m6_probe_state_cleanup_prod_notrace`

Production medians from the compact smoke:

- Q3: `0.059s -> 0.051s`, speedup `1.156863`, correctness diff 0
- Q9: `0.134s -> 0.114s`, speedup `1.175439`, correctness diff 0
- Q20: `0.061s -> 0.045s`, speedup `1.355556`, correctness diff 0

This is not a performance claim beyond removing stale construction and ownership
from probe state. The xtrace root remains unchanged: the next meaningful
Milestone 6 work must still reduce or bypass regular probe/match work.

### 61. Probe Continuation Tokens Belong With Dense Hash State

After the build-only salt cleanup, `ProbeState` still carried
`ht_offsets_and_salts_v` only to remember where a candidate should resume linear
probing after RowMatcher rejected the current row pointer. That state duplicated
the already-dense `hashes_dense_v` lifetime. The clean replacement is to keep the
continuation token in `hashes_dense_v`:

- the first unselected pass stores the token at the original row slot, which is
  also the dense input slot for that pass
- selected continuation passes store the token densely and use
  a compact compare-row map only to map RowMatcher's original-row no-match output
  back to the dense token slot
- RowMatcher still receives original probe row ids, so LHS key comparison and RHS
  row-pointer indexing keep their existing contract

This deletes `ProbeState::ht_offsets_and_salts_v` without adding a second pointer
location vector or changing RowMatcher semantics. An intermediate dense-only
attempt failed `[api][jit]` because RowMatcher's selection indexes both the LHS
probe key and RHS row-location vector; that failure is the useful invariant.

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- `rg -n "enable_verification|PragmaEnableVerification" src test scripts benchmark/tpch/jit`
  returned no matches
- direct shell smoke confirms the removed verification pragma now errors as unknown
- profile Q3/Q9/Q20 SF1 verified in
  `/private/tmp/duckdb_jit_m6_probe_continuation_split_profile`
- no-trace production Q3/Q9/Q20 SF1 verified in
  `/private/tmp/duckdb_jit_m6_probe_continuation_split_prod_notrace`

Profile medians:

- Q3: `0.061068s -> 0.060205s`, speedup `1.014334`, correctness diff 0
- Q9: `0.135810s -> 0.135843s`, speedup `0.999757`, correctness diff 0
- Q20: `0.064099s -> 0.061425s`, speedup `1.043533`, correctness diff 0

No-trace production medians:

- Q3: `0.060s -> 0.060s`, speedup `1.000000`, correctness diff 0
- Q9: `0.133s -> 0.134s`, speedup `0.992537`, correctness diff 0
- Q20: `0.061s -> 0.061s`, speedup `1.000000`, correctness diff 0

Direct Time Profiler evidence:

- short repeated-Q9 trace: `/tmp/trace_zsh_20260626_184042.trace`
- samples: 5,057 over 5.79s
- `ProbeForPointersInternal<true,false>`: `23.9%` self
- `ht_entry_t::ExtractSalt`: `3.4%` self
- `SelectionVector::set_index`: `0.89%` self
- a 29s trace at `/tmp/trace_zsh_20260626_184210.trace` had 28,300 samples but
  was mostly unsymbolicated generated code, so it is not useful for C++ frame
  attribution

Conclusion: keep this as state deletion and a better continuation ownership
boundary. Do not treat it as a root Milestone 6 performance win or CBO admission
signal. The regular hash probe loop remains the frame to reduce or bypass.

### 62. Trace-Only Probe Micro-Wins Are Not Enough

A follow-up experiment carried the raw `ht_entry_t` packed value through
`ProbeForPointersInternal<true,false>` instead of carrying an `ht_entry_t` wrapper.
The edit was source-local and focused gates passed, but it only shaped the probe
loop mechanically.

Evidence:

- profile Q3/Q9/Q20 in `/private/tmp/duckdb_jit_m6_probe_entry_value_profile`
  was neutral
- short repeated-Q9 trace `/tmp/trace_zsh_20260626_185006.trace` reduced
  `ProbeForPointersInternal<true,false>` self time from `23.9%` to `22.4%`
- no-trace production verifier failed in
  `/private/tmp/duckdb_jit_m6_probe_entry_value_final_prod_notrace`; Q9
  `auto_speedup_vs_off=0.978571`, below the `0.98` floor
- the edit was reverted
- reverted-state no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_probe_entry_value_reverted_prod_notrace`

Reverted-state production medians:

- Q3: `0.063s -> 0.062s`, speedup `1.016129`, correctness diff 0
- Q9: `0.138s -> 0.140s`, speedup `0.985714`, correctness diff 0
- Q20: `0.063s -> 0.064s`, speedup `0.984375`, correctness diff 0

Conclusion: a lower sampled C++ self percentage in this probe frame is not enough.
Milestone 6 needs deletion of a boundary or a dataflow lifetime change that survives
production medians. Do not keep source micro-shaping unless the no-trace verifier
supports it.

### 63. Probe Hashes Should Start In Probe-Owned Dense State

The common no-null-filter `JoinHashTable::Probe` path used to allocate a local
`Vector hashes`, hash into it, then immediately copy it into
`ProbeState::hashes_dense_v` before the regular probe loop could use and mutate it
for continuation tokens. That made the dense probe hash vector look like temporary
caller state even though the probe loop owns its lifetime.

Change:

- split `GetRowPointersFromDenseHashesInternal` from the existing densifying
  `GetRowPointersInternal`
- added `GetRowPointersWithDenseHashes` for callers that already have dense hashes
  in `ProbeState::hashes_dense_v`
- routed the no-null-filter `Probe` path to hash directly into
  `ProbeState::hashes_dense_v`
- left precomputed hashes and null-filtered hashes on the existing densifier because
  those vectors are externally owned or sparse by original row id

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- final-source no-trace production Q3/Q9/Q20 SF1 verified in
  `/private/tmp/duckdb_jit_m6_dense_probe_hash_flat_prod_notrace`
- final-source profile Q3/Q9/Q20 SF1 verified in
  `/private/tmp/duckdb_jit_m6_dense_probe_hash_flat_profile`

Production medians:

- Q3: `0.061s -> 0.062s`, speedup `0.983871`, correctness diff 0
- Q9: `0.137s -> 0.139s`, speedup `0.985612`, correctness diff 0
- Q20: `0.063s -> 0.063s`, speedup `1.000000`, correctness diff 0

Profile medians:

- Q3: `0.062534s -> 0.062283s`, speedup `1.004030`, correctness diff 0
- Q9: `0.139142s -> 0.141199s`, speedup `0.985432`, correctness diff 0
- Q20: `0.065517s -> 0.064241s`, speedup `1.019863`, correctness diff 0

Direct Time Profiler evidence:

- pre-change trace: `/tmp/trace_zsh_20260626_185947.trace`, 3,904 samples,
  `ProbeForPointersInternal<true,false>` `22.1%` self
- final-source trace: `/tmp/trace_zsh_20260626_191125.trace`, 3,826 samples,
  mostly unsymbolicated generated code, so not used for C++ frame attribution
- collapsed-stack check for the old `VectorBuffer::Copy` or
  `GetRowPointersInternal<true>` probe-copy path: 19 matching stacks before this
  cleanup, no matches after it

Conclusion: keep this as an ownership cleanup because it deletes a real temporary
hash vector/copy boundary from the common regular probe path and passes production.
Do not claim it as the Milestone 6 root fix. The regular probe loop itself remains
the limiting frame.

### 64. SLJIT Fast Probes Should Compare Salt Bits, Not Packed Pointer Salts

The fast SLJIT regular hash probe helpers were still materializing the legacy
packed salt value with `ht_entry_t::ExtractSalt`. That form ORs in the pointer
mask so it can compare against `ht_entry_t::GetSalt`, but the probe only needs
the high salt bits. The core DuckDB probe loop already uses that shape.

Change:

- `SljitHashJoinFindFirstChainPointer` now computes `hash & ht_entry_t::SALT_MASK`
  and compares against `entry.GetSaltWithNulls()`
- fast pair-key no-chain probes compare
  `entry_value & ht_entry_t::SALT_MASK` against `hash & ht_entry_t::SALT_MASK`
- fast single-key no-chain probes use the same raw salt-bit comparison,
  including the INT64-to-INT32 path

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- direct smoke proves the removed verification pragma remains unknown
- no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_sljit_probe_salt_bits_prod_notrace`
- profile Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_sljit_probe_salt_bits_profile`

Production medians:

- Q3: `0.062s -> 0.061s`, speedup `1.016393`, correctness diff 0
- Q9: `0.138s -> 0.138s`, speedup `1.000000`, correctness diff 0
- Q20: `0.063s -> 0.064s`, speedup `0.984375`, correctness diff 0

Profile medians:

- Q3: `0.063494s -> 0.063163s`, speedup `1.005240`, correctness diff 0
- Q9: `0.139591s -> 0.140836s`, speedup `0.991160`, correctness diff 0
- Q20: `0.064989s -> 0.066878s`, speedup `0.971755`, correctness diff 0

Direct Time Profiler evidence:

- trace: `/tmp/trace_zsh_20260626_192419.trace`
- samples: 2,535 over 3.25s
- collapsed-stack artifact:
  `/private/tmp/duckdb_jit_m6_sljit_probe_salt_bits_trace.collapsed`
- named `ExtractSalt` stacks: 0
- named `GetSaltWithNulls` stacks: 0
- named fast-helper stacks: 0 because the hot generated-code addresses are mostly
  unsymbolicated

Conclusion: keep this as source and instruction-shape cleanup because it removes
the packed-salt helper from the fast SLJIT probe source and production remains
inside the verifier gate. Do not treat it as root progress or CBO evidence. The
trace is mostly generated addresses, and Q9 production is neutral.

### 65. RHS Row-Pointer Gather Fallbacks Should Have One Owner

The post-join projection/materialization helpers had the same ownership split in
several places: try the direct fixed-column RHS row-pointer gather when the hash
table layout exposes fixed sources, otherwise fall back to
`JoinHashTable::GatherRHSColumn`. One reference-projection path still called the
generic gather directly. That was stale and made the direct gather rule look like
a call-site choice instead of a property of RHS row-pointer gathering.

Change:

- added `GatherHashJoinRHSColumn`
- centralized `ExecutionTryDirectGatherHashJoinRHSFixedColumn` plus generic
  `JoinHashTable::GatherRHSColumn` fallback there
- routed RHS reference projection, output reference materialization, generated
  RHS projection, and expression-input source building through the shared helper
- asserted the fallback `hash_table` dependency at the fallback site

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- direct shell smoke proves the removed verification pragma remains unknown
- no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_rhs_gather_helper_prod_notrace`
- profile Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_rhs_gather_helper_profile`

Production medians:

- Q3: `0.061s -> 0.062s`, speedup `0.983871`, correctness diff 0
- Q9: `0.136s -> 0.138s`, speedup `0.985507`, correctness diff 0
- Q20: `0.062s -> 0.063s`, speedup `0.984127`, correctness diff 0

Profile medians:

- Q3: `0.061837s -> 0.062454s`, speedup `0.990121`, correctness diff 0
- Q9: `0.139239s -> 0.141463s`, speedup `0.984279`, correctness diff 0
- Q20: `0.063081s -> 0.063702s`, speedup `0.990251`, correctness diff 0

Direct Time Profiler evidence:

- discarded trace: `/tmp/trace_duckdb_jit_m6_rhs_gather_helper_q9_20260626_193509.trace`
  captured only 13 startup samples because shell redirection was outside the
  launched command
- useful trace:
  `/tmp/trace_zsh_jit_m6_rhs_gather_helper_q9_20260626_193620.trace`
- samples: 2,561 over 3.30s
- collapsed-stack artifact:
  `/private/tmp/duckdb_jit_m6_rhs_gather_helper_q9_zsh.collapsed`
- `ProbeForPointersInternal<true,false>`: `23.16%` self
- `ht_entry_t::ExtractSalt`: `3.79%` self, under hash-table finalize
- no named RHS gather helper appears in the hot stack

Conclusion: keep this as source deletion and direct-gather consistency. It removes
duplicated ownership code and makes the RHS row-pointer gather rule uniform, but it
does not move Q9 production or the CPU-sampled root. The next Milestone 6 fix must
reduce or bypass the regular probe loop and keep downstream descriptors live; more
projection-side gather plumbing is not enough.

### 66. Join Build Salts Should Stay As Raw Salt Bits Too

The useful RHS-gather trace still showed `ht_entry_t::ExtractSalt` under
`JoinHashTable::Finalize`. The SLJIT fast probe source already stopped carrying
packed pointer-mask salts, and the generic probe loop already compares raw high
salt bits. Join build/finalize was the stale remaining convention: it wrote
`hash | POINTER_MASK` into `InsertState::salt_v`, then compared table entries via
the packed-salt helpers.

Change:

- `ApplyBitmaskAndGetSaltBuild` now writes `hash & ht_entry_t::SALT_MASK`
- `InsertHashesLoop` compares `(entry.GetValue() & ht_entry_t::SALT_MASK)` to
  that raw salt
- `JoinHashTable` keeps aggregate hash table's packed tentative-entry contract
  untouched; this is only the join build/probe convention
- `src/execution/join_hashtable.cpp` has no remaining `ExtractSalt`, `GetSalt`, or
  `GetSaltWithNulls` references

The first trace after switching the stored salt still showed
`ht_entry_t::GetSaltWithNulls` under finalize. That intermediate source was not
kept. The final source compares the raw entry value directly, matching the probe
loop's instruction shape.

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- direct shell smoke proves the removed verification pragma remains unknown
- 9-repeat no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_join_build_raw_salt_bits_prod9_notrace`
- profile Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_join_build_raw_salt_bits_profile`

Production medians, 9 repeats:

- Q3: `0.061s -> 0.062s`, speedup `0.983871`, correctness diff 0
- Q9: `0.138s -> 0.139s`, speedup `0.992806`, correctness diff 0
- Q20: `0.063s -> 0.063s`, speedup `1.000000`, correctness diff 0

Profile medians:

- Q3: `0.062437s -> 0.062792s`, speedup `0.994346`, correctness diff 0
- Q9: `0.141995s -> 0.143777s`, speedup `0.987606`, correctness diff 0
- Q20: `0.063511s -> 0.064805s`, speedup `0.980032`, correctness diff 0

Direct Time Profiler evidence:

- intermediate trace:
  `/tmp/trace_zsh_jit_m6_join_build_salt_bits_q9_20260626_194441.trace`
  still showed `GetSaltWithNulls` under `JoinHashTable::Finalize`
- final trace:
  `/tmp/trace_zsh_jit_m6_join_build_raw_salt_bits_q9_20260626_194757.trace`
- samples: 2,575 over 3.33s
- collapsed-stack artifact:
  `/private/tmp/duckdb_jit_m6_join_build_raw_salt_bits_q9.collapsed`
- named `ExtractSalt` stacks: 0
- named `GetSaltWithNulls` stacks: 0
- named `ProbeForPointers` stacks: 0
- named `JoinHashTable::Finalize` stacks: 0
- limitation: the final trace is mostly unsymbolicated generated/duckdb addresses,
  so it is a negative helper-frame check, not precise root attribution

Conclusion: keep this as a source and instruction-shape cleanup because it removes
the stale packed-salt convention from `JoinHashTable` and production remains inside
the verifier floor. It is not the Milestone 6 root fix: Q9 still needs regular
probe loop or downstream descriptor lifetime work that moves production medians.

### 67. Probe Scratch Belongs To The Probe Iteration, Not Probe State

The next `JoinHashTable` probe cleanup removed one more stale ownership field:
`ProbeState::keys_to_compare_row_sel`. Selected probe continuation still needs a
map from compact compare rows back to original probe rows because `RowMatcher`
reports rows in the original probe-row coordinate system. That map does not need
probe-state lifetime. The selected path now borrows the unused tail of
`match_sel` for the current compare pass, remaps no-match continuation tokens
before appending new matches, and leaves the flat path indexed directly by row id.

This cleanup exposed a separate SF10 Q9 correctness bug. The final row-pointer
grouped update descriptor could read a second-join projection input column that
had been intentionally omitted by compressed group-key or precomputed payload skip
projection. At SF10 the stale skipped `VARCHAR` vector crashed in
`AggregateReverseMemCpy` during string compression. The fix is to reject that
descriptor with the explicit fact `group_key_omitted_input`; the existing
sidecar/batch path then consumes the live compressed key. This is not a fallback
workaround. It is the correct ownership rule: an omitted projection column is not
a valid input-vector descriptor source.

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- direct shell smoke proves the removed verification pragma remains unknown
- the isolated SF10 Q9 repro that previously crashed now completes
- SF1 profile Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_fixed_profile`
- SF1 no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_fixed_prod_notrace`
- SF10 Q9 profile and production verified in
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_q9_sf10_fixed_profile` and
  `/private/tmp/duckdb_jit_m6_probe_compare_scratch_q9_sf10_fixed_prod_notrace`

Production medians:

- SF1 Q3: `0.060s -> 0.051s`, speedup `1.176471`, correctness diff 0
- SF1 Q9: `0.142s -> 0.121s`, speedup `1.173554`, correctness diff 0
- SF1 Q20: `0.063s -> 0.045s`, speedup `1.400000`, correctness diff 0
- SF10 Q9: `1.486s -> 1.140s`, speedup `1.303509`, correctness diff 0

Direct Time Profiler evidence:

- useful final trace:
  `/tmp/trace_duckdb_jit_m6_probe_compare_scratch_fixed_q9_sqlarg_20260626_202233.trace`
- samples: 3,824 over 4.54s
- `ProbeForPointersInternal<true,false>`: `22.3%` self
- `TemplatedSignHashFunction`: `4.9%` self
- `InsertHashesLoop<false>`: `4.9%` self
- `JoinHashTable::GetRowPointersWithDenseHashes`: `25.4%` total

Conclusion: keep the scratch deletion and descriptor guard. The former removes
state that never needed object lifetime; the latter prevents an invalid
input-vector descriptor from reading skipped projection data. This is still not
the Milestone 6 root win. The regular probe loop remains the top sampled C++
frame, so the next useful work is probe/match ownership or grouped hash/state
lifetime, not broader CBO admission.

### 68. Probe And Build Scratch Should Not Share A Base Class

`JoinHashTable::SharedState` looked harmless, but it encoded the wrong ownership
model. Probe and insert both need selection vectors, but they do not share a
lifetime or a contract. Probe state owns compare/no-match selections for
RowMatcher plus dense probe hashes. Insert state owns build salts, remaining rows,
compare rows, match rows, no-match rows, and RHS row locations. Keeping those
behind one base made stale probe/build fields easier to preserve.

Change:

- deleted `JoinHashTable::SharedState`
- moved probe compare/no-match selections into `ProbeState`
- moved build compare/no-match selections into `InsertState`
- kept `key_match_sel` owned by `InsertState` because RowMatcher mutates that
  selection in place
- kept selected probe row identity unchanged because RowMatcher reports original
  probe row ids

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- direct shell smoke proves the removed verification pragma remains unknown
- SF1 profile Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_join_state_split_profile`
- SF1 no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_join_state_split_prod_notrace`
- SF10 Q9 production verified in
  `/private/tmp/duckdb_jit_m6_join_state_split_q9_sf10_prod_notrace`

Production medians:

- SF1 Q3: `0.061s -> 0.051s`, speedup `1.196078`, correctness diff 0
- SF1 Q9: `0.140s -> 0.118s`, speedup `1.186441`, correctness diff 0
- SF1 Q20: `0.065s -> 0.046s`, speedup `1.413043`, correctness diff 0
- SF10 Q9: `1.483s -> 1.139s`, speedup `1.302019`, correctness diff 0

Direct Time Profiler evidence:

- trace: `/tmp/trace_duckdb_20260626_204054.trace`
- samples: 3,894 over 4.68s
- `ProbeForPointersInternal<true,false>`: `23.1%` self
- `InsertHashesLoop<false>`: `5.6%` self
- `TemplatedSignedNumericRangeSelection<long long, false>`: `4.5%` self

Conclusion: keep the state split because it removes false sharing between build
and probe scratch ownership. It is not root performance progress. The next
Milestone 6 step must still change regular probe/match lifetime or grouped
hash/state reuse enough to move the top sampled frame.

### 69. Build-Side Selection Accessors Are Local Churn, Not The Probe Fix

The direct Q9 trace after the state split showed `InsertHashesLoop<false>` as the
second named C++ self frame. That loop owned its build scratch selections already,
so the accessor calls were local instruction churn rather than a needed API
boundary.

Change:

- `PerformKeyComparison` resets `key_match_sel` through its owned buffer
- `InsertMatchesAndIncrementMisses` reads build match/no-match/compare
  selections through owned buffers
- `InsertHashesLoop` writes salt-match and remaining selections through owned
  buffers
- RowMatcher still owns the in-place mutation of `key_match_sel`
- selected probe row identity is unchanged

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- direct shell smoke proves the removed verification pragma remains unknown
- SF1 profile Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_join_build_sel_direct_profile`
- SF1 no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_join_build_sel_direct_prod_notrace`
- SF10 Q9 production verified in
  `/private/tmp/duckdb_jit_m6_join_build_sel_direct_q9_sf10_prod_notrace`

Production medians:

- SF1 Q3: `0.061s -> 0.049s`, speedup `1.244898`, correctness diff 0
- SF1 Q9: `0.136s -> 0.117s`, speedup `1.162393`, correctness diff 0
- SF1 Q20: `0.062s -> 0.044s`, speedup `1.409091`, correctness diff 0
- SF10 Q9: `1.495s -> 1.140s`, speedup `1.311404`, correctness diff 0

Direct Time Profiler evidence:

- trace: `/tmp/trace_duckdb_20260626_204916.trace`
- samples: 3,787 over 4.52s
- `ProbeForPointersInternal<true,false>`: `23.0%` self
- `InsertHashesLoop<false>`: `5.0%` self, down from `5.6%` in the previous trace
- `SelectionVector::set_index`: `0.8%` self
- `SelectionVector::get_index_unsafe`: `1.1%` self

Conclusion: keep the direct-buffer cleanup because the scratch selections are
owned and the change removes accessor overhead in a sampled build frame. It is
still local instruction cleanup. The next root fix must move or bypass regular
probe work; `ProbeForPointersInternal<true,false>` remains the top frame.

### 70. Direct No-Chain Pair Probe Is A Real Boundary Deletion

The Q9 trace kept pointing at regular hash join probe work after the state and
selection cleanups. The useful deletion is not another scratch-vector rename; it
is bypassing the generic sequence for a proven simple shape:

```text
Hash(keys) -> GetRowPointers -> RowMatcher
```

For inner joins with exactly two 64-bit equality keys, no NULL handling, no
residual predicate, no chain matcher, and no duplicate build chains, the runtime
now hashes both probe keys, probes the existing regular hash table, compares both
row-layout build keys directly, and emits row pointers plus the match selection
in one compact loop. The gate is deliberately strict:

- `INNER` join only
- exactly two equality predicates
- probe and build key physical types are `INT64` or `UINT64`
- no `IS NOT DISTINCT FROM` semantics
- no build NULLs and no probe NULL filtering
- no residual predicate and no chain matcher
- no duplicate-chain continuation

Two attempted follow-up micro-optimizations were measured and removed. Splitting
the direct loop by selector/salt shape and then adding a first-bucket branch
expanded code and made Q9 profile mode worse. The retained implementation keeps
the direct probe compact and only avoids the stale generic hash/probe/match
passes.

Verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- direct shell smoke proves the removed verification pragma remains unknown
- SF1 profile Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_pair_probe_compact_profile`
- SF1 no-trace production Q3/Q9/Q20 verified in
  `/private/tmp/duckdb_jit_m6_pair_probe_compact_prod_notrace`
- SF10 Q9 production verified in
  `/private/tmp/duckdb_jit_m6_pair_probe_compact_q9_sf10_prod_notrace`

Production medians:

- SF1 Q3: `0.062s -> 0.051s`, speedup `1.215686`, correctness diff 0
- SF1 Q9: `0.144s -> 0.115s`, speedup `1.252174`, correctness diff 0
- SF1 Q20: `0.064s -> 0.045s`, speedup `1.422222`, correctness diff 0
- SF10 Q9: `1.618s -> 1.116s`, speedup `1.449821`, correctness diff 0

Direct Time Profiler evidence:

- trace: `/tmp/trace_duckdb_20260626_213759.trace`
- samples: 4,055 over 4.88s
- `ProbeForPointersFlatInternal<true>`: 7 samples
- `ProbeInt64PairNoChainLoop<long long, long long>`: `32.0%` self
- `InsertHashesLoopProbe<false, false>`: `4.8%` self
- `TemplatedSignHashFunction`: `3.8%` self

### 71. Helper Hints Did Not Beat Ownership Cleanup

After the direct pair probe, the next clean refactor was to make row emission a
local consumer contract rather than scattering `row_pointers` and `match_sel`
writes through each fast loop. The retained change adds that consumer shape to
the core direct pair probe and the SLJIT all-valid pair no-chain probe without
changing the emitted layout.

The useful build-side cleanup was concrete ownership, not hints:

- `ApplyBitmaskAndGetSaltBuild` now writes the flat salt vector directly instead
  of using `FlatVector::Writer<hash_t>` in the hot finalize path.
- `JoinHashTable::Finalize` writes the loaded row hashes directly into the flat
  hash vector.
- `InsertHashesLoopProbe` carries one loaded `entry_value` through occupied,
  salt, and pointer extraction instead of calling separate entry accessors.

Header-level force-inline hints for `ht_entry_t` and `BloomFilter::GetMask` were
tried and removed. Time Profiler continued to attribute samples to those helpers,
and the production medians did not justify carrying hint-only code.

Final retained verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `git diff --check` passed
- The removed verification pragma remains unknown
- SF1 coverage-CBO production verified in
  `/private/tmp/duckdb_jit_m6_retained_probe_cleanup_coverage_prod_notrace`

Final retained production medians:

- Q3: `0.064s -> 0.052s`, correctness diff 0
- Q9: `0.150s -> 0.120s`, correctness diff 0
- Q20: `0.064s -> 0.046s`, correctness diff 0

Useful trace:

- `/tmp/trace_duckdb_20260626_221418.trace`, 2,179 samples over 2.90s
- Remaining roots include `InsertHashesLoopProbe`, `ht_entry_t::GetValue`,
  `TemplatedSignHashFunction`, and `BloomFilter::GetMask`
- Next work should remove deeper build/probe ownership boundaries, not add more
  helper-level annotations.

Conclusion: keep the direct no-chain pair probe because it deletes the generic
hash/probe/match boundary for the Q9 pair-key shape and moves SF10 Q9 materially.
The next root is now inside that direct loop: hash computation, regular-table
pointer chasing, and build/finalize hash insertion remain sampled. Do not broaden
this gate to chains, NULL-aware joins, residuals, or outer/mark joins until those
contracts have equally direct ownership and correctness evidence.

### 72. Build Probe Cleanup Removed Stale State But Did Not Finish Milestone 6

The next pass stayed in the Milestone 6 regular hash join hot path and kept only
changes that removed stale state or duplicated ownership:

- serial build insertion now has a dedicated known-empty helper, so the common
  non-parallel empty-slot path stores the new entry without re-reading the slot
  it just proved empty
- matching-key chain insertion is split into its own helper, removing the old
  `EXPECT_EMPTY` template branch from the build helper
- build probe offset advancement now uses the real hash-table `bitmask`; the
  old `bitmask | SALT_MASK` mask was stale after offset and salt stopped sharing
  the same word in the build path
- the SLJIT matched-row consumer now covers all-valid single-key no-chain probes
  as well as pair-key no-chain probes, so no-chain fast paths share the same
  row-pointer plus match-selection emission contract

This is a cleanliness and instruction-local cleanup, not the Milestone 6 root
win. The final SF1 coverage-CBO artifact is
`/private/tmp/duckdb_jit_m6_consumer_build_cleanup_final_coverage_prod_notrace`:

- Q3: `0.062s -> 0.051s`, correctness diff 0
- Q9: `0.147s -> 0.117s`, correctness diff 0
- Q20: `0.063s -> 0.045s`, correctness diff 0

Focused verification:

- `make reldebug -j12` passed
- `[api][jit]` passed
- `build/reldebug/test/unittest test/sql/join --print-failing-tests` exited 0
- `python3 benchmark/jit/verify_jit_architecture.py` passed
- `python3 benchmark/tpch/jit/verify_tpch_benchmark.py`
  `/private/tmp/duckdb_jit_m6_consumer_build_cleanup_final_coverage_prod_notrace`
  passed

The current valid Time Profiler trace is
`/tmp/trace_duckdb_20260626_223044.trace`, 3,172 samples over 3.89s. The
remaining top frames are still:

- `ht_entry_t::GetValue`: `6.5%`
- `InsertHashesLoopProbe`: `5.7%`
- `TemplatedSignHashFunction`: `5.3%`
- `BloomFilter::GetMask`: `4.6%`
- `ExecuteAllValidInt64PairNoChainProbe`: `2.6%`

One attempted stdin-fed xtrace,
`/tmp/trace_duckdb_20260626_222949.trace`, sampled only startup and is invalid.
Do not use it as performance evidence.

The next Milestone 6 step must still remove a real probe/build ownership
boundary: either carry matched row descriptors directly into grouped state lookup
and payload update, or reduce the surviving regular hash-table probe/build
traffic itself. Local helper reshaping is exhausted unless it deletes measured
loads, branches, or materialized descriptor state.

## Bottom Line

The proven path is aggressive fusion that removes materialization and keeps data
hot. FLOAT/DOUBLE CTAS proved the model. TPC-H needs the same principle applied
to its actual types and its grouped aggregate state layout. SIMD and GPU are
worth pursuing, but only when the data path is fused enough that compute is the
limiting factor rather than selection, materialization, state lookup, launch, or
copyback overhead.
