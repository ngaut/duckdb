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

## Bottom Line

The proven path is aggressive fusion that removes materialization and keeps data
hot. FLOAT/DOUBLE CTAS proved the model. TPC-H needs the same principle applied
to its actual types and its grouped aggregate state layout. SIMD and GPU are
worth pursuing, but only when the data path is fused enough that compute is the
limiting factor rather than selection, materialization, state lookup, launch, or
copyback overhead.
