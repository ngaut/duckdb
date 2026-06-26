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
  aggregate insertion and state-address resolution, not a stale JIT wrapper
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
  `expression_tree_source_indices` stale
- normal filter execution used the remapped `source_index` and looked correct
- fused filtered aggregate execution used the stale expression-tree source list
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
without bloom because it used the advanced offset/salt, but bloom saw a stale
hash and introduced false negatives. The fix is a useful principle: when a
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

### 16. Grouped Aggregate Fast Paths Must Split Existing-Group and New-Group Cases

The first regular hash aggregate shortcut only handled all-existing groups. It
was correct for low-cardinality aggregates, but TPC-H Q20 and synthetic
high-cardinality grouped aggregates spend real time creating new groups. The
next data-centric step is a separate fixed-width all-new fast path:

- probe the pointer table for the batch
- claim empty slots with normal linear probing
- append group rows and initialize aggregate states once
- update primitive aggregate state directly
- fall back before semantic mutation on same-salt matches, duplicates, NULL
  semantics, VARCHAR keys, or unsupported payloads

This keeps the state-address contract centralized in DuckDB's aggregate hash
table instead of teaching SLJIT how to own tuple-data layout. The JIT runtime
now tries existing groups first for reuse-heavy batches, then fixed-width new
groups for creation-heavy batches, then falls back to the generic
`ResolveStateAddresses` path.

Measured checkpoint:

| Workload | Result |
| --- | ---: |
| Focused API coverage | `direct_existing_grouped_primitive_update` and `direct_new_grouped_primitive_update` both verified |
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

## Bottom Line

The proven path is aggressive fusion that removes materialization and keeps data
hot. FLOAT/DOUBLE CTAS proved the model. TPC-H needs the same principle applied
to its actual types and its grouped aggregate state layout. SIMD and GPU are
worth pursuing, but only when the data path is fused enough that compute is the
limiting factor rather than selection, materialization, state lookup, launch, or
copyback overhead.
