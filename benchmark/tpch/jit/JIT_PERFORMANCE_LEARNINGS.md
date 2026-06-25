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
- selected-present fast paths for aggregate source loading
- hoisting group-selection base pointers
- hoisting group-data base pointers where safe
- avoiding null checks when runtime facts prove all payloads are present

These improved generated-region runtime, but did not create a large end-to-end
TPC-H win by themselves.

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

### Failed Experiments Are Profiling Data

Do not reverse a correct direction just because the first implementation is bad.
First locate the cause:

- short group runs killed one-entry caching
- per-row slot-map loads killed dense remapping
- launch/copyback overhead killed per-vector GPU
- setup/commit scans killed sparse eager-zero on larger domains

After the cause is known, either narrow the optimization to the shape where it
wins or redesign it around the real bottleneck.

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
