# DuckDB JIT Verification

This directory contains the local guardrails for execution-region JIT work.
Architecture and policy rules live in
`benchmark/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`.

## Commands

Architecture verifier:

```sh
python3 benchmark/jit/verify_jit_architecture.py
```

Refactor guard:

```sh
python3 benchmark/jit/run_jit_refactor_guard.py
```

Install repo-local guard hooks:

```sh
python3 benchmark/jit/install_refactor_guard_hooks.py
```

The hooks divide responsibility instead of repeating the same work.
Pre-commit validates the staged tree with the build, architecture, Python, and
JIT unit ratchet, then publishes the verified Git tree hash. When HEAD matches
that receipt, pre-push reuses the result and adds only the generic and TPC-H
production gates required by performance-sensitive branch changes. A missing
or stale receipt makes pre-push run the complete guard. Generic candidate gates
use five order-alternating policy pairs. The pre-push SF10 comparison uses ten,
giving each leading-policy order five samples so process/cache order cannot
choose the median. Focused triage remains an explicit operator action, never an
automatic failed-candidate rerun.

The accepted TPC-H artifact retains the maximum raw JIT-auto runtime from its
ten samples for each query. A candidate median must stay within that observed
high-water mark plus the existing 2% or 2 ms allowance. Off-normalized timing
remains secondary noise evidence and never makes a raw regression pass.
The full refactor guard runs this historical raw-runtime comparison before the
generic production matrix, matching standalone baseline qualification instead
of preheating the machine with an unrelated suite.

TPC-H benchmark and comparison gate:

```sh
python3 benchmark/tpch/jit/tpch_benchmark.py --duckdb build/reldebug/duckdb --queries all --policies off auto
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --queries all
```

Generic production workload gate:

```sh
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --threads 4
```

The generic gate covers arithmetic, filters, CASE-heavy expressions, multiple
aggregate lanes, filtered scans, mixed numeric/date plus nullable-string
predicates, persistent column-vs-column comparisons, single- and multi-source
nullable persistent scans, grouped DISTINCT, dense computed multi-aggregate
grouping, projected, affine, nullable multi-lane, and bounded wide-lane
sorted-run grouping, sparse monotonic grouping, and joins. Arithmetic,
CASE, multi-aggregate, scan-expression, scan-filter, mixed-predicate,
column-comparison, both nullable
classes, and the proven grouped workloads require compiled speedups at their
configured thread counts. The mixed-predicate gate requires at least 1.25x at
one thread and 1.20x at four threads. The column-comparison gate requires at
least 1.25x at both one and four threads. Dense multi-aggregate grouping
requires at least 1.80x at one thread and 1.60x at four threads; projected
sorted-run grouping requires 3.10x and 1.75x respectively, while affine
arithmetic grouping requires 2.75x and 1.45x. Nullable multi-lane sorted runs
require 1.70x and 1.45x; 16-lane shared-affine sorted runs require 2.60x and
2.22x. Sparse monotonic grouping requires 2.40x and 1.08x;
the four-thread query shares a two-million-group state scan with JIT-off, so its
parallel floor protects the generated sink win without claiming that shared
downstream work. Join workloads without a proven
compiled route have a bounded auto-policy slowdown and may remain vectorized.
The generic runner never adds repetitions after a candidate failure. Run the
same command with `--repeats 10` only when deliberately promoting a result.
The full refactor guard acquires ten repetitions up front because it is a ship
qualification, while direct candidate runs retain the five-repetition default.

Generated scan-filter ownership is semantic and workload-independent. It moves
only exception-free static predicates over the retained source layout into the
region; a constant integral modulo or integer division is safe when its divisor
cannot be zero (or signed `-1`). If the backend cannot lower an admitted
predicate, the normal DuckDB scan filter remains in charge—no filter is
duplicated or silently dropped.

The four-thread scan-filter promotion proves 2.016x over ten alternating pairs
(51.823 ms JIT-off versus 25.712 ms JIT-auto). Its 1.85x thread-specific floor
preserves generated ownership of the safe modulo filter while leaving room for
storage-scan variance.

Join-heavy validation also covers exact perfect-hash dynamic filters. Storage
executes exact PHJ conjuncts before generic residual predicates while the
adaptive wrapper remains active. A query-local filter/table identity binds the
compiled probe to the same membership definition, but does not claim that
storage enforced it on every batch: adaptive filtering may pause. The generated
perfect-hash probe is the single publication boundary and always checks source
validity, key range, and sparse build membership before emitting a match.
Runtime tracing reports
`hash_join_probe.perfect_probe.exact_source_filter` when this contract fires.
The generic exact-filter join preserves a 1.15x single-thread compiled speedup;
the four-thread gate retains its separate 1.08x floor because its shorter raw
runtime has a larger proportional noise envelope.

Direct selected-view terminals may request identity-preferred perfect-hash
output. All-match batches omit disposable match-selection stores; a batch with a
miss reruns the compact-selection kernel before it is published. Trace receipts
name the elided path or the retry, so the fast path never hides selection
semantics.

When the bound RHS predicate vector is all-valid, complementary aggregate lanes
also account for each group row. The terminal omits its redundant represented-row
counter; nullable vectors retain the general accumulator and cannot use that
receipt.
For an observed one- or two-key group domain, the all-valid terminal merges
vector-local totals once per known group and commits before any new-key fallback.

Grouped key min/max facts belong to the executable, not to individual chunks.
When trusted operator statistics prove a signed narrowing cast, projected and
row-pointer grouped runtimes consume that proof directly and skip the former
full-vector cast-fit scan. Unknown ranges keep the checked per-chunk path.
Flat, identity-selected, all-valid fallback vectors reduce that check directly
over contiguous data so the compiler can SIMD-vectorize it; selected or nullable
vectors retain the general validity-aware loop.
Canonical one-lane SUM states likewise initialize directly from preaggregated
values; custom aggregate layouts retain the generic initializer. Input-order
grouped-state addresses are published with a null address selection, so identity
order does not pay a per-group selection branch in any callback backend.

When a production run verifies a durable performance improvement, the same
increment must tighten the corresponding checked-in speedup floor or refresh
the accepted comparison artifact. A performance change is incomplete while
the regression gate still accepts the older, slower performance level.
Read-only fixtures are built once per shared setup and reopened for each sample;
the timed policies no longer rebuild multi-million-row tables. The selective
grouped multi-aggregate floors are 1.22x at one thread and 1.17x at four threads,
against independent ten-repeat proofs no lower than 1.238x and 1.195x. The
two-way conjunction carries 1.31x and 1.25x floors; the three-way variant adds
1.25x and 1.20x floors. The neighboring non-null grouped workload has 1.16x and
1.13x thread-specific floors. The mixed-predicate promotion proves 1.338x at
one thread and 1.286x at four threads.
The high-cardinality two-fragment `LIKE` scan proves 1.604x at one thread and
1.556x at four threads over ten production repetitions. Its 1.55x and 1.50x
floors protect filter-operator-owned batch selection, cold exact-candidate
verification, one-pass survivor compaction, and combined 48-byte ARM64 pair
scans with an overlapping vector tail instead of the former generated per-row
runtime call. An all-match batch returns the input count, so the caller reuses
the input view and ignores the compact-selection scratch without a separate
lazy-selection state machine.
The mixed-source complementary string join promotion receipts prove 1.332x at
one thread and 1.250x at four threads. Its 1.31x and 1.24x floors protect the
generic direct terminal: a runtime-adaptive pipeline-local group accumulator,
a range-proven BIGINT-to-INTEGER selected no-chain probe, and an executable-owned
perfect-hash RHS predicate classifier. Once all local tasks collectively cover a
dictionary with direct probe rows, one task builds an immutable artifact for an
external-string predicate. It owns the dictionary buffer, its all-valid fact,
and one classification byte per slot. Fixed-width values and two inline string
constants retain their direct packed comparison because another dictionary pass
cannot repay its extra indirection. Publication and every reader use DuckDB's
atomic shared pointer, so no task can observe a callable classification without
its owner; post-publication lookups are byte loads with no mutex acquisition.
For direct perfect-hash output with an incremental source, the build index is
the normalized key minus the hash minimum. An all-valid terminal directly
accumulates an incremental inline group at its ordinal, committing only after
the full vector proves the known one- or two-key domain. Any NULL, long string,
or unseen key leaves state untouched and uses the general transform path.
Generated sorted-run aggregation proves 3.222x at one thread and 1.902x at four
threads over ten alternating production repetitions; its checked-in floors are
3.10x and 1.75x. The same pending owner preaggregates raw equivalence keys for
affine arithmetic groups and applies the invariant transform once per published
group. The one-thread promotion proves 3.101x; the four-thread floor remains
1.45x pending an independent promotion. Pipeline-local state
keeps an unpublished boundary group across scheduler fairness yields. Parallel
radix finalization compares conservative key intervals. Exact intervals coalesce
into a covering hull when the summary budget is reached, so storage pressure
cannot invalidate local monotonic uniqueness. Disjoint summaries skip rehash
while shared boundary keys still reconcile normally. The backend
kernel accepts exact keys, proven signed narrowing casts, and integral
compression through one generated ABI. Single-lane all-valid and nullable
kernels retain their tuned representations. Multi-lane kernels compile one
primitive descriptor list and bind a cached runtime lane array, so nullable
`SUM`, nullable `COUNT`, and `COUNT(*)` execute together without a nullability
specialization matrix. The nullable multi-lane workload proves 1.785x and
1.505x over ten production repetitions; its floors are 1.70x and 1.45x.
The sparse-key variant crosses the exact interval budget and proves that the
bounded conservative hull remains useful. After identity address publication,
column-wise fixed-width materialization, and one-word canonical SUM flags, ten
production repetitions measure 2.676x at one thread; the independent
four-thread promotion measures 1.134x. Their floors are 2.40x and 1.08x.
Generated run reducers deliberately unroll at most eight aggregate lanes. They
are generated lazily only after runtime ordering economics accepts the stream,
and publish only the observed key-cast and nullability specialization. A
16-lane sorted-run workload therefore exercises the bounded generic route while
the rest of the region remains compiled; ten-repeat production medians prove
1.223x at one thread and 1.311x at four threads, protected by 1.15x and 1.20x
floors. Payload-source descriptor storage is pipeline-local and rebound without
per-chunk allocation. Generated run lowering requires a 64-bit SLJIT machine
word and the required register file; unsupported targets retain the exact
generic reducer until paired-register lowering exists.
Selections, non-flat inputs, unproven casts, and unsupported primitive types
retain the exact fallback.
Selected one-key regular hash probes use the compressed INTEGER table directly
from a BIGINT source only when source statistics prove the narrowing and the
runtime proves a no-chain matched-output layout. The loop remains generic over
table identity and retains Bloom, salt, and selection semantics.
Hybrid SIMD admission uses one shared scalar-operation cost contract: fully
packed select/count/sum kernels retain simple comparisons, while scalar-terminal
hybrids require enough predicate work to amortize mask dispatch. AND hybrids
evaluate their packed masks branchlessly and classify only the completed mask;
OR remains available to fully packed kernels but stays scalar in hybrids after a
neutral matched proof. Mixed masks visit only set lanes with count-trailing-zero
iteration. Specialized predicates may use the same loop for the longest ordered
SIMD-supported AND prefix, then execute their generic residual directly without
an intermediate selection vector.

Filtered hash-build validation covers source-filter selection composition and
direct build ingress. Runtime tracing reports `filter.selected_input_zero_copy`
when one generated filter evaluates a previous selection against the original
producer chunk, and
`hash_join_build.selected_source_view` when the terminal consumes a selection
without an intervening projection. These selected-view paths must have no
runtime delegation entries.

Workload preparation and expected-result materialization always run with JIT
off. The gate resets events and counters after preparation, immediately before
the measured query, so compiled/runtime coverage can only be credited to the
target workload.

Runtime proof is a separate traced mode:

```sh
python3 benchmark/jit/generic_benchmark.py --duckdb build/reldebug/duckdb --trace-runtime
```

Traced mode verifies that compiled regions execute and records runtime paths;
it does not enforce performance thresholds. Use production timing for
regression decisions.
