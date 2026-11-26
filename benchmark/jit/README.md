# DuckDB JIT Verification

This directory holds the checks an execution-region JIT change must pass. The
architecture contract is `benchmark/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`.

## Build

Configure the build once with the TPC-H extension linked into the binary; the
guard, the gates and the `[jit]` tests load it:

```sh
GEN=ninja BUILD_TPCH=1 LINK_CORE_EXTENSIONS=1 CMAKE_BUILD_PARALLEL_LEVEL=12 make reldebug
```

The full guard also checks the assertion build (assertions, ASan and UBSan), configured the same way:

```sh
GEN=ninja BUILD_TPCH=1 LINK_CORE_EXTENSIONS=1 CMAKE_BUILD_PARALLEL_LEVEL=12 make relassert
```

Later builds use the repository-required thread count:

```sh
cmake --build build/reldebug -j12
```

## The guard

```sh
python3 benchmark/jit/run_jit_refactor_guard.py unit
python3 benchmark/jit/run_jit_refactor_guard.py full
```

The guard stops at the first check that fails. It has no options: a level is
the same checks for every change. The builds run first; the checks that only
verify results then run side by side (four at a time, each unittest under a
temp directory of its own); the performance gates run last and alone, so that
nothing else runs while they count cycles. Every check prints how long it took.

`unit` runs:

1. the build;
2. the architecture boundaries (`verify_jit_boundaries.py`);
3. the generated settings and metrics (`scripts/generate_*.py --check`);
4. the harness's own tests (`test_*.py` in this directory);
5. side by side: the JIT unit suite (`build/reldebug/test/unittest "[jit]"`) and a fixed-seed differential fuzz
   campaign.

`full` also builds the assertion build after the build, runs a longer campaign, and adds to the checks that run side
by side:

- the JIT, join, subquery, aggregate, CTE and TPC-H SQL suites with the JIT
  forced on (`test/configs/jit_cbo_correctness.json`); the default unittest
  runner does not load the JIT backend, so this is where compiled kernels
  meet the general plan shapes;
- the assertion build's JIT unit suite (`build/relassert/test/unittest "[jit]"`), which runs the
  debug-only checks and stops at the first undefined behavior;

and then runs the performance gates: `generic` and `encodings` on one and on four threads, `tpch` on one.

### Git hooks

```sh
git config core.hooksPath benchmark/jit/git_hooks
```

The pre-commit hook runs the `unit` level. A `full` guard that passes records
the git tree of the files it verified, as they are on disk, in the repository's
git directory. The pre-push hook pushes `HEAD` when that record is the tree of
`HEAD`, and otherwise runs the `full` level first. A change to any file that
git does not ignore is a different tree, so a record never covers a later
change.

## Differential fuzz

`jit_differential_fuzz.py` generates seeded random queries over the TPC-H
schema and runs each with the JIT off, at the default cost model and with
compilation forced, at one and at several threads. It compares the results,
including the errors the native engine raises.

The campaign builds its own database: TPC-H at `--scale-factor` in which every
column the generator references is NULL in one row of sixteen. TPC-H has no
floating-point column, so the database adds FLOAT and DOUBLE columns derived
from TPC-H ones that mix in both NaN signs, both infinities and both zeros,
and likewise derives the types TPC-H never stores: TINYINT, UTINYINT, SMALLINT
and USMALLINT at both ends of their ranges, UBIGINT above BIGINT, signed
HUGEINT, DECIMAL(38,6), BOOLEAN and TIMESTAMP. Predicates over several columns
also appear as values (select lists, group keys, aggregate arguments), where
the input slots a compiled predicate reads differ from its columns.

```sh
python3 benchmark/jit/jit_differential_fuzz.py --queries 3000 --seed 7 --scale-factor 1 --report report.json
python3 benchmark/jit/jit_differential_fuzz.py --replay report.json
```

A report holds a coverage summary and every failing or inconclusive query;
`--replay` re-checks those. A pass requires every requested query to be
compared under both JIT policies: an unusable native reference or a JIT
out-of-memory result is inconclusive, not a pass. The exit code is 0 for a
complete pass, 1 for findings and 2 for incomplete coverage.

## Performance gate

```sh
python3 benchmark/jit/performance_gate.py generic --threads 1
python3 benchmark/jit/performance_gate.py generic --threads 4
python3 benchmark/jit/performance_gate.py encodings --threads 1
python3 benchmark/jit/performance_gate.py encodings --threads 4
python3 benchmark/jit/performance_gate.py tpch --threads 1
python3 benchmark/jit/performance_gate.py tpch --threads 1 --workloads q06 q18 --out-dir gate_q06_q18
```

A suite is a list of workloads in `workloads.py`; a workload is one query and
its checked-in thresholds per thread count: the least speedup, the most JIT
cost and whether the JIT compiles it. A suite run covers the workloads that
have thresholds for the requested thread count. `generic` covers the plan shapes
the JIT compiles, each on a fixture its `setup_sql` builds; `encodings` runs
plain analytical queries over the column encodings real tables have (NULLs,
dictionary-compressed strings, narrow integers, decimals, doubles, a joined
dimension, randomly ordered keys), including shapes the JIT must decline;
`tpch` is the 22 TPC-H queries at scale factor 10.

### What a run does

Every sample is a fresh shell on a read-only fixture database. The shell first
runs the query on the native engine, which loads the fixture into the buffer
pool and produces the result the sample must reproduce. It then runs the query
under the sampled policy; only that statement is measured. The policies
alternate in which is sampled first. After the timed samples, one more JIT
sample traces the runtime for the counter checks.

A build writes each fixture once: `--fixture-cache` (by default
`~/.cache/duckdb_jit_gate_fixtures`) keeps the fixtures of the two builds used
last, by a digest of the binary and of the setup SQL, so the gates of a suite on
one and on four threads, and any later gate of the same build, reuse them.

### Cost

Wall time measures the host as much as the query: other processes lower the
clock of the performance cores, push the query's threads onto efficiency cores
and make them wait for a core. The gate measures the work instead, from the
kernel's per-process counters (`process_usage.py`, macOS):

```text
cost = median over samples (instructions retired)
       x performance-core cycles / performance-core instructions, summed over the
         middle half of the samples ranked by that rate
```

That is the cycles the query takes when every thread runs on a performance
core. The instruction count does not depend on the host, and the cycles per
instruction come only from the cores the scheduler does not slow down. A sample
the host stalls retires the same instructions several times slower; ranking
leaves it out of the rate instead of letting it dominate the sum.

A workload passes when:

- every sample's result equals the native engine's;
- the native samples record no JIT counters, and the JIT samples compile
  without error and compile a region exactly when the workload says they do
  on that thread count;
- the traced sample executes a compiled region, takes the runtime paths the
  workload requires, and satisfies the counter contract (`counter_contract.py`):
  a runner the cost model selected either reports why it did not compile or
  records, at runtime, every proof of the work the model credited to it;
- `cost(off) / cost(auto)` is at least the workload's floor, when the JIT compiles it; a query the JIT does not
  compile runs the native engine under both policies, so their cycles compare only the host's noise, and its JIT run
  must instead retire at most 2% more instructions than the native run (the cost of the decision);
- `cost(auto)` is at most the workload's ceiling.

Floors and ceilings are one-way ratchets: a verified improvement raises the
floor and lowers the ceiling in the same change, and nothing loosens them.

### Limits of the metric

- Competing CPU load does not move the cost. Competing memory traffic does: a
  process that saturates the memory bus or faults pages heavily raises the
  cycles per instruction of both policies. The speedup absorbs most of it and
  the ceilings allow for the rest; a ceiling that fails while the host is
  swapping or copying memory is the host's failure, and the fix is to rerun.
- Cost is the sum of the work of all threads. On several threads it does not
  see a query that keeps fewer cores busy, so a change that serializes a
  pipeline can hold its floor and still take longer. `summary.csv` reports the
  median wall time of both policies for that reason; compare them on a quiet
  host when a change touches scheduling or a parallel sink.

### Artifacts

The gate writes to `--out-dir` (a new temporary directory by default):

- `runs.csv`: one row per sample with its wall time, runnable time,
  instructions, performance-core instructions and cycles, correctness diff and
  region counts;
- `summary.csv`: one row per workload with both costs, the speedup, the
  thresholds and the median wall times;
- `counters.json`: the `duckdb_jit_counters()` rows of every sample.

The exit code is 1 when any workload fails.

## Comparing two builds

```sh
python3 benchmark/jit/ab_gate.py --base /path/to/baseline/duckdb
python3 benchmark/jit/ab_gate.py --base /path/to/baseline/duckdb --apply
python3 benchmark/jit/ab_gate.py --base /path/to/baseline/duckdb --gates tpch:1 --workloads q09 q18
```

A threshold moves only on evidence from both builds measured in one session. `ab_gate.py` gets that evidence for the
workloads a change moves, without measuring the others: it screens every workload of the gates once on each build
(`--base` and `--new`, the current build by default) and keeps those whose instructions on either engine differ by more
than 2%, whose cycles differ by more than 15%, or whose compiled region count differs. It measures only those, the
builds alternating, for three rounds of five samples per policy, and prints each one's speedup on both builds, how the
native and JIT cycles and the JIT instructions moved, and the thresholds the rounds support. `--workloads` skips the
screen and measures the named workloads.

`--apply` writes those thresholds to `workloads.py`: a ceiling falls to 5% above the new build's highest JIT instruction
count and a floor rises to 90% of its lowest speedup. A floor falls only where the native engine took fewer cycles in
every round, by at least 5% at the median: the speedup then drops without the JIT doing worse.

Both builds read fixtures the base build writes, so a change to how storage writes data needs `--fixtures-per-build`.
A change whose instructions stay within the tolerance but moves cycles less than 15% is not screened in; the guard's
gates still hold it to the floors.
