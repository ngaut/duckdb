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

The guard runs its checks in order and stops at the first that fails. It has
no options: a level is the same checks for every change.

`unit` runs:

1. the build;
2. the architecture boundaries (`verify_jit_boundaries.py`);
3. the generated settings and metrics (`scripts/generate_*.py --check`);
4. the harness's own tests (`test_*.py` in this directory);
5. the JIT unit suite (`build/reldebug/test/unittest "[jit]"`);
6. a fixed-seed differential fuzz campaign.

`full` runs the same checks with a longer campaign, and then:

7. the JIT, join, subquery, aggregate, CTE and TPC-H SQL suites with the JIT
   forced on (`test/configs/jit_cbo_correctness.json`); the default unittest
   runner does not load the JIT backend, so this is where compiled kernels
   meet the general plan shapes;
8. the assertion build and its JIT unit suite (`build/relassert/test/unittest "[jit]"`), which runs the
   debug-only checks and stops at the first undefined behavior;
9. the performance gates: `generic` and `encodings` on one and on four threads, `tpch` on one.

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
- `cost(off) / cost(auto)` is at least the workload's floor;
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
