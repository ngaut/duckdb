# DuckDB JIT Verification

This directory contains architecture, correctness, and performance guardrails
for execution-region JIT. The stable design contract is
`benchmark/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`.

## Fast verification

Build with the repository-required thread count:

```sh
cmake --build build/reldebug --config RelWithDebInfo -j12
```

Run static architecture checks and Python tests:

```sh
python3 benchmark/jit/verify_jit_architecture.py
python3 -m unittest \
  benchmark/jit/test_benchmark_common.py \
  benchmark/jit/test_run_jit_refactor_guard.py \
  benchmark/jit/test_generic_benchmark.py \
  benchmark/tpch/jit/test_compare_tpch_benchmark.py \
  benchmark/tpch/jit/test_tpch_benchmark.py \
  benchmark/tpch/jit/test_run_tpch_regression_gate.py \
  benchmark/tpch/jit/test_verify_tpch_benchmark.py
```

Run JIT correctness:

```sh
build/reldebug/test/unittest "[jit]"
```

## Generic production workloads

The generic matrix covers expression, filter, nullable, persistent scan,
grouped aggregate, DISTINCT, numeric join, string join, selected-view, and
parallel workload classes. Workload definitions, thread-specific speedup
floors, accepted raw-runtime baselines, and derived noise ceilings are the
source of truth in `benchmark/jit/generic_benchmark.py`.

Candidate runs use five alternating repetitions:

```sh
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 1 --repeats 5
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 4 --repeats 5
```

Use ten repetitions only for deliberate promotion or explicit noise
qualification:

```sh
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 1 --repeats 10
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 4 --repeats 10
```

Production samples disable runtime tracing and verification. A failed
five-repeat candidate is reported as-is; the runner does not schedule an
automatic larger retry.

The matrix uses one DuckDB shell process. Each workload's fixture is prepared
immediately before its samples, then the shell closes the database to
`:memory:`. It repeats that close/reopen boundary before every timed attempt, so
workload chronology, connection state, and buffer-manager freshness are
preserved. This changes process-launch cost, not sample semantics: correctness
artifacts, counters, policy order, and the query-only timer remain per attempt.
In particular, macOS no longer performs a provenance assessment for every
sample.

Each speedup sample is the `off/auto` ratio from the same alternating repeat;
the gate uses the median paired ratio. The raw JIT-auto median is checked
independently, so pairing cannot hide an absolute runtime regression.

## TPC-H regression gate

The standalone gate builds, measures, verifies result artifacts, compares
against accepted raw-runtime and speedup contracts, and runs a separate traced
proof pass for every compiled or accelerated-runner-selected query.

Production timing uses one DuckDB shell process for the complete query matrix.
Each query baseline is prepared immediately before its samples; preparation and
every attempt close back to `:memory:` so the next sample reopens the same
checkpointed database with fresh connection and buffer-manager state. When
tracing is enabled, untimed counter collection runs after every candidate timer
in the same script and cannot perturb the candidate measurements.

SF10 uses the default accepted state:

```sh
python3 benchmark/tpch/jit/run_tpch_regression_gate.py \
  --queries all --repeats 5
```

SF1 uses its own state:

```sh
python3 benchmark/tpch/jit/run_tpch_regression_gate.py \
  --queries all --scale-factor 1 --repeats 5 \
  --baseline-state benchmark/tpch/jit/local_baselines/tpch_refactor_guard_sf1_state.json
```

Promotion requires a complete 22-query, ten-repeat production artifact:

```sh
python3 benchmark/tpch/jit/run_tpch_regression_gate.py \
  --queries all --promote-baseline --promotion-repeats 10
```

The default SF10 state is
`benchmark/tpch/jit/local_baselines/tpch_refactor_guard_state.json`. Accepted
state and artifacts are local and ignored by Git. State paths are relative, so
moving the checkout does not invalidate them. Disposable candidates remain
under `benchmark/tpch/jit/tmp/`.

The gate keeps a validated immutable scale-factor-keyed template under the
ignored `local_baselines/databases/` directory. An exclusive process lock
protects template creation, stale locks recover automatically, and an invalid
template is regenerated atomically. Each invocation benchmarks a private
working clone, so correctness tables cannot mutate the template or serialize
independent gates. macOS and Linux use filesystem copy-on-write when available;
other filesystems fall back to a normal copy. Use `--no-database-cache` only
when dbgen itself must be exercised.

## Refactor guard

The combined guard selects work from the changed paths and composes static,
correctness, generic, and TPC-H checks:

```sh
python3 benchmark/jit/run_jit_refactor_guard.py
```

Install repository-local Git hooks with:

```sh
python3 benchmark/jit/install_refactor_guard_hooks.py
```

Pre-commit validates the staged tree and writes a receipt for its Git tree.
Pre-push reuses a matching receipt and adds required production performance
gates. A missing or stale receipt causes the complete guard to run. TPC-H runs
before the generic matrix so historical comparison is not preheated by an
unrelated sustained workload.

Successful production verification writes a second exact-tree receipt. A
normal push reuses it without rerunning benchmarks. If starting `git push`
itself activates macOS security scanning, prequalify while the host is quiet:

```sh
benchmark/jit/git_hooks/pre-push
git push
```

The first command performs the same production guard and publishes the receipt;
the second only verifies that the current Git tree still matches it.

The combined guard samples process CPU before setup and rejects a median above
ten percent of logical CPU capacity. The TPC-H gate samples again after its
working database is ready, and the combined guard rechecks before each generic
measurement. On macOS, sustained `syspolicyd` or XProtect activity is rejected
independently because one busy core can distort a single-thread benchmark even
when machine-wide utilization looks low. Admission retries a bounded series of
cheap samples so short setup-triggered scans can settle, but no timed workload
starts while the host is contaminated. Every timing-bearing run also requires
an immediate clean post-sample; load that begins mid-run invalidates the timing
artifact instead of being normalized away. This prevents unrelated work from
producing misleading raw-runtime failures.

The combined guard forwards baseline configuration unchanged. The TPC-H gate
alone resolves and validates the accepted baseline contract.

A failed five-pair candidate remains failed. Diagnose it from the original
artifact and profile the affected workload separately; neither retries nor
normalization may change the gate verdict.

## Interpreting a result

Correctness, compilation, execution proof, and timing are independent:

- result differences or compile errors always fail;
- a CBO-selected accelerated query needs execution proof or an explicit
  failure receipt;
- raw JIT-auto runtime ceilings are independent of `jit_policy=off` timing;
- normalized and paired ratios help analyze noise but cannot excuse a raw
  regression;
- a verified improvement must tighten its checked-in floor or promote its
  accepted baseline in the same change.

Benchmark scratch output is disposable. Accepted baselines are the only timing
artifacts with durable regression meaning.
