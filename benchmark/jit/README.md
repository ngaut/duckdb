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
  benchmark/jit/test_generic_benchmark.py \
  benchmark/tpch/jit/test_compare_tpch_benchmark.py \
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
parallel workload classes. Workload definitions and their thread-specific
floors are the source of truth in `benchmark/jit/generic_benchmark.py`.

Candidate runs use five alternating repetitions:

```sh
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 1 --repeats 5
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 4 --repeats 5
```

Use ten repetitions only for deliberate promotion or ship qualification:

```sh
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 1 --repeats 10
python3 benchmark/jit/generic_benchmark.py \
  --duckdb build/reldebug/duckdb --threads 4 --repeats 10
```

Production samples disable runtime tracing and verification. A failed
five-repeat candidate is reported as-is; the runner does not schedule an
automatic larger retry.

Each speedup sample is the `off/auto` ratio from the same alternating repeat;
the gate uses the median paired ratio. The raw JIT-auto median is checked
independently, so pairing cannot hide an absolute runtime regression.

## TPC-H regression gate

The standalone gate builds, measures, verifies result artifacts, compares
against accepted raw-runtime and speedup contracts, and runs a separate traced
proof pass for every compiled or accelerated-runner-selected query.

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
