# JIT Broad Query Plan

Last updated: 2026-07-07

This file is the TPC-H milestone pointer. The generic production architecture,
contracts, and refactor gates live in
`benchmark/jit/JIT_PRODUCTION_RECIPE_DESIGN.md`; do not duplicate them here.

## Current Milestone

Slim the JIT implementation by deleting stale code, stale docs, and obsolete
verification scaffolding without weakening the production recipe architecture or
masking performance regressions.

The active work is:

- keep recipe construction as the admission boundary
- keep CBO facts and runtime counters contract-locked
- delete historical route logs and duplicate design notes
- delete dead variant branches only after descriptor-level tests cover the
  replacement route
- preserve the refactor guard and TPC-H regression gate as the automatic safety
  net

## Non-Negotiable Direction

JIT wins by deleting real execution work:

- hash-join output materialization
- projection-source chunks that only feed another fused stage
- copied post-join projection batches
- aggregate state-address vectors and address buffers
- repeated hash-table probes while a resolved target is still live
- repeated selection, validity, and cast work after runtime facts prove the
  shape
- small per-row update calls that destroy vector-sized batching

CBO tuning must not hide backend ownership gaps. If a selected region regresses,
optimize the backend/runtime path or fix the architecture contract.

## Verification Required Before Claiming Progress

For deletion-only JIT refactors:

```bash
python3 benchmark/jit/verify_jit_architecture.py
cmake --build build/reldebug --config RelWithDebInfo -j12
python3 benchmark/jit/run_jit_refactor_guard.py --level unit --no-build --skip-architecture --skip-py-compile
```

For runtime/planner/backend changes:

```bash
python3 benchmark/tpch/jit/run_tpch_regression_gate.py --baseline <accepted-baseline> --no-build
```

If the gate fails, keep the direction only when the architecture is correct and
continue with root-cause backend optimization. Do not loosen or tighten CBO as a
cover for a runtime ownership problem.
