# LuaJIT Expression Benchmarking and Profiling

## 1. Introduction

This document outlines the setup for micro-benchmarks designed to evaluate the performance characteristics of JIT-compiling expressions using LuaJIT within DuckDB. It also discusses conceptual findings based on expected behavior, as direct execution and timing are not performed in this environment.

The benchmarks aim to measure and compare:
- **C++ Baseline Time:** Time taken by DuckDB's standard `ExpressionExecutor` (with JIT explicitly disabled via configuration) to evaluate an expression.
- **JIT First Run Time:** The combined time for an expression's first execution via the JIT path using `ExpressionExecutor` (with JIT enabled and configured to trigger immediately). This includes internal translation, LuaJIT compilation, and the first execution.
- **JIT Cached Run Execution Time:** Time taken for subsequent executions of the (now cached) JITed Lua function via `ExpressionExecutor`.

This breakdown helps understand the overheads and potential speedups of the JIT approach using a more realistic execution path.

## 2. Benchmark Setup

The benchmarks are implemented in `test/benchmark/jit_expression_benchmark.cpp`.

- **Environment:** DuckDB environment, using `ClientContext` and two `ExpressionExecutor` instances per scenario: one for the C++ baseline (JIT disabled) and one for the JIT path.
- **Timing:** `std::chrono::high_resolution_clock` is used.
- **Expressions:** Actual `duckdb::BoundExpression` objects are created for benchmarking using helper functions.
- **Data:** Input data (numeric and string) is prepared in `duckdb::DataChunk` using `duckdb::Vector`s. Data generation helpers create varied data with optional NULLs.
- **JIT Control:** JIT behavior is controlled by setting `ClientConfig` options:
    - For C++ baseline: `ctx_cpp->config.enable_luajit_jit = false;`
    - For JIT path: `ctx_jit->config.enable_luajit_jit = true; ctx_jit->config.luajit_jit_trigger_count = 0; ctx_jit->config.luajit_jit_complexity_threshold = 0;` (to ensure JIT on first execution).
- **Iterations:** Execution times are averaged over multiple runs. The "First Run" JIT time inherently includes one-time translation and compilation.

## 3. Benchmark Scenarios Implemented

The `RunScenario` function in the benchmark code sets up and executes the following for each defined expression:

1.  **C++ Baseline Path:**
    *   An `ExpressionExecutor` (`cpp_executor`) is created with a `ClientContext` where JIT is disabled.
    *   The `BoundExpression` is added, input `DataChunk` is set.
    *   `cpp_executor.ExecuteExpression()` is called multiple times, and the average execution time is recorded.

2.  **LuaJIT Path:**
    *   A separate `ExpressionExecutor` (`jit_executor`) is created with a `ClientContext` configured to enable JIT and trigger it immediately.
    *   The same `BoundExpression` structure (a new copy) is added.
    *   **First Run (Compile + Exec):**
        *   The first call to `jit_executor.ExecuteExpression()` is timed. This time includes the internal call to `LuaTranslator`, LuaJIT compilation (via `LuaJITStateWrapper::CompileStringAndSetGlobal`), and the first execution of the JITed function (via `LuaJITStateWrapper::PCallGlobal` using `FFIVector`s prepared by `CreateFFIVectorFromDuckDBVector`). This is `JIT_FirstRun_ms`.
        *   The `ExpressionState` is checked to ensure JIT compilation was attempted and succeeded.
    *   **Subsequent Runs (Cached):**
        *   `jit_executor.ExecuteExpression()` is called multiple additional times. The average time for these subsequent calls is recorded as `JIT_CachedExec_ms`. These runs use the cached Lua function.
    *   *Note: Separate `TranslationTime_ms` and `CompilationTime_ms` are now part of the `JIT_FirstRun_ms` and marked as -1 in the output table, as they are internal to `ExpressionExecutor::Execute`.*

**Implemented Scenarios (all run with 0% and ~50% NULLs):**

*   **A_AddInt:** `col_int1 + col_int2`.
*   **C_MulConstInt:** `col_int1 * 10`. (Scenario B_GtInt was similar to A_AddInt in terms of numeric operations, so not explicitly repeated here but covered by principle).
*   **D_StrEq:** `col_str1 == col_str2` (VARCHAR inputs, boolean output).
*   **E_LowerStr:** `LOWER(col_str1)` (VARCHAR input, VARCHAR output - tests string FFI output helpers).
*   **F_IsNotNullInt:** `col_int1 IS NOT NULL` (Integer input, boolean output).
*   **G_CaseInt:** `CASE WHEN col_int1 > 10 THEN col_int2 ELSE col_int1 * 2 END` (Integer inputs/output).

## 4. Conceptual Benchmark Results (Illustrative Table Structure)

Actual timing data requires execution. The benchmark code outputs data in the following format. Values shown are illustrative placeholders.

| Scenario      | DataType | DataSize | NullPct | CppBaseline_ms | JIT_FirstRun_ms | JIT_CachedExec_ms | TranslateOnce_ms | CompileOnce_ms |
|---------------|----------|----------|---------|----------------|-----------------|-------------------|------------------|----------------|
| A_AddInt      | INTEGER  | 10000    | 0.0     | 0.05           | 0.50            | 0.15              | -1.0             | -1.0           |
| A_AddInt_Nulls| INTEGER  | 10000    | 0.5     | 0.06           | 0.55            | 0.18              | -1.0             | -1.0           |
| A_AddInt      | INTEGER  | 1000000  | 0.0     | 5.00           | 15.00           | 8.00              | -1.0             | -1.0           |
| D_StrEq       | VARCHAR  | 10000    | 0.0     | 0.20           | 1.50            | 0.80              | -1.0             | -1.0           |
| D_StrEq_Nulls | VARCHAR  | 10000    | 0.5     | 0.22           | 1.60            | 0.85              | -1.0             | -1.0           |
| E_LowerStr    | VARCHAR  | 10000    | 0.0     | 0.30           | 2.00            | 1.20              | -1.0             | -1.0           |
| G_CaseInt     | INTEGER  | 10000    | 0.0     | 0.10           | 0.80            | 0.30              | -1.0             | -1.0           |

*(TranslateOnce_ms and CompileOnce_ms are -1.0 as they are now internal to JIT_FirstRun_ms).*

## 5. Analysis and Observations (Conceptual, based on new structure)

*   **JIT Overheads (`JIT_FirstRun_ms`):** This metric now represents the "cold start" cost for a JITable expression. It includes translation, LuaJIT compilation, initial FFI setup, and the first execution. This value will always be significantly higher than `JIT_CachedExec_ms` and likely higher than `CppBaseline_ms` for many expressions.
*   **Cached JIT Execution (`JIT_CachedExec_ms`) vs. C++ Baseline (`CppBaseline_ms`):** This is the most important comparison for understanding steady-state performance.
    *   **Simple Numeric Ops:** For operations like `col_int1 + col_int2`, `JIT_CachedExec_ms` may still struggle to outperform `CppBaseline_ms`. DuckDB's vectorized C++ executor is highly optimized for these. The FFI overhead for each row (accessing data and nullmasks via pointers from Lua) can be significant in aggregate compared to tight C++ loops.
    *   **String Operations:**
        *   Comparisons (`D_StrEq`): Performance will depend on the efficiency of `ffi.string()` creation in Lua and Lua's string comparison relative to DuckDB's native string comparison. It could be competitive.
        *   Functions producing strings (`E_LowerStr`): This now tests the FFI C helper functions for writing string results. The overhead of calling back into C for each output string (`duckdb_ffi_add_string_to_output_vector`) plus Lua string manipulation might make this slower than a fully native C++ implementation.
    *   **Conditional Logic (`G_CaseInt`):** JIT compilation *might* offer benefits here if the translated Lua code results in fewer branches or more predictable execution for the CPU compared to a generic C++ CASE expression evaluation, especially if the conditions are complex. However, FFI overhead is still a factor.
    *   **`IS NOT NULL` (`F_IsNotNullInt`):** This is a very simple check. JIT is unlikely to be faster than the direct C++ nullmask access.
*   **Amortization:** The JIT approach becomes viable if:
    1.  `JIT_CachedExec_ms` is substantially lower than `CppBaseline_ms`.
    2.  The expression is executed over enough total rows (across multiple chunks, reusing the compiled `ExpressionState`) to amortize the `JIT_FirstRun_ms` overhead. The `luajit_jit_trigger_count` setting directly influences this by delaying JIT until a certain number of rows have already been processed by the (presumably faster for initial runs) C++ path.
*   **Impact of `CreateFFIVectorFromDuckDBVector`:** The cost of this function (preparing `FFIVector`s, especially flattening constant/dictionary vectors and creating boolean nullmasks) is incurred on *every* execution of the JITed function (as part of the `PCallGlobal` setup within the conceptual `ExpressionExecutor::Execute` JIT path). This per-chunk setup cost is part of the measured JIT execution times.
*   **Heuristics (`luajit_jit_complexity_threshold`, `luajit_jit_trigger_count`):**
    *   These settings are crucial. If `JIT_CachedExec_ms` is often not much better than `CppBaseline_ms`, then `luajit_jit_trigger_count` needs to be high enough so that JIT is only used for very hot expressions.
    *   Similarly, `luajit_jit_complexity_threshold` should ensure that only expressions complex enough to potentially benefit from JIT (and overcome FFI overheads) are considered. The current `GetExpressionComplexity` is basic; a more sophisticated cost model might be needed.

## 6. Profiling (Conceptual)

(Content remains largely the same: use `perf`, LuaJIT profiler to find hotspots in FFI, Lua code, or data prep.)

## 7. Conclusion

The refactored benchmark framework provides a more realistic comparison by using `ExpressionExecutor` for both C++ baseline (with JIT disabled via config) and JIT paths. This allows direct measurement of "cold start" JIT overhead versus "warm" cached JIT execution.
Conceptual results suggest that for simple expressions, DuckDB's native C++ execution is likely to be faster than or comparable to even cached JIT execution due to FFI overheads. The JIT approach holds more promise for very complex expressions or functions where the work done per row *within Lua* is substantial, and where the compilation overhead can be amortized over many rows or repeated executions. The configuration options for enabling JIT and setting thresholds are critical for practical application, ensuring JIT is only attempted where it's most likely to yield benefits. String output handling via FFI C helpers adds overhead but enables more complex string functions.
