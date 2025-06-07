# LuaJIT Expression Benchmarking and Profiling

## 1. Introduction

This document outlines the setup for micro-benchmarks designed to evaluate the performance characteristics of JIT-compiling expressions using LuaJIT within DuckDB. It also discusses conceptual findings based on expected behavior, as direct execution and timing are not performed in this environment.

The benchmarks aim to measure and compare:
- **C++ Baseline Time:** Time taken by DuckDB's standard `ExpressionExecutor` (with JIT conceptually disabled or using the standard C++ path) to evaluate an expression.
- **JIT First Run Total Time:** The combined time for an expression's first execution via the JIT path. This includes:
    - **Translation Time:** Converting the `BoundExpression` to Lua row logic.
    - **Compilation Time:** LuaJIT compiling the full Lua function string.
    - **Execution Time:** The first execution of the JITed Lua function.
- **JIT Cached Run Execution Time:** Time taken for subsequent executions of the already compiled JITed Lua function.

This breakdown helps understand the overheads and potential speedups of the JIT approach.

## 2. Benchmark Setup

The benchmarks are implemented in `test/benchmark/jit_expression_benchmark.cpp`.

- **Environment:** DuckDB environment, using `ClientContext` and `ExpressionExecutor`.
- **Timing:** `std::chrono::high_resolution_clock` is used.
- **Expressions:** Actual `duckdb::BoundExpression` objects are created for benchmarking.
- **Data:** Input data (numeric and string) is prepared in `duckdb::DataChunk` using `duckdb::Vector`s. `FFIVector` and `FFIString` structs bridge this data to LuaJIT (zero-copy for input reads) via `CreateFFIVectorFromDuckDBVector`.
- **JIT Control:** A conceptual global flag (`g_force_jit_path`) is assumed to control whether `ExpressionExecutor::ShouldJIT` allows JIT attempts. For C++ baseline measurements, this flag would be false. For JIT measurements, it would be true. (This is a PoC simplification for controlling execution paths).
- **Iterations:** Execution times are averaged over multiple runs. Translation and compilation for JIT are measured once during the first JIT run.

## 3. Benchmark Scenarios Implemented

The `RunScenario` function in the benchmark code executes the following for each defined expression:

1.  **C++ Baseline Path (Conceptual / Simplified):**
    *   For `A_AddInt (col1 + col2)`, a manual C++ loop is implemented directly in the benchmark for a clear baseline without `ExpressionExecutor` overheads or internal JIT path selections.
    *   For other scenarios, a true baseline using `ExpressionExecutor` with JIT forced off would be ideal but is complex to set up perfectly in this PoC stage. These are marked with `-1.0` or use the manual loop if simple enough.

2.  **LuaJIT Path:**
    *   Uses an `ExpressionExecutor` instance.
    *   The `BoundExpression` is added to the executor.
    *   **First Run:**
        *   `LuaTranslator::TranslateExpressionToLuaRowLogic` is called (Translation Time measured).
        *   A unique Lua function name is generated.
        *   `ConstructFullLuaFunctionScript` (a test helper) creates the full Lua code.
        *   `LuaJITStateWrapper::CompileStringAndSetGlobal` compiles the function (Compilation Time measured).
        *   The JITed function is executed via `LuaJITStateWrapper::PCallGlobal` with FFI arguments prepared by `CreateFFIVectorFromDuckDBVector` (Execution time for this first call is measured).
        *   `JIT_FirstRun_TotalTime_ms` = Translation + Compilation + First Execution.
    *   **Subsequent Runs (Cached):**
        *   The same compiled Lua function is called again via `PCallGlobal` multiple times.
        *   `JIT_CachedRun_ExecTime_ms` is the average execution time of these subsequent calls.

**Implemented Scenarios:**

*   **A_AddInt:** `col_int1 + col_int2` (with and without nulls).
*   **B_GtInt:** `col_int1 > col_int2` (boolean output).
*   **C_MulConstInt:** `col_int1 * 10`.
*   **D_StrEq:** `col_str1 == col_str2` (VARCHAR inputs, boolean output).

## 4. Conceptual Benchmark Results (Illustrative Table Structure)

Actual timing data requires execution. The benchmark code is structured to output data in the following format. Values shown are illustrative placeholders.

| Scenario        | DataType | DataSize | HasNulls | CppBaseline_ms | JIT_FirstRunTotal_ms | JIT_CachedExec_ms | Translate_ms | Compile_ms |
|-----------------|----------|----------|----------|----------------|----------------------|-------------------|--------------|------------|
| A_AddInt        | INTEGER  | 1000     | N        | 0.02           | 0.20                 | 0.08              | 0.01         | 0.05       |
| A_AddInt        | INTEGER  | 100000   | N        | 2.00           | 7.00                 | 4.50              | 0.01         | 0.05       |
| A_AddInt_Nulls  | INTEGER  | 100000   | Y        | 2.20           | 7.50                 | 4.80              | 0.01         | 0.05       |
| B_GtInt         | INTEGER  | 100000   | N        | -1.0           | 7.20                 | 4.60              | 0.01         | 0.06       |
| C_MulConstInt   | INTEGER  | 100000   | N        | -1.0           | 6.80                 | 4.20              | 0.01         | 0.04       |
| D_StrEq         | VARCHAR  | 1000     | N        | -1.0           | 0.80                 | 0.50              | 0.02         | 0.10       |
| D_StrEq         | VARCHAR  | 100000   | N        | -1.0           | 55.00                | 40.00             | 0.02         | 0.10       |
| D_StrEq_Nulls   | VARCHAR  | 100000   | Y        | -1.0           | 60.00                | 43.00             | 0.02         | 0.10       |

*(Note: CppBaseline_ms is -1.0 where a manual C++ loop for that specific BoundExpression was not implemented in the benchmark code provided for this PoC step; a full benchmark would require these or a reliable way to force ExpressionExecutor's C++ path).*

## 5. Analysis and Observations (Conceptual, based on new structure)

*   **JIT Overheads (Translation + Compilation):**
    *   `Translate_ms` (converting `BoundExpression` to Lua row logic) is expected to be very small, likely in microseconds or low single-digit milliseconds, even for moderately complex expressions.
    *   `Compile_ms` (LuaJIT compiling the full Lua function string) is also expected to be very fast, likely sub-millisecond to a few milliseconds for typical expressions. LuaJIT is designed for rapid compilation.
    *   The `JIT_FirstRun_TotalTime_ms` captures these overheads plus one execution. This will always be higher than `JIT_CachedExec_ms`.

*   **Cached JIT Execution vs. C++ Baseline:**
    *   The key comparison is `JIT_CachedExec_ms` vs. `CppBaseline_ms`.
    *   **Simple Numeric Ops (e.g., A_AddInt):** For very simple, per-element arithmetic like `col1 + col2`, the `JIT_CachedExec_ms` might still be slower than, or at best comparable to, a highly optimized C++ loop (like the manual one for `A_AddInt`). This is because the FFI call overhead for each element access from Lua (`input1_data[i]`, `input1_nullmask[i]`, etc.) can outweigh the benefit of JIT for trivial operations. DuckDB's C++ execution path is already heavily vectorized and optimized.
    *   **String Comparisons (e.g., D_StrEq):** String operations involve more work per element (pointer dereferencing, length checks, character-by-character comparison). LuaJIT's FFI for `FFIString` (accessing `ptr` and `len`, then `ffi.string` to create Lua strings) and its string comparison speed are good. It's possible that `JIT_CachedExec_ms` could become competitive with or even outperform the C++ baseline if the C++ path has its own overheads (e.g., function call dispatch per row for comparisons if not perfectly inlined/optimized). However, creating Lua strings from FFI data still has some cost.
    *   **Expressions with Constants (e.g., C_MulConstInt):** Constants are embedded directly into the Lua code by the translator. This is efficient. Performance characteristics would be similar to other simple numeric ops, largely depending on FFI overhead vs. C++ vector processing.

*   **Amortization of Overhead:**
    *   The JIT approach is beneficial if the (Translation + Compilation) overhead can be amortized over many executions of the same compiled expression (i.g., in a hot loop of a query processing pipeline for many data chunks) or over a very large number of rows where `JIT_CachedExec_ms` is significantly faster than `CppBaseline_ms`.
    *   If `JIT_CachedExec_ms` is not substantially faster than `CppBaseline_ms`, then JIT might only be worthwhile for expressions that are executed extremely frequently.

*   **Impact of `CreateFFIVectorFromDuckDBVector`:**
    *   This function now handles `FLAT_VECTOR`, `CONSTANT_VECTOR`, and `DICTIONARY_VECTOR` (by flattening) for inputs, and also prepares `FFIString` arrays for `VARCHAR` vectors.
    *   The creation of temporary buffers for constant/dictionary vectors or for the flat `bool*` nullmask *adds overhead* to the JIT path (specifically, before `PCallGlobal`). This overhead is currently *not* separately measured in the benchmark structure but is part of the setup for the JIT execution step. In a real scenario, this data preparation cost also needs to be considered.

*   **Limitations in Current Benchmark:**
    *   **C++ Baseline:** The C++ baseline is either a manual loop (for `A_AddInt`) or not fully implemented using `ExpressionExecutor` with JIT reliably turned off. A true comparison requires running the *same* `ExpressionExecutor` evaluation path, one with JIT and one without.
    *   **String Output:** Operations that produce new strings (e.g., `CONCAT`) and need to write them back to an output `FFIVector` of `FFIString` are not benchmarked for execution due to the complexity of memory management for string results via FFI (requiring C helper callbacks).
    *   **`ExpressionExecutor::Execute` JIT Path:** The benchmark currently bypasses the JIT decision and call logic within `ExpressionExecutor::Execute` and calls the compiled Lua function directly (after manually triggering compilation via `LuaTranslator` and `LuaJITStateWrapper`). A full test of the integrated `Execute` path (with its `ShouldJIT`, caching, and FFI call setup) would be the next step for realism.

## 6. Profiling (Conceptual)

As before, actual profiling tools (`perf`, LuaJIT's `-jp` profiler) would be essential to understand where time is spent within the JITed Lua code, FFI calls, and the C++ parts of the `ExpressionExecutor` when the JIT path is active. This would help identify precise bottlenecks (e.g., specific FFI casts, Lua operations, or data copying in `CreateFFIVectorFromDuckDBVector`).

## 7. Conclusion

Refactoring the benchmark to use `BoundExpression`s and a more structured approach to measuring translation, compilation, and execution times provides a better framework for evaluating the LuaJIT PoC. The key performance question remains whether the `JIT_CachedExec_ms` can overcome both the initial `JIT_FirstRun_TotalTime_ms` overhead and be significantly faster than DuckDB's highly optimized C++ execution path for a given expression and data volume. Simple expressions are less likely to show a benefit, while more complex ones (not yet benchmarked) hold more promise. The efficiency of `CreateFFIVectorFromDuckDBVector` is also a factor for input data preparation.
