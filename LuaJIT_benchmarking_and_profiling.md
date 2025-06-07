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
- **Data:** Input data (numeric, string, temporal) is prepared in `duckdb::DataChunk` using `duckdb::Vector`s. Data generation helpers create varied data with optional NULLs.
- **JIT Control:** JIT behavior is controlled by setting `ClientConfig` options:
    - For C++ baseline: `ctx_cpp->config.enable_luajit_jit = false;`
    - For JIT path: `ctx_jit->config.enable_luajit_jit = true; ctx_jit->config.luajit_jit_trigger_count = 0; ctx_jit->config.luajit_jit_complexity_threshold = 0;` (to ensure JIT on first execution for benchmark purposes).
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
    *   *Note: Separate `TranslationTime_ms` and `CompilationTime_ms` are marked as -1 in the output table, as they are now internal to `ExpressionExecutor::Execute`'s first JIT run.*

**Implemented Scenarios (all run with 0% and ~50% NULLs, for DataSize 10k and 1M):**

*   **A_AddInt:** `col_int1 + col_int2`.
*   **C_MulConstInt:** `col_int1 * 10`.
*   **D_StrEq:** `col_str1 == col_str2` (VARCHAR inputs, boolean output).
*   **E_LowerStr:** `LOWER(col_str1)` (VARCHAR input, VARCHAR output).
*   **F_IsNotNullInt:** `col_int1 IS NOT NULL` (Integer input, boolean output).
*   **G_CaseInt:** `CASE WHEN col_int1 > 10 THEN col_int2 ELSE col_int1 * 2 END`.
*   **H_AbsInt:** `ABS(col_int1)`.
*   **I_RoundDbl:** `ROUND(col_dbl1, 2)`.
*   **J_ConcatStr:** `col_str1 || col_str2`.
*   **K_LengthStr:** `LENGTH(col_str1)`.
*   **L_ReplaceStr:** `REPLACE(col_str1, 's_', 'S_')`.
*   **M_LpadStr:** `LPAD(col_str1, 15, '#')`.
*   **N_TrimStr:** `TRIM(col_str1)`.
*   **O_EqDate:** `col_date1 == col_date2`.
*   **P_ExtractYearDate:** `EXTRACT(YEAR FROM col_date1)`.
*   **Q_ExtractHourTs:** `EXTRACT(HOUR FROM col_ts1)`.

## 4. Conceptual Benchmark Results (Illustrative)

| Scenario             | DataType    | DataSize | NullPct | CppBaseline_ms | JIT_FirstRun_ms | JIT_CachedExec_ms | TranslateOnce_ms | CompileOnce_ms |
|----------------------|-------------|----------|---------|----------------|-----------------|-------------------|------------------|----------------|
| A_AddInt             | INTEGER     | 10000    | 0.0     | 0.05           | 0.60            | 0.12              | -1.0             | -1.0           |
| A_AddInt_Nulls       | INTEGER     | 10000    | 0.5     | 0.06           | 0.65            | 0.15              | -1.0             | -1.0           |
| A_AddInt             | INTEGER     | 1000000  | 0.0     | 5.00           | 18.00           | 7.50              | -1.0             | -1.0           |
| A_AddInt_Nulls       | INTEGER     | 1000000  | 0.5     | 6.00           | 20.00           | 9.00              | -1.0             | -1.0           |
| C_MulConstInt        | INTEGER     | 1000000  | 0.0     | 4.50           | 17.00           | 7.00              | -1.0             | -1.0           |
| D_StrEq              | VARCHAR     | 10000    | 0.0     | 0.20           | 1.80            | 0.90              | -1.0             | -1.0           |
| D_StrEq_Nulls        | VARCHAR     | 10000    | 0.5     | 0.22           | 1.90            | 1.00              | -1.0             | -1.0           |
| D_StrEq              | VARCHAR     | 1000000  | 0.0     | 20.00          | 50.00           | 35.00             | -1.0             | -1.0           |
| E_LowerStr           | VARCHAR     | 1000000  | 0.0     | 25.00          | 60.00           | 45.00             | -1.0             | -1.0           |
| F_IsNotNullInt       | INTEGER     | 1000000  | 0.5     | 3.00           | 16.00           | 6.00              | -1.0             | -1.0           |
| G_CaseInt            | INTEGER     | 1000000  | 0.0     | 8.00           | 25.00           | 10.00             | -1.0             | -1.0           |
| H_AbsInt             | INTEGER     | 1000000  | 0.0     | 4.00           | 17.00           | 6.50              | -1.0             | -1.0           |
| I_RoundDbl           | DOUBLE      | 1000000  | 0.0     | 7.00           | 22.00           | 9.00              | -1.0             | -1.0           |
| J_ConcatStr          | VARCHAR     | 1000000  | 0.0     | 22.00          | 55.00           | 40.00             | -1.0             | -1.0           |
| K_LengthStr          | VARCHAR     | 1000000  | 0.0     | 18.00          | 45.00           | 30.00             | -1.0             | -1.0           |
| L_ReplaceStr         | VARCHAR     | 1000000  | 0.0     | 30.00          | 70.00           | 55.00             | -1.0             | -1.0           |
| M_LpadStr            | VARCHAR     | 1000000  | 0.0     | 35.00          | 80.00           | 65.00             | -1.0             | -1.0           |
| N_TrimStr            | VARCHAR     | 1000000  | 0.0     | 28.00          | 65.00           | 50.00             | -1.0             | -1.0           |
| O_EqDate             | DATE        | 1000000  | 0.0     | 5.50           | 19.00           | 8.50              | -1.0             | -1.0           |
| P_ExtractYearDate    | DATE        | 1000000  | 0.0     | 6.00           | 20.00           | 9.50              | -1.0             | -1.0           |
| Q_ExtractHourTs      | TIMESTAMP   | 1000000  | 0.0     | 6.50           | 21.00           | 10.00             | -1.0             | -1.0           |

*(TranslateOnce_ms and CompileOnce_ms are -1.0 as they are now internal to JIT_FirstRun_ms).*

## 5. Analysis and Observations (Conceptual)

This analysis is based on the *conceptual* benchmark results presented above.

*   **Overhead Analysis:**
    *   The `JIT_FirstRun_ms` consistently shows a significant overhead compared to both `CppBaseline_ms` and `JIT_CachedExec_ms`. This is expected, as it includes the one-time costs of expression translation to Lua, LuaJIT compilation of the generated Lua code into bytecode, and the initial setup for the first execution.
    *   For example, in `A_AddInt` (1M rows), C++ is 5.00ms, JIT First Run is 18.00ms, but Cached JIT is 7.50ms. The ~13ms difference between JIT First Run and C++ baseline (or ~10.5ms vs cached JIT) represents the initial JIT overhead for this scenario.
    *   This overhead underscores the importance of the `luajit_jit_trigger_count` heuristic: JIT compilation should only be attempted for expressions that are executed frequently enough to amortize this initial cost.

*   **Performance Comparison (Cached JIT vs. C++):**
    *   **Numeric Operations (A_AddInt, C_MulConstInt, H_AbsInt, I_RoundDbl):**
        *   Cached JIT execution times (`JIT_CachedExec_ms`) are conceptually shown as slightly slower than or, at best, comparable to the C++ baseline for simple arithmetic. For instance, `A_AddInt` (1M rows) shows C++ at 5.00ms and Cached JIT at 7.50ms. `H_AbsInt` (1M rows) shows C++ at 4.00ms and Cached JIT at 6.50ms.
        *   This suggests that for basic arithmetic, DuckDB's highly optimized vectorized C++ engine is hard to beat, and the per-row FFI call overhead (accessing data and nullmasks from Lua) might outweigh the benefits of JITed Lua code.
        *   More complex math functions like `ROUND` (especially if involving multiple steps in Lua) might show slightly more competitive cached JIT times, but still likely impacted by FFI.
    *   **String Operations (D_StrEq, E_LowerStr, J_ConcatStr, K_LengthStr, L_ReplaceStr, M_LpadStr, N_TrimStr):**
        *   String operations generally exhibit higher FFI overhead due to the need to convert between DuckDB `string_t` and Lua strings (potentially involving data copying for `ffi.string()`) and calling back into C for output string allocation (`duckdb_ffi_add_string_to_output_vector`).
        *   `D_StrEq` (VARCHAR equality): Cached JIT (35.00ms for 1M rows) is slower than C++ (20.00ms).
        *   `E_LowerStr` (LOWER): Cached JIT (45.00ms) is significantly slower than C++ (25.00ms). This is plausible as each string needs to be passed to Lua, processed, and then the result passed back to C++ to be written into the output vector.
        *   `J_ConcatStr` (`||`): Similar to other string ops, FFI overhead likely makes it slower.
        *   `K_LengthStr` (`LENGTH`): Relatively simple, but still involves FFI for string input. C++ (18.00ms) vs JIT Cached (30.00ms).
        *   `L_ReplaceStr`, `M_LpadStr`, `N_TrimStr`: These more complex string functions also show JIT being slower. The Lua implementations, while functional, might not be as optimized as potential C++ library functions, and FFI overhead for each argument and result dominates.
    *   **Temporal Operations (O_EqDate, P_ExtractYearDate, Q_ExtractHourTs):**
        *   `O_EqDate` (DATE equality): Dates are represented as integers, so performance is similar to integer comparisons. C++ (5.50ms) vs JIT Cached (8.50ms).
        *   `P_ExtractYearDate`, `Q_ExtractHourTs` (`EXTRACT`): These involve FFI calls to C helper functions (`duckdb_ffi_extract_from_date`, etc.). While the core extraction logic is in C++, each row involves a Lua-to-C call. C++ (6.00-6.50ms) vs JIT Cached (9.50-10.00ms). The FFI call overhead per row is likely the dominant factor here, making JIT slower.
    *   **Logical Operations (F_IsNotNullInt):**
        *   This is a very simple operation. C++ (3.00ms) vs JIT Cached (6.00ms). The JIT path involves accessing the nullmask through FFI, which is less direct than the C++ path.
    *   **Conditional Operations (G_CaseInt):**
        *   The `CASE` expression shows JIT Cached (10.00ms) being slower than C++ (8.00ms). While JIT can handle branches well, the FFI overhead for accessing input values for conditions and results likely still makes it less competitive for this type of CASE statement with simple conditions/results.

*   **Impact of Factors:**
    *   **Expression Complexity:**
        *   The conceptual data suggests that even for moderately complex expressions like `G_CaseInt`, the current JIT approach (with per-row FFI calls) does not outperform C++. This implies the `luajit_jit_complexity_threshold` would need to be set quite high, or target expressions where the *internal Lua computation* is very extensive, which is not the case for most SQL functions that map to simple Lua operations or FFI calls.
    *   **Data Size:**
        *   Larger data sizes help amortize the `JIT_FirstRun_ms` overhead if `JIT_CachedExec_ms` is faster than `CppBaseline_ms`. However, in our conceptual data, `JIT_CachedExec_ms` is generally not faster. So, larger data sizes primarily make the *absolute* time difference of the first run less impactful on total query time if the expression is part of a much larger query, but they don't make the JIT itself faster than C++ per row.
    *   **Data Types (FFI Overhead):**
        *   `VARCHAR`: High FFI overhead is evident. Creating Lua strings from `FFIString` (potentially copying) and calling C helpers for output strings are costly per row.
        *   `DATE`, `TIMESTAMP`: Handled as integers/bigints, so FFI overhead is similar to numeric types.
        *   `INTERVAL`: Handled as `FFIInterval` structs. Accessing fields (`.months`, `.days`, `.micros`) in Lua from this struct is efficient. However, if intervals were frequently passed to/from C FFI helper functions as full structs, this could also add overhead compared to direct C++ struct manipulation.
    *   **Null Percentage:**
        *   The conceptual data shows a slight increase in execution time for both C++ and JIT paths when 50% nulls are introduced. This is expected due to conditional logic to handle nulls (checking nullmasks). The relative performance difference between C++ and JIT doesn't seem to change dramatically due to nulls in this conceptual model; both paths need to handle them.

*   **Heuristic Evaluation:**
    *   Based on these *conceptual* numbers, the default JIT heuristic thresholds (`luajit_jit_complexity_threshold = 5`, `luajit_jit_trigger_count = 1000`) might be too optimistic.
    *   If cached JIT is rarely faster than C++, then `luajit_jit_trigger_count` should probably be very high (or JIT disabled by default) to avoid unnecessary JIT attempts that slow down queries.
    *   The `luajit_jit_complexity_threshold` might also need to be higher, targeting only expressions where the internal Lua computation (not FFI calls) is extremely significant. The current complexity measure is also very basic.

*   **Bottleneck Identification (Hypothetical):**
    *   **Per-row FFI calls:** Accessing `data` and `nullmask` pointers from input `FFIVector`s for each row inside the Lua loop is a primary suspect for overhead.
    *   **FFI calls for results:** For `VARCHAR` output, `duckdb_ffi_add_string_to_output_vector` is called per row. For `EXTRACT`, the FFI helper is called per row. These C transitions are expensive.
    *   **Data marshalling for FFIString:** Converting between `string_t` and Lua strings.
    *   **`CreateFFIVectorFromDuckDBVector`:** The cost of preparing `FFIVector`s for each chunk (flattening, creating boolean nullmasks) adds to the per-chunk overhead for JIT execution.

*   **Conclusion on PoC Performance:**
    *   The conceptual benchmark analysis suggests that the current LuaJIT PoC, while functional, would likely not provide significant performance benefits over DuckDB's native C++ vectorized execution for most common SQL expressions and functions.
    *   The overhead associated with FFI (data transfer, function calls between C++ and Lua) on a per-row basis appears to be a major limiting factor.
    *   JIT compilation might only be beneficial for extremely complex expressions where the computational work done *entirely within Lua* for each row is very high, and FFI interactions are minimized. This is rare for typical SQL expressions which often map to relatively simple operations or call out to specialized (C/C++) functions.
    *   The true strength of LuaJIT (e.g., its performance on pure Lua computation loops) is not fully leveraged when each step in the loop involves FFI calls.
    *   For this PoC to be practically faster, a different approach might be needed, perhaps one that processes entire batches of data within a single JITed function call (reducing per-row FFI overhead), or by JIT-compiling to a lower-level representation than Lua bytecode if targeting very simple expressions.

## 8. Future Work & Potential Optimizations (Conceptual)

(No changes needed here from previous version, as this section is about future ideas rather than current results).

*   Batch Processing in Lua
*   Advanced Lua Optimizations
*   Alternative JIT Targets
*   Improved Heuristics
*   Reduced FFI Overhead (e.g. shared memory, more complex FFI calls that operate on batches)
*   Specialized Lua Bytecode for Simple Expressions.
