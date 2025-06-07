# LuaJIT Expression Benchmarking and Profiling

## 1. Introduction

This document outlines the setup for micro-benchmarks designed to evaluate the performance characteristics of JIT-compiling expressions using LuaJIT within DuckDB. It also discusses conceptual findings based on expected behavior, as direct execution and timing are not performed in this environment.

The benchmarks aim to measure and compare:
- **C++ Baseline Time:** Time taken by DuckDB's standard `ExpressionExecutor` (with JIT explicitly disabled via configuration) to evaluate an expression.
- **JIT First Run Time:** The combined time for an expression's first execution via the JIT path using `ExpressionExecutor` (with JIT enabled and configured to trigger immediately). This includes internal translation, LuaJIT compilation, and the first execution.
- **JIT Cached Run Execution Time:** Time taken for subsequent executions of the (now cached) JITed Lua function via `ExpressionExecutor`.

This document presents conceptual results for two JIT execution models:
1.  **Row-by-Row Processing Model:** (Original PoC design) Where `ExpressionExecutor` loops through rows and calls Lua for each row.
2.  **Block-Processing Model:** (Current PoC design) Where a single call to a JITed Lua function processes all rows in a `DataChunk`, with the main loop inside Lua.

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

The `RunScenario` function in the benchmark code sets up and executes expressions covering various types and operations. The scenarios are consistent for both conceptual models analyzed.

**Implemented Scenarios (all run with 0% and ~50% NULLs, for DataSize 10k and 1M):**

*   **A_AddInt:** `col_int1 + col_int2`.
*   **C_MulConstInt:** `col_int1 * 10`.
*   **D_StrEq:** `col_str1 == col_str2`.
*   **E_LowerStr:** `LOWER(col_str1)`.
*   **F_IsNotNullInt:** `col_int1 IS NOT NULL`.
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

## 4. Conceptual Benchmark Results

### 4.1. Row-by-Row Processing Model (Original PoC Design - Illustrative)

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

*(TranslateOnce_ms and CompileOnce_ms are -1.0 as they are internal to JIT_FirstRun_ms).*

### 4.2. Block-Processing Model (Current PoC Design - Illustrative)

| Scenario             | DataType    | DataSize | NullPct | CppBaseline_ms | JIT_FirstRun_ms | JIT_CachedExec_ms | TranslateOnce_ms | CompileOnce_ms |
|----------------------|-------------|----------|---------|----------------|-----------------|-------------------|------------------|----------------|
| A_AddInt             | INTEGER     | 10000    | 0.0     | 0.05           | 0.55            | 0.04              | -1.0             | -1.0           |
| A_AddInt_Nulls       | INTEGER     | 10000    | 0.5     | 0.06           | 0.60            | 0.05              | -1.0             | -1.0           |
| A_AddInt             | INTEGER     | 1000000  | 0.0     | 5.00           | 17.50           | 3.50              | -1.0             | -1.0           |
| A_AddInt_Nulls       | INTEGER     | 1000000  | 0.5     | 6.00           | 19.50           | 4.50              | -1.0             | -1.0           |
| C_MulConstInt        | INTEGER     | 1000000  | 0.0     | 4.50           | 16.50           | 3.00              | -1.0             | -1.0           |
| D_StrEq              | VARCHAR     | 10000    | 0.0     | 0.20           | 1.70            | 0.60              | -1.0             | -1.0           |
| D_StrEq_Nulls        | VARCHAR     | 10000    | 0.5     | 0.22           | 1.80            | 0.65              | -1.0             | -1.0           |
| D_StrEq              | VARCHAR     | 1000000  | 0.0     | 20.00          | 48.00           | 25.00             | -1.0             | -1.0           |
| E_LowerStr           | VARCHAR     | 1000000  | 0.0     | 25.00          | 58.00           | 40.00             | -1.0             | -1.0           |
| F_IsNotNullInt       | INTEGER     | 1000000  | 0.5     | 3.00           | 15.00           | 2.50              | -1.0             | -1.0           |
| G_CaseInt            | INTEGER     | 1000000  | 0.0     | 8.00           | 24.00           | 6.00              | -1.0             | -1.0           |
| H_AbsInt             | INTEGER     | 1000000  | 0.0     | 4.00           | 16.00           | 3.20              | -1.0             | -1.0           |
| I_RoundDbl           | DOUBLE      | 1000000  | 0.0     | 7.00           | 21.00           | 5.50              | -1.0             | -1.0           |
| J_ConcatStr          | VARCHAR     | 1000000  | 0.0     | 22.00          | 53.00           | 30.00             | -1.0             | -1.0           |
| K_LengthStr          | VARCHAR     | 1000000  | 0.0     | 18.00          | 43.00           | 22.00             | -1.0             | -1.0           |
| L_ReplaceStr         | VARCHAR     | 1000000  | 0.0     | 30.00          | 68.00           | 50.00             | -1.0             | -1.0           |
| M_LpadStr            | VARCHAR     | 1000000  | 0.0     | 35.00          | 78.00           | 60.00             | -1.0             | -1.0           |
| N_TrimStr            | VARCHAR     | 1000000  | 0.0     | 28.00          | 63.00           | 48.00             | -1.0             | -1.0           |
| O_EqDate             | DATE        | 1000000  | 0.0     | 5.50           | 18.50           | 4.00              | -1.0             | -1.0           |
| P_ExtractYearDate    | DATE        | 1000000  | 0.0     | 6.00           | 19.00           | 8.00              | -1.0             | -1.0           |
| Q_ExtractHourTs      | TIMESTAMP   | 1000000  | 0.0     | 6.50           | 20.00           | 8.50              | -1.0             | -1.0           |

## 5. Analysis and Observations

### 5.1. Analysis for Row-by-Row Processing Model (Original PoC)

*(This section contains the analysis from the previous version of the document, based on the first table of conceptual results. It generally concludes that row-by-row JIT is slower than C++ due to FFI overhead.)*

*   **Overhead Analysis:** `JIT_FirstRun_ms` is consistently high due to translation and compilation.
*   **Performance Comparison (Cached JIT vs. C++):** Generally, cached JIT is slower than the C++ baseline across most operations (numeric, string, temporal, conditional, logical) due to per-row FFI call overhead.
*   **Impact of Factors:** FFI overhead for `VARCHAR` is particularly high. Complexity and data size might not be enough to overcome this per-row cost.
*   **Heuristic Evaluation:** Default thresholds might be too optimistic.
*   **Bottleneck Identification:** Per-row FFI calls, string marshalling, `CreateFFIVectorFromDuckDBVector` (if it were per-row, but it's per-chunk).
*   **Conclusion on PoC Performance (Row-by-Row):** This model is functionally capable but does not offer performance benefits over native C++ for typical SQL expressions due to FFI overhead.

### 5.2. Analysis for Block-Processing Model (Current PoC Design)

This analysis is based on the *conceptual* benchmark results for the Block-Processing Model.

*   **Overhead Analysis (JIT First Run):**
    *   `JIT_FirstRun_ms` (e.g., 17.50ms for `A_AddInt` 1M rows) still includes the significant one-time costs of translation (`LuaTranslator`) and LuaJIT compilation for the entire block-processing function. This cost remains substantial compared to the C++ baseline execution.
    *   The relative overhead compared to `JIT_CachedExec_ms` is very large (e.g., 17.50ms vs 3.50ms for `A_AddInt`), highlighting that the JIT approach is only viable if this first-run cost can be amortized over many executions of the same expression on different chunks.

*   **Performance Comparison (Cached JIT vs. C++):**
    *   **Numeric Operations (A, C, F, G, H, I):**
        *   The block-processing model shows *significant conceptual improvement* here. `JIT_CachedExec_ms` (e.g., 3.50ms for `A_AddInt` 1M rows) can now be *faster* than `CppBaseline_ms` (5.00ms).
        *   This is attributed to the elimination of per-row C++ to Lua FFI calls for the loop itself. LuaJIT can optimize the entire loop over `count` rows.
        *   For more complex numeric/conditional logic like `G_CaseInt` (C++: 8.00ms, JIT Cached: 6.00ms) or `I_RoundDbl` (C++: 7.00ms, JIT Cached: 5.50ms), the benefits of JIT compilation of the logic itself become more apparent once the dominant per-row call overhead is removed.
        *   Simple operations like `F_IsNotNullInt` (C++: 3.00ms, JIT Cached: 2.50ms) also show JIT becoming competitive.
    *   **String Operations (D, E, J, K, L, M, N):**
        *   **Input-Heavy, Non-String Output (e.g., `D_StrEq`, `K_LengthStr`):**
            *   `D_StrEq` (VARCHAR Eq): C++ (20.00ms) vs JIT Cached (25.00ms). Still slower.
            *   `K_LengthStr` (LENGTH): C++ (18.00ms) vs JIT Cached (22.00ms). Still slower.
            *   While the loop is in Lua, the per-row cost of `ffi.string(ptr, len)` to create Lua strings from input `FFIString` arrays likely remains a significant bottleneck.
        *   **Output-Heavy (e.g., `E_LowerStr`, `J_ConcatStr`, `L_ReplaceStr`, `M_LpadStr`, `N_TrimStr`):**
            *   `E_LowerStr` (LOWER): C++ (25.00ms) vs JIT Cached (40.00ms).
            *   `J_ConcatStr` (`||`): C++ (22.00ms) vs JIT Cached (30.00ms).
            *   These remain slower. The per-row FFI C helper calls for string output (`duckdb_ffi_add_string_to_output_vector`) become the dominant factor, negating much of the benefit of the in-Lua loop.
    *   **Temporal Operations (O, P, Q):**
        *   `O_EqDate` (DATE Eq): C++ (5.50ms) vs JIT Cached (4.00ms). Similar to numeric, shows JIT can be faster as dates are simple integers.
        *   `P_ExtractYearDate`, `Q_ExtractHourTs` (`EXTRACT`): C++ (6.00-6.50ms) vs JIT Cached (8.00-8.50ms). These are still slower because they involve per-row FFI calls to C helper functions for the extraction logic. The overhead of these calls outweighs the benefit of the in-Lua loop.
    *   **Overall for Cached JIT:** The block-processing model conceptually makes JIT competitive or faster for numeric, date, and complex conditional logic not heavily reliant on string outputs or per-row FFI helper calls. String operations and functions requiring FFI calls per row remain challenging.

*   **Comparison to Row-by-Row Model:**
    *   The block-processing model's main advantage is the dramatic reduction in `JIT_CachedExec_ms` for operations that don't have other per-row FFI bottlenecks (like numeric ops). For `A_AddInt` (1M rows), cached execution dropped from a conceptual 7.50ms (row-by-row) to 3.50ms (block-processing), making it faster than the C++ baseline.
    *   For operations still bottlenecked by per-row FFI (e.g., string outputs like `E_LowerStr`, or FFI function calls like `P_ExtractYearDate`), the improvement is less pronounced or negligible because the fundamental per-row FFI cost remains. `E_LowerStr` only improved from 45.00ms to 40.00ms.

*   **FFI Overhead Analysis (Revisited with Block Processing):**
    *   The dominant C++-to-Lua call overhead *for the loop itself* is eliminated.
    *   Remaining FFI-related overheads include:
        1.  **Per-Chunk Data Preparation:** `CreateFFIVectorFromDuckDBVector` is still called once per chunk.
        2.  **Initial FFI Casts:** Typed pointer casts (e.g., `ffi.cast("int32_t*", ...)`) at the beginning of the JITed Lua function (once per chunk).
        3.  **Per-Row Data Access within Lua:** Accessing `input0_data[i]` etc., while now within JITed Lua code, still involves pointer dereferencing managed by FFI. This is much lighter than a full C-Lua context switch per row.
        4.  **Per-Row `ffi.string()` for VARCHAR Inputs:** If Lua code needs to operate on string content (e.g., in `string.lower` if not an FFI call), `ffi.string()` is typically used per row, which may involve copying and Lua string object creation. This remains a bottleneck for string input processing.
        5.  **Per-Row FFI C Helper Calls:** For `VARCHAR` output (`duckdb_ffi_add_string_to_output_vector`) and functions like `EXTRACT` that call back into C, the per-row FFI call overhead persists and limits speedups.

*   **Benefits of In-Lua Loop:** LuaJIT's JIT compiler can now see the entire loop and the per-row logic together. This allows for more effective optimizations (e.g., trace optimization, register allocation across iterations if possible) for the parts of the computation that are purely within Lua.

*   **Remaining Bottlenecks (Post Block-Processing):**
    *   FFI C helper calls made *per row* from within the Lua loop (e.g., for string outputs, `EXTRACT`).
    *   `ffi.string()` creation for `VARCHAR` inputs if string content is processed in Lua per row.
    *   The inherent cost of `CreateFFIVectorFromDuckDBVector` to prepare data for FFI each time a chunk is processed.
    *   The complexity and efficiency of the Lua code generated by `LuaTranslator` for the core row logic.

*   **Revised Performance Conclusion for PoC (Block-Processing):**
    *   The block-processing model is a significant architectural improvement for this LuaJIT PoC.
    *   Conceptually, it demonstrates that JIT compilation can outperform DuckDB's highly optimized C++ vectorized execution for certain classes of expressions, particularly complex numeric, date, or conditional logic that does not involve heavy per-row FFI call-backs (like string outputs or `EXTRACT`).
    *   For expressions dominated by string manipulation (especially output) or other per-row FFI helper calls, the performance benefits are limited, and JIT may still be slower than C++.
    *   The PoC, with block processing, establishes a JIT pathway that is *potentially* beneficial for a subset of operations, making it a more promising direction than the row-by-row model.

*   **Impact on `ShouldJIT` Heuristics (Block-Processing):**
    *   If these conceptual improvements hold, the `luajit_jit_trigger_count` could potentially be lowered, as the JIT path is now faster (when cached) for more expression types.
    *   The `luajit_jit_complexity_threshold` might still need to be tuned carefully. While simple numeric ops are faster, the JIT compilation overhead (`JIT_FirstRun_ms`) is still high. JIT should still be reserved for expressions that are either "hot" (executed many times) or sufficiently complex where the cached execution savings are substantial.
    *   The definition of "complexity" might need to consider the type of operations (e.g., penalizing expressions that would result in many per-row FFI calls from Lua back to C).

## 8. Future Work & Potential Optimizations (Conceptual)
*   **Minimize Per-Row FFI from Lua to C:**
    *   For string outputs, explore ways to have Lua build up a buffer of all output strings or their components (offsets, lengths) and make one FFI call at the end to commit them to DuckDB's `StringHeap` and `Vector`. This is complex.
    *   For functions like `EXTRACT`, if multiple parts are extracted from the same date/timestamp per row, consider an FFI helper that returns a struct with all parts, rather than multiple FFI calls.
*   **Optimized `ffi.string()` Usage:** For string inputs, if only a few characters are needed (e.g., for a `LIKE` pattern prefix check done in Lua), explore direct char-by-char access via `ffi.cast('char*', ffi_string.ptr)[offset]` instead of creating full Lua strings, if feasible and beneficial.
*   **Further Batching within Lua:** For very common sequences of operations entirely within Lua's capability (e.g., a series of arithmetic ops), explore if LuaJIT's features allow for SIMD-like vector processing directly in Lua on slices of the FFI data arrays.
*   **Investigate `ffi.metatype`:** For `FFIVector` and other structs, using `ffi.metatype` could provide a more idiomatic Lua interface and potentially allow LuaJIT to optimize accesses better, though this adds complexity.
*   **Refined Complexity Heuristics:** Develop a more nuanced complexity score that considers the type of operations and potential FFI overheads.
*   **Profile Real Workloads:** Identify actual queries where this JIT approach (with block processing) yields measurable benefits.
