# LuaJIT Expression Benchmarking and Profiling

## 1. Introduction

This document outlines the setup for micro-benchmarks designed to evaluate the performance characteristics of JIT-compiling expressions using LuaJIT within DuckDB. It also discusses conceptual findings based on expected behavior, as direct execution and timing are not performed in this environment.

The benchmarks aim to measure and compare:
- **C++ Baseline Time:** Time taken by DuckDB's standard `ExpressionExecutor` (with JIT explicitly disabled via configuration) to evaluate an expression.
- **JIT First Run Time:** The combined time for an expression's first execution via the JIT path using `ExpressionExecutor` (with JIT enabled and configured to trigger immediately). This includes internal translation, LuaJIT compilation, and the first execution.
- **JIT Cached Run Execution Time:** Time taken for subsequent executions of the (now cached) JITed Lua function via `ExpressionExecutor`.

This document presents conceptual results for different stages of the JIT PoC:
1.  **Row-by-Row Processing Model:** (Original PoC design)
2.  **Block-Processing Model:** (Intermediate PoC design with in-Lua loop but per-row FFI for string output)
3.  **Block-Processing Model with Advanced VARCHAR FFI:** (Current PoC design with batched string output)

## 2. Benchmark Setup

The benchmarks are implemented in `test/benchmark/jit_expression_benchmark.cpp`.

- **Environment:** DuckDB environment, using `ClientContext` and two `ExpressionExecutor` instances per scenario.
- **Timing:** `std::chrono::high_resolution_clock`.
- **Expressions:** `duckdb::BoundExpression` objects.
- **Data:** `duckdb::DataChunk` with `duckdb::Vector`s, supporting NULLs.
- **JIT Control:** `ClientConfig` options (`enable_luajit_jit`, `luajit_jit_trigger_count`, `luajit_jit_complexity_threshold`). For benchmarks, JIT is forced on first execution.
- **Iterations:** Execution times are averaged.

## 3. Benchmark Scenarios Implemented

The `RunScenario` function executes expressions covering various types and operations.

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

### 4.1. Row-by-Row Processing Model (Original PoC - Illustrative)

| Scenario             | DataType    | DataSize | NullPct | CppBaseline_ms | JIT_FirstRun_ms | JIT_CachedExec_ms |
|----------------------|-------------|----------|---------|----------------|-----------------|-------------------|
| A_AddInt             | INTEGER     | 1000000  | 0.0     | 5.00           | 18.00           | 7.50              |
| E_LowerStr           | VARCHAR     | 1000000  | 0.0     | 25.00          | 60.00           | 45.00             |
| G_CaseInt            | INTEGER     | 1000000  | 0.0     | 8.00           | 25.00           | 10.00             |
*(Abbreviated table for brevity; full table in previous versions. Translate/Compile times are -1.0 as they are part of JIT_FirstRun_ms)*

### 4.2. Block-Processing Model with Advanced VARCHAR FFI (Batch Output - Current PoC Design - Illustrative)

This table reflects conceptual performance *after* implementing block-processing (loop inside Lua) AND the advanced VARCHAR FFI (batch string output via Lua table and one FFI call).

| Scenario             | DataType    | DataSize | NullPct | CppBaseline_ms | JIT_FirstRun_ms | JIT_CachedExec_ms |
|----------------------|-------------|----------|---------|----------------|-----------------|-------------------|
| A_AddInt             | INTEGER     | 10000    | 0.0     | 0.05           | 0.55            | 0.04              |
| A_AddInt_Nulls       | INTEGER     | 10000    | 0.5     | 0.06           | 0.60            | 0.05              |
| A_AddInt             | INTEGER     | 1000000  | 0.0     | 5.00           | **17.00**       | **3.50**          |
| A_AddInt_Nulls       | INTEGER     | 1000000  | 0.5     | 6.00           | **19.00**       | **4.50**          |
| C_MulConstInt        | INTEGER     | 1000000  | 0.0     | 4.50           | **16.00**       | **3.00**          |
| D_StrEq              | VARCHAR     | 10000    | 0.0     | 0.20           | 1.70            | 0.60              |
| D_StrEq_Nulls        | VARCHAR     | 10000    | 0.5     | 0.22           | 1.80            | 0.65              |
| D_StrEq              | VARCHAR     | 1000000  | 0.0     | 20.00          | **47.00**       | **25.00**         |
| E_LowerStr           | VARCHAR     | 1000000  | 0.0     | 25.00          | **55.00**       | **30.00**         |
| F_IsNotNullInt       | INTEGER     | 1000000  | 0.5     | 3.00           | **14.50**       | **2.50**          |
| G_CaseInt            | INTEGER     | 1000000  | 0.0     | 8.00           | **23.00**       | **5.00**          |
| H_AbsInt             | INTEGER     | 1000000  | 0.0     | 4.00           | **15.50**       | **3.20**          |
| I_RoundDbl           | DOUBLE      | 1000000  | 0.0     | 7.00           | **20.00**       | **5.00**          |
| J_ConcatStr          | VARCHAR     | 1000000  | 0.0     | 22.00          | **50.00**       | **28.00**         |
| K_LengthStr          | VARCHAR     | 1000000  | 0.0     | 18.00          | **42.00**       | **22.00**         |
| L_ReplaceStr         | VARCHAR     | 1000000  | 0.0     | 30.00          | **65.00**       | **40.00**         |
| M_LpadStr            | VARCHAR     | 1000000  | 0.0     | 35.00          | **75.00**       | **50.00**         |
| N_TrimStr            | VARCHAR     | 1000000  | 0.0     | 28.00          | **60.00**       | **42.00**         |
| O_EqDate             | DATE        | 1000000  | 0.0     | 5.50           | **18.00**       | **4.00**          |
| P_ExtractYearDate    | DATE        | 1000000  | 0.0     | 6.00           | **18.50**       | **8.00**          |
| Q_ExtractHourTs      | TIMESTAMP   | 1000000  | 0.0     | 6.50           | **19.50**       | **8.50**          |

*(TranslateOnce_ms and CompileOnce_ms are -1.0 as they are internal to JIT_FirstRun_ms. Bolded JIT numbers indicate conceptual changes from a block-processing model *without* batched string output).*

## 5. Analysis and Observations

### 5.1. Analysis for Row-by-Row Processing Model (Original PoC)
*(This section remains for historical comparison, highlighting the initial performance challenges.)*
*   **Conclusion (Row-by-Row):** This model was functionally capable but generally slower than C++ due to high per-row FFI call overhead.

### 5.2. Analysis for Block-Processing Model (No Batched String Output - Intermediate)
*(This section would contain the analysis based on the table from Turn 33, where the loop was in Lua but strings were still outputted per-row via FFI helpers from Lua.)*
*   **Conclusion (Block Processing, Per-Row String FFI):** Showed significant improvement for numeric/fixed-width types. String operations and FFI-heavy functions like EXTRACT remained slower than C++ due to per-row FFI calls from Lua.

### 5.3. Analysis for Block-Processing Model with Advanced VARCHAR FFI (Batch Output - Current PoC)

This analysis focuses on the conceptual results from Table 4.2, which includes both the in-Lua loop (block processing) and the batched string output mechanism.

*   **Overhead Analysis (JIT First Run):**
    *   `JIT_FirstRun_ms` values (e.g., conceptually ~16-18ms for 1M integer rows, ~47-55ms for 1M string rows) remain substantially higher than C++ baseline execution. This cost includes translation, full Lua function compilation, and initial FFI setup.
    *   The advanced VARCHAR FFI might slightly alter the generated Lua code size for string functions, but the overall first-run overhead is still dominated by the general JIT process.

*   **Performance Comparison (Cached JIT vs. C++):**
    *   **Numeric/Fixed-Width Operations (A, C, F, G, H, I, O):**
        *   These continue to show the benefits of block processing. Cached JIT execution (e.g., `A_AddInt`: 3.50ms, `G_CaseInt`: 5.00ms) is conceptually faster than the C++ baseline (5.00ms and 8.00ms respectively for 1M rows). This is because the main loop is JITed by LuaJIT, and FFI overhead per row is minimal for direct data access.
    *   **String Operations (Input-Heavy, Non-String Output, e.g., `D_StrEq`, `K_LengthStr`):**
        *   `D_StrEq` (VARCHAR Eq): C++ (20.00ms) vs JIT Cached (25.00ms).
        *   `K_LengthStr` (LENGTH): C++ (18.00ms) vs JIT Cached (22.00ms).
        *   These are *not* directly affected by the string *output* batching. Their performance is similar to the previous block-processing model. The per-row cost of `ffi.string()` for input strings within the Lua loop remains the primary bottleneck if the string content needs to be processed by Lua. (The `LENGTH` optimization to use `.len` directly was deferred).
    *   **String Operations (Output-Heavy, e.g., `E_LowerStr`, `J_ConcatStr`, `L_ReplaceStr`, `M_LpadStr`, `N_TrimStr`):**
        *   `E_LowerStr` (LOWER): C++ (25.00ms) vs JIT Cached (30.00ms). Previous block model: 40.00ms.
        *   `J_ConcatStr` (`||`): C++ (22.00ms) vs JIT Cached (28.00ms). Previous block model: 30.00ms.
        *   `L_ReplaceStr`: C++ (30.00ms) vs JIT Cached (40.00ms). Previous block model: 50.00ms.
        *   **Conceptual Improvement:** These scenarios show a marked improvement compared to the block-processing model *without* batched string output. Reducing N FFI calls for string writing to 1 batch call (plus Lua table overhead) makes a difference. However, they are still conceptually slower than the C++ baseline.
    *   **Temporal Operations with FFI Helper Calls (`P_ExtractYearDate`, `Q_ExtractHourTs`):**
        *   `P_ExtractYearDate`: C++ (6.00ms) vs JIT Cached (8.00ms).
        *   `Q_ExtractHourTs`: C++ (6.50ms) vs JIT Cached (8.50ms).
        *   These are unaffected by string output batching and perform as in the previous block-processing model, limited by per-row FFI calls to C for extraction.

*   **Comparison to Previous Models:**
    *   **vs. Row-by-Row:** The block-processing model (even without batched string output) was a major leap.
    *   **vs. Block-Processing (Per-Row String Output):** The advanced VARCHAR FFI (batched output) provides a significant conceptual improvement specifically for string-producing functions, bringing their `JIT_CachedExec_ms` closer to C++ times, though not necessarily surpassing them.

*   **FFI Overhead Analysis (Advanced VARCHAR FFI):**
    *   The main C++-to-Lua loop overhead is eliminated by block processing.
    *   For string outputs, N per-row FFI calls are replaced by:
        *   N Lua table assignments (`results_table[i+1] = val`).
        *   1 FFI call to `duckdb_ffi_add_lua_string_table_to_output_vector`.
        *   This batch FFI call internally iterates the Lua table (N times) and calls `vector->SetValue()` (N times). While `SetValue` still has costs, the C++-Lua context switches are reduced.
    *   The trade-off (Lua table ops + 1 complex FFI call vs. N simple FFI calls) appears conceptually beneficial for string output.

*   **Remaining Bottlenecks for Strings:**
    *   **Input Strings:** Per-row `ffi.string()` creation if Lua needs to operate on the string content. The `LENGTH` optimization (using `.len` directly from `FFIString`) was deferred but would help here.
    *   **Output Batching Overheads:** Lua table creation and assignments, and the C-side iteration of this table within the batch FFI helper.
    *   **Complexity of Lua String Ops:** If the Lua code for string manipulation (e.g., in `REPLACE`) is inherently less efficient than optimized C++ library functions, this will show.

*   **Revised Performance Conclusion for Strings (with Batch Output):**
    *   Batching string outputs makes JITing `VARCHAR`-producing expressions (conceptually) more competitive than before. While still potentially slower than highly optimized C++ routines, the gap is narrowed.
    *   The viability of JIT for string functions now depends more heavily on the cost of processing input strings and the internal efficiency of the Lua string manipulation itself, rather than just output FFI call overhead.

*   **Impact on `ShouldJIT` Heuristics:**
    *   With improved cached performance for numerics and some string operations, the `luajit_jit_trigger_count` could potentially be lowered.
    *   `luajit_jit_complexity_threshold` might still need to be relatively high for string functions, as their C++ baselines are strong. The "complexity" should ideally factor in the cost of FFI interactions (e.g., number of FFI-intensive input/output types).

## 8. Future Work & Potential Optimizations (Conceptual)
*   **Implement `LENGTH(varchar_col)` Optimization:** Change `LuaTranslator` for `BoundReferenceExpression` (VARCHAR) to allow `GenerateValueBoundFunction` for `LENGTH` to access the `.len` field of the `FFIString` struct directly.
*   **Further Minimize Per-Row FFI from Lua to C (for EXTRACT etc.):** If an expression involves multiple `EXTRACT` calls on the same date/timestamp, consider a single FFI helper that returns a struct/table with all requested parts.
*   **Explore Batch `ffi.string()`:** Investigate if there's any FFI mechanism to convert an array of `FFIString` into an array of Lua strings more efficiently than one by one in a Lua loop.
*   **(As before)** Profile real workloads, refine heuristics, consider operator-level JIT, alternative JIT targets.
