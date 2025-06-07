# DuckDB LuaJIT Integration Proof-of-Concept: Final Summary

## 1. Introduction

This document provides a developer-oriented overview and final summary for the initial phase of the proof-of-concept (PoC) integration of LuaJIT for Just-In-Time (JIT) compilation of expressions within DuckDB. The primary goal of this PoC was to explore the feasibility, architectural integration (culminating in a block-processing model with advanced VARCHAR FFI), and potential performance characteristics of using LuaJIT to accelerate query expression evaluation.

For a consolidated overview of the entire investigation including initial research, evaluations, design, and PoC outcomes, please refer to the [LuaJIT for DuckDB Summary](./LuaJIT_for_DuckDB_Summary.md).

## 2. Architecture Overview (Block-Processing Model with Advanced VARCHAR FFI)

The LuaJIT integration PoC utilizes a **block-processing model**. A single call to a JITed Lua function processes all rows within a `DataChunk`. The main loop over rows resides *inside* the JITed Lua function. This architecture was further refined with an advanced FFI mechanism for handling `VARCHAR` outputs to minimize FFI call overhead.

### Key Components:

*   **`LuaJITStateWrapper` (`duckdb/common/luajit_wrapper.hpp`, `.cpp`):**
    *   Manages LuaJIT Virtual Machine instances (`lua_State*`).
    *   Handles initialization, including global FFI CDEFs for core data structures and C helper functions.
    *   Registers custom C FFI helper functions with Lua.

*   **FFI Data Structures (`duckdb/common/luajit_ffi_structs.hpp`):**
    *   `FFIVector`, `FFIString`, `FFIInterval` define C-style data representations for Lua.
    *   **`CreateFFIVectorFromDuckDBVector` (`luajit_ffi_vector.cpp`):** Converts `duckdb::Vector` to `FFIVector`.
    *   **FFI C Helper Functions:**
        *   Standard helpers: `duckdb_ffi_add_string_to_output_vector`, `duckdb_ffi_set_string_output_null` (now largely superseded for block output but kept for potential single-row FFI needs or other contexts), and date/timestamp extraction functions (`duckdb_ffi_extract_from_date`, etc.).
        *   **Advanced VARCHAR Output Helper:** `duckdb_ffi_add_lua_string_table_to_output_vector(lua_State* L)`: This new function is called *once* per chunk for `VARCHAR`-producing expressions. It takes a Lua table (populated by the JITed Lua code with string results or nils for the chunk) and populates the DuckDB output `VARCHAR` vector.

*   **Expression Translation (`LuaTranslator`, `LuaTranslatorContext` - Block-Processing & Advanced VARCHAR FFI Aware):**
    *   **`LuaTranslatorContext` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
        *   Initialized by `ExpressionExecutor` with unique, ordered input `LogicalType`s for the Lua function and a map from original `DataChunk` column indices to 0-based Lua argument indices.
        *   Provides methods like `GetInputLuaFFIType(lua_arg_idx)` and `GetLuaArgIndex(original_chunk_col_idx)`.
    *   **`LuaTranslator` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
        *   `TranslateExpressionToLuaRowLogic()`: Generates a Lua code *snippet* for processing a single row `i`. This snippet:
            *   Assumes Lua local variables (e.g., `input0_data`, `input0_nullmask`) for accessing input vector data are pre-defined and cast by the outer Lua function shell.
            *   Computes the expression's value and null status, storing them in Lua locals `current_row_value` and `current_row_is_null`.
            *   **For `VARCHAR` output expressions:** Appends Lua code `results_table[i+1] = current_row_value` to add the string result (or `nil`) to a Lua table named `results_table`.
            *   **`LENGTH(varchar_col)` Optimization (Deferred):** The design noted that for `LENGTH` on a direct `VARCHAR` column reference, `LuaTranslator` could ideally generate code to use `inputX_data[i].len` directly from the `FFIString` struct. This specific optimization was deferred in the PoC implementation, with `LENGTH` currently operating on Lua strings created via `ffi.string()`.

*   **Integration into `ExpressionExecutor` (`duckdb/execution/expression_executor.hpp`, `.cpp`):**
    *   **`ExpressionExecutor::ConstructFullLuaFunctionScript()` (Block-Processing & Advanced VARCHAR FFI Aware):**
        *   Generates the complete Lua function string for block processing.
        *   Includes `ffi.cdef` for types and C helpers.
        *   Defines the Lua function signature: `function_name(output_vec_ffi, input0_ffi, ..., count)`.
        *   Generates initial FFI casts for `output_vec_ffi` and each `inputX_ffi` to typed Lua local data/nullmask pointers.
        *   **For `VARCHAR` output expressions:** Prepends `local results_table = {}` before the main loop.
        *   Generates the main `for i = 0, count - 1 do ... end` loop.
        *   Embeds the row logic snippet from `LuaTranslator` (which sets `current_row_value`, `current_row_is_null`, and for `VARCHAR` output, also populates `results_table`).
        *   **Output Handling (within the loop, after the snippet):**
            *   If `current_row_is_null` is true: sets `output_nullmask[i] = true`.
            *   If false: sets `output_nullmask[i] = false`.
                *   For **non-VARCHAR** types: Assigns `current_row_value` to `output_data[i]` (with specific handling for `BOOLEAN` to `0/1`, `INTERVAL` fields).
                *   For **VARCHAR** types: No direct data assignment here; data is in `results_table`.
        *   **Batch VARCHAR Output Call (after the loop):** If the expression output is `VARCHAR`, appends the call `duckdb_ffi_add_lua_string_table_to_output_vector(output_vec_ffi, results_table, count)`.
    *   **JIT Path in `ExpressionExecutor::Execute()`:**
        *   Identifies unique input columns, creates the mapping for `LuaTranslatorContext`.
        *   Calls `LuaTranslator` and then `ConstructFullLuaFunctionScript`.
        *   Compiles and executes the generated block-processing Lua function once per chunk.

### Workflow (Block-Processing with Advanced VARCHAR FFI):
1.  **JIT Decision & Input Analysis (in `ExpressionExecutor::Execute`):** As before, identify unique inputs, create mapping, initialize `LuaTranslatorContext`.
2.  **Compile (if needed):**
    a.  `LuaTranslator::TranslateExpressionToLuaRowLogic()` generates the Lua snippet (which sets `current_row_val`/`_is_null`, and for `VARCHAR` output, adds to `results_table`).
    b.  `ExpressionExecutor::ConstructFullLuaFunctionScript()` builds the full block-processing Lua function, including `results_table` logic and the batch FFI call if output is `VARCHAR`.
    c.  `LuaJITStateWrapper` compiles the script.
3.  **Execute JITed Function (if compiled):** Prepare `FFIVector`s. Call the JITed Lua function once per chunk. If it was a `VARCHAR` expression, this function internally calls `duckdb_ffi_add_lua_string_table_to_output_vector` at the end.
4.  **Fallback:** To C++ path if JIT fails.

## 3. Build System Integration (Conceptual)
(No change from previous documentation.)

## 4. JIT Heuristics and Configuration
(No change in the configuration options or their `SET` command implementation. Effectiveness re-evaluated in Benchmarking.)

## 5. Caching Mechanism
(No change from previous documentation.)

## 6. Error Handling and Fallback
(No change from previous documentation.)

## 7. Adding Support for New Expressions or Data Types
(Process similar to before, but `LuaTranslator` snippets must set `current_row_val`/`_is_null`, and if outputting `VARCHAR`, also populate `results_table`. `ConstructFullLuaFunctionScript` handles the shell.)

## 8. Debugging JITed Code
(No change from previous documentation.)

## 9. Benchmarking (Reflecting Advanced VARCHAR FFI)

*   **Framework & Methodology:** Unchanged (`test/benchmark/jit_expression_benchmark.cpp`).
*   **Conceptual Findings Summary (Block-Processing with Advanced VARCHAR FFI):**
    *   **Overall:** The block-processing model itself provides significant conceptual speedups for numeric/fixed-width types by moving the main loop into Lua, thus reducing C++-to-Lua FFI call overhead from per-row to per-chunk.
    *   **`VARCHAR` Output Expressions (e.g., `LOWER`, `CONCAT`, `REPLACE`):**
        *   The advanced VARCHAR FFI (batch output via Lua table and a single FFI helper call) further improves the `JIT_CachedExec_ms` for these scenarios compared to a block-processing model that still uses per-row FFI calls for string output from Lua. This makes JITing string functions conceptually more competitive, though still often slower than native C++ due to remaining overheads.
    *   **Remaining FFI Overheads:**
        *   Per-chunk data preparation (`CreateFFIVectorFromDuckDBVector`).
        *   Initial FFI casts for vector data/nullmasks at the start of the JITed Lua function.
        *   **`VARCHAR` Inputs:** Per-row `ffi.string()` creation within the Lua loop if Lua code needs to operate on string content (a primary bottleneck for string-heavy expressions).
        *   **`VARCHAR` Outputs (Batched):** While FFI calls are reduced from N to 1, new overheads include Lua table creation per chunk, N Lua table index assignments, and the C-side iteration of this Lua table by `duckdb_ffi_add_lua_string_table_to_output_vector`.
        *   Per-row FFI C helper calls from Lua for functions like `EXTRACT`.
    *   **JIT Viability:** Conceptually, block processing with batched VARCHAR output makes JIT a more viable option for a broader range of complex expressions, especially those heavy on numeric/fixed-width computations. For string-heavy expressions, while improved, performance parity or gains over C++ remain challenging primarily due to input string handling (`ffi.string()`) and the overheads of the batch output mechanism itself.

## 10. Current PoC Limitations
*   **Performance - Remaining FFI Overheads for Strings:**
    *   **`VARCHAR` Inputs:** The per-row creation of Lua strings from `FFIString` inputs (via `ffi.string(ptr, len)`) inside the JITed Lua loop is a major performance bottleneck for expressions that process string content. The `LENGTH(varchar_col)` optimization to use `.len` directly was designed but deferred.
    *   **`VARCHAR` Output Batching Overheads:** While batching reduces FFI calls, creating and populating a Lua table for all string results, then iterating this table in C via Lua API calls, introduces new overheads that can still be significant.
*   **Expression Support:** Coverage is broad but not exhaustive (e.g., complex `INTERVAL` arithmetic, some SQL functions, `LIKE` patterns).
*   **Input Mapping & Complexity Model:** (As before - `ExpressionExecutor` input mapping for JIT is simplified; `GetExpressionComplexity` is basic).

## 11. Future Work
*   **Performance - Optimize VARCHAR FFI (Inputs):**
    *   **Implement `LENGTH(varchar_col)` Optimization:** Prioritize modifying `LuaTranslator` for `BoundReferenceExpression` (VARCHAR) and `GenerateValueBoundFunction` for `LENGTH` to directly use `inputX_data[i].len`.
    *   Explore more Lua functions that can operate directly on `char*` and `len` (from `FFIString`) to avoid `ffi.string()` creation where possible. This might involve writing more custom C FFI helpers that accept `char*, len` and are called from Lua.
*   **Performance - Analyze and Optimize VARCHAR Output Batching:** Profile the Lua table population and the C-side iteration in `duckdb_ffi_add_lua_string_table_to_output_vector` to see if further optimizations are possible (e.g., different Lua data structures, optimizing C-side table traversal).
*   **Broader Expression/Type Support & Refined JIT Mechanics:** (As before).
*   **Operator-Level JIT:** If expression-level JIT for string/UDF-heavy workloads remains challenging due to FFI costs even with these advancements, research JITing larger portions of query plans (e.g., physical operator segments) where FFI overhead can be amortized over more computation.

## 12. Overall Conclusion of PoC

This Proof-of-Concept has successfully established an end-to-end JIT compilation pipeline for SQL expressions in DuckDB using LuaJIT. The architecture evolved to a **block-processing model**, where a single call to a JITed Lua function processes an entire `DataChunk`. This was further refined with an **advanced FFI for `VARCHAR` outputs**, batching string results via a Lua table to reduce FFI call overhead from per-row to per-chunk. SQL-configurable heuristics (`enable_luajit_jit`, `luajit_jit_complexity_threshold`, `luajit_jit_trigger_count`) were also implemented, providing control over JIT behavior.

The block-processing model, particularly with batched string outputs, represents a significant architectural improvement. **Conceptually, this model makes JIT compilation a more viable performance optimization for certain classes of expressions compared to earlier row-by-row FFI approaches.** Specifically, complex numeric, date, or conditional logic expressions (not heavily reliant on per-row FFI C helper calls from Lua) are expected to see performance benefits, potentially matching or exceeding native C++ execution times once JIT compilation overhead is amortized.

However, **significant FFI-related performance challenges persist, especially for `VARCHAR` processing.**
*   The per-row creation of Lua strings from input `FFIString` structs (via `ffi.string()`) inside the JITed Lua loop remains a primary bottleneck for string manipulation functions.
*   While batching `VARCHAR` output reduces FFI call frequency, the overheads of Lua table manipulation and the C-side processing of this table still impose costs.
*   Functions that inherently require per-row FFI calls back to C (like `EXTRACT`) also see limited gains from the in-Lua loop.

The PoC provides crucial insights:
1.  **Feasibility:** Integrating LuaJIT for expression JITting is architecturally sound.
2.  **Block Processing is Key:** This model is superior for reducing primary FFI call overhead.
3.  **Targeted Benefits:** Performance gains are (conceptually) most likely for compute-bound expressions on fixed-width types where the logic can largely stay within Lua.
4.  **FFI Remains a Challenge:** Interaction with complex types like strings across the C++/Lua boundary, even in a block model, requires careful design to minimize overhead.

Future work should prioritize optimizing these FFI interactions for strings (both input and output) and further refining heuristics based on real-world performance data. This PoC forms a strong foundation for such future explorations into JIT compilation within DuckDB.
