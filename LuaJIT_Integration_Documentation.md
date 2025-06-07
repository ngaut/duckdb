# DuckDB LuaJIT Integration Proof-of-Concept: Final Summary

## 1. Introduction

This document provides a developer-oriented overview and final summary for the initial phase of the proof-of-concept (PoC) integration of LuaJIT for Just-In-Time (JIT) compilation of expressions within DuckDB. The primary goal of this PoC was to explore the feasibility, architectural integration (culminating in a block-processing model), and potential performance characteristics of using LuaJIT to accelerate query expression evaluation.

For a consolidated overview of the entire investigation including initial research, evaluations, design, and PoC outcomes, please refer to the [LuaJIT for DuckDB Summary](./LuaJIT_for_DuckDB_Summary.md).

## 2. Architecture Overview (Block-Processing Model)

The LuaJIT integration PoC evolved to a **block-processing model**. In this model, a single call to a JITed Lua function processes all rows within a `DataChunk`. The main loop over rows resides *inside* the JITed Lua function, significantly reducing C++-to-Lua FFI call overhead compared to earlier row-by-row approaches.

### Key Components:

*   **`LuaJITStateWrapper` (`duckdb/common/luajit_wrapper.hpp`, `.cpp`):**
    *   Manages LuaJIT Virtual Machine instances (`lua_State*`).
    *   Handles initialization (creating a Lua state, loading standard libraries including FFI). Globally defines FFI CDEFs for core data structures (`FFIVector`, `FFIString`, `FFIInterval`) and C helper functions.
    *   Registers custom C FFI helper functions with Lua.
    *   Provides methods to compile Lua function strings (`CompileStringAndSetGlobal`) and call them (`PCallGlobal`).

*   **FFI Data Structures (`duckdb/common/luajit_ffi_structs.hpp`):**
    *   `FFIVector`: Represents a data vector for Lua. Contains `void* data`, `bool* nullmask`, `idx_t count`, `LogicalTypeId ffi_logical_type_id`, `VectorType ffi_duckdb_vector_type`, and `duckdb::Vector* original_duckdb_vector`.
    *   `FFIString`: For `VARCHAR` elements (`char* ptr`, `uint32_t len`).
    *   `FFIInterval`: For `INTERVAL` elements (`int32_t months, days; int64_t micros`).
    *   **`CreateFFIVectorFromDuckDBVector` (`luajit_ffi_vector.cpp`):** Converts `duckdb::Vector` to `FFIVector`, handling various vector and data types, and creating flat boolean nullmasks.
    *   **FFI C Helper Functions:** For string output (`duckdb_ffi_add_string_to_output_vector`, `duckdb_ffi_set_string_output_null`) and date/timestamp part extraction (`duckdb_ffi_extract_from_date`, etc.), callable from Lua.

*   **Expression Translation (`LuaTranslator`, `LuaTranslatorContext` - Block-Processing Aware):**
    *   **`LuaTranslatorContext` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
        *   Initialized by `ExpressionExecutor` with the unique, ordered list of `LogicalType`s that correspond to the arguments of the JITed Lua function.
        *   Stores a map from original `DataChunk` column indices to these 0-based Lua argument indices.
        *   Provides `GetInputLuaFFIType(lua_arg_idx)` for element FFI type strings and `GetLuaArgIndex(original_chunk_col_idx)` for mapping.
        *   Provides `GetOutputTypeLuaFFIType()` for the output vector.
    *   **`LuaTranslator` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
        *   `TranslateExpressionToLuaRowLogic()`: Generates a Lua code *snippet* for processing a single row. This snippet assumes specific Lua local variables (e.g., `input0_data`, `input0_nullmask`, and the loop index `i`) are already defined by an outer function shell. It computes the expression's value and null status for the current row `i` and assigns them to predefined Lua locals: `current_row_value` and `current_row_is_null`.
        *   `GenerateValueExpression` (and its recursive helpers for `BoundConstantExpression`, `BoundReferenceExpression`, etc.): These now generate Lua code that sets intermediate `[prefix]_val` and `[prefix]_is_null` Lua variables. `BoundReferenceExpression` translation uses `LuaTranslatorContext::GetLuaArgIndex()` to refer to the correct `inputX_data[i]`.

*   **Integration into `ExpressionExecutor` (`duckdb/execution/expression_executor.hpp`, `.cpp`):**
    *   `ExpressionExecutor` holds a `LuaJITStateWrapper`. `ExpressionState` tracks JIT status.
    *   **JIT Path in `ExpressionExecutor::Execute()` (Block-Processing):**
        *   **Input Analysis:** Identifies unique `BoundReferenceExpression`s in the expression tree. Collects their `LogicalType`s and original column indices. Builds a map from original column indices to 0-based Lua argument indices.
        *   **`LuaTranslatorContext` Creation:** Instantiated with the unique input types and the mapping.
        *   **`ShouldJIT()` Heuristic:** Decides eligibility based on configuration and `ExpressionState`.
        *   **Caching & Compilation (if `ShouldJIT` is true and not already compiled):**
            1.  `LuaTranslator::TranslateExpressionToLuaRowLogic()` generates the row logic snippet using the new context.
            2.  `ExpressionExecutor::ConstructFullLuaFunctionScript()` (static helper) generates the complete Lua function string. This function:
                *   Includes necessary `ffi.cdef` type and C function declarations (ideally defined globally once, but included per script for PoC robustness).
                *   Defines the Lua function signature: `function_name(output_vec_ffi, input0_ffi, input1_ffi, ..., count)`.
                *   Generates initial FFI casts for `output_vec_ffi` and each `inputX_ffi` to typed Lua local data and nullmask pointers (e.g., `local input0_data = ffi.cast('int32_t*', input0_ffi.data)`).
                *   Generates the main loop: `for i = 0, count - 1 do ... end`.
                *   Embeds the row logic snippet (from `LuaTranslator`) inside this loop.
                *   After the snippet, adds Lua code to handle the `current_row_value` and `current_row_is_null`: sets `output_nullmask[i]`, and for non-nulls, assigns `current_row_value` to `output_data[i]` (with type-specific handling for `VARCHAR` via FFI C helpers, `BOOLEAN` to 0/1 conversion, and `INTERVAL` struct field assignment).
            3.  `luajit_wrapper_.CompileStringAndSetGlobal()` compiles this full script.
        *   **Execution (if compiled successfully):**
            1.  Input `FFIVector`s are prepared for the *unique, ordered* input columns using `CreateFFIVectorFromDuckDBVector`.
            2.  Output `FFIVector` is prepared.
            3.  The named Lua function is called once per chunk via `luajit_wrapper_.PCallGlobal()`.
        *   **Error Handling & Fallback:** Unchanged; errors lead to fallback to `ExecuteStandard()`.

### Workflow (Block-Processing):
1.  **JIT Decision & Input Analysis (in `ExpressionExecutor::Execute`):** If JIT is enabled, identify unique input columns, create mapping, and initialize `LuaTranslatorContext`. Call `ShouldJIT()`.
2.  **Compile (if needed):** If JIT is viable and not yet compiled:
    a.  `LuaTranslator::TranslateExpressionToLuaRowLogic()` generates the Lua snippet for per-row logic.
    b.  `ExpressionExecutor::ConstructFullLuaFunctionScript()` builds the full block-processing Lua function string around this snippet.
    c.  `LuaJITStateWrapper` compiles the full script.
3.  **Execute JITed Function (if compiled):** Prepare `FFIVector`s for the chunk. Call the JITed Lua function *once* for the entire chunk.
4.  **Fallback:** If JIT fails or `ShouldJIT` is false, use `ExecuteStandard` C++ path.

## 3. Build System Integration (Conceptual)
(No change from previous documentation: LuaJIT as a `third_party` static library.)

## 4. JIT Heuristics and Configuration
(No change in the configuration options themselves or their `SET` command implementation from previous documentation. The effectiveness of these heuristics is re-evaluated based on the block-processing model's performance in Section 9.)

## 5. Caching Mechanism
(No change from previous documentation: caching is per `ExpressionState`.)

## 6. Error Handling and Fallback
(No change from previous documentation: errors lead to fallback.)

## 7. Adding Support for New Expressions or Data Types (Block-Processing Context)
*   **FFI Data Structures:** (Largely same) Define C structs, update `CreateFFIVectorFromDuckDBVector`, update Lua CDEFs (now in `ConstructFullLuaFunctionScript` and/or global `LuaJITStateWrapper` init), add C helpers if needed.
*   **Expression Translation (`LuaTranslator`):**
    *   `GenerateValue` overloads must produce Lua snippets that set `[result_prefix]_val` and `[result_prefix]_is_null`.
    *   `BoundReferenceExpression` translation now uses `LuaTranslatorContext::GetLuaArgIndex()` to refer to the correct `inputX_data[i]` Lua variable.
    *   The overall snippet from `TranslateExpressionToLuaRowLogic` will be embedded in the loop by `ConstructFullLuaFunctionScript`.
*   **Unit Testing:** Tests in `luajit_translator_test.cpp` verify the generated snippets. Tests in `jit_expression_executor_test.cpp` verify end-to-end execution.

## 8. Debugging JITed Code
(No change from previous documentation.)

## 9. Benchmarking (Reflecting Block-Processing Model)

*   **Framework:** `test/benchmark/jit_expression_benchmark.cpp` using `ExpressionExecutor` for both C++ and JIT paths.
*   **Methodology:** JIT path configured for immediate compilation. Metrics: `CppBaseline_ms`, `JIT_FirstRun_ms`, `JIT_CachedExec_ms`.
*   **Conceptual Findings Summary (Block-Processing Model from `LuaJIT_benchmarking_and_profiling.md`):**
    *   **`JIT_FirstRun_ms`:** Remains significant due to translation and full-function compilation.
    *   **`JIT_CachedExec_ms` vs. `CppBaseline_ms`:**
        *   **Numeric/Fixed-Width Types (including DATE, simple INTERVAL ops):** Conceptually, block processing shows significant improvements. Cached JIT execution can become competitive with or even slightly faster than C++ for complex expressions due to the elimination of per-row C++-to-Lua call overhead. LuaJIT can optimize the entire inner loop.
        *   **String Operations (Input-Heavy, e.g., `LENGTH(str)`, `StrEq`):** Moderate conceptual improvements. The loop itself is faster in Lua, but per-row `ffi.string()` creation from input `FFIString` arrays within the Lua loop remains a bottleneck.
        *   **String Operations (Output-Heavy, e.g., `LOWER(str)`, `REPLACE`):** Limited conceptual improvement. Per-row FFI C helper calls from Lua for string output (`duckdb_ffi_add_string_to_output_vector`) are still required and become a dominant factor.
        *   **Functions with Per-Row FFI Calls (e.g., `EXTRACT`):** Similar to string outputs, the per-row FFI calls to C helpers from Lua limit performance gains, even with the main loop in Lua.
    *   **Overall JIT Viability (Conceptual):** Block processing makes LuaJIT (conceptually) more competitive for a range of complex, non-FFI-heavy expressions (especially numeric and fixed-width types). However, operations dominated by per-row FFI interactions (like string manipulation with FFI helpers) still face performance challenges compared to native C++.

## 10. Current PoC Limitations (Post Block-Processing Refactor)
*   **Performance - Remaining FFI Overheads:**
    *   **`ffi.string()` for VARCHAR Inputs:** Creating Lua strings from input `FFIString` structs (`ffi.string(ptr, len)`) inside the Lua loop on a per-row basis is a significant remaining overhead for string processing expressions.
    *   **Per-Row FFI C Helper Calls:** Operations requiring callbacks to C FFI helpers from within the Lua loop for each row (e.g., `duckdb_ffi_add_string_to_output_vector` for `VARCHAR` results, or `duckdb_ffi_extract_from_date` for `EXTRACT`) are still major performance bottlenecks.
    *   **Initial Data Preparation:** `CreateFFIVectorFromDuckDBVector` (flattening, nullmask conversion) is still a per-chunk cost on the C++ side.
*   **Expression Support:** While broad, not all expressions/functions are JITted (e.g., complex `INTERVAL` arithmetic, some specific SQL functions, `LIKE` patterns).
*   **Input Mapping in `ExpressionExecutor::Execute`:** The current method of identifying unique input columns and mapping them to Lua function arguments (input0, input1, ...) based on `ExpressionIterator` and `BoundReferenceExpression::index` is functional but might need more robustness for deeply nested or very complex shared subexpressions.
*   **`GetExpressionComplexity`:** Remains a basic node count, not fully reflecting computational cost or FFI implications.

## 11. Future Work
*   **Performance - Minimize FFI Overhead (Priority):**
    *   **VARCHAR Input Optimization:** Investigate reducing `ffi.string()` calls. Can more operations be done directly on `FFIString.ptr` in Lua? Are there ways to batch string conversions or operations if multiple string inputs are processed similarly?
    *   **VARCHAR Output Optimization:** For string results, explore alternatives to per-row `duckdb_ffi_add_string_to_output_vector`. Could Lua code build up intermediate string representations (e.g., in a Lua table of pointers/lengths) and have a single FFI call at the end of the chunk to commit these to DuckDB's `StringHeap`? This is complex but potentially high-impact.
    *   **Batch FFI Helpers:** For functions like `EXTRACT`, if multiple parts are needed or if it's part of a sequence of FFI-heavy operations, consider if more complex FFI helpers that perform more work per call could be beneficial.
*   **Broader Expression/Type Support:** (As before, but with new performance context)
*   **Refined JIT Mechanics & Heuristics:**
    *   Develop a more advanced `GetExpressionComplexity` model that accounts for FFI costs associated with types and operations.
    *   Re-evaluate JIT trigger thresholds based on real performance data from the block-processing model.
*   **Operator-Level JIT:** If expression-level JIT remains challenging for certain operations (especially string/UD_heavy ones due to FFI), consider JITing parts of physical operator logic where more work can be done without crossing the FFI boundary repeatedly.
*   **Alternative JIT Targets:** (As before)

## 12. Overall Conclusion of PoC

This Proof-of-Concept has successfully evolved to implement an end-to-end JIT compilation pipeline using LuaJIT, now based on a **block-processing model**. This model, where the main loop over data rows occurs within the JITed Lua function, significantly reduces the C++-to-Lua FFI call overhead that was a major bottleneck in earlier row-by-row designs. The system supports a wide range of SQL expression types, including numerics, strings, conditionals, and temporal functions (like `EXTRACT` via FFI helpers), and features SQL-configurable JIT heuristics.

The refactoring to block-processing conceptually improves the performance landscape. For expressions primarily involving numeric or other fixed-width data types and where the computation can be largely contained within Lua, this model shows potential to be competitive with, or even exceed, DuckDB's native C++ performance for complex expressions once the initial compilation cost is amortized.

However, significant FFI-related performance challenges persist, particularly for:
1.  Processing `VARCHAR` inputs, where `ffi.string()` may be called per row within the Lua loop.
2.  Operations that require per-row callbacks from Lua to C FFI helper functions, most notably for producing `VARCHAR` results or for functions like `EXTRACT`.

The PoC has provided crucial insights:
*   **Feasibility:** Integrating LuaJIT and translating DuckDB expressions to Lua for JIT compilation is architecturally feasible.
*   **Block Processing:** This is a more viable execution model than row-by-row for reducing FFI call overhead.
*   **Persistent FFI Costs:** Even with block processing, FFI interactions for specific data types (especially strings) and helper functions remain critical performance determinants.

Future efforts to enhance performance via this LuaJIT approach must focus on minimizing these remaining FFI bottlenecks, potentially through more advanced data marshalling strategies, batch-oriented FFI helpers, or by carefully selecting only those expressions for JIT where the computational gain within Lua significantly outweighs all FFI costs.
