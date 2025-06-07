# DuckDB LuaJIT Integration Proof-of-Concept

## 1. Introduction

This document provides a developer-oriented overview of the proof-of-concept (PoC) integration of LuaJIT for Just-In-Time (JIT) compilation of expressions within DuckDB. The primary goal of this PoC was to explore the feasibility and potential performance characteristics of using LuaJIT to accelerate query expression evaluation.

For a consolidated overview of the entire investigation including initial research, evaluations, design, and PoC outcomes, please refer to the [LuaJIT for DuckDB Summary](./LuaJIT_for_DuckDB_Summary.md).

## 2. Architecture Overview

The LuaJIT integration PoC comprises several key C++ components and a defined workflow for JIT compilation and execution.

### Key Components:

*   **`LuaJITStateWrapper` (`duckdb/common/luajit_wrapper.hpp`, `.cpp`):**
    *   Manages LuaJIT Virtual Machine instances (`lua_State*`).
    *   Handles initialization (creating a Lua state, loading standard libraries including FFI).
    *   Provides methods to execute Lua scripts (`ExecuteString`) and compile Lua function strings into named global Lua functions (`CompileStringAndSetGlobal`). Errors can be captured into a `std::string`.
    *   Includes `PCallGlobal` to call a named Lua function with `FFIVector` arguments and capture runtime errors.
    *   Manages cleanup (`lua_close()`).

*   **FFI Data Structures (`duckdb/common/luajit_ffi_structs.hpp`):**
    *   Defines C-style structs for FFI:
        *   `FFIVector`: Represents a data vector for Lua. Contains `void* data`, `bool* nullmask` (flat boolean array), `idx_t count`, `LogicalTypeId ffi_logical_type_id`, and `VectorType ffi_duckdb_vector_type`.
        *   `FFIString`: Represents string elements with `char* ptr` and `uint32_t len`. For `VARCHAR` vectors, `FFIVector.data` points to an array of `FFIString`.
    *   **`CreateFFIVectorFromDuckDBVector` (`luajit_ffi_vector.cpp`):** This crucial C++ helper function converts a `duckdb::Vector` into an `FFIVector` suitable for Lua.
        *   It handles `FLAT_VECTOR`, `CONSTANT_VECTOR` (by creating and populating temporary flat buffers), and `DICTIONARY_VECTOR` (by flattening into temporary buffers) for numeric and `VARCHAR` types.
        *   It converts DuckDB's bitmasked `ValidityMask` into a flat `bool*` nullmask.
        *   Temporary buffers for nullmasks and flattened data are owned by a `std::vector<std::vector<char>>` passed by the caller.
        *   For `VARCHAR` vectors, it creates an array of `FFIString` structs, populating `ptr` and `len` from DuckDB's `string_t` values.

*   **Expression Translation (`LuaTranslator`, `LuaTranslatorContext`):**
    *   **`LuaTranslator` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
        *   Now translates actual `duckdb::BoundExpression` subtypes (currently `BoundConstantExpression`, `BoundReferenceExpression`, `BoundOperatorExpression` for numerics and basic string comparisons) into Lua code strings.
        *   `TranslateExpressionToLuaRowLogic` is the core method generating element-wise Lua processing logic.
    *   **`LuaTranslatorContext`:** Stores `LogicalType`s of input vectors to aid the translator in generating type-correct Lua FFI casts (e.g., `ffi.cast('int32_t*',...)`, `ffi.cast('FFIString*',...)`) and type-specific Lua code (e.g., `ffi.string(...)` for `VARCHAR` column access).

*   **Integration into `ExpressionExecutor` (`duckdb/execution/expression_executor.hpp`, `.cpp`):**
    *   `ExpressionExecutor` now holds a `LuaJITStateWrapper luajit_wrapper_`.
    *   `ExpressionState` (in `expression_executor_state.hpp`) stores JIT state:
        *   `bool attempted_jit_compilation`
        *   `bool jit_compilation_succeeded`
        *   `std::string jitted_lua_function_name` (unique name for the cached Lua function)
        *   `idx_t execution_count` (for trigger heuristic)
    *   **JIT Path in `ExpressionExecutor::Execute()`:**
        *   **`ShouldJIT()` Heuristic:** Decides if an expression is eligible for JIT based on:
            *   Conceptual JIT enable flag (`DBConfigOptions::enable_luajit_jit`).
            *   Previous JIT attempt status for the current `ExpressionState`.
            *   Conceptual expression complexity (`GetExpressionComplexity()`) vs. `DBConfigOptions::luajit_jit_complexity_threshold`.
            *   `ExpressionState::execution_count` vs. `DBConfigOptions::luajit_jit_trigger_count`.
            *   Supported expression types for translation.
        *   **Caching & Compilation:** If `ShouldJIT()` is true and the expression isn't already compiled:
            1.  `attempted_jit_compilation` is set.
            2.  Input `LogicalType`s are gathered for `LuaTranslatorContext`.
            3.  `LuaTranslator::TranslateExpressionToLuaRowLogic` generates the core Lua processing logic.
            4.  A unique Lua function name is generated (`GenerateUniqueJitFunctionName`) and stored in `ExpressionState`.
            5.  `ConstructFullLuaFunctionScript` (static helper) creates the complete Lua function string (including FFI cdefs, signature, input/output FFIVector casting, and the processing loop wrapping the row logic).
            6.  `luajit_wrapper_.CompileStringAndSetGlobal()` compiles this script. `jit_compilation_succeeded` is updated.
        *   **Execution (Conceptual):** If `jit_compilation_succeeded` is true, the `Execute` path *would* then prepare `FFIVector`s from the input `DataChunk` and output `Vector` (using `CreateFFIVectorFromDuckDBVector`), and call the named JITed Lua function using `luajit_wrapper_.PCallGlobal()`. (Note: For the PoC, this final execution step within `Execute` is still largely conceptual, with tests driving the FFI call more directly after confirming compilation via `Execute`).
        *   **Error Handling & Fallback:** The JIT attempt (compilation & conceptual execution) is wrapped in a `try-catch` block.
            *   Lua compilation or runtime errors captured by `LuaJITStateWrapper` are re-thrown as `duckdb::RuntimeException`.
            *   If an exception occurs, `jit_compilation_succeeded` is set to `false` for the current `ExpressionState` (preventing further JIT attempts for this expression instance).
            *   Execution then falls back to `ExecuteStandard()` (the original C++ interpreter path).
    *   `ExpressionExecutor::ExecuteStandard()`: Contains the original C++ expression evaluation logic. `ExpressionState::execution_count` is incremented here if JIT was not used or failed.

### Workflow:

1.  **Initialization:** `ExpressionExecutor` is initialized. For each expression added via `AddExpression()`, an `ExpressionState` is created (with `execution_count = 0`, JIT flags `false`).
2.  **Execution Call:** `ExpressionExecutor::Execute(...)` is called for an expression and `DataChunk`.
3.  **JIT Decision:** `ShouldJIT(expr, state)` is evaluated. It considers:
    *   Global JIT enabled flag.
    *   If this expression state already has a successfully compiled function.
    *   If JIT previously failed for this state.
    *   Expression complexity threshold.
    *   Execution count threshold.
4.  **JIT Path (if `ShouldJIT` is true):**
    *   **Compile (if first suitable attempt):**
        *   `LuaTranslator` converts the `BoundExpression` to Lua row logic, using `LuaTranslatorContext` (with input `LogicalType`s) for type-aware translation.
        *   A unique Lua function name is generated and stored in `ExpressionState`.
        *   A full Lua script is constructed (with FFI cdefs, casts, loop) using the row logic.
        *   `LuaJITStateWrapper::CompileStringAndSetGlobal` compiles and defines this function in the Lua state.
        *   `ExpressionState::jit_compilation_succeeded` is updated. Errors lead to `RuntimeException` and fallback.
    *   **Execute (if compiled successfully):**
        *   (Conceptual in `Execute`, demonstrated in tests) `FFIVector`s are created from the input `DataChunk`'s `Vector`s and the output `Vector` using `CreateFFIVectorFromDuckDBVector`.
        *   The named Lua function is called via `LuaJITStateWrapper::PCallGlobal`, passing `FFIVector*` arguments. Runtime errors lead to `RuntimeException` and fallback.
        *   If successful, results are in the output `Vector`, and `Execute` returns.
5.  **C++ Fallback Path:** If JIT is not used, or if any JIT step fails and an exception is caught (or `jit_compilation_succeeded` is false), `ExpressionExecutor::ExecuteStandard()` is called to evaluate the expression using the original C++ interpreter. `ExpressionState::execution_count` is incremented.

## 3. Build System Integration (Conceptual)

(Content remains largely the same as before - LuaJIT as a `third_party` static library linked into DuckDB.)

## 4. Adding Support for New Expressions or Data Types

Extending the JIT capabilities involves modifications across several components:

1.  **FFI Data Structures (`luajit_ffi_structs.hpp` & `luajit_ffi_vector.cpp`):**
    *   **New C Structs:** If a new DuckDB type needs a distinct C representation for Lua FFI (beyond `FFIVector` for basic types/`FFIString` arrays), define it in `luajit_ffi_structs.hpp`. Update Lua `ffi.cdef` in `ConstructFullLuaFunctionScript` accordingly.
    *   **`CreateFFIVectorFromDuckDBVector`:** Extend this function in `luajit_ffi_vector.cpp` to handle new `LogicalType`s or `VectorType`s when creating `FFIVector`s from `duckdb::Vector`. This includes populating `FFIVector::data` (possibly allocating temporary buffers) and the flat `nullmask`.
    *   **FFI C Helpers for Output:** For complex output types (especially variable-size like new strings), implement C helper functions callable from Lua (e.g., `duckdb_ffi_add_custom_type_to_output`). These need to be registered with Lua in `LuaJITStateWrapper`.

2.  **Expression Translation (`LuaTranslator`):**
    *   **Input `BoundExpression` Subtypes:** The translator now accepts `const duckdb::Expression&`.
    *   **Add `GenerateValue` Overload:** For new `duckdb::BoundExpression` subtypes (e.g., `BoundFunctionExpression`, `BoundAggregateExpression`), add a new static `GenerateValue(const NewBoundExprType& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns)` method.
    *   **Update Dispatch:** Add a `case` for the new `ExpressionClass` in `LuaTranslator::GenerateValueExpression`.
    *   **Implement Translation:**
        *   The new `GenerateValue` method must produce a Lua code string for the expression's value.
        *   **Data Access & Types:** Use `LuaTranslatorContext::GetInputLogicalType()` and `GetInputLuaFFIType()` to generate type-correct Lua FFI casts and data access (e.g., `ffi.string(input_vectors[col_idx_1_based].data[i].ptr, ...)` for `VARCHAR`).
        *   **Operator/Function Logic:** Translate to Lua operators or generate calls to other (possibly FFI'd C) functions.
        *   **Null Propagation:** Collect `column_binding`s of all `BoundReferenceExpression`s used. `TranslateExpressionToLuaRowLogic` uses these for the primary null check. Internal null semantics of complex expressions (like `CASE` or SQL functions) must be implemented in their `GenerateValue` method.
        *   **Boolean Results:** If the `BoundExpression::return_type` is `BOOLEAN`, `TranslateExpressionToLuaRowLogic` converts the Lua `true/false` result to `1/0`.
        *   **String Results (Output):** This remains a complex area. If `expr.return_type` is `VARCHAR`, the generated Lua code for `value_expr_str` should result in a Lua string. `TranslateExpressionToLuaRowLogic` currently has a placeholder for how this Lua string would be written back to an output `FFIVector` of `FFIString` (it would require FFI C helper functions like `duckdb_ffi_add_string_to_output`).

3.  **Unit Testing (Crucial):**
    *   **FFI Tests (`test/unittest/luajit_ffi_test.cpp`):** Test new FFI data structure interactions, especially for `CreateFFIVectorFromDuckDBVector` with new `Vector` types/layouts.
    *   **Translator Tests (`test/unittest/luajit_translator_test.cpp`):** Add tests for new `BoundExpression` subtypes, verifying the generated Lua row logic string, including type handling and null propagation.
    *   **Executor Tests (`test/unittest/jit_expression_executor_test.cpp`):** Add end-to-end tests using `ExpressionExecutor::ExecuteExpression`. These tests should:
        *   Use the new `BoundExpression` types.
        *   Verify correct results when the JIT path is taken.
        *   Test JIT compilation errors and runtime errors, ensuring fallback to the C++ path and correct results from the fallback.
        *   Test JIT heuristics (enable flags, trigger counts, complexity - by simulating config changes).

## 5. JIT Heuristics and Configuration

*   **`ShouldJIT()` Method:** Located in `ExpressionExecutor`, this method determines if JIT compilation should be attempted for a given expression and its state.
*   **Heuristics:**
    1.  **Global Enable Flag:** Checks `DBConfigOptions::enable_luajit_jit` (conceptual, via `ClientContext`). JIT is skipped if false.
    2.  **Previous Failure:** If `ExpressionState::attempted_jit_compilation` is true and `jit_compilation_succeeded` is false, JIT is skipped for that expression instance.
    3.  **Complexity Threshold:** `GetExpressionComplexity(expr)` (a basic recursive node counter in PoC) is compared against `DBConfigOptions::luajit_jit_complexity_threshold`. JIT is skipped for simple expressions.
    4.  **Execution Count Trigger:** `ExpressionState::execution_count` (incremented on non-JITed executions) is compared against `DBConfigOptions::luajit_jit_trigger_count`. JIT is only attempted after the expression has been executed enough times via the C++ path.
    5.  **Supported Expression Type:** Checks if the `expr.GetExpressionClass()` is supported by the `LuaTranslator`.
*   **Configuration (Conceptual):**
    *   Settings like `enable_luajit_jit`, `luajit_jit_complexity_threshold`, `luajit_jit_trigger_count` are defined conceptually in `DBConfigOptions` (see `settings.hpp`, `config.cpp`).
    *   These would be settable via SQL `SET` commands or pragmas in a full implementation. Tests simulate these by modifying config objects directly.

## 6. Caching Mechanism

*   **Scope:** Caching is per `ExpressionState`. Each instance of an expression in a query plan gets its own `ExpressionState`.
*   **Process:**
    1.  When `ExpressionExecutor::Execute` encounters a JITable expression for the first time (and heuristics pass), it translates and compiles it.
    2.  A unique Lua function name is generated (e.g., `jitted_duckdb_expr_func_X`) using `GenerateUniqueJitFunctionName`.
    3.  This name is stored in `ExpressionState::jitted_lua_function_name`.
    4.  `LuaJITStateWrapper::CompileStringAndSetGlobal` defines this function in the current Lua state.
    5.  `ExpressionState::jit_compilation_succeeded` is set.
    6.  On subsequent calls to `Execute` for the same `ExpressionState`, if `jit_compilation_succeeded` is true, the executor knows the function is already compiled and (conceptually) directly calls the named Lua function via FFI, bypassing translation and Lua compilation.

## 7. Error Handling and Fallback

*   **Error Capture:**
    *   `LuaJITStateWrapper::CompileStringAndSetGlobal` and `PCallGlobal` now accept a `std::string& out_error_message` parameter to capture Lua errors.
*   **Exception Propagation:**
    *   In `ExpressionExecutor::Execute`, if Lua compilation (`CompileStringAndSetGlobal`) or Lua runtime execution (`PCallGlobal`) fails, the captured error message is used to throw a `duckdb::RuntimeException`.
*   **Fallback Mechanism:**
    *   The JIT attempt in `ExpressionExecutor::Execute` is wrapped in a `try-catch (duckdb::Exception)` block.
    *   If a JIT-related exception is caught (either compilation or runtime):
        *   `ExpressionState::jit_compilation_succeeded` is set to `false`. This ensures `ShouldJIT` will return `false` for subsequent calls for this expression instance, preventing re-attempts of failed JIT.
        *   The exception is logged (conceptually).
        *   Execution falls through to the `ExecuteStandard()` method, which contains the original C++ interpreter logic for the expression.
    *   If `ShouldJIT` initially returns `false`, execution also proceeds directly to `ExecuteStandard()`.

## 8. Debugging JITed Code

(Content remains largely the same as before: Inspect Lua, Print from Lua, FFI Issues, LuaJIT tools.)

## 9. Benchmarking

*   **Framework:** `test/benchmark/jit_expression_benchmark.cpp`.
*   **Methodology:**
    *   Uses actual `duckdb::BoundExpression` objects.
    *   **C++ Baseline:** For some scenarios (like simple integer addition), a manual C++ loop is used for a clear, low-overhead baseline. For others, using `ExpressionExecutor` with JIT forced off is the goal but is complex for the PoC (marked as TBD in benchmark code).
    *   **JIT Timings:**
        *   `TranslationTime_ms`: `LuaTranslator::TranslateExpressionToLuaRowLogic` for `BoundExpression`.
        *   `CompilationTime_ms`: `LuaJITStateWrapper::CompileStringAndSetGlobal` for the generated Lua function.
        *   `JIT_CachedRun_ExecTime_ms`: Average time for multiple calls to the compiled Lua function via `LuaJITStateWrapper::PCallGlobal`, using `FFIVector`s prepared by `CreateFFIVectorFromDuckDBVector`.
        *   `JIT_FirstRun_TotalTime_ms`: Sum of translation, compilation, and one execution.
*   **Analysis:** Documented in `LuaJIT_benchmarking_and_profiling.md`, comparing JIT path components with the C++ baseline.

## 10. Current PoC Limitations

*   **Expression Support:** `LuaTranslator` only supports `BoundConstantExpression`, `BoundReferenceExpression`, and `BoundOperatorExpression` (for numeric types, basic string comparisons). Logical operators (AND/OR/NOT), LIKE, CONCAT, CASE, and function calls via their specific `BoundExpression` subtypes are not yet translated.
*   **String Output FFI:** Writing string results from Lua back to C++ `FFIVector<FFIString>` (e.g., for `CONCAT`) is not implemented due to memory management complexity (requires FFI C helpers and string heap integration). Lua code for string output is placeholder.
*   **Type System:** Full type coverage and robust casting in generated Lua for all DuckDB types (DECIMAL, INTERVAL, complex types like STRUCT, LIST, MAP) is not implemented.
*   **`ExpressionExecutor` JIT Call Path:** The actual FFI call to the JITed function from within `ExpressionExecutor::Execute` is still conceptual for general cases (mapping expression children to `DataChunk` columns and then to FFI arguments). Unit tests drive these calls more directly after ensuring compilation through `Execute`.
*   **Configuration Plumbing:** JIT settings (`enable_luajit_jit`, thresholds) are conceptually defined but not fully plumbed through SQL `SET`/`PRAGMA` commands. Tests simulate these by direct config modification or assuming default values.
*   **`GetExpressionComplexity`:** The current implementation is a very basic placeholder.

## 11. Future Work

(Content remains largely the same: Full DuckDB Expression/Type Integration, Robust String Output, Complex Types, More Functions/Operators, Advanced JIT Cache, Error Propagation, Performance Optimization, Full Vector Type Handling, Configuration Plumbing.)
```
