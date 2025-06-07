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
    *   Registers custom C FFI helper functions (e.g., for string output, date/timestamp extraction) with the Lua state, making them callable from JITed Lua code.
    *   Provides methods to execute Lua scripts (`ExecuteString`) and compile Lua function strings into named global Lua functions (`CompileStringAndSetGlobal`), capturing errors into a `std::string`.
    *   Includes `PCallGlobal` to call a named Lua function with `FFIVector` arguments and capture runtime errors.
    *   Manages cleanup (`lua_close()`).

*   **FFI Data Structures (`duckdb/common/luajit_ffi_structs.hpp`):**
    *   Defines C-style structs for FFI:
        *   `FFIVector`: Represents a data vector for Lua. Contains `void* data`, `bool* nullmask` (flat boolean array), `idx_t count`, `LogicalTypeId ffi_logical_type_id`, `VectorType ffi_duckdb_vector_type`, and `duckdb::Vector* original_duckdb_vector` (for output vectors, allowing C helpers to modify the original DuckDB vector).
        *   `FFIString`: Represents string elements with `char* ptr` and `uint32_t len`. For `VARCHAR` vectors, `FFIVector.data` points to an array of `FFIString`.
        *   `FFIInterval`: Represents interval elements with `months`, `days`, `micros`, mirroring DuckDB's `interval_t`.
    *   **`CreateFFIVectorFromDuckDBVector` (`luajit_ffi_vector.cpp`):** This C++ helper converts a `duckdb::Vector` into an `FFIVector`.
        *   Handles `FLAT_VECTOR`, `CONSTANT_VECTOR` (by creating temporary flat buffers), and `DICTIONARY_VECTOR` (by flattening) for numeric, `VARCHAR`, `DATE`, `TIMESTAMP`, and `INTERVAL` types.
        *   Converts DuckDB's bitmasked `ValidityMask` into a flat `bool*` nullmask.
        *   For `VARCHAR`, creates an array of `FFIString` structs. For `INTERVAL`, creates an array of `FFIInterval` structs.
        *   Temporary buffers are owned by a `std::vector<std::vector<char>>` passed by the caller.
    *   **FFI C Helper Functions (declared in `luajit_ffi_structs.hpp`, implemented in `luajit_ffi_vector.cpp`):**
        *   `duckdb_ffi_add_string_to_output_vector()`: Called from Lua to write a string result to an output `VARCHAR` vector. Uses `Vector::SetValue()`.
        *   `duckdb_ffi_set_string_output_null()`: Called from Lua to set a `NULL` in an output `VARCHAR` vector.
        *   `duckdb_ffi_extract_year_from_date()`, `duckdb_ffi_extract_from_date()`, `duckdb_ffi_extract_from_timestamp()`: Called from Lua to perform date/timestamp part extraction using DuckDB's internal functions.

*   **Expression Translation (`LuaTranslator`, `LuaTranslatorContext`):**
    *   **`LuaTranslator` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
        *   Translates `duckdb::BoundExpression` subtypes into Lua code strings. Currently supports:
            *   `BoundConstantExpression` (numerics, VARCHAR, DATE, TIMESTAMP).
            *   `BoundReferenceExpression` (numerics, VARCHAR, DATE, TIMESTAMP, INTERVAL).
            *   `BoundOperatorExpression` (numeric arithmetic; numeric, VARCHAR, DATE, TIMESTAMP comparisons; `OPERATOR_CONCAT`, `OPERATOR_NOT`, `OPERATOR_IS_NULL`, `OPERATOR_IS_NOT_NULL`).
            *   `BoundFunctionExpression` for string functions (`LOWER`, `UPPER`, `LENGTH`, `SUBSTRING`, `CONCAT`), numeric functions (`ABS`, `ROUND`, `FLOOR`, `CEIL`), and date/timestamp extraction (`EXTRACT`/`DATE_PART`, `YEAR`).
            *   `BoundCaseExpression` (multi-branch `CASE WHEN ... THEN ... ELSE ... END`).
        *   `TranslateExpressionToLuaRowLogic` is the core method generating element-wise Lua processing logic. For `VARCHAR` return types, it now generates Lua code that calls the FFI C helpers (`duckdb_ffi_add_string_to_output_vector` / `duckdb_ffi_set_string_output_null`) for output.
    *   **`LuaTranslatorContext`:** Stores `LogicalType`s of input vectors, providing methods like `GetInputLuaFFIType()` to help the translator generate type-correct Lua FFI casts (e.g., `ffi.cast('int32_t*',...)`, `ffi.cast('FFIString*',...)`, `ffi.cast('FFIInterval*',...)`) and type-specific Lua code (e.g., `ffi.string(...)` for `VARCHAR` column access, or accessing `.months` for `INTERVAL`).

*   **Integration into `ExpressionExecutor` (`duckdb/execution/expression_executor.hpp`, `.cpp`):**
    *   `ExpressionExecutor` holds a `LuaJITStateWrapper luajit_wrapper_`.
    *   `ExpressionState` stores JIT state: `attempted_jit_compilation`, `jit_compilation_succeeded`, `jitted_lua_function_name`, and `execution_count`.
    *   **JIT Path in `ExpressionExecutor::Execute()`:**
        *   **`ShouldJIT()` Heuristic:** Decides eligibility based on:
            *   `ClientConfig::enable_luajit_jit` (read from context).
            *   `ExpressionState` flags (previous attempts/failures).
            *   `GetExpressionComplexity()` (counts expression nodes) vs. `ClientConfig::luajit_jit_complexity_threshold`.
            *   `ExpressionState::execution_count` vs. `ClientConfig::luajit_jit_trigger_count`.
            *   Supported `BoundExpression` types by the `LuaTranslator`.
        *   **Caching & Compilation (if `ShouldJIT` is true and not already compiled):**
            1.  `LuaTranslator` converts the `BoundExpression` to Lua row logic.
            2.  A unique Lua function name is generated (`GenerateUniqueJitFunctionName`) and stored.
            3.  `ConstructFullLuaFunctionScript` (static helper) creates the complete Lua function string (including FFI cdefs for all supported types/helpers, function signature, input/output FFIVector casting using type info from `LuaTranslatorContext`, and the processing loop).
            4.  `luajit_wrapper_.CompileStringAndSetGlobal()` compiles and defines this function in Lua. Errors are caught.
        *   **Execution (if compiled successfully):**
            1.  Input `FFIVector`s are prepared from the input `DataChunk`'s `Vector`s using `CreateFFIVectorFromDuckDBVector`. This includes handling different vector types (FLAT, CONSTANT, DICTIONARY) and data types (numerics, VARCHAR, DATE, TIMESTAMP, INTERVAL).
            2.  Output `FFIVector` is prepared, linking it to the actual output `duckdb::Vector` via `original_duckdb_vector` for FFI C helpers.
            3.  The named Lua function is called via `luajit_wrapper_.PCallGlobal()`. Runtime errors are caught.
            4.  If successful, results are in the output `Vector`.
        *   **Error Handling & Fallback:** The JIT attempt is wrapped in `try-catch`. Lua errors become `duckdb::RuntimeException`. If JIT fails at any stage, `jit_compilation_succeeded` is set to `false`, and execution falls back to `ExecuteStandard()`.
    *   `ExpressionExecutor::ExecuteStandard()`: Contains the original C++ interpreter logic. `ExpressionState::execution_count` is incremented here if JIT was not used or failed.

### Workflow:
(Largely similar to before, but now uses `BoundExpression`s, more sophisticated FFI data prep, and has a more concrete JIT execution path within `ExpressionExecutor`.)
1.  **JIT Decision:** `ExpressionExecutor::Execute` calls `ShouldJIT()`.
2.  **Compile (if needed):** If JIT is viable and not yet compiled for this `ExpressionState`, translate `BoundExpression` to Lua, generate full function script, compile with `LuaJITStateWrapper`. Store function name and success status in `ExpressionState`.
3.  **Execute JITed Function (if compiled):** Prepare input/output `FFIVector`s from `DataChunk`/`Vector`s using `CreateFFIVectorFromDuckDBVector`. Call the named Lua function via `PCallGlobal`.
4.  **Fallback:** If JIT fails or `ShouldJIT` is false, use `ExecuteStandard` C++ path. Update `execution_count`.

## 3. Build System Integration (Conceptual)
(Content remains the same: LuaJIT as a `third_party` static library.)

## 4. JIT Heuristics and Configuration
*   **`ShouldJIT()` Method:** Implemented in `ExpressionExecutor`.
*   **Heuristics & Configuration Options (in `ClientConfig::options`):**
    1.  `enable_luajit_jit` (bool): Master switch for enabling JIT.
    2.  `luajit_jit_complexity_threshold` (int64_t): Minimum complexity (based on `GetExpressionComplexity()` node count) for an expression to be JITed.
    3.  `luajit_jit_trigger_count` (int64_t): Number of times an expression must be executed via C++ path before JIT is attempted.
*   **`ExpressionState` Tracking:** `attempted_jit_compilation`, `jit_compilation_succeeded`, and `execution_count` track status per expression instance.
*   **SQL `SET` Commands (Conceptual):**
    *   **`settings.hpp` / `config.cpp`:** These `DBConfigOptions` would be added with `Setting` objects, making them accessible via `GetOptionByName` and modifiable via `SetOption`.
    *   **Parser (`transform_set.cpp`):** `Transformer::TransformSet` would parse `SET enable_luajit_jit = TRUE;` etc.
    *   **Binder (`bind_set.cpp`):** `Binder::BindSetVariable` would bind these settings.
    *   **Execution (`physical_set.cpp`):** `PhysicalSet::GetData` would apply the new values to `ClientContext::config.options`.

## 5. Caching Mechanism
(Content remains largely the same: caching is per `ExpressionState`, unique function name stored, compiled once.)

## 6. Error Handling and Fallback
(Content remains largely the same: `LuaJITStateWrapper` captures errors, `ExpressionExecutor` throws `RuntimeException`, fallback to `ExecuteStandard`.)

## 7. Adding Support for New Expressions or Data Types
(Largely similar, but now emphasizes working with `duckdb::BoundExpression` subtypes and `duckdb::LogicalType` in `LuaTranslatorContext` and `LuaTranslator`.)
*   **FFI Data Structures:** Update `luajit_ffi_structs.hpp` for new C representations, `luajit_ffi_vector.cpp` for `CreateFFIVectorFromDuckDBVector` logic, and Lua CDEFs in `ConstructFullLuaFunctionScript`. Add FFI C helpers if needed for complex output.
*   **Expression Translation:**
    *   Add `GenerateValue` overloads in `LuaTranslator` for new `BoundExpression` subtypes.
    *   Update `ExpressionExecutor::ShouldJIT` to recognize new JIT-able expression classes.
    *   Implement translation logic, using `LuaTranslatorContext` for input type info to generate correct FFI casts (e.g., `ffi.cast('FFIInterval*', ...)`), data access (e.g., `.months`), and `ffi.string()` for VARCHARs.
    *   For functions, add cases to `GenerateValueBoundFunction`.
    *   For `VARCHAR` or complex type outputs, ensure Lua code calls appropriate FFI C helpers (e.g., `duckdb_ffi_add_string_to_output_vector`).
*   **Unit Testing:** Add comprehensive tests in `luajit_ffi_test.cpp`, `luajit_translator_test.cpp`, and `jit_expression_executor_test.cpp`.

## 8. Debugging JITed Code
(Content remains largely the same.)

## 9. Benchmarking
*   **Framework:** `test/benchmark/jit_expression_benchmark.cpp`.
*   **Methodology:**
    *   Uses actual `duckdb::BoundExpression` objects.
    *   Compares two `ExpressionExecutor` instances: one with JIT disabled (via `ClientContext::config.enable_luajit_jit = false`) for **C++ Baseline**, and one with JIT enabled (and low thresholds) for the **JIT Path**.
    *   **Metrics:** `CppBaseline_ms`, `JIT_FirstRun_ms` (capturing initial translation, compilation, and first execution via `ExpressionExecutor`), and `JIT_CachedExec_ms` (subsequent executions via `ExpressionExecutor` using the cached Lua function).
    *   Separate `TranslateOnce_ms` and `CompileOnce_ms` are marked as -1.0 in output as these are now internal to `ExpressionExecutor::Execute`'s first JIT run.
*   **Scenarios:** Covers numeric arithmetic/comparisons, string comparisons, string functions (LOWER), IS NOT NULL, and CASE expressions, with varying data sizes and null percentages.
*   **Analysis:** Documented in `LuaJIT_benchmarking_and_profiling.md`.

## 10. Current PoC Limitations
*   **Expression Support:** While expanded, not all `BoundExpression` types or SQL functions/operators are JIT-enabled (e.g., `BoundConjunctionExpression` for AND/OR, `BoundLikeExpression`, many date/interval functions beyond basic EXTRACT, complex `CASE` conditions involving non-boolean results needing further casting).
*   **String Output:** Relies on `Vector::SetValue()` in FFI C helpers, which is convenient but might not be the most performant for high-volume string generation compared to direct `StringHeap` manipulation.
*   **Interval Type:** Input FFI for `INTERVAL` is done (array of `FFIInterval`). Translation for operations on intervals (arithmetic, complex comparisons) is largely missing. Interval output via FFI C helpers is not implemented.
*   **`ExpressionExecutor` JIT Call Path:** The logic for determining the exact input `duckdb::Vector`s for an arbitrary expression's children in `ExpressionExecutor::Execute` is still simplified (relies on order of `BoundReferenceExpression`s found by `ExpressionIterator`). Complex expressions with shared sub-expressions or non-sequential column access might not map inputs correctly to the JITed Lua function's arguments.
*   **Configuration Plumbing:** SQL `SET` commands for JIT options are designed but not implemented in parser/binder/physical operators.
*   **`GetExpressionComplexity`:** The current implementation is basic (node count).

## 11. Future Work
(Largely similar, but some items like basic caching, error handling, and more expression types have progressed.)
*   Full translation coverage for all relevant `BoundExpression` subtypes.
*   Robust and performant string/complex type output mechanisms (e.g., direct `StringHeap` interaction via FFI).
*   Full FFI and translation support for all temporal and nested types (STRUCT, LIST, MAP).
*   Sophisticated JIT cache invalidation strategies (e.g., on schema changes or function redefinition).
*   More advanced `GetExpressionComplexity` model.
*   Complete SQL `SET`/`PRAGMA` integration for JIT configuration.
*   Rigorous performance tuning and profiling against a wider range of queries.
*   Investigate direct LLVM IR generation from DuckDB expressions as an alternative or complementary approach.
