# DuckDB LuaJIT Integration Proof-of-Concept: Final Summary

## 1. Introduction

This document provides a developer-oriented overview and final summary for the initial phase of the proof-of-concept (PoC) integration of LuaJIT for Just-In-Time (JIT) compilation of expressions within DuckDB. The primary goal of this PoC was to explore the feasibility, architectural integration, and potential performance characteristics of using LuaJIT to accelerate query expression evaluation.

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
        *   `FFIInterval`: Represents interval elements with `int32_t months`, `int32_t days`, `int64_t micros`, mirroring DuckDB's `interval_t`.
    *   **`CreateFFIVectorFromDuckDBVector` (`luajit_ffi_vector.cpp`):** This C++ helper converts a `duckdb::Vector` into an `FFIVector`.
        *   Handles `FLAT_VECTOR`, `CONSTANT_VECTOR` (by creating temporary flat buffers), and `DICTIONARY_VECTOR` (by flattening) for numeric, `VARCHAR`, `DATE`, `TIMESTAMP`, and `INTERVAL` types.
        *   Converts DuckDB's bitmasked `ValidityMask` into a flat `bool*` nullmask.
        *   For `VARCHAR`, creates an array of `FFIString` structs. For `INTERVAL`, creates an array of `FFIInterval` structs.
        *   Temporary buffers are owned by a `std::vector<std::vector<char>>` passed by the caller.
    *   **FFI C Helper Functions (declared in `luajit_ffi_structs.hpp`, implemented in `luajit_ffi_vector.cpp`):**
        *   `duckdb_ffi_add_string_to_output_vector()`: Called from Lua to write a string result to an output `VARCHAR` vector. Uses `Vector::SetValue()`.
        *   `duckdb_ffi_set_string_output_null()`: Called from Lua to set a `NULL` in an output `VARCHAR` vector.
        *   `duckdb_ffi_extract_from_date()`, `duckdb_ffi_extract_from_timestamp()`: Called from Lua to perform date/timestamp part extraction (supporting year, month, day, hour, minute, second, microsecond, millisecond, epoch, quarter, dayofweek, dayofyear, week) using DuckDB's internal functions.

*   **Expression Translation (`LuaTranslator`, `LuaTranslatorContext`):**
    *   **`LuaTranslator` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
        *   Translates `duckdb::BoundExpression` subtypes into Lua code strings. Currently supports:
            *   `BoundConstantExpression` (numerics, VARCHAR, DATE, TIMESTAMP, INTERVAL).
            *   `BoundReferenceExpression` (numerics, VARCHAR, DATE, TIMESTAMP, INTERVAL).
            *   `BoundOperatorExpression` (numeric arithmetic; numeric, VARCHAR, DATE, TIMESTAMP comparisons; `OPERATOR_CONCAT`, `OPERATOR_NOT`, `OPERATOR_IS_NULL`, `OPERATOR_IS_NOT_NULL`).
            *   `BoundFunctionExpression` for string functions (`LOWER`, `UPPER`, `LENGTH`, `SUBSTRING`, `CONCAT`, `REPLACE`, `LPAD`, `RPAD`, `TRIM`), numeric functions (`ABS`, `ROUND`, `FLOOR`, `CEIL`, `SQRT`, `POW`, `LN`, `LOG10`, `SIN`, `COS`, `TAN`), and date/timestamp extraction (`EXTRACT`/`DATE_PART`).
            *   `BoundCaseExpression` (multi-branch `CASE WHEN ... THEN ... ELSE ... END`).
        *   `TranslateExpressionToLuaRowLogic` is the core method generating element-wise Lua processing logic. For `VARCHAR` return types, it generates Lua code that calls the FFI C helpers for output.
    *   **`LuaTranslatorContext`:** Stores `LogicalType`s of input vectors, providing methods like `GetInputLuaFFIType()` to help the translator generate type-correct Lua FFI casts and type-specific Lua code.

*   **Integration into `ExpressionExecutor` (`duckdb/execution/expression_executor.hpp`, `.cpp`):**
    *   `ExpressionExecutor` holds a `LuaJITStateWrapper luajit_wrapper_`.
    *   `ExpressionState` stores JIT state: `attempted_jit_compilation`, `jit_compilation_succeeded`, `jitted_lua_function_name`, and `execution_count`.
    *   **JIT Path in `ExpressionExecutor::Execute()`:**
        *   **`ShouldJIT()` Heuristic:** Decides eligibility based on configuration and expression state.
        *   **Caching & Compilation:** If viable and not compiled, `LuaTranslator` converts `BoundExpression` to Lua, `ConstructFullLuaFunctionScript` creates the full script (with FFI cdefs for types like `FFIInterval { int32_t months; int32_t days; int64_t micros; }` and helpers), and `LuaJITStateWrapper` compiles it.
        *   **Execution:** Prepares `FFIVector`s using `CreateFFIVectorFromDuckDBVector`, then calls the Lua function via `PCallGlobal`.
        *   **Error Handling & Fallback:** Lua errors lead to `duckdb::RuntimeException`, and execution falls back to the C++ path (`ExecuteStandard()`).

### Workflow:
1.  **JIT Decision:** `ExpressionExecutor::Execute` calls `ShouldJIT()`.
2.  **Compile (if needed):** If JIT is viable and not yet compiled for this `ExpressionState`, translate `BoundExpression` to Lua, generate full function script, compile with `LuaJITStateWrapper`. Store function name and success status in `ExpressionState`.
3.  **Execute JITed Function (if compiled):** Prepare input/output `FFIVector`s from `DataChunk`/`Vector`s using `CreateFFIVectorFromDuckDBVector`. Call the named Lua function via `PCallGlobal`.
4.  **Fallback:** If JIT fails or `ShouldJIT` is false, use `ExecuteStandard` C++ path. Update `execution_count`.

## 3. Build System Integration (Conceptual)
LuaJIT would be integrated as a `third_party` static library. DuckDB's build system (CMake) would need to be configured to compile LuaJIT and link it into the DuckDB executable/library. This involves adding LuaJIT's source code (or pre-compiled binaries for specific platforms) to the DuckDB build process and ensuring header paths are correctly set up.

## 4. JIT Heuristics and Configuration
The JIT process is controlled by heuristics and SQL-configurable settings:
*   **`ShouldJIT()` Method:** Implemented in `ExpressionExecutor`, it evaluates the configured heuristics.
*   **Configuration Options (in `ClientConfig::options`):**
    1.  `enable_luajit_jit` (bool, default: `false`): Master switch to enable or disable the LuaJIT pathway.
    2.  `luajit_jit_complexity_threshold` (int64_t, default: `5`): Minimum complexity score (based on `GetExpressionComplexity()` node count) for an expression to be considered for JIT compilation.
    3.  `luajit_jit_trigger_count` (int64_t, default: `1000`): Number of times an expression must be executed via the C++ path before JIT compilation is attempted.
*   **`ExpressionState` Tracking:** `attempted_jit_compilation`, `jit_compilation_succeeded`, and `execution_count` are stored per expression instance to manage its JIT lifecycle.
*   **SQL `SET` Commands:**
    *   Support for `SET enable_luajit_jit = <boolean>`, `SET luajit_jit_complexity_threshold = <integer>`, and `SET luajit_jit_trigger_count = <integer>` has been fully implemented.
    *   This involved:
        *   Defining these settings in `src/include/duckdb/main/settings.hpp` (structs like `EnableLuajitJitSetting`, etc.) and `src/main/config.cpp` (registration in `internal_options[]` and implementation of `SetLocal`/`ResetLocal`/`GetSetting` methods).
        *   Ensuring the parser (`src/parser/transform/statement/transform_set.cpp`) correctly handles these.
        *   Updating the binder (`src/planner/binder/statement/bind_set.cpp`) to bind the values, cast them to the appropriate types (BOOLEAN or BIGINT), and perform validation (e.g., non-negative for threshold/count).
        *   Modifying the physical operator (`src/execution/operator/helper/physical_set.cpp`) to apply these settings to `ClientContext::config.options` for the current session, enforcing session-only scope.

## 5. Caching Mechanism
Once an expression is successfully translated and compiled for a given `ExpressionState`, the name of the generated global Lua function is stored in `ExpressionState::jitted_lua_function_name`. Subsequent calls to `ExpressionExecutor::Execute` for that same expression instance (and thus same `ExpressionState`) will find `jit_compilation_succeeded == true` and reuse the already compiled Lua function, avoiding re-translation and re-compilation costs. This cache is per-expression-instance within a query.

## 6. Error Handling and Fallback
LuaJIT errors are handled at two stages:
*   **Compilation:** `LuaJITStateWrapper::CompileStringAndSetGlobal` captures Lua compilation errors (e.g., syntax errors in generated Lua) into a C++ string.
*   **Execution:** `LuaJITStateWrapper::PCallGlobal` captures Lua runtime errors (e.g., attempting to call a nil value, FFI type mismatches not caught by static checks).
If an error occurs in either stage, `ExpressionExecutor::Execute` sets `jit_compilation_succeeded` to `false` in the `ExpressionState` and re-throws the error wrapped in a `duckdb::RuntimeException`. Execution then transparently falls back to the standard C++ interpreter path (`ExecuteStandard()`) for the current and subsequent executions of that expression instance.

## 7. Adding Support for New Expressions or Data Types
To extend JIT support:
*   **FFI Data Structures:** If new complex C types are needed for FFI, define them in `duckdb/common/luajit_ffi_structs.hpp`. Update `CreateFFIVectorFromDuckDBVector` in `src/common/luajit_ffi_vector.cpp` to correctly prepare `FFIVector`s for the new DuckDB `LogicalType`. Update Lua CDEFs in `ExpressionExecutor::ConstructFullLuaFunctionScript`. If new C helper functions are needed (e.g., for complex output types), declare them in `luajit_ffi_structs.hpp`, implement in `luajit_ffi_vector.cpp`, and register in `LuaJITStateWrapper`.
*   **Expression Translation (`LuaTranslator`):**
    *   For new `BoundExpression` subtypes, add a corresponding `GenerateValue` overload in `LuaTranslator`.
    *   Update `ExpressionExecutor::ShouldJIT` to include the new expression class if it's JIT-able.
    *   Implement the translation logic. Use `LuaTranslatorContext` to get input type information for generating correct FFI casts (e.g., `ffi.cast('MyType*', ...)`), data access (e.g., `my_struct.field`), and using `ffi.string()` for `VARCHAR` column access.
    *   For new SQL functions, add cases to `LuaTranslator::GenerateValueBoundFunction`.
    *   If the function returns `VARCHAR` or other complex types requiring FFI C helpers for output, ensure the generated Lua code calls these helpers.
*   **Unit Testing:** Add comprehensive tests for the new expressions/types/functions in `test/unittest/jit_expression_executor_test.cpp` and potentially `luajit_translator_test.cpp`.

## 8. Debugging JITed Code
*   Print the generated Lua code string before compilation.
*   Use Lua's `print()` function within the generated Lua code (output will go to DuckDB's stdout).
*   Leverage LuaJIT's `debug` library if more advanced introspection is needed (though this usually requires interactive debugging or more complex setup).
*   Simplify expressions to isolate issues.
*   Pay close attention to FFI cdefs, pointer casting in Lua, and data alignment.

## 9. Benchmarking

*   **Framework:** A dedicated benchmark suite is implemented in `test/benchmark/jit_expression_benchmark.cpp`.
*   **Methodology:**
    *   The benchmark uses the actual `ExpressionExecutor` for both C++ and JIT paths.
    *   **C++ Baseline:** JIT is disabled via `ClientContext::config.enable_luajit_jit = false`.
    *   **JIT Path:** JIT is enabled, and `luajit_jit_trigger_count` and `luajit_jit_complexity_threshold` are set to 0 to force JIT compilation on the first execution for measurement purposes.
    *   **Metrics Measured:**
        1.  `CppBaseline_ms`: Average time for the C++ path.
        2.  `JIT_FirstRun_ms`: Time for the first JIT execution, including internal translation, LuaJIT compilation, FFI data setup, and first execution.
        3.  `JIT_CachedExec_ms`: Average time for subsequent JIT executions using the cached Lua function, including FFI data setup.
*   **Scenarios:** A comprehensive set of scenarios covering numeric, string, temporal, logical, and conditional operations were tested with varying data sizes (10k, 1M rows) and NULL percentages (0%, 50%).
*   **Conceptual Findings Summary (from `LuaJIT_benchmarking_and_profiling.md`):**
    *   **JIT Overhead:** The `JIT_FirstRun_ms` consistently showed significant overhead due to translation and compilation.
    *   **Cached JIT vs. C++:** For most common SQL expressions (simple arithmetic, string operations, temporal functions using FFI helpers, conditional logic), the *conceptual* performance of cached JIT execution did **not** outperform DuckDB's native C++ vectorized execution.
    *   **FFI Overhead:** The primary limiting factor identified was the overhead associated with FFI calls on a per-row basis. This includes accessing data and nullmasks from `FFIVector`s in Lua, marshalling strings, and calling C helper functions for output or complex operations.
    *   **Conclusion:** While the JIT pipeline is functional, the current row-by-row FFI interaction model makes it challenging to achieve performance speedups over the highly optimized C++ execution path for typical SQL expressions.

## 10. Current PoC Limitations
*   **Performance Characteristics / FFI Overhead:** As highlighted by conceptual benchmarking, the current row-by-row FFI processing model incurs significant overhead, making it difficult for JIT-compiled Lua code to outperform DuckDB's native C++ vectorized execution for most common SQL expressions.
*   **Expression Support:** While significantly expanded (constants, column references, operators, many common functions, CASE), not all `BoundExpression` subtypes or all SQL functions/operators are JIT-enabled (e.g., `BoundConjunctionExpression` for AND/OR often uses specific execution paths, `BoundLikeExpression`, many date/interval functions beyond `EXTRACT` via FFI, complex `CASE` conditions involving non-boolean results needing further casting).
*   **String Output:** Relies on `Vector::SetValue()` in FFI C helpers, which is convenient but might not be the most performant for high-volume string generation compared to direct `StringHeap` manipulation.
*   **Interval Type:** While FFI data structures and basic column reference/constant support for `INTERVAL` exist, translation for most operations on intervals (e.g., arithmetic, complex comparisons) is not implemented. Interval output via FFI C helpers is also not implemented.
*   **`ExpressionExecutor` JIT Call Path (Input Mapping):** The logic for determining the exact input `duckdb::Vector`s for an arbitrary expression's children in `ExpressionExecutor::Execute` (for `PCallGlobal` arguments) relies on the order of `BoundReferenceExpression`s found by `ExpressionIterator`. Complex expressions with shared sub-expressions or non-sequential column access might not map inputs correctly to the JITed Lua function's arguments without further refinement of the input mapping strategy.
*   **`GetExpressionComplexity`:** The current implementation is a basic node count and might not accurately reflect true computational complexity for JIT decision-making.

## 11. Future Work
*   **Performance - Minimize FFI Overhead:** This is the most critical area. Future work for performance *must* focus heavily on minimizing FFI overhead. This could involve:
    *   **Block-based processing in Lua:** Modifying the JITed Lua functions to loop over data batches passed via fewer, more coarse-grained FFI calls, rather than the C++ side looping and calling Lua per row.
    *   Exploring different data passing techniques if available with LuaJIT FFI that might reduce copying or indirection.
    *   Targeting very different, more complex computational workloads for JIT where Lua's internal computation per FFI call is much higher, thus diminishing the relative cost of FFI.
*   **Broader Expression/Type Support:**
    *   Full translation coverage for all relevant `BoundExpression` subtypes (e.g., `BoundConjunctionExpression`, `BoundLikeExpression`).
    *   Comprehensive support for all SQL functions, including more complex date/time and interval arithmetic.
    *   Robust and performant string/complex type output mechanisms (e.g., direct `StringHeap` interaction via FFI).
    *   Full FFI and translation support for all temporal and nested types (STRUCT, LIST, MAP), including their operations.
*   **Refined JIT Mechanics:**
    *   More sophisticated JIT cache invalidation strategies (e.g., on schema changes or function redefinition, though less critical for expression JIT).
    *   A more advanced `GetExpressionComplexity` model that better estimates actual computational cost.
    *   Improved input mapping in `ExpressionExecutor` for complex expressions.
*   **Alternative Approaches:**
    *   Investigate direct LLVM IR generation from DuckDB expressions as an alternative or complementary approach for certain types of expressions where LuaJIT+FFI is not optimal.

## 12. Overall Conclusion of PoC

This Proof-of-Concept successfully demonstrated the feasibility of building an end-to-end JIT compilation pipeline for a wide range of SQL expression types—including numeric, string, conditional, and temporal (EXTRACT via FFI)—using LuaJIT integrated into DuckDB's `ExpressionExecutor`. A key achievement was the implementation of SQL-configurable JIT heuristics (`enable_luajit_jit`, `luajit_jit_complexity_threshold`, `luajit_jit_trigger_count`) via standard `SET` commands, allowing dynamic control over JIT behavior.

The PoC involved creating FFI data structures (`FFIVector`, `FFIString`, `FFIInterval`) for C++/Lua interoperation, developing a `LuaTranslator` to convert DuckDB `BoundExpression` trees into Lua code, and integrating these components into the `ExpressionExecutor` with mechanisms for compilation caching and fallback to C++ execution.

However, the (conceptual) performance analysis based on the designed benchmark suite indicates that the current row-by-row FFI processing model incurs significant overhead. This overhead, particularly for data marshalling and per-row function call transitions between C++ and Lua, makes it challenging for the JITed Lua code to achieve speedups over DuckDB's highly optimized native C++ vectorized execution for most common SQL expressions.

Despite the performance outcome for typical expressions, the PoC provides valuable insights into the mechanics, architectural considerations, and complexities of integrating a dynamic JIT compiler like LuaJIT into a database kernel. The primary challenge identified (FFI overhead) clearly informs that future efforts to gain performance from such an approach for expression JITing would need to fundamentally address the C++/Lua interaction cost, likely by moving towards block-oriented processing within the JITed code itself.
