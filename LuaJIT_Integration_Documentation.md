# DuckDB LuaJIT Integration Proof-of-Concept

## 1. Introduction

This document provides a developer-oriented overview of the proof-of-concept (PoC) integration of LuaJIT for Just-In-Time (JIT) compilation of expressions within a DuckDB-like environment. The primary goal of this PoC is to explore the feasibility and potential performance characteristics of using LuaJIT to accelerate query expression evaluation.

For a consolidated overview of the entire investigation including initial research, evaluations, design, and PoC outcomes, please refer to the [LuaJIT for DuckDB Summary](./LuaJIT_for_DuckDB_Summary.md).

## 2. Architecture Overview

The LuaJIT integration PoC comprises several key C++ components and a defined workflow for JIT compilation and execution.

### Key Components:

*   **`LuaJITStateWrapper` (`duckdb/common/luajit_wrapper.hpp`, `.cpp`):**
    *   Manages LuaJIT Virtual Machine instances (`lua_State*`).
    *   Handles initialization (creating a Lua state, loading standard libraries including FFI).
    *   Provides methods to execute Lua code strings (`ExecuteString`), which includes LuaJIT's compilation of the string to bytecode and then to machine code.
    *   Manages cleanup (`lua_close()`).

*   **FFI Data Structures (`duckdb/common/luajit_ffi_structs.hpp`):**
    *   Defines C-style structs (`FFIVector`, `FFIString`) that represent how DuckDB vector data is exposed to Lua via the Foreign Function Interface (FFI).
    *   `FFIVector`: Contains pointers to raw data buffers (`void* data`), nullmasks (`bool* nullmask`), and element count (`idx_t count`). This allows Lua code to directly read/write C++ data.
    *   `FFIString`: Represents individual string elements with a character pointer (`char* ptr`) and length (`uint32_t len`). An `FFIVector` for strings would have its `data` member point to an array of `FFIString` structs.

*   **Simplified Expression Nodes (`duckdb/planner/luajit_expression_nodes.hpp`):**
    *   A simplified class hierarchy for expression tree nodes (`BaseExpression`, `ConstantExpression`, `ColumnReferenceExpression`, `BinaryOperatorExpression`, `UnaryOperatorExpression`, `CaseExpression`) is used for this PoC.
    *   These nodes define the structure of expressions that the `LuaTranslator` can process. This is distinct from DuckDB's main `BoundExpression` system for simplicity in the PoC.

*   **`LuaTranslator` (`duckdb/main/luajit_translator.hpp`, `.cpp`):**
    *   Responsible for converting the simplified C++ expression trees (composed of `luajit_expression_nodes`) into Lua code strings.
    *   `LuaTranslatorContext` provides contextual information during translation (e.g., names for input vector tables in Lua).
    *   `TranslateExpressionToLuaRowLogic` is the core method that generates Lua code for element-wise processing, including null propagation and type-specific operations.

*   **Integration into `ExpressionExecutor` (Conceptual for PoC - `duckdb/execution/expression_executor.hpp`, `.cpp`):**
    *   The `ExpressionExecutor` is modified to include an instance of `LuaJITStateWrapper`.
    *   `ExpressionState` (part of `expression_executor_state.hpp`) is augmented with JIT-related flags (`attempted_jit_compilation`, `jit_compilation_succeeded`) and the name of the compiled Lua function (`jitted_lua_function_name`).
    *   A conceptual JIT path is added to `ExpressionExecutor::Execute()`. This path would:
        1.  Check if an expression `ShouldJIT()`.
        2.  If not already compiled, translate it using `LuaTranslator`, generate a full Lua function string (including FFI cdefs and a loop over elements), and compile it using `LuaJITStateWrapper::ExecuteString()`. The compiled function would be registered in Lua (e.g., as a global).
        3.  Prepare `FFIVector` arguments pointing to the input `DataChunk` data and output `Vector` data.
        4.  Call the compiled Lua function via FFI, passing the `FFIVector` pointers.
    *   If the JIT path fails or is not applicable, execution falls back to the standard C++ interpreter path.
    *   *(Note: For the PoC, unit tests often drive the JIT compilation and execution steps more directly to test the mechanism, as full integration into `ExpressionExecutor` for arbitrary DuckDB expressions is complex.)*

### Workflow:

1.  **Expression Analysis:** An expression is identified as a candidate for JIT compilation (conceptual `ShouldJIT`).
2.  **Translation:** The C++ expression tree (using PoC nodes) is passed to `LuaTranslator`, which generates a Lua code string representing the per-row logic.
3.  **Lua Function Generation:** This row logic is wrapped into a full Lua function string. This string includes:
    *   `require("ffi")` and FFI C definitions (`ffi.cdef`) for `FFIVector` and `FFIString`.
    *   A Lua function signature that accepts `FFIVector*` arguments for output and inputs, and an element count.
    *   Casting of `void*` data pointers in `FFIVector` to typed pointers (e.g., `int*`, `FFIString*`) within Lua.
    *   A loop (`for i = 0, count - 1 do ... end`) that iterates over the elements.
    *   Inside the loop, the translated row logic is inserted, adapted to use the casted FFI vector arguments.
4.  **Compilation:** The `LuaJITStateWrapper::ExecuteString()` method is called with the full Lua function string. LuaJIT compiles this string to bytecode and then to optimized machine code, making the function available in the Lua environment.
5.  **Execution:**
    *   `FFIVector` (and `FFIString`) structs are prepared on the C++ side, pointing directly to the memory of input and output data (e.g., from `std::vector` in tests, or conceptually from DuckDB `Vector`s).
    *   The compiled Lua function is retrieved from the Lua state (e.g., by name).
    *   Arguments (pointers to `FFIVector`s and the element count) are pushed onto the Lua stack.
    *   `lua_pcall()` is used to execute the JITed Lua function.
    *   The Lua function reads input data, performs calculations, and writes results directly back to the C++ output vector's memory via FFI pointers.

## 3. Build System Integration (Conceptual)

LuaJIT is integrated into DuckDB's CMake-based build system as follows:

*   **LuaJIT Source:** Conceptually, LuaJIT's source code is placed in the `third_party/luajit` directory.
*   **Main `CMakeLists.txt`:**
    *   `add_subdirectory(third_party/luajit)` is added to include LuaJIT's build process.
*   **`third_party/luajit/CMakeLists.txt`:**
    *   This file is responsible for building LuaJIT. For the PoC, it defines a placeholder static library target named `luajit`.
    *   It ensures that LuaJIT's include directories (e.g., `third_party/luajit/src` where `lua.hpp`, `luajit.h` etc., reside) are made available to targets that link against `luajit`.
    *   In a real build, this CMake file would compile LuaJIT's C source files (e.g., `lj_*.c`, `lib_*.c`) into the `luajit` static library.
*   **Linking:** DuckDB's main library target (e.g., `duckdb`) is linked against the `luajit` static library. This makes LuaJIT's functions available and allows DuckDB source files to include LuaJIT headers.

## 4. Adding Support for New Expressions or Data Types

Extending the JIT capabilities involves modifications across several components:

1.  **FFI Data Structures (`luajit_ffi_structs.hpp`):**
    *   If a new data type requires a specific C-style representation for FFI that differs from existing ones (e.g., a complex struct for `DECIMAL` or `INTERVAL`), define it here.
    *   Update the corresponding Lua FFI `cdef` string (typically found in `GenerateFullLuaJitFunction` helpers or a common Lua prelude script) to match the new C struct definition.

2.  **Expression Nodes (`luajit_expression_nodes.hpp`):**
    *   If introducing a new kind of expression (e.g., a new function type, a different type of operator like N-ary), add a new class derived from `BaseExpression`.
    *   Update `LuaJITExpressionType` enum.
    *   If it's a new operator type for existing expression classes (e.g., a new binary operator), update the relevant operator enum (e.g., `LuaJITBinaryOperatorType`).
    *   Add helper functions (e.g., `MakeLua...`) for easily constructing these new nodes.

3.  **Translator Logic (`LuaTranslator`):**
    *   **Add `GenerateValue` Overload:** For a new `BaseExpression` subtype, add a new static `GenerateValue(const NewExpressionType& expr, LuaTranslatorContext& ctx, std::vector<idx_t>& referenced_columns)` method in `luajit_translator.cpp` and declare it in the header.
    *   **Update Dispatch:** Add a `case` for the new `LuaJITExpressionType` in the main `LuaTranslator::GenerateValueExpression` switch statement to call your new `GenerateValue` overload.
    *   **Implement Translation:**
        *   The new `GenerateValue` method must produce a Lua code string that computes the value of the expression.
        *   **Data Access:** For column references, use `ctx.GetInputVectorsTable()` and the column index to generate code like `input_vectors[idx+1].data[i]`. If the column has a special FFI type (like `FFIString`), generate code to convert it to a usable Lua type (e.g., `ffi.string(input_vectors[idx+1].data[i].ptr, input_vectors[idx+1].data[i].len)`). This requires type information, which might need to be passed via `LuaTranslatorContext`.
        *   **Operator Logic:** Translate the operation into equivalent Lua operators or function calls.
        *   **Null Propagation:** Crucially, collect all input `ColumnReferenceExpression` indices in the `referenced_columns` vector. The main `TranslateExpressionToLuaRowLogic` method uses this vector to generate the primary null check: if any direct input is null, the output is null. For complex expressions like `CASE` or functions that have their own null semantics (e.g., `COALESCE`), the `GenerateValue` method itself might need to produce more nuanced Lua code to handle nulls internally before the outer check.
        *   **Boolean Results:** If the expression results in a boolean, ensure the Lua code evaluates to Lua `true` or `false`. The `TranslateExpressionToLuaRowLogic` method will then convert this to `1` or `0` when assigning to `output_vector.data[i]` if the target C++ type is integer.
        *   **String Results:** This is complex. If an expression produces a string, the generated Lua code (e.g., from `CONCAT`) will result in a Lua string. Assigning this directly to `output_vector.data[i]` (which is `void*`) is incorrect. The output `FFIVector` needs to be for `FFIString`, and the Lua code must populate `output_vector.data[i].ptr` and `output_vector.data[i].len`. This usually requires calling back to C (via FFI) to allocate memory in DuckDB's string heap or using pre-allocated output buffers, which is an advanced topic not fully covered in the PoC.

4.  **Unit Testing (Crucial):**
    *   **FFI Tests (`test/unittest/luajit_ffi_test.cpp`):** If new FFI data structures or complex data marshalling for existing types (like writing strings back to C++) are introduced, add tests here to verify the FFI bridge in isolation.
    *   **Translator Tests (`test/unittest/luajit_translator_test.cpp`):** For every new expression type, operator, or function, add test cases that:
        *   Construct the PoC expression node.
        *   Call `LuaTranslator::TranslateExpressionToLuaRowLogic()`.
        *   Assert that the generated Lua code string for the row logic is exactly as expected, including correct data access, operator translation, and null-handling logic.
    *   **Executor Tests (`test/unittest/jit_expression_executor_test.cpp`):** Add end-to-end tests that:
        *   Use `ExpressionExecutor` (or the PoC's direct JIT call mechanism).
        *   Test the new JITed expression with sample data in `FFIVector`s.
        *   Verify that the C++ output vectors contain the correct results after the JITed Lua function executes.

## 5. Debugging JITed Code

Debugging code that crosses the C++/Lua boundary and involves JIT compilation can be challenging.

*   **Inspect Generated Lua Code:**
    *   The most direct way is to get the Lua code string produced by `LuaTranslator::TranslateExpressionToLuaRowLogic()` and the full function string from `GenerateFullLuaJitFunction` (or similar helpers). Print this string to the console or a log file.
    *   Manually review the Lua code for correctness, especially FFI casts, array indexing (0-based in Lua loops, 1-based for table access if `input_vectors` is a table), and null logic.

*   **Printing from Lua:**
    *   Temporarily add `print()` statements within the generated Lua code string at various points to trace execution flow and inspect intermediate values.
    *   Example: `print("i=", i, " col0_val=", input_vectors[1].data[i], " col0_null=", input_vectors[1].nullmask[i])`
    *   Ensure that the C++ environment where `LuaJITStateWrapper::ExecuteString()` or `lua_pcall()` is called allows Lua's `print` output to be visible (e.g., it usually goes to `stdout`).

*   **FFI Issues:** These are common sources of bugs:
    *   **`ffi.cdef` Mismatches:** Ensure the C struct definitions in Lua `ffi.cdef` exactly match the C++ struct definitions (`FFIVector`, `FFIString`). Pay attention to data types, member names, and packing/alignment (though usually not an issue for simple structs).
    *   **`ffi.cast` Errors:** Incorrectly casting pointers in Lua (e.g., `ffi.cast("int*", void_ptr)` when `void_ptr` actually points to `double`s) can lead to crashes or incorrect data. Double-check type casts against the actual data being passed.
    *   **Pointer Validity:** Ensure C++ pointers passed to Lua (via `lua_pushlightuserdata` for `FFIVector*`) remain valid for the entire duration of the Lua call. Dangling pointers will cause crashes.
    *   **Data Alignment:** Usually handled by LuaJIT FFI, but be aware if working with SIMD types or unusual C structs.

*   **LuaJIT's Tools (for advanced debugging):**
    *   If a problematic Lua snippet is isolated, it can be run directly with the `luajit` command-line interpreter.
    *   LuaJIT has a `-jdump` option that provides detailed information about the JIT compiler's activity, including dumped bytecode, IR, and assembled machine code. This is advanced but powerful for understanding JIT behavior or diagnosing miscompilations.
    *   Use `jit.opt.start(3)` or other `jit.opt` flags in Lua for more aggressive compilation or specific JIT features.
    *   The `jit.util` module can provide introspection into JITed traces.

## 6. Benchmarking

Performance assessment is done using micro-benchmarks:

*   **Framework:** `test/benchmark/jit_expression_benchmark.cpp` contains the benchmarking code.
*   **Methodology:**
    *   Measures translation time, LuaJIT compilation time, and JITed execution time separately.
    *   Compares JITed execution time against a manually written C++ loop performing the same logic (C++ Baseline).
    *   Uses `std::chrono` for timing.
    *   Tests different expression scenarios and data sizes.
*   **Analysis:** The results and analysis (conceptual in this PoC) are documented in `LuaJIT_benchmarking_and_profiling.md`. This includes discussing overheads, potential speedups, and bottlenecks.

## 7. Current PoC Limitations

This proof-of-concept has several known limitations:

*   **Limited Expression/Operator Support:** Only a subset of SQL expressions, operators, and functions are translated.
*   **Simplified Expression Nodes:** Uses PoC-specific expression nodes, not yet fully integrated with DuckDB's main `BoundExpression` hierarchy. Translation from `BoundExpression` to `luajit_expression_nodes::BaseExpression` is a missing step for full integration.
*   **String FFI for Output:** Writing string results from Lua back to C++ `FFIVector<FFIString>` is complex due to memory management of new string data. The PoC primarily focuses on reading strings or string operations that produce non-string results.
*   **Type System:** Full type propagation from C++ to Lua and ensuring type correctness in generated Lua for all DuckDB types (e.g., DECIMAL, DATE, TIMESTAMP, INTERVAL, STRUCT, LIST) is not complete. DATE/TIMESTAMP are handled as integers.
*   **`ExpressionExecutor` JIT Path:** The JIT execution path within `ExpressionExecutor::Execute()` is still somewhat conceptual. Unit tests currently drive JIT execution more directly to validate the core mechanisms.
*   **Error Handling:** Error reporting from Lua runtime errors back to C++ is basic (prints to stderr). Robust error object propagation is needed.
*   **No JIT Cache:** Compiled Lua functions are not yet cached and reused across multiple `Execute` calls for the same expression structure. Each JIT attempt (conceptually) recompiles.
*   **DataChunk/Vector Integration:** Full, robust conversion from DuckDB's `DataChunk` and `Vector` (with `UnifiedVectorFormat` complexities like constant, dictionary vectors) to flat `FFIVector` representations needs careful implementation. The PoC uses `std::vector` for data backing in tests.

## 8. Future Work

Based on the "Refinement and Further Operator Integration" plan, future work includes:

*   **Full DuckDB Expression Integration:** Implement translation from DuckDB's `BoundExpression` types to the LuaJIT PoC expression nodes or directly to Lua.
*   **Robust Type Handling:** Full support for all relevant DuckDB data types in the FFI layer and translator, including proper casting in Lua (e.g., using type information passed in `LuaTranslatorContext`).
*   **Advanced String Output:** Implement mechanisms for Lua to write string results back to `FFIVector<FFIString>`, likely involving FFI calls to C++ memory management routines (e.g., allocating from DuckDB's string heap).
*   **Complex Data Types:** Add FFI and translator support for `STRUCT` and `LIST` types.
*   **Wider Function/Operator Support:** Implement translation for more SQL functions, operators, and remaining expression types (e.g., full SQL `CASE`, date functions via FFI calls to C helpers).
*   **JIT Cache:** Implement a cache for compiled Lua functions within `ExpressionExecutorState` or `ExpressionExecutor` to avoid recompilation.
*   **Error Propagation:** Improve error handling to propagate Lua errors as C++ exceptions or error objects.
*   **Performance Optimization:** Profile and optimize critical paths in the FFI bridge, Lua code generation, and Lua execution.
*   **Vector Type Handling:** Properly handle different DuckDB vector types (Flat, Constant, Dictionary) when preparing data for `FFIVector`. This might involve flattening dictionary vectors or specializing Lua code for constant vectors.
*   **Configuration:** Add session/system level switches to enable/disable JIT and control its behavior.
