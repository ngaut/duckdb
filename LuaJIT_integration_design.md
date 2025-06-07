# High-Level Design: LuaJIT Integration in DuckDB

## 1. Introduction

This document outlines a high-level design for integrating LuaJIT as a Just-In-Time (JIT) compilation engine within DuckDB. The primary goal is to accelerate query execution, particularly for complex expressions in analytical queries, by translating parts of the query plan into Lua code and then JIT-compiling this Lua code using LuaJIT. This design leverages LuaJIT's fast compilation, high-performance execution, and powerful Foreign Function Interface (FFI).

## 2. Goals

*   Improve performance of CPU-bound expression evaluation.
*   Keep compilation overhead low, suitable for analytical query latencies.
*   Minimize data copying between DuckDB's C++ environment and Lua.
*   Ensure robust error handling and provide a fallback to existing interpreter.
*   Maintain DuckDB's ease of use and embedding.

## 3. Target Areas for JIT Compilation

The initial focus for JIT compilation will be **scalar expression evaluation** within DuckDB's vectorized query execution engine. This includes expressions found in:

*   `SELECT` clause projections
*   `WHERE` clause filters
*   `HAVING` clause filters
*   `CASE` statements
*   Function calls (built-in and potentially user-defined if they can be translated to Lua)
*   Arithmetic operations, comparisons, logical operations, etc.

Aggregations and window functions are more complex and will be considered as future targets after successfully implementing and evaluating expression JIT compilation.

## 4. Translation Process (DuckDB Expression -> Lua Code)

DuckDB currently parses SQL into its own internal representation (IR) or expression tree. This existing IR will be the source for Lua code generation.

1.  **Identify JIT Candidates:** During query planning or before query execution, expressions suitable for JIT compilation are identified. Heuristics might be applied (e.g., complexity, estimated cost, presence of functions amenable to Lua translation).
2.  **Expression Tree Traversal:** The DuckDB expression tree for a candidate expression will be traversed.
3.  **Lua Code Generation:** Each node in the expression tree will be translated into an equivalent Lua code snippet.
    *   Constants: Translated directly into Lua numbers, strings, booleans.
    *   Column References: Will be represented as accesses to data pointers passed into the Lua function (via FFI).
    *   Function Calls:
        *   Simple built-in functions (e.g., `abs()`, `lower()`, arithmetic ops) can be translated to direct Lua equivalents or LuaJIT FFI calls to underlying C implementations if they are faster or handle specific types (e.g., DuckDB's `DECIMAL`).
        *   More complex DuckDB functions might be exposed to Lua via FFI.
    *   Control Flow (e.g., `CASE`): Translated into Lua `if/then/else` constructs.
4.  **Lua Function Structure:** The generated Lua code for an expression will be wrapped in a Lua function. This function will take parameters representing input vectors (as FFI pointers to DuckDB's `Vector` data structures or their relevant components) and an output vector.

    ```lua
    -- Conceptual Lua function structure for an expression
    -- 'vector_process_template' would be a pre-defined Lua helper
    -- or this logic generated dynamically.
    local ffi = require("ffi")

    -- FFI definitions for DuckDB structures (simplified)
    ffi.cdef[[
        typedef struct {
            void* data;         // Pointer to the data buffer
            // bool* nullmask;  // Pointer to nullmask
            // uint64_t count;   // Number of elements in the vector (passed separately)
            // ... other necessary vector components like type info, sel_vector
        } duckdb_vector_data;

        // Example: Expose a DuckDB C function for use in Lua
        // int my_duckdb_function(int a, int b);
    ]]

    -- Generated function for a specific expression, e.g., col_a + col_b * 2
    return function(out_vec_data, col_a_data, col_b_data, count, sel_vector)
        -- sel_vector is an optional selection vector (array of indices)
        local out_ptr = ffi.cast(out_vec_data.type .. "*", out_vec_data.data)
        local a_ptr = ffi.cast(col_a_data.type .. "*", col_a_data.data)
        local b_ptr = ffi.cast(col_b_data.type .. "*", col_b_data.data)
        -- Handling for nulls and selection vectors needs to be integrated here

        for i = 0, count - 1 do
            local idx = sel_vector and sel_vector[i] or i -- Adjust index if selection vector is used
            -- TODO: Add null handling for a_ptr[idx] and b_ptr[idx]
            out_ptr[idx] = a_ptr[idx] + b_ptr[idx] * 2
            -- TODO: Set nullmask for out_ptr[idx] if needed
        end
    end
    ```

## 5. Compilation and Execution Pipeline

1.  **Lua State Management:**
    *   DuckDB will manage a pool of LuaJIT states (`lua_State`), likely one per worker thread to ensure thread safety and allow concurrent JIT compilation and execution.
    *   Each Lua state will be initialized with LuaJIT libraries (base, FFI) and any necessary FFI C definitions for DuckDB structures and functions.
2.  **Code Compilation:**
    *   The generated Lua code string is loaded into a Lua state using `luaL_loadstring`.
    *   LuaJIT automatically JIT-compiles this code to machine code when it's first executed (or via `jit.compile` if explicit control is needed).
    *   Compiled Lua functions can be cached (e.g., in the Lua registry or a C++ map) based on the expression structure to avoid recompilation for identical expressions.
3.  **Execution:**
    *   The JIT-compiled Lua function is retrieved from the cache or compiled on the fly.
    *   Pointers to DuckDB's `Vector` data (or relevant parts like data buffers, nullmasks, type information, and selection vectors) are passed to the Lua function using the Lua C API (e.g., `lua_pushlightuserdata` for pointers, `lua_pushinteger` for counts) and FFI.
    *   The Lua function is called via `lua_pcall`.
    *   The Lua function iterates over the vector elements (respecting any selection vector), applies the expression logic, and writes results to the output vector's data buffer and nullmask, all via FFI pointers.

## 6. Data Marshalling (C++ <-> Lua via FFI)

This is the most critical part for performance. The goal is **zero-copy** access to DuckDB's columnar vector data.

*   **DuckDB `Vector` Structures:**
    *   FFI C definitions (`ffi.cdef`) will be created that precisely mirror the layout of DuckDB's `Vector` class or a simplified C-compatible struct that provides access to its essential data buffers (`data_ptr`, `nullmask`), count, type information, and selection vector.
    *   Care must be taken with C++ features like `std::vector` within these structs; FFI works best with C-style arrays or pointers. It might be necessary to expose accessors or use `data()` pointers for `std::vector`.
*   **Passing Data:**
    *   Pointers to the actual data buffers and nullmasks of DuckDB vectors will be passed to Lua functions.
    *   LuaJIT's FFI will allow the JIT-compiled code to directly dereference these pointers and read/write data in place.
    *   Type information (e.g., `TypeId` from DuckDB) will also be passed to Lua, so the Lua FFI code can `ffi.cast` the `void*` data pointers to the correct C types (e.g., `int*`, `double*`, `char**`).
*   **Null Handling:**
    *   Nullmasks will be passed as `bool*` or `uint64_t*` (for bitmasked nulls) via FFI.
    *   The generated Lua code must include logic to check the nullmask for input vectors and set the nullmask for the output vector.
*   **Strings:** String data (e.g., `duckdb_string_t`) will require careful FFI definition. If strings are stored in a central heap or string dictionary, pointers to these strings can be manipulated.
*   **Ownership and Lifecycles:**
    *   DuckDB's C++ side retains ownership of vector data.
    *   Lua code operates on these borrowed pointers. This is safe as long as the Lua execution is synchronous and completes within the lifetime of the C++ data.
    *   No Lua garbage collection should attempt to manage DuckDB's primary vector data.

## 7. Error Handling

*   **Lua Compilation Errors:** Errors during `luaL_loadstring` (syntax errors in generated Lua) will be caught and reported by DuckDB. This would typically indicate a bug in the Lua code generator.
*   **Lua Runtime Errors:** Errors during `lua_pcall` (e.g., type mismatches, arithmetic errors if not handled in Lua code, FFI errors) will be caught.
    *   DuckDB will retrieve the error message from the Lua stack.
    *   The system should log the error and potentially the generated Lua code for debugging.
*   **FFI Errors:** Incorrect FFI definitions or misuse of FFI (e.g., incorrect pointer casting) can lead to crashes. Thorough testing of FFI bindings is essential.

## 8. Fallback Mechanism

*   If JIT compilation or execution fails for any reason (compilation error, runtime error in Lua, heuristic decides not to JIT), DuckDB **must** fall back to its existing, well-tested expression interpreter.
*   This ensures robustness and that queries can always complete, even if the JIT pathway has issues.
*   A configuration option to disable LuaJIT compilation entirely (for debugging or performance comparison) should be provided.

## 9. Implementation Considerations

*   **Incremental Development:** Start with a small subset of simple expressions and functions.
*   **Benchmarking:** Continuously benchmark the performance (compilation + execution) against the existing interpreter to validate benefits.
*   **Testing:** Extensive testing is required for the Lua code generator, FFI bindings, and various data types/expression combinations.
*   **Memory:** Monitor memory usage of Lua states and compiled code cache.

## 10. Conclusion

Integrating LuaJIT offers a promising avenue for enhancing DuckDB's expression evaluation performance. By leveraging LuaJIT's FFI for zero-copy data access and its fast JIT compiler, DuckDB can potentially achieve significant speedups for CPU-bound analytical queries. Careful design of the translation layer, data marshalling via FFI, and robust error handling with a fallback mechanism will be key to a successful implementation.
