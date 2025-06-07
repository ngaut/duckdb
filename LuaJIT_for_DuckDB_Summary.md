# LuaJIT for DuckDB: Research, Evaluation, Design, and PoC Summary

## 1. Introduction

This document summarizes the investigation into using LuaJIT as a Just-In-Time (JIT) compilation framework for DuckDB. The goal was to explore ways to enhance DuckDB's performance, particularly for CPU-bound analytical query expressions. The investigation included initial research on JIT in databases, evaluation of LuaJIT against other frameworks, a high-level integration design, and a proof-of-concept (PoC) for core functionality.

*(Note: Initial broad research into JIT in databases like HyPer, ClickHouse, and Peloton, and evaluation of other JIT frameworks like LLVM and asmjit, were conducted prior to focusing on LuaJIT. These steps highlighted the trade-offs between compilation speed, runtime performance, and integration complexity, guiding the specific investigation into LuaJIT.)*

## 2. LuaJIT Evaluation for DuckDB (`LuaJIT_evaluation_for_DuckDB.md`)

LuaJIT was evaluated as a candidate JIT framework for DuckDB, focusing on its unique strengths.

*   **Key LuaJIT Features:**
    *   **High Performance:** Known for its very fast trace JIT compiler.
    *   **FFI (Foreign Function Interface):** A crucial feature allowing efficient, low-overhead calls to C functions and direct access to C data structures from Lua code.
    *   **Fast Compilation Times:** Significantly faster than heavier frameworks like LLVM.
    *   **Small Footprint & Embeddable:** Well-suited for integration into applications like DuckDB.

*   **Evaluation Areas:**
    *   **C++ Interoperability:** Excellent, primarily via the Lua C API and LuaJIT's FFI. The FFI allows bypassing much of the manual binding code typically needed.
    *   **Data Marshalling:** The FFI is key to enabling zero-copy (or near zero-copy) access to DuckDB's native C++ data structures (e.g., `Vector` data buffers and nullmasks) from Lua. This is critical for performance.
    *   **Suitability for DuckDB:**
        *   **Pros:** Fast compilation, good runtime performance, powerful FFI for data access, higher-level language for expressing JIT logic compared to raw assembly or LLVM IR.
        *   **Cons:** Introduction of another language (Lua) and runtime, potential complexities in managing the boundary between C++ and Lua (e.g., error handling, object lifetimes if not careful), and the need for team expertise in Lua.
    *   **Best Practices for Embedding:** Covered Lua state management (likely per-thread for DuckDB), error handling (using `lua_pcall`), memory management considerations between Lua's GC and C++, and designing C-friendly APIs for FFI.

*   **Preliminary Recommendation:** LuaJIT was deemed a viable and promising candidate, particularly if its FFI could be effectively used for direct data access to DuckDB's vectors. Its main advantages were seen as fast compilation and a higher-level abstraction for JIT code compared to lower-level JIT tools.

## 3. High-Level Design for LuaJIT Integration (`LuaJIT_integration_design.md`)

A high-level design was proposed for integrating LuaJIT into DuckDB's query execution engine.

*   **Target for JIT:** Initial focus on **scalar expression evaluation** within DuckDB's vectorized model (e.g., `SELECT` list expressions, `WHERE`/`HAVING` filters).
*   **Translation Process:**
    1.  DuckDB's existing expression tree/IR would be traversed.
    2.  Nodes in this tree would be translated into Lua code strings.
    3.  This Lua code would be wrapped in a Lua function designed to operate on data passed via FFI.
*   **Compilation/Execution Pipeline:**
    1.  Manage LuaJIT states (e.g., per thread).
    2.  Load generated Lua code string into LuaJIT, which then JIT-compiles it.
    3.  Cache compiled Lua functions.
    4.  During query execution, pass pointers to DuckDB `Vector` data (buffers, nullmasks, count, type info, selection vectors) to the Lua function using FFI.
    5.  The Lua function executes, reading from input vector FFI pointers and writing to the output vector FFI pointer.
*   **Data Marshalling (FFI Focus):**
    *   Zero-copy access is paramount. LuaJIT FFI would allow Lua code to directly operate on memory owned by DuckDB's C++ vectors.
    *   `ffi.cdef` would be used in Lua to define C-compatible structures mirroring essential parts of DuckDB's `Vector` layout.
*   **Error Handling:** Robust mechanisms for catching Lua compilation and runtime errors, and propagating them to DuckDB, with a clear fallback.
*   **Fallback Mechanism:** Crucially, if JIT compilation or execution fails, DuckDB must revert to its existing expression interpreter.

## 4. Proof-of-Concept (PoC) (`LuaJIT_PoC_expressions.md`)

A PoC was developed (as code documented in Markdown) to validate the core mechanics of JITing simple expressions with LuaJIT and C++.

*   **PoC Components:**
    *   Simplified C++ representation for expressions (e.g., `col + const`, `col > col`).
    *   A C++ `LuaTranslator` to convert these expressions into Lua code strings. The Lua code was specifically structured to use FFI for array processing.
    *   C++ `main` function to:
        *   Initialize a (mocked) LuaJIT state.
        *   Translate sample expressions.
        *   Load/compile the Lua code.
        *   Prepare C++ integer arrays (mocking `Vector` data).
        *   Pass pointers to these arrays to the Lua function using `lua_pushlightuserdata`.
        *   Call the Lua function, which directly manipulated the C++ array data.
        *   Verify results.
*   **Key Outcomes & Observations:**
    *   **FFI Data Marshalling:** The PoC confirmed that passing C++ array pointers to Lua and operating on them directly via FFI is feasible and conceptually straightforward for basic types. This validated the zero-copy access approach.
    *   **Ease of Use:** LuaJIT's FFI (`ffi.cdef` and pointer casting in Lua) was found to be powerful for C interop.
    *   **Overhead (Conceptual):**
        *   LuaJIT's compilation speed is expected to be very low for small, generated expression strings.
        *   The call overhead from C++ to Lua, while present, is minimized by FFI. For sufficiently complex expressions, JIT gains should outweigh this.
        *   FFI data access speed is near-native.
    *   **Challenges:** The PoC highlighted the importance of correct FFI type definitions and careful Lua stack management in the C++ host. Debugging across the language boundary was noted as a potential complexity.
    *   **Feasibility:** The PoC successfully demonstrated the fundamental viability of the proposed LuaJIT integration for expression JITing.

## 5. Overall Conclusion and Next Steps

The investigation suggests that LuaJIT is a strong candidate for introducing JIT compilation capabilities into DuckDB, primarily for accelerating scalar expression evaluation. Its key advantages are very fast compilation times (crucial for an analytical system that handles ad-hoc queries) and a powerful FFI that enables efficient, zero-copy access to DuckDB's native data structures.

While challenges exist, such as managing an additional language runtime and the complexities of the C++/Lua boundary, the PoC demonstrated that the core technical hurdles can be overcome.

**Recommended Next Steps (if pursuing this further):**

1.  **Real-world Benchmarking:** Implement the PoC with actual LuaJIT linking and benchmark its performance (compilation + execution) against DuckDB's current interpreter for a representative set of expressions and data types.
2.  **FFI for DuckDB Vectors:** Develop robust FFI definitions for DuckDB's actual `Vector` and `string_t` (and other relevant) C++ structures. This is the most critical technical step.
3.  **Refine Translator:** Expand the C++-to-Lua translator to cover a broader range of DuckDB expressions, functions, and data types, including comprehensive null handling.
4.  **Integrate into DuckDB:** Carefully integrate the LuaJIT execution path into DuckDB's query execution model, including thread-local Lua states, code caching, and the fallback mechanism.
5.  **Extensive Testing:** Rigorous testing of correctness, performance, and stability across various query types and data.
