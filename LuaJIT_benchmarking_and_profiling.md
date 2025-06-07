# LuaJIT Expression Benchmarking and Profiling

## 1. Introduction

This document outlines the setup for micro-benchmarks designed to evaluate the performance characteristics of JIT-compiling expressions using LuaJIT within a DuckDB-like C++ environment. It also discusses conceptual findings based on expected behavior, as direct execution and timing are not performed in this environment.

The benchmarks aim to measure:
- **Translation Time:** Time taken by the C++ `LuaTranslator` to convert a PoC expression tree into Lua code for row processing.
- **Compilation Time:** Time taken by LuaJIT to compile the generated Lua function string.
- **JIT Execution Time:** Time taken to execute the compiled Lua function over a dataset using FFI for data exchange.
- **C++ Baseline Time:** Time taken by a manually written C++ loop performing the equivalent operation directly on the data.

## 2. Benchmark Setup

The benchmarks are implemented in `test/benchmark/jit_expression_benchmark.cpp`.

- **Environment:** Conceptual C++ environment with LuaJIT integration.
- **Timing:** `std::chrono::high_resolution_clock` is used for measuring durations in milliseconds (ms) or microseconds (µs).
- **Data:** Input data (numeric and string) is pre-generated in `std::vector`s. `FFIVector` and `FFIString` structs are used to pass pointers to this data to LuaJIT (zero-copy). Data sizes are varied (e.g., 1,000, 100,000 elements).
- **Iterations:** Execution times for both JIT path and C++ baseline are averaged over multiple iterations to get stable readings. Translation and compilation times are typically measured once per setup.

## 3. Benchmark Scenarios Implemented

1.  **Scenario A: Simple Arithmetic (`col0 + col1`)**
    *   **Input:** Two integer vectors.
    *   **Output:** One integer vector.
    *   **Operation:** Element-wise addition with null propagation.
    *   **C++ Baseline:** Loop performing `out[i] = col0[i] + col1[i]` with null checks.
    *   **LuaJIT Path:** `LuaTranslator` generates code for `input1_data[i] + input2_data[i]`. Lua function takes two input `FFIVector*` (int type) and one output `FFIVector*`.

2.  **Scenario D: String Operation (`length(col0 || 'suffix') > threshold`)**
    *   **Input:** One string vector (`FFIVector` pointing to `FFIString` arrays).
    *   **Output:** One integer vector (boolean 0/1).
    *   **Operation:** For each input string, append a constant suffix, calculate the length of the result, and compare against a threshold. Null propagation for input string.
    *   **C++ Baseline:** Loop performing `std::string temp = string_store[i] + suffix; out[i] = (temp.length() > threshold) ? 1 : 0;` with null checks.
    *   **LuaJIT Path:** The Lua row logic is manually crafted for this benchmark to correctly use `ffi.string()` for the input `FFIString` and perform concatenation and length checks in Lua. The output is a boolean (0/1). This tests FFI for string inputs and Lua string manipulation.

    *(Scenarios B - Simple Comparison (`col0 > col1`) and C - Expression with Constant (`col0 * 10`) were planned but might share similar characteristics to Scenario A for numeric types. Their specific results would differ based on operation complexity but follow the same measurement pattern.)*

## 4. Conceptual Benchmark Results (Illustrative Table)

Since code execution is not possible, actual timing data cannot be generated. The table below is **illustrative** of what one might expect and what to look for. Values are placeholders.

| Scenario                    | DataSize | TranslationTime (ms) | CompilationTime (ms) | JITExecutionTime (ms) | CppBaselineTime (ms) | JIT Speedup (vs C++) | Notes                               |
|-----------------------------|----------|----------------------|----------------------|-----------------------|----------------------|----------------------|-------------------------------------|
| A_AddInt                    | 1,000    | 0.01                 | 0.05                 | 0.1                   | 0.02                 | 0.2x                 | Overhead dominates for small data   |
| A_AddInt                    | 100,000  | 0.01                 | 0.05                 | 5.0                   | 2.0                  | 0.4x                 | FFI overhead, simple op             |
| D_StringConcatAndLength     | 1,000    | 0.01                 | 0.08                 | 0.5                   | 0.2                  | 0.4x                 | String ops, FFI overhead            |
| D_StringConcatAndLength     | 100,000  | 0.01                 | 0.08                 | 35.0                  | 20.0                 | 0.57x                | Lua string ops might be slower      |
| (More complex numeric expr) | 100,000  | 0.05                 | 0.15                 | 10.0                  | 15.0                 | 1.5x                 | JIT potentially faster for complex  |

**Interpreting Conceptual Results:**

*   **Translation Time:** Expected to be very low (microseconds to tens of microseconds) as it's simple string manipulation for the PoC translator.
*   **Compilation Time:** LuaJIT is very fast. Expected to be low (tens of microseconds to a few milliseconds, depending on complexity of generated Lua code).
*   **JIT Execution Time vs. C++ Baseline:**
    *   For very simple arithmetic on numeric types (like `A_AddInt`), the JIT path (including FFI call overhead for each element access from Lua) might be **slower** than a direct C++ loop. The C++ compiler can heavily optimize simple loops.
    *   For string operations (`D_StringConcatAndLength`), Lua's string manipulation is efficient, but the FFI overhead to bring string data (pointer/length) into Lua and then operate might still make it slower than direct C++ `std::string` operations for simple cases.
    *   **Where JIT Might Win (Conceptual):** JIT is more likely to show benefits when:
        *   The expression is significantly more complex (many operations, function calls that can be inlined in Lua).
        *   The C++ interpreter has significant dispatch overhead per operation or per row, which JIT eliminates by creating specialized code. (Our C++ baseline is a direct loop, so it has minimal overhead).
        *   Data access patterns in LuaJIT allow for better cache utilization than the C++ version (less likely for simple vector iteration).
        *   LuaJIT's trace compiler can perform optimizations that a C++ compiler might not for a generic interpreter loop.
*   **FFI Overhead:** Each access to `FFIVector.data[i]` or `.nullmask[i]` from Lua code involves FFI overhead. While LuaJIT's FFI is fast, for tight loops on simple data, this per-element overhead can accumulate and be significant compared to direct C++ pointer access.

## 5. Analysis and Observations (Conceptual)

*   **Overhead of JIT:** For small data sizes or very simple expressions, the combined overhead of translation, compilation, and FFI calls will likely make the LuaJIT path slower than a highly optimized C++ direct execution path.
*   **Compilation is Fast:** LuaJIT's compilation is a key advantage over systems like LLVM for query JITing, making it suitable for dynamic query environments.
*   **FFI is Critical:** The performance of the JITed code heavily depends on how efficiently data can be exchanged via FFI. Zero-copy access (passing pointers) is essential, but per-element FFI access from Lua still has costs.
*   **String Operations:**
    *   Reading strings (via `FFIString` and `ffi.string()`) in Lua is feasible and relatively efficient.
    *   Manipulating strings in Lua (concat, length, find) is also efficient within Lua.
    *   The main challenge for string-heavy workloads is writing string *results* back to C++ `FFIVector<FFIString>` structures, as this involves memory management for the new string data. This was sidestepped in the benchmark by having string operations produce boolean/numeric results.
*   **Potential Bottlenecks:**
    *   Per-element FFI access from Lua in tight loops for very simple operations.
    *   Memory bandwidth if data access is not optimized (though this affects C++ too).
    *   Complex data type marshalling (not deeply explored in these benchmarks beyond basic FFIString).
*   **When JIT is Favored:**
    *   Complex expressions where the translation and compilation overhead is amortized over many operations saved per row.
    *   When the alternative is a C++ interpreter with high dispatch costs.
    *   If LuaJIT's trace compiler can achieve specific optimizations (e.g., across multiple "fused" operations within the Lua code) that are hard to get in a generic C++ interpreter.

## 6. Profiling (Conceptual)

Actual profiling (using tools like `perf` on Linux, or LuaJIT's own profiler `jit.p`) would be needed to:

*   Identify exact hotspots within the JITed Lua code.
*   Quantify FFI call overhead more precisely.
*   Analyze cache effects.
*   Compare instruction paths of JITed code vs. C++ baseline.

Without execution, we can only hypothesize based on LuaJIT's known strengths (trace compilation, fast FFI) and weaknesses (FFI overhead in very tight loops on trivial ops).

## 7. Conclusion

The micro-benchmarks (even conceptually) highlight that LuaJIT offers very fast translation and compilation, making it attractive for dynamic JIT. However, for simple element-wise operations on large vectors, the FFI overhead per element can make it hard to beat optimized, direct C++ execution. The benefits of LuaJIT are more likely to appear with more complex expressions where the per-row work done in JITed Lua code is substantial enough to amortize FFI costs and compilation overhead. String operations are viable, especially for reading and processing, but outputting new strings via FFI requires careful design.
