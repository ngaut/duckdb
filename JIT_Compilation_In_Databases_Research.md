# JIT Compilation Approaches in Database Systems: HyPer, ClickHouse, and Peloton

This document summarizes existing Just-In-Time (JIT) compilation approaches in database systems, focusing on HyPer, ClickHouse, and Peloton, based on publicly available information and general knowledge. It outlines common patterns, challenges, and successful strategies.

## 1. HyPer

*   **Strategy:** HyPer, a high-performance main-memory database system (later acquired and integrated into Tableau as HyperDB), is renowned for its approach of compiling entire SQL queries (or significant portions) into machine code. It translates queries into LLVM Intermediate Representation (IR), which is then optimized by LLVM and compiled into native machine code.
*   **Granularity:** Query-level compilation. The goal is to create highly specialized code for each specific query.
*   **Key Concepts:**
    *   **Data-Centric Code Generation:** The generated code is tailored to the specific data types, cardinalities, and operations of the query, minimizing overhead.
    *   **Push-Based Execution:** Data is pushed through the compiled query operators.
    *   **Avoidance of Interpretive Overhead:** By compiling queries, HyPer eliminates the overhead associated with traditional Volcano-style interpretation of query plans.
    *   **Efficient Use of CPU Resources:** Compiled code can often make better use of CPU caches and instruction pipelines.
*   **Challenges:**
    *   **Compilation Overhead:** LLVM compilation can be time-consuming. For short-running OLTP-like queries, this overhead might exceed the execution time benefits.
    *   **Development Complexity:** Building and maintaining an LLVM-based JIT compilation engine is a significant engineering task.
*   **Successes:**
    *   Demonstrated substantial performance improvements for analytical (OLAP) queries, often outperforming traditional interpreters by significant margins.
    *   Influenced many subsequent research projects and commercial systems.

## 2. ClickHouse

*   **Strategy:** ClickHouse, an open-source columnar database system designed for OLAP, utilizes JIT compilation primarily for expression evaluation and parts of query execution, rather than always compiling entire query plans.
*   **Granularity:** Expression-level and operator-level JIT compilation. For example, it can compile arithmetic expressions, predicates in `WHERE` clauses, or arguments to aggregate functions.
*   **Key Concepts:**
    *   **Vectorized Query Execution:** ClickHouse's core execution model is highly vectorized. JIT compilation is used to make the inner loops of this vectorized execution even faster by compiling expressions that operate on these vectors.
    *   **LLVM Backend:** Uses LLVM for JIT compilation, benefiting from its powerful optimization capabilities.
    *   **Runtime Code Generation:** Code is generated at runtime based on the specific expressions and types involved in the query.
*   **Challenges:**
    *   **Selective Compilation:** Determining which expressions are worth JIT-compiling requires heuristics to balance compilation overhead against execution speedup.
    *   **LLVM Overhead:** Even for expressions, LLVM compilation can introduce latency, though generally less than compiling entire queries.
*   **Successes:**
    *   Achieves extremely high performance for analytical queries, partly due to its efficient vectorized engine and the selective JIT compilation of performance-critical expressions.
    *   Effectively reduces the CPU cost of evaluating complex expressions over large volumes of data.

## 3. Peloton (Research System)

*   **Strategy:** Peloton was a research database system from Carnegie Mellon University (CMU), designed for main-memory hybrid transaction/analytical processing (HTAP) workloads. It aimed to use JIT compilation extensively, drawing inspiration from systems like HyPer.
*   **Granularity:** Primarily query-level compilation, similar to HyPer.
*   **Key Concepts:**
    *   **Adaptive Compilation:** Explored ideas of recompiling queries or parts of queries based on observed data characteristics and workload patterns.
    *   **LLVM-based:** Leveraged LLVM for its code generation.
    *   **Focus on HTAP:** Attempted to address the challenge of making JIT compilation viable for both OLTP (low latency) and OLAP (high throughput) queries.
*   **Challenges:**
    *   **Compilation Latency for OLTP:** The overhead of JIT compilation is particularly problematic for short-lived OLTP queries, where execution times are already very small.
    *   **Complexity of Adaptive Systems:** Dynamically adapting query plans and managing recompilation adds significant complexity to the database engine.
    *   **Project Discontinuation:** Peloton is no longer actively developed, but its research contributions remain relevant.
*   **Successes (Research Context):**
    *   Advanced the understanding of JIT compilation techniques in database systems, particularly for main-memory architectures.
    *   Explored innovative approaches to reduce compilation overhead and adapt execution strategies.

## 4. Common Patterns, Challenges, and Successful Strategies

### Common Patterns

*   **LLVM as a Backend:** Most database systems implementing JIT compilation (including HyPer, ClickHouse, Peloton, and others like Impala, Spark/Tungsten) utilize LLVM due to its mature infrastructure, powerful optimization passes, and support for multiple architectures.
*   **Synergy with Vectorized Execution:** JIT compilation is often paired with vectorized execution. Vectorization processes data in batches (vectors), and JIT-compiled code can efficiently operate on these batches, especially for expression evaluation.
*   **Data-Centric/Type-Specific Code:** Generated code is specialized to the actual data types and value characteristics of the query, avoiding generic code paths and unnecessary branching.

### Common Challenges

*   **Compilation Overhead:** This is the most significant hurdle. The time taken to compile the code can outweigh the execution speedup, especially for:
    *   Short-running queries (common in OLTP).
    *   Ad-hoc queries that are not run frequently.
*   **Development and Maintenance Complexity:** Building, debugging, and maintaining a JIT compilation engine is a complex software engineering task.
*   **Selectivity and Heuristics:** Deciding *what* to compile (entire query vs. expressions), *when* to compile (e.g., after a few interpretations, or immediately), and *what level of optimization* to apply is critical and often requires sophisticated heuristics.
*   **Memory Management:** The generated code and associated structures consume memory, which needs to be managed.

### Successful Strategies

*   **Focus on Hot Spots (Selective Compilation):** Instead of compiling everything, identify and JIT-compile only the most performance-critical parts of a query, such as complex expressions, inner loops of joins, or frequently executed user-defined functions (as seen in ClickHouse).
*   **Caching of Compiled Code:** Store and reuse previously compiled code for identical or structurally similar queries/expressions. This amortizes the compilation cost over multiple executions.
*   **Tiered Execution / Adaptive Compilation:**
    *   Start execution in an interpreter or with a quickly compiled (less optimized) version of the code.
    *   Monitor execution, and if a query runs frequently or for a long time, trigger recompilation with more aggressive optimizations.
*   **Faster Compilation Techniques:**
    *   Using lighter-weight JIT frameworks than LLVM for simpler tasks if appropriate (e.g., asmjit).
    *   Template-based code generation, which can be faster than general-purpose compilation.
    *   Improving the speed of the LLVM compilation pipeline itself (e.g., by selecting fewer optimization passes for faster compiles).
*   **Asynchronous Compilation:** Perform compilation in a background thread, allowing the query to start executing in interpreted mode and seamlessly switch to compiled code once ready.
*   **Query Parameterization:** For queries that differ only by literal values, compile a parameterized version of the query once and reuse it.

This overview provides a general understanding of JIT compilation techniques in these database systems. The field continues to evolve, with ongoing research into minimizing overhead and maximizing the benefits of JIT compilation across diverse workloads.
