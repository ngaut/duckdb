# LuaJIT Advanced VARCHAR FFI Design & Other Function Extensions (Block-Processing)

## 1. Objective

This document details designs to minimize FFI overhead for `VARCHAR` expressions and extend JIT support to more functions, all within the block-processing model. Key areas:
1.  Optimizing `VARCHAR` inputs, particularly for `LENGTH`.
2.  Batching `VARCHAR` outputs (covered previously, recapped here).
3.  Introducing C FFI helpers for `STARTS_WITH`, `CONTAINS`, and simple `LIKE` patterns.
4.  Adding support for more mathematical functions.
5.  Adding support for `DATE_TRUNC` via a C FFI helper.

This builds upon the block-processing architecture where the main loop over rows is inside the JITed Lua function.

## 2. Recap of `FFIVector` and `FFIString`
(No change from previous version of this document - Turn 37)
Input `VARCHAR` vectors are presented to Lua as an array of `FFIString` structs (`FFIString*`), accessible in Lua as `inputX_data[i]`.

## 3. Input String Optimizations

### 3.1. `LENGTH(varchar_col)` Direct `.len` Access (Implemented)

*   **Optimization:** For the `LENGTH(varchar_column_reference)` SQL function (and its alias `STRLEN`), if the argument is a direct reference to a `VARCHAR` column, the `LuaTranslator` (specifically `GenerateValueBoundFunction`) now generates optimized Lua code.
*   **Generated Lua Snippet by `LuaTranslator`:**
    ```lua
    -- For LENGTH(inputX_col) where inputX is a VARCHAR column reference
    if inputX_nullmask[i] then
        current_row_is_null = true
    else
        current_row_is_null = false
        current_row_value = inputX_data[i].len -- Direct access to FFIString.len
    end
    ```
*   **Fallback:** If the argument to `LENGTH` is not a direct `VARCHAR` column reference (e.g., `LENGTH(LOWER(col))`, `LENGTH('constant_string')`), `LuaTranslator` evaluates the child expression into a Lua string (e.g., `tval0_val`) and then generates `current_row_value = #tval0_val`.

### 3.2. Other String Operations
For most other string operations requiring Lua to manipulate string content, per-row `ffi.string(inputX_data[i].ptr, inputX_data[i].len)` to create Lua strings is still necessary.

## 4. Output String Strategy: Batching via Lua Table
(This section remains as previously defined in Turn 37, detailing `results_table` and `duckdb_ffi_add_lua_string_table_to_output_vector`. This mechanism is used by string-producing functions like `LOWER`, `CONCAT`, `REPLACE`, etc.)

### ... (sections 4.1 to 4.4 remain unchanged) ...

## 5. `STARTS_WITH`, `CONTAINS`, and `LIKE` FFI Helpers (Implemented)

To improve performance for common string predicates and simple `LIKE` patterns, dedicated C FFI helper functions are implemented.

### 5.1. C FFI Helper Functions

*   **Declarations (`src/include/duckdb/common/luajit_ffi_structs.hpp`):**
    ```cpp
    extern "C" DUCKDB_API bool duckdb_ffi_starts_with(const char* str_data, int str_len, const char* prefix_data, int prefix_len);
    extern "C" DUCKDB_API bool duckdb_ffi_contains(const char* str_data, int str_len, const char* substr_data, int substr_len);
    ```
*   **Implementations (`src/common/luajit_ffi_vector.cpp`):**
    *   `duckdb_ffi_starts_with`: Creates `string_t` for the input string and prefix, then calls `duckdb::StartsWith::Operation()`.
    *   `duckdb_ffi_contains`: Creates `string_t` for the input string and substring, then calls `duckdb::Contains::Operation()`.
*   **Registration (`src/common/luajit_wrapper.cpp`):** Registered globally in `LuaJITStateWrapper`.
*   **CDEFs (`ExpressionExecutor::ConstructFullLuaFunctionScript`):** Signatures added to the `ffi.cdef` block:
    ```lua
    bool duckdb_ffi_starts_with(const char* str_data, int32_t str_len, const char* prefix_data, int32_t prefix_len);
    bool duckdb_ffi_contains(const char* str_data, int32_t str_len, const char* substr_data, int32_t substr_len);
    ```

### 5.2. `LuaTranslator` Updates for `STARTS_WITH`, `CONTAINS`, and `LIKE`

*   **`GenerateValueBoundFunction` in `src/main/luajit_translator.cpp`:**
    *   **`STARTS_WITH(str, prefix)`:** Evaluates children to Lua strings (e.g., `tval0_val`, `tval1_val`). Generates code: `current_row_value = duckdb_ffi_starts_with(tval0_val, #tval0_val, tval1_val, #tval1_val)`.
    *   **`CONTAINS(str, substr)`:** Similar, calling `duckdb_ffi_contains`.
    *   **`LIKE(str, pattern)`:**
        - If `pattern` is a `BoundConstantExpression` (VARCHAR):
            - `'prefix%'` (no other wildcards): Translates to `duckdb_ffi_starts_with(str_val, #str_val, "prefix_literal", prefix_len)`.
            - `'%substring%'` (no other wildcards in substring): Translates to `duckdb_ffi_contains(str_val, #str_val, "substring_literal", substring_len)`.
            - Other `LIKE` patterns: Sets `current_row_is_null = true` (effectively not JITable by this simple translation).
        - If `pattern` is not a constant: Sets `current_row_is_null = true`.

## 6. Additional Math Functions (Implemented)

The `LuaTranslator::GenerateValueBoundFunction` has been extended to support more standard Lua math functions:
*   `DEGREES(numeric)` -> `math.deg(arg_val)`
*   `RADIANS(numeric)` -> `math.rad(arg_val)`
*   `TRUNC(numeric)` / `TRUNCATE(numeric)` -> Uses `math.modf(arg_val)` to extract the integer part.
*   `SIGN(numeric)` -> Implemented with a Lua nested conditional: `if v > 0 then 1 elseif v < 0 then -1 else 0 end`.
*   `EXP(numeric)` -> `math.exp(arg_val)`
*   `LOG2(numeric)` -> `math.log(arg_val) / math.log(2)` (with check for `arg_val <= 0` resulting in NULL).

Null propagation for arguments is handled by the standard wrapper logic in `GenerateValueBoundFunction`.

## 7. `DATE_TRUNC` Function with C FFI Helper (Implemented)

### 7.1. C FFI Helper Function for `DATE_TRUNC`

*   **Declaration (`src/include/duckdb/common/luajit_ffi_structs.hpp`):**
    ```cpp
    extern "C" DUCKDB_API int64_t duckdb_ffi_date_trunc(const char* part_str, int64_t value, bool is_timestamp);
    ```
*   **Implementation (`src/common/luajit_ffi_vector.cpp`):**
    *   Parses `part_str` to `DatePartSpecifier` (using existing `StringToDatePartSpecifier`).
    *   If `is_timestamp` is true, casts `value` to `timestamp_t`, calls `duckdb::Timestamp::Truncate()`, returns result as `int64_t`.
    *   Else (for `DATE`), casts `value` to `date_t` (via `int32_t`), calls `duckdb::Date::Truncate()`, converts result `date_t` to `int64_t`.
    *   Includes basic error handling.
*   **Registration (`src/common/luajit_wrapper.cpp`):** Registered globally as `duckdb_ffi_date_trunc`.
*   **CDEF (`ExpressionExecutor::ConstructFullLuaFunctionScript`):** Signature added to `ffi.cdef`.

### 7.2. `LuaTranslator` Update for `DATE_TRUNC`

*   **`GenerateValueBoundFunction` in `src/main/luajit_translator.cpp`:**
    *   For `DATE_TRUNC(part_const_str, date_or_ts_arg)`:
        - Ensures `part_const_str` is a non-NULL `VARCHAR` `BoundConstantExpression`.
        - Determines if `date_or_ts_arg` is `TIMESTAMP` or `DATE`.
        - Generates Lua code to call `current_row_value = duckdb_ffi_date_trunc(escaped_part_str, temporal_arg_lua_var, is_timestamp_bool_literal)`.
        - The return type is `TIMESTAMP` (as `int64_t`) from the FFI helper.

## 8. Benefits and Considerations (Overall)

*   **`LENGTH` Optimization:** Directly accessing `.len` for `LENGTH(varchar_col_ref)` avoids an unnecessary `ffi.string()` conversion, improving performance for this specific common case.
*   **String Predicates via FFI:** `STARTS_WITH`, `CONTAINS`, and simple `LIKE` now leverage efficient C++ code, which is generally better than pure Lua for these tasks, though per-row FFI calls remain.
*   **`DATE_TRUNC` via FFI:** Complex date/time logic is handled by robust C++ code.
*   **Expanded Math Functions:** Broadens the range of JIT-able expressions.
*   **Remaining Overheads:**
    *   For most string functions (other than optimized `LENGTH`), `VARCHAR` inputs still require per-row `ffi.string()` if their content is needed by Lua operations.
    *   FFI calls, even to fast C functions, have an inherent per-call overhead.
    *   The batch `VARCHAR` output mechanism (Section 4) has its own overheads (Lua table ops, C-side table iteration).

This set of enhancements further refines the JIT capabilities, particularly for common string and date operations, by strategically using FFI helpers and direct data access where beneficial.
