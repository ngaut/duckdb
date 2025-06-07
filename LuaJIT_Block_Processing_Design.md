# LuaJIT Block-Based Data Processing Design

## 1. Objective

The primary goal of this design is to significantly reduce the FFI (Foreign Function Interface) call overhead between C++ and LuaJIT. Instead of the C++ `ExpressionExecutor` looping through rows and calling a JITed Lua function (or parts of it) for each row, this design proposes that a single call to a JITed Lua function will process all rows within a `DataChunk`. The main loop over rows will reside *inside* the JITed Lua function.

## 2. `FFIVector` Structure Review

The existing `duckdb::ffi::FFIVector` C-style struct remains central to passing data to Lua. Its definition is:

```c++
namespace duckdb {
namespace ffi {

struct FFIVector {
    void* data;         // Pointer to the actual data buffer (e.g., int*, FFIString*, FFIInterval*)
    bool* nullmask;     // Pointer to a flat boolean nullmask (true means NULL).
    idx_t count;        // Number of elements (rows) in this vector for the current chunk.

    LogicalTypeId ffi_logical_type_id; // DuckDB's LogicalTypeId for the data. Used by Lua to cast data pointer.
    VectorType    ffi_duckdb_vector_type;  // Original DuckDB vector type (FLAT, CONSTANT, etc.).
    duckdb::Vector* original_duckdb_vector; // Pointer to the original DuckDB vector, primarily for output.
};

// Other structs like FFIString and FFIInterval remain as defined:
struct FFIString {
    char* ptr;
    uint32_t len;
};

struct FFIInterval {
    int32_t months;
    int32_t days;
    int64_t micros;
};

} // namespace ffi
} // namespace duckdb
```

From C++, for each `duckdb::Vector` in an input `DataChunk` and for the output `Vector`, an `FFIVector` instance is populated. The `data` and `nullmask` pointers will point to the beginning of the (potentially flattened) data for the entire chunk. The `count` will be the number of rows in the current chunk.

## 3. Generated Lua Function Structure

The `ExpressionExecutor::ConstructFullLuaFunctionScript` method will generate a Lua function with the following structure. This function is called once per `DataChunk`.

```lua
-- Global ffi.cdef for FFIVector, FFIString, FFIInterval, and C helper functions.
-- This is defined once when the LuaJITStateWrapper is initialized.
-- Example:
-- ffi.cdef[[
--   typedef struct FFIVector { void* data; bool* nullmask; uint64_t count;
--                              int ffi_logical_type_id; int ffi_duckdb_vector_type;
--                              void* original_duckdb_vector; } FFIVector;
--   typedef struct FFIString { char* ptr; uint32_t len; } FFIString;
--   typedef struct FFIInterval { int32_t months; int32_t days; int64_t micros; } FFIInterval;
--   typedef signed char int8_t;
--   typedef int int32_t;
--   typedef long long int64_t;
--   void duckdb_ffi_add_string_to_output_vector(void* ffi_vec_ptr, uint64_t row_idx, const char* str_data, uint32_t str_len);
--   void duckdb_ffi_set_string_output_null(void* ffi_vec_ptr, uint64_t row_idx);
--   // ... other C helper function signatures ...
-- ]]

-- The dynamically generated JIT function:
function JIT_DUCKDB_FUNCTION_XYZ(output_vec_ffi, input1_ffi, input2_ffi, ..., count)
    -- 1. Initial FFI Casts for each FFIVector* to typed Lua FFI pointers
    -- These casts are based on FFIVector.ffi_logical_type_id.
    -- (Actual enum values for TYPE_ID_ENUM would be substituted by C++)

    local output_data -- Typed pointer for output data
    if output_vec_ffi.ffi_logical_type_id == VARCHAR_TYPE_ID_ENUM then
        -- For VARCHAR output, direct data pointer might not be used if relying on FFI helpers.
        -- It's still good to have for consistency or direct manipulation if ever needed.
        output_data = ffi.cast("FFIString*", output_vec_ffi.data)
    elseif output_vec_ffi.ffi_logical_type_id == INTEGER_TYPE_ID_ENUM then
        output_data = ffi.cast("int32_t*", output_vec_ffi.data)
    -- ... other output types like BIGINT, DOUBLE, DATE, TIMESTAMP, INTERVAL ...
    elseif output_vec_ffi.ffi_logical_type_id == INTERVAL_TYPE_ID_ENUM then
        output_data = ffi.cast("FFIInterval*", output_vec_ffi.data)
    else
        -- Error or default cast if type is unknown (should not happen with proper generation)
    end
    local output_nullmask = ffi.cast("bool*", output_vec_ffi.nullmask)

    -- Input vector 1
    local input1_data
    if input1_ffi.ffi_logical_type_id == INTEGER_TYPE_ID_ENUM then
        input1_data = ffi.cast("int32_t*", input1_ffi.data)
    elseif input1_ffi.ffi_logical_type_id == VARCHAR_TYPE_ID_ENUM then
        input1_data = ffi.cast("FFIString*", input1_ffi.data)
    elseif input1_ffi.ffi_logical_type_id == DATE_TYPE_ID_ENUM then
        input1_data = ffi.cast("int32_t*", input1_ffi.data)
    elseif input1_ffi.ffi_logical_type_id == TIMESTAMP_TYPE_ID_ENUM then
        input1_data = ffi.cast("int64_t*", input1_ffi.data)
    elseif input1_ffi.ffi_logical_type_id == INTERVAL_TYPE_ID_ENUM then
        input1_data = ffi.cast("FFIInterval*", input1_ffi.data)
    -- ... etc. for all relevant input types for input1_ffi
    end
    local input1_nullmask = ffi.cast("bool*", input1_ffi.nullmask)

    -- ... repeat FFI casts for input2_ffi, input3_ffi, etc.

    -- 2. Main Processing Loop (iterates from 0 to count-1, matching C/C++ 0-based indexing)
    for i = 0, count - 1 do
        -----------------------------------------------------------------------------------
        -- Begin Row Logic (this self-contained block is generated by
        -- LuaTranslator::TranslateExpressionToLuaRowLogic)
        -----------------------------------------------------------------------------------
        local current_row_value
        local current_row_is_null = false -- Assume not null initially for this row's result

        -- Example: Expression involving input1 (numeric) and a constant
        -- (e.g., SQL: input_column1 + 10)

        -- Check for NULL in input(s) required for the current expression
        if input1_nullmask[i] then -- Or: if input1_nullmask[i] or input2_nullmask[i] then ...
            current_row_is_null = true
        else
            -- All required inputs for this specific expression are NOT NULL.
            -- Perform the actual operation.
            -- 'input1_data[i]' directly accesses the i-th element.
            current_row_value = input1_data[i] + 10

            -- If the operation itself can result in NULL (e.g. division by zero if not caught,
            -- or a function like sqrt(-1)), 'current_row_value' might become 'nil'.
            if current_row_value == nil then
                 current_row_is_null = true
            end
        end

        -- Storing the result for the current row 'i'
        -- This part depends on the output type of the expression.
        if output_vec_ffi.ffi_logical_type_id == VARCHAR_TYPE_ID_ENUM then
            if current_row_is_null then
                duckdb_ffi_set_string_output_null(output_vec_ffi, i)
                output_nullmask[i] = true -- Also ensure host-side nullmask is aware
            else
                -- current_row_value is expected to be a Lua string here
                duckdb_ffi_add_string_to_output_vector(output_vec_ffi, i, current_row_value, #current_row_value)
                output_nullmask[i] = false -- FFI helper might also set this
            end
        else -- For numeric, date, timestamp, interval, boolean
            if current_row_is_null then
                output_nullmask[i] = true
                -- Optional: output_data[i] = 0 (or some default for fixed-width types,
                -- if not writing garbage is desired, though nullmask handles validity)
            else
                output_nullmask[i] = false
                output_data[i] = current_row_value -- Direct assignment for compatible types
            end
        end
        -----------------------------------------------------------------------------------
        -- End Row Logic
        -----------------------------------------------------------------------------------
    end
end
```

## 4. Per-Data-Type Access Strategy within Lua Loop

Inside the `for i = 0, count - 1 do` loop:

*   **Numeric Types (INTEGER, BIGINT, DOUBLE), DATE, TIMESTAMP:**
    *   **Input:** Direct array access on the casted FFI data pointer, e.g., `local val = input_int_data[i]`.
    *   **Output:** Direct assignment to the casted output data pointer, e.g., `output_double_data[i] = result_val`.
*   **INTERVAL Type:**
    *   **Input:** Access fields from the `FFIInterval` struct, e.g., `local months = input_interval_array[i].months`.
    *   **Output:** Assign fields to the output `FFIInterval` struct, e.g., `output_interval_array[i].months = res_months; output_interval_array[i].days = res_days; ...`.
*   **VARCHAR Type:**
    *   **Input:** `input_string_array[i]` provides an `FFIString` struct. To use it in Lua string operations, it's typically converted to a Lua string: `local lua_s = ffi.string(input_string_array[i].ptr, input_string_array[i].len)`. This conversion might involve a memory copy.
    *   **Output:** As reiterated, string output continues to use the per-row FFI C helper calls (`duckdb_ffi_add_string_to_output_vector`, `duckdb_ffi_set_string_output_null`) due to the complexity of interacting with DuckDB's `StringHeap` directly from Lua. The "Row Logic" generated by `LuaTranslator` for expressions returning `VARCHAR` will embed these calls.
*   **BOOLEAN Type:**
    *   **Input:** If boolean inputs are stored as `int8_t` (0 or 1) in FFI, access is `input_bool_data[i]`. Comparison in Lua would be `input_bool_data[i] == 1`.
    *   **Output:** Store as `int8_t`, e.g., `output_bool_data[i] = lua_boolean_result and 1 or 0`.

## 5. Role of `LuaTranslator::TranslateExpressionToLuaRowLogic`

With the block processing design, `LuaTranslator::TranslateExpressionToLuaRowLogic` is simplified. Its responsibility is to generate *only the "Row Logic"* block shown in the template above.
This generated Lua code snippet will:
*   Assume that the loop variable `i` (for the current row index) is already defined and available.
*   Assume that typed Lua local variables for accessing input vector data (e.g., `input1_data`, `input1_nullmask`) and output vector data/nullmasks (e.g., `output_data`, `output_nullmask`) are already defined and correctly cast by the outer function shell.
*   Perform null checks for its *direct inputs* (which are individual values from the input vectors for row `i`).
*   Generate the Lua code for the actual expression evaluation for row `i`.
*   Assign the computed `current_row_value` and `current_row_is_null` status to the appropriate output variables (`output_data[i]`, `output_nullmask[i]`) or call FFI C helpers for string output.

## 6. Role of `ExpressionExecutor::ConstructFullLuaFunctionScript`

This static helper function in `ExpressionExecutor` takes on greater responsibility:
*   It receives the "Row Logic" string from `LuaTranslator`.
*   It uses `LuaTranslatorContext` to understand the data types of all input vectors and the output vector.
*   It generates the complete Lua function string:
    *   The Lua function signature `function JIT_DUCKDB_FUNCTION_XYZ(output_vec_ffi, input1_ffi, ..., count)`.
    *   The initial FFI casts for all input `FFIVector*` arguments to their respective typed Lua FFI data pointers (e.g., `local input1_data = ffi.cast("int32_t*", input1_ffi.data)`), based on `ffi_logical_type_id` from each `FFIVector`. Similarly for output vector.
    *   The main processing loop: `for i = 0, count - 1 do ... end`.
    *   It embeds the "Row Logic" (from `LuaTranslator`) inside this loop.
*   It does NOT include the `ffi.cdef` part, as that is now handled once globally when the `LuaJITStateWrapper` is initialized.

## 7. Benefits of Block Processing

*   **Reduced FFI Call Overhead:** The primary benefit. Instead of C++ calling into Lua for each row (or even multiple times per row for complex expressions), there's now only one C++ to Lua call per `DataChunk`.
*   **Loop Optimization by LuaJIT:** The main loop over rows is now part of the Lua code that LuaJIT compiles. LuaJIT's JIT compiler can potentially optimize this loop very effectively, including trace optimizations for common paths within the row logic.
*   **Simpler `LuaTranslator` Output:** `LuaTranslator` focuses purely on the logic for a single row, making its output more modular.

## 8. Remaining Challenges and Overheads

*   **FFI C Helpers for VARCHAR Output:** Per-row calls to C functions like `duckdb_ffi_add_string_to_output_vector` still exist and introduce FFI overhead for each output string.
*   **`ffi.string()` for VARCHAR Input:** If input `VARCHAR` data needs to be manipulated as Lua strings (e.g., for string functions not implemented as C FFI helpers), `ffi.string(ptr, len)` is called per row per relevant input. This involves creating a Lua string object, which can have overhead (memory allocation, copying).
*   **Initial FFI Casts:** The typed pointer casts (e.g., `ffi.cast("int32_t*", ...)` for each vector are done once per chunk at the beginning of the JITed Lua function. This is a minor, fixed overhead per chunk.
*   **Data Preparation (`CreateFFIVectorFromDuckDBVector`):** The cost of preparing `FFIVector`s (flattening constant/dictionary vectors, creating boolean nullmasks) is still incurred once per chunk on the C++ side before calling the JITed Lua function.
*   **Type Dispatch in Lua for Inputs (Generated Code):** The `if/elseif` chain for casting input vector data pointers based on `ffi_logical_type_id` adds a small, fixed overhead at the start of the JITed function. This could be avoided if separate JITed functions were generated for each unique signature of input types, but that would increase compilation frequency.
