#pragma once

#include "duckdb/common/types/value.hpp" // For idx_t
#include "duckdb/common/types/logical_type.hpp" // For LogicalTypeId
#include "duckdb/common/enums/vector_type.hpp" // For VectorType

#include <vector> // For std::vector<vector<char>>

// Forward declaration for lua_State
struct lua_State;

namespace duckdb {

// Forward declare core DuckDB classes if they were to be used directly in helpers here.
// For this PoC, we are defining C-style structs, so direct DuckDB class dependencies
// in this header are minimized for simplicity of FFI.
class Vector;
// struct UnifiedVectorFormat; // If we were to write helpers taking this

namespace ffi {

// Simplified C-style representation of key Vector components for FFI.
// This struct is what LuaJIT's FFI will understand.
// It's designed to be populated from C++ and passed as a pointer to Lua.
struct FFIVector {
    void* data;         // Pointer to the actual data buffer (e.g., int*, double*)
    bool* nullmask;     // Pointer to a flat boolean nullmask. True means NULL.
                        // Note: DuckDB's internal ValidityMask is more complex (e.g., bitmasked).
                        // For FFI, a flat boolean array is simpler to start with.
                        // Conversion might be needed if DuckDB uses bitmasked nulls primarily.

    // Essential information that the Lua script would need:
    idx_t count;        // Number of elements in the vector/arrays.
    // int type_id;     // A simple enum or int representing the data type (e.g., 0 for INT, 1 for DOUBLE)
                        // This helps Lua cast `data` to the correct pointer type.
                        // For this PoC, we'll often assume the type or pass it implicitly.

    // Optional: Exposing selection vectors can be complex.
    // idx_t* sel_vector; // Pointer to a selection vector, if any.
    // idx_t sel_count;  // Number of elements in the selection vector (could be different from `count`).

    // For this initial version, `type_id` and `sel_vector` are omitted for simplicity.
    // The unit tests will assume the type of `data` for casting in Lua.

    // New members for better type handling and vector classification
    LogicalTypeId ffi_logical_type_id; // DuckDB's LogicalTypeId for the data
    VectorType    ffi_duckdb_vector_type;  // Original DuckDB vector type (FLAT, CONSTANT, etc.)
                                       // Lua might not use this directly, but C++ host can.
    // bool          is_temporary_buffer; // Flag if 'data' points to a temp buffer from temp_buffers_owner
                                       // Might be useful for debugging or complex scenarios.
};

// C-style struct for string data in FFIVector when data is of type VARCHAR.
// FFIVector.data would then be a pointer to an array of these structs.
struct FFIString {
    char* ptr;          // Pointer to the start of the string characters
    uint32_t len;       // Length of the string
    // Optionally, a small buffer for short strings (similar to DuckDB's string_t) could be added
    // char prefix[4]; // Example for very short strings, not used in this PoC for simplicity
};

// Conceptual helper function to populate FFIVector from a DuckDB Vector.
// The actual implementation of such a function would require careful handling of
// DuckDB's Vector internals, especially its UnifiedVectorFormat and ValidityMask.
// This function's signature is provided for conceptual completeness.
// In the PoC tests, we will manually populate FFIVector from std::vector for simplicity.
// For Phase 1 of full JIT integration, we implement this.
void CreateFFIVectorFromDuckDBVector(duckdb::Vector& duckdb_vec, idx_t count,
                                     duckdb::ffi::FFIVector& out_ffi_vec,
                                     std::vector<std::vector<char>>& temp_buffers_owner);

} // namespace ffi
} // namespace duckdb
