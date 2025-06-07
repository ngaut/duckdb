#include "duckdb/common/luajit_ffi_structs.hpp"

// This file is intended to host helper functions for bridging
// DuckDB's internal data structures (like Vector, UnifiedVectorFormat)
// with the C-style FFIVector structs defined for LuaJIT FFI.

// For the initial Proof-of-Concept, direct manipulation of DuckDB's Vector
// to populate FFIVector is complex due to private members and the
// intricacies of UnifiedVectorFormat (e.g., constant, dictionary, flat vectors)
// and ValidityMask (bitmasked nulls).

// Therefore, the unit tests will manually create and populate FFIVector
// instances using std::vector as backing storage to demonstrate the FFI mechanism
// with LuaJIT.

// A full implementation of a function like `GetFFIVectorFromDuckDBVector`
// would likely need to be a method of the `Vector` class itself, or a friend function,
// or use a carefully designed public API to access the necessary data pointers and
// nullmask information (potentially converting bitmasked nulls to flat boolean arrays
// if FFIVector.nullmask is defined as bool*).
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp" // For VectorBuffer
#include "duckdb/common/vector_operations/vector_operations.hpp" // For ToUnifiedFormat
#include "duckdb/common/types/unmanaged_vector_data.hpp"

namespace duckdb {
namespace ffi {

// Helper to get the size of a single data element for a given logical type.
static FFI_TYPE_SIZE_SIGNATURE {
    switch(type.id()) {
        case LogicalTypeId::INTEGER: return sizeof(int32_t);
        case LogicalTypeId::BIGINT: return sizeof(int64_t);
        case LogicalTypeId::DOUBLE: return sizeof(double);
        case LogicalTypeId::VARCHAR: return sizeof(FFIString); // Points to an array of FFIString structs
        // Add other numeric types as needed (FLOAT, SMALLINT, TINYINT, HUGEINT, DECIMAL)
        default:
            throw NotImplementedException("FFI: Unsupported logical type for GetFFITypeSize: " + type.ToString());
    }
}


void CreateFFIVectorFromDuckDBVector(duckdb::Vector& duckdb_vec, idx_t count,
                                     duckdb::ffi::FFIVector& out_ffi_vec,
                                     std::vector<std::vector<char>>& temp_buffers_owner) {
    if (count == 0) {
        out_ffi_vec.data = nullptr;
        out_ffi_vec.nullmask = nullptr; // Or a static always-null/always-valid mask
        out_ffi_vec.count = 0;
        out_ffi_vec.ffi_logical_type_id = duckdb_vec.GetType().id();
        out_ffi_vec.ffi_duckdb_vector_type = duckdb_vec.GetVectorType();
        return;
    }

    // 1. Get UnifiedVectorFormat for the duckdb_vec.
    UnifiedVectorFormat unified_data;
    duckdb_vec.ToUnifiedFormat(count, unified_data);

    out_ffi_vec.count = count; // DuckDB vectors might have higher capacity, but count is effective size
    out_ffi_vec.ffi_logical_type_id = duckdb_vec.GetType().id();
    out_ffi_vec.ffi_duckdb_vector_type = duckdb_vec.GetVectorType();
    // out_ffi_vec.is_temporary_buffer = false; // Default

    // 2. Populate out_ffi_vec.data and out_ffi_vec.nullmask
    // The nullmask for FFI is a flat bool array. DuckDB's ValidityMask is bitmasked.
    // We need to convert DuckDB's bitmasked validity to a flat bool array.
    // This flat array needs to be owned by temp_buffers_owner.

    temp_buffers_owner.emplace_back(count * sizeof(bool));
    bool* flat_nullmask_ptr = reinterpret_cast<bool*>(temp_buffers_owner.back().data());
    out_ffi_vec.nullmask = flat_nullmask_ptr;

    for (idx_t i = 0; i < count; i++) {
        idx_t source_idx = unified_data.sel->get_index(i); // Get index in underlying data
        flat_nullmask_ptr[i] = !unified_data.validity.RowIsValid(source_idx);
    }

    // Handle data based on original vector type before unification (or after for some)
    VectorType vector_type = duckdb_vec.GetVectorType();
    LogicalTypeId logical_type_id = duckdb_vec.GetType().id();

    if (logical_type_id == LogicalTypeId::VARCHAR) {
        // Special handling for VARCHAR: data buffer will be an array of FFIString structs.
        temp_buffers_owner.emplace_back(count * sizeof(FFIString));
        FFIString* ffi_string_array = reinterpret_cast<FFIString*>(temp_buffers_owner.back().data());
        out_ffi_vec.data = ffi_string_array;
        // out_ffi_vec.is_temporary_buffer = true; // Always a temp buffer for FFIString array

        string_t* duckdb_strings = UnifiedVectorFormat::GetData<string_t>(unified_data);
        for (idx_t i = 0; i < count; i++) {
            if (!flat_nullmask_ptr[i]) { // Only process if not null
                idx_t source_idx = unified_data.sel->get_index(i);
                const string_t& duckdb_str = duckdb_strings[source_idx];
                ffi_string_array[i].ptr = const_cast<char*>(duckdb_str.GetDataUnsafe());
                ffi_string_array[i].len = duckdb_str.GetSize();
            } else {
                ffi_string_array[i].ptr = nullptr;
                ffi_string_array[i].len = 0;
            }
        }
    } else if (vector_type == VectorType::FLAT_VECTOR) {
        // For numeric flat vectors, data can be pointed to directly if no sel_vector implies full buffer use.
        // However, UnifiedVectorFormat always gives a sel_vector.
        // If unified_data.sel is incremental (0,1,2...) and covers the whole vector, we could point directly.
        // For simplicity and consistency, especially with potential selection vectors,
        // we will copy to a temporary buffer if not all data is used or if it's not contiguous.
        // However, UnifiedVectorFormat::GetData already gives the direct pointer from the underlying buffer.
        out_ffi_vec.data = UnifiedVectorFormat::GetData(unified_data);
        // No temp buffer needed here if Lua directly uses this, but be careful with selection vectors.
        // The current unified_data.sel already maps indices correctly for this data pointer.
    } else if (vector_type == VectorType::CONSTANT_VECTOR) {
        idx_t element_size = GetFFITypeSize(duckdb_vec.GetType());
        temp_buffers_owner.emplace_back(count * element_size);
        char* temp_data_buffer = temp_buffers_owner.back().data();
        out_ffi_vec.data = temp_data_buffer;
        // out_ffi_vec.is_temporary_buffer = true;

        data_ptr_t constant_data_ptr = UnifiedVectorFormat::GetData(unified_data);
        // bool is_constant_null = !unified_data.validity.RowIsValid(0); // Already handled by flat_nullmask_ptr

        // Fill the temporary buffer with the repeated constant value if not null.
        // The flat_nullmask_ptr will indicate which elements are null.
        for (idx_t i = 0; i < count; ++i) {
            if (!flat_nullmask_ptr[i]) { // Only copy if not null
                 memcpy(temp_data_buffer + (i * element_size), constant_data_ptr, element_size);
            }
        }
    } else if (vector_type == VectorType::DICTIONARY_VECTOR) {
        idx_t element_size = GetFFITypeSize(duckdb_vec.GetType());
        temp_buffers_owner.emplace_back(count * element_size);
        char* temp_data_buffer = temp_buffers_owner.back().data();
        out_ffi_vec.data = temp_data_buffer;
        // out_ffi_vec.is_temporary_buffer = true;

        data_ptr_t child_data_ptr = UnifiedVectorFormat::GetData(unified_data);
        for (idx_t i = 0; i < count; ++i) {
            idx_t source_idx_in_child = unified_data.sel->get_index(i);
            if (!flat_nullmask_ptr[i]) {
                 memcpy(temp_data_buffer + (i * element_size),
                       child_data_ptr + (source_idx_in_child * element_size),
                       element_size);
            }
        }
    } else {
        throw NotImplementedException("FFI: VectorType not yet supported for CreateFFIVectorFromDuckDBVector: " +
                                      VectorTypeToString(vector_type));
    }
}


// Other potential helpers could go here, e.g., for specific type conversions
// or for creating DuckDB Vectors from FFIVectors (less common for JIT use case).

} // namespace ffi
} // namespace duckdb
