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
#include "duckdb/common/types/date.hpp"     // For Date
#include "duckdb/common/types/timestamp.hpp" // For Timestamp
#include "duckdb/common/types/interval.hpp"  // For Interval

static FFI_TYPE_SIZE_SIGNATURE {
    switch(type.id()) {
        case LogicalTypeId::INTEGER: return sizeof(int32_t);
        case LogicalTypeId::BIGINT: return sizeof(int64_t);
        case LogicalTypeId::DOUBLE: return sizeof(double);
        case LogicalTypeId::DATE: return sizeof(date_t); // int32_t
        case LogicalTypeId::TIMESTAMP: return sizeof(timestamp_t); // int64_t
        case LogicalTypeId::INTERVAL: return sizeof(FFIInterval); // struct of int32, int32, int64
        case LogicalTypeId::VARCHAR: return sizeof(FFIString);
        default:
            throw NotImplementedException("FFI: Unsupported logical type for GetFFITypeSize: " + type.ToString());
    }
}


void CreateFFIVectorFromDuckDBVector(duckdb::Vector& duckdb_vec, idx_t count,
                                     duckdb::ffi::FFIVector& out_ffi_vec,
                                     std::vector<std::vector<char>>& temp_buffers_owner) {
    out_ffi_vec.original_duckdb_vector = &duckdb_vec; // Store pointer to original vector for output helpers

    if (count == 0) {
        out_ffi_vec.data = nullptr;
        out_ffi_vec.nullmask = nullptr;
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
        // out_ffi_vec.is_temporary_buffer = true;

        string_t* duckdb_strings = UnifiedVectorFormat::GetData<string_t>(unified_data);
        for (idx_t i = 0; i < count; i++) {
            if (!flat_nullmask_ptr[i]) {
                idx_t source_idx = unified_data.sel->get_index(i);
                const string_t& duckdb_str = duckdb_strings[source_idx];
                ffi_string_array[i].ptr = const_cast<char*>(duckdb_str.GetDataUnsafe());
                ffi_string_array[i].len = duckdb_str.GetSize();
            } else {
                ffi_string_array[i].ptr = nullptr;
                ffi_string_array[i].len = 0;
            }
        }
    } else if (logical_type_id == LogicalTypeId::INTERVAL) {
        temp_buffers_owner.emplace_back(count * sizeof(FFIInterval));
        FFIInterval* ffi_interval_array = reinterpret_cast<FFIInterval*>(temp_buffers_owner.back().data());
        out_ffi_vec.data = ffi_interval_array;

        interval_t* duckdb_intervals = UnifiedVectorFormat::GetData<interval_t>(unified_data);
        for (idx_t i = 0; i < count; i++) {
            if (!flat_nullmask_ptr[i]) {
                idx_t source_idx = unified_data.sel->get_index(i);
                const interval_t& duckdb_interval = duckdb_intervals[source_idx];
                ffi_interval_array[i].months = duckdb_interval.months;
                ffi_interval_array[i].days = duckdb_interval.days;
                ffi_interval_array[i].micros = duckdb_interval.micros;
            } else {
                ffi_interval_array[i].months = 0;
                ffi_interval_array[i].days = 0;
                ffi_interval_array[i].micros = 0;
            }
        }
    }
    // Numeric types (INTEGER, BIGINT, DOUBLE, DATE, TIMESTAMP)
    else if (vector_type == VectorType::FLAT_VECTOR) {
        out_ffi_vec.data = UnifiedVectorFormat::GetData(unified_data);
    } else if (vector_type == VectorType::CONSTANT_VECTOR) {
        idx_t element_size = GetFFITypeSize(duckdb_vec.GetType());
        temp_buffers_owner.emplace_back(count * element_size);
        char* temp_data_buffer = temp_buffers_owner.back().data();
        out_ffi_vec.data = temp_data_buffer;

        data_ptr_t constant_data_ptr = UnifiedVectorFormat::GetData(unified_data);
        for (idx_t i = 0; i < count; ++i) {
            if (!flat_nullmask_ptr[i]) {
                 memcpy(temp_data_buffer + (i * element_size), constant_data_ptr, element_size);
            }
        }
    } else if (vector_type == VectorType::DICTIONARY_VECTOR) {
        idx_t element_size = GetFFITypeSize(duckdb_vec.GetType());
        temp_buffers_owner.emplace_back(count * element_size);
        char* temp_data_buffer = temp_buffers_owner.back().data();
        out_ffi_vec.data = temp_data_buffer;

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
                                      VectorTypeToString(vector_type) + " for type " + logical_type_id.ToString());
    }
}


// Other potential helpers could go here, e.g., for specific type conversions
// or for creating DuckDB Vectors from FFIVectors (less common for JIT use case).

} // namespace ffi
} // namespace duckdb

// --- Implementation of FFI C Helper Functions ---
// These are extern "C" and will be registered with Lua.

// Note: These helpers assume that ffi_vec_ptr->original_duckdb_vector is correctly set.
// Error checking (e.g., if original_duckdb_vector is null, or wrong type) is omitted for brevity.
extern "C" DUCKDB_API void duckdb_ffi_add_string_to_output_vector(void* ffi_vec_ptr, duckdb::idx_t row_idx, const char* str_data, uint32_t str_len) {
    if (!ffi_vec_ptr) return;
    auto* ffi_meta = reinterpret_cast<duckdb::ffi::FFIVector*>(ffi_vec_ptr);
    if (!ffi_meta->original_duckdb_vector) return;

    duckdb::Vector* actual_vector = reinterpret_cast<duckdb::Vector*>(ffi_meta->original_duckdb_vector);

    if (actual_vector->GetType().id() != duckdb::LogicalTypeId::VARCHAR) return;
    if (row_idx >= ffi_meta->count) return;

    try {
        // SetValue handles string heap allocation.
        actual_vector->SetValue(row_idx, duckdb::Value(std::string(str_data, str_len)));
        // SetValue should also clear the nullmask for this row.
        // If not, uncomment: duckdb::FlatVector::SetNull(*actual_vector, row_idx, false);
    } catch (...) { /* TODO: Error propagation to Lua */ }
}

extern "C" DUCKDB_API void duckdb_ffi_set_string_output_null(void* ffi_vec_ptr, duckdb::idx_t row_idx) {
    if (!ffi_vec_ptr) return;
    auto* ffi_meta = reinterpret_cast<duckdb::ffi::FFIVector*>(ffi_vec_ptr);
    if (!ffi_meta->original_duckdb_vector) return;

    duckdb::Vector* actual_vector = reinterpret_cast<duckdb::Vector*>(ffi_meta->original_duckdb_vector);

    // No type check needed for SetNull, but good for consistency if this was type specific
    // if (actual_vector->GetType().id() != duckdb::LogicalTypeId::VARCHAR) return;
     if (row_idx >= ffi_meta->count) return;

    duckdb::FlatVector::SetNull(*actual_vector, row_idx, true);
}

// FFI Helpers for EXTRACT
static int64_t ExtractDatePart(duckdb::date_t date, duckdb::DatePartSpecifier specifier) {
    switch (specifier) {
    case duckdb::DatePartSpecifier::YEAR: return duckdb::Date::ExtractYear(date);
    case duckdb::DatePartSpecifier::MONTH: return duckdb::Date::ExtractMonth(date);
    case duckdb::DatePartSpecifier::DAY: return duckdb::Date::ExtractDay(date);
    // Add other parts as needed: DOY, WEEK, etc.
    default: throw duckdb::NotImplementedException("Unsupported date part for FFI EXTRACT");
    }
}

static int64_t ExtractTimestampPart(duckdb::timestamp_t ts, duckdb::DatePartSpecifier specifier) {
     switch (specifier) {
    case duckdb::DatePartSpecifier::YEAR: return duckdb::Timestamp::GetDate(ts).year; // Simplified from duckdb::Timestamp::ExtractYear etc.
    case duckdb::DatePartSpecifier::MONTH: return duckdb::Timestamp::GetDate(ts).month;
    case duckdb::DatePartSpecifier::DAY: return duckdb::Timestamp::GetDate(ts).day;
    case duckdb::DatePartSpecifier::HOUR: return duckdb::Timestamp::GetTime(ts).hour;
    case duckdb::DatePartSpecifier::MINUTE: return duckdb::Timestamp::GetTime(ts).min;
    case duckdb::DatePartSpecifier::SECOND: return duckdb::Timestamp::GetTime(ts).sec;
    case duckdb::DatePartSpecifier::MILLISECONDS: return duckdb::Timestamp::GetTime(ts).msec;
    case duckdb::DatePartSpecifier::MICROSECONDS: return duckdb::Timestamp::GetTime(ts).usec; // DuckDB timestamp_t is micros
    // Add other parts
    default: throw duckdb::NotImplementedException("Unsupported timestamp part for FFI EXTRACT");
    }
}

// These need to parse part_str. A map or if-else chain.
static duckdb::DatePartSpecifier StringToDatePartSpecifier(const char* part_str) {
    std::string part = duckdb::StringUtil::Lower(part_str);
    if (part == "year") return duckdb::DatePartSpecifier::YEAR;
    if (part == "month") return duckdb::DatePartSpecifier::MONTH;
    if (part == "day") return duckdb::DatePartSpecifier::DAY;
    if (part == "hour") return duckdb::DatePartSpecifier::HOUR;
    if (part == "minute") return duckdb::DatePartSpecifier::MINUTE;
    if (part == "second") return duckdb::DatePartSpecifier::SECOND;
    if (part == "milliseconds") return duckdb::DatePartSpecifier::MILLISECONDS;
    if (part == "microseconds") return duckdb::DatePartSpecifier::MICROSECONDS;
    throw duckdb::NotImplementedException("Unknown date part string for FFI EXTRACT: %s", part_str);
}


extern "C" DUCKDB_API int64_t duckdb_ffi_extract_from_date(int32_t date_val, const char* part_str) {
    try {
        duckdb::date_t date = duckdb::date_t(date_val);
        duckdb::DatePartSpecifier specifier = StringToDatePartSpecifier(part_str);
        return ExtractDatePart(date, specifier);
    } catch (...) { return -1; /* Error, or find a way to signal error to Lua */ }
}

extern "C" DUCKDB_API int64_t duckdb_ffi_extract_from_timestamp(int64_t ts_val, const char* part_str) {
    try {
        duckdb::timestamp_t ts = duckdb::timestamp_t(ts_val);
        duckdb::DatePartSpecifier specifier = StringToDatePartSpecifier(part_str);
        return ExtractTimestampPart(ts, specifier);
    } catch (...) { return -1; /* Error */ }
}

extern "C" DUCKDB_API int64_t duckdb_ffi_extract_year_from_date(int32_t date_val) {
    try {
        duckdb::date_t date = duckdb::date_t(date_val);
        return duckdb::Date::ExtractYear(date);
    } catch (...) { return -1; /* Error, or find a way to signal error to Lua */ }
}
