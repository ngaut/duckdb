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
    if (part == "decade") return duckdb::DatePartSpecifier::DECADE;
    if (part == "century") return duckdb::DatePartSpecifier::CENTURY;
    if (part == "millennium") return duckdb::DatePartSpecifier::MILLENNIUM;
    if (part == "microseconds") return duckdb::DatePartSpecifier::MICROSECONDS;
    if (part == "milliseconds") return duckdb::DatePartSpecifier::MILLISECONDS;
    if (part == "second") return duckdb::DatePartSpecifier::SECOND;
    if (part == "minute") return duckdb::DatePartSpecifier::MINUTE;
    if (part == "hour") return duckdb::DatePartSpecifier::HOUR;
    if (part == "epoch") return duckdb::DatePartSpecifier::EPOCH;
    if (part == "dow") return duckdb::DatePartSpecifier::DAYOFWEEK; // day of week (Sunday = 0, Saturday = 6)
    if (part == "isodow") return duckdb::DatePartSpecifier::ISODAYOFWEEK; // ISO day of week (Monday = 1, Sunday = 7)
    if (part == "week") return duckdb::DatePartSpecifier::WEEK; // week number
    if (part == "quarter") return duckdb::DatePartSpecifier::QUARTER;
    if (part == "doy") return duckdb::DatePartSpecifier::DAYOFYEAR;
    // "isoyear" might be complex if it depends on week number logic
    // "timezone", "timezone_hour", "timezone_minute" are ignored for non-TZ timestamps
    throw duckdb::NotImplementedException("Unknown date part string for FFI EXTRACT: %s", part_str);
}


extern "C" DUCKDB_API int64_t duckdb_ffi_extract_from_date(int32_t date_val, const char* part_str) {
    try {
        duckdb::date_t date = duckdb::date_t(date_val);
        duckdb::DatePartSpecifier specifier = StringToDatePartSpecifier(part_str);
        // Date::ExtractField handles many common parts.
        // Need to add specific handlers for parts not covered by Date::ExtractField if any.
        switch (specifier) {
            case duckdb::DatePartSpecifier::EPOCH: return duckdb::Date::Epoch(date);
            case duckdb::DatePartSpecifier::DAYOFWEEK: return duckdb::Date::DayOfWeek(date); // Sunday is 0 in DuckDB
            case duckdb::DatePartSpecifier::ISODAYOFWEEK: return duckdb::Date::ExtractISODayOfWeek(date); // Monday is 1
            case duckdb::DatePartSpecifier::WEEK: return duckdb::Date::ExtractWeekNumber(date, duckdb::Date::ExtractISODayOfWeek(date), duckdb::Date::ExtractDayOfTheYearRegular(date)); // Using DuckDB's internal logic for ISO week
            case duckdb::DatePartSpecifier::DAYOFYEAR: return duckdb::Date::ExtractDayOfTheYearRegular(date);
            case duckdb::DatePartSpecifier::QUARTER: return duckdb::Date::ExtractQuarter(date);
            // YEAR, MONTH, DAY, DECADE, CENTURY, MILLENNIUM are typically handled by Date::ExtractField or specific functions
            case duckdb::DatePartSpecifier::YEAR: return duckdb::Date::ExtractYear(date);
            case duckdb::DatePartSpecifier::MONTH: return duckdb::Date::ExtractMonth(date);
            case duckdb::DatePartSpecifier::DAY: return duckdb::Date::ExtractDay(date);
            // For date, time parts are 0
            case duckdb::DatePartSpecifier::HOUR:
            case duckdb::DatePartSpecifier::MINUTE:
            case duckdb::DatePartSpecifier::SECOND:
            case duckdb::DatePartSpecifier::MILLISECONDS:
            case duckdb::DatePartSpecifier::MICROSECONDS:
                return 0;
            default:
                 throw duckdb::NotImplementedException("Unsupported date part for FFI EXTRACT from DATE: %s", part_str);
        }
    } catch (...) { return -1; /* Error, or find a way to signal error to Lua */ }
}

extern "C" DUCKDB_API int64_t duckdb_ffi_extract_from_timestamp(int64_t ts_val, const char* part_str) {
    try {
        duckdb::timestamp_t ts = duckdb::timestamp_t(ts_val);
        duckdb::DatePartSpecifier specifier = StringToDatePartSpecifier(part_str);
        // Timestamp::ExtractField handles many common parts.
        // Need to add specific handlers for parts not covered by Timestamp::ExtractField if any.
        switch (specifier) {
            case duckdb::DatePartSpecifier::EPOCH: return duckdb::Timestamp::GetEpochSeconds(ts);
            case duckdb::DatePartSpecifier::YEAR: return duckdb::Timestamp::ExtractYear(ts);
            case duckdb::DatePartSpecifier::MONTH: return duckdb::Timestamp::ExtractMonth(ts);
            case duckdb::DatePartSpecifier::DAY: return duckdb::Timestamp::ExtractDay(ts);
            case duckdb::DatePartSpecifier::HOUR: return duckdb::Timestamp::ExtractHour(ts);
            case duckdb::DatePartSpecifier::MINUTE: return duckdb::Timestamp::ExtractMinute(ts);
            case duckdb::DatePartSpecifier::SECOND: return duckdb::Timestamp::ExtractSecond(ts);
            case duckdb::DatePartSpecifier::MILLISECONDS: return duckdb::Timestamp::ExtractMillisecond(ts);
            case duckdb::DatePartSpecifier::MICROSECONDS: return duckdb::Timestamp::ExtractMicrosecond(ts);
            case duckdb::DatePartSpecifier::DAYOFWEEK: return duckdb::Date::DayOfWeek(duckdb::Timestamp::GetDate(ts));
            case duckdb::DatePartSpecifier::ISODAYOFWEEK: return duckdb::Date::ExtractISODayOfWeek(duckdb::Timestamp::GetDate(ts));
            case duckdb::DatePartSpecifier::DAYOFYEAR: return duckdb::Date::ExtractDayOfTheYearRegular(duckdb::Timestamp::GetDate(ts));
            case duckdb::DatePartSpecifier::QUARTER: return duckdb::Date::ExtractQuarter(duckdb::Timestamp::GetDate(ts));
            case duckdb::DatePartSpecifier::WEEK: {
                duckdb::date_t date_part = duckdb::Timestamp::GetDate(ts);
                return duckdb::Date::ExtractWeekNumber(date_part, duckdb::Date::ExtractISODayOfWeek(date_part), duckdb::Date::ExtractDayOfTheYearRegular(date_part));
            }
            default:
                throw duckdb::NotImplementedException("Unsupported date part for FFI EXTRACT from TIMESTAMP: %s", part_str);
        }
    } catch (...) { return -1; /* Error */ }
}

extern "C" DUCKDB_API int64_t duckdb_ffi_extract_year_from_date(int32_t date_val) {
    try {
        duckdb::date_t date = duckdb::date_t(date_val);
        return duckdb::Date::ExtractYear(date); // Already correct
    } catch (...) { return -1; /* Error, or find a way to signal error to Lua */ }
}


extern "C" DUCKDB_API int duckdb_ffi_add_lua_string_table_to_output_vector(lua_State* L) {
    // Expected Lua call: duckdb_ffi_add_lua_string_table_to_output_vector(output_vec_ffi_ptr, results_table, count_val)
    // Stack upon C entry:
    // 1: output_vec_ffi_ptr (lightuserdata)
    // 2: results_table (Lua table)
    // 3: count (integer)
    if (lua_gettop(L) != 3) {
        return luaL_error(L, "duckdb_ffi_add_lua_string_table_to_output_vector: incorrect argument count (expected 3: FFIVector*, table, count)");
    }
    if (!lua_islightuserdata(L, 1)) {
        return luaL_error(L, "duckdb_ffi_add_lua_string_table_to_output_vector: arg 1 must be lightuserdata (FFIVector*)");
    }
    if (!lua_istable(L, 2)) {
        return luaL_error(L, "duckdb_ffi_add_lua_string_table_to_output_vector: arg 2 must be a table");
    }
    if (!lua_isinteger(L, 3)) {
        return luaL_error(L, "duckdb_ffi_add_lua_string_table_to_output_vector: arg 3 must be an integer (count)");
    }

    duckdb::ffi::FFIVector* output_ffi_meta = reinterpret_cast<duckdb::ffi::FFIVector*>(lua_touserdata(L, 1));
    // Lua table is at stack index 2
    duckdb::idx_t count = lua_tointeger(L, 3);

    if (!output_ffi_meta || !output_ffi_meta->original_duckdb_vector) {
        return luaL_error(L, "duckdb_ffi_add_lua_string_table_to_output_vector: Invalid FFIVector metadata passed");
    }
    duckdb::Vector* actual_vector = reinterpret_cast<duckdb::Vector*>(output_ffi_meta->original_duckdb_vector);
    if (actual_vector->GetType().id() != duckdb::LogicalTypeId::VARCHAR) {
        return luaL_error(L, "duckdb_ffi_add_lua_string_table_to_output_vector: Output vector is not of VARCHAR type");
    }

    // Ensure the vector is flat; ExpressionExecutor should have already set this up.
    // If not, this might be needed: actual_vector->SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    // And ensure it can hold 'count' items, though Resize might be too aggressive if count is less than capacity.
    // duckdb::FlatVector::SetCount(*actual_vector, count); // SetCount is usually for logical size.

    for (duckdb::idx_t i = 0; i < count; ++i) {
        lua_rawgeti(L, 2, i + 1); // Get results_table[i+1] (1-indexed Lua table)
        int lua_type_on_stack = lua_type(L, -1); // Type of the value just pushed

        if (lua_type_on_stack == LUA_TNIL) {
            duckdb::FlatVector::SetNull(*actual_vector, i, true);
        } else if (lua_type_on_stack == LUA_TSTRING) {
            size_t len;
            const char* str_data = lua_tolstring(L, -1, &len);
            // SetValue handles string heap allocation for flat vectors.
            actual_vector->SetValue(i, duckdb::Value(std::string(str_data, len)));
            // SetValue should also mark as not null, but being explicit is safe.
             duckdb::FlatVector::SetNull(*actual_vector, i, false);
        } else {
            // Error: element in table is not a string or nil.
            // To prevent partial writes or undefined behavior, pop value and error out.
            lua_pop(L, 1);
            return luaL_error(L, "duckdb_ffi_add_lua_string_table_to_output_vector: table element at index %d is not a string or nil (type: %s)",
                              i + 1, lua_typename(L, lua_type_on_stack));
        }
        lua_pop(L, 1); // Pop the string/nil value from stack
    }
    return 0; // Number of results pushed onto Lua stack by this C function (none)
}
