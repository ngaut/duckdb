#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/logical_type.hpp" // For LogicalType
// #include "duckdb/planner/luajit_expression_nodes.hpp" // No longer primary input, but might be used internally if we map to it
#include <string>
#include <vector>
#include <sstream> // For std::stringstream

// Forward declare DuckDB BoundExpression types
namespace duckdb {
class Expression; // DuckDB's base class for bound expressions
class BoundConstantExpression;
class BoundReferenceExpression;
class BoundOperatorExpression;
class BoundFunctionExpression; // Forward declare
class BoundCaseExpression;   // Forward declare
}


namespace duckdb {

// Context for translation
class LuaTranslatorContext {
public:
    // Constructor now takes a vector of input LogicalTypes
    DUCKDB_API LuaTranslatorContext(const std::vector<LogicalType>& input_types);

    // Get Lua type string (e.g., "int32_t", "FFIString") for FFI data elements.
    DUCKDB_API std::string GetInputLuaFFIType(idx_t lua_arg_idx) const; // Takes lua arg index
    DUCKDB_API const LogicalType& GetInputLogicalType(idx_t lua_arg_idx) const; // Takes lua arg index
    DUCKDB_API idx_t GetNumInputs() const; // Number of unique inputs to the Lua function

    // Get Lua FFI type string for the output vector's data
    DUCKDB_API std::string GetOutputTypeLuaFFIType(const LogicalType& type) const;

    // Get the Lua argument index for a given original DataChunk column_index
    DUCKDB_API idx_t GetLuaArgIndex(idx_t original_chunk_col_idx) const;


private:
    // These now correspond to the unique, ordered inputs for the Lua function
    std::vector<LogicalType> unique_input_logical_types_;
    std::vector<std::string> unique_input_lua_ffi_types_;
    // Map from original DataChunk column index to the Lua function argument index (0-based)
    std::unordered_map<idx_t, idx_t> chunk_col_to_lua_arg_map_;
};

class LuaTranslator {
public:
    // Main function now takes duckdb::Expression
    // Returns a Lua code snippet that sets 'current_row_value' and 'current_row_is_null'
    DUCKDB_API static std::string TranslateExpressionToLuaRowLogic(const Expression& expr, LuaTranslatorContext& ctx);

private:
    // Recursive helper. Generates Lua code to compute the expression and store its result
    // and null status into Lua locals: result_var_name_val and result_var_name_is_null.
    // temp_var_idx is used to generate unique names for intermediate results of children.
    static std::string GenerateValueExpression(const Expression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);

    // Overloads for different DuckDB BoundExpression types
    static std::string GenerateValue(const BoundConstantExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);
    static std::string GenerateValue(const BoundReferenceExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);
    static std::string GenerateValue(const BoundOperatorExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);
    static std::string GenerateValue(const BoundFunctionExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);
    static std::string GenerateValue(const BoundCaseExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);
};

} // namespace duckdb
