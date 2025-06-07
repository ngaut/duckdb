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
// Add other BoundExpression subtypes as needed (e.g., BoundFunctionExpression, BoundCaseExpression)
}


namespace duckdb {

// Context for translation
class LuaTranslatorContext {
public:
    // Constructor now takes a vector of input LogicalTypes
    DUCKDB_API LuaTranslatorContext(const std::vector<LogicalType>& input_types);

    DUCKDB_API std::string GetInputVectorsTable() const;

    // Get Lua type string (e.g., "int*", "double*", "FFIString*") for casting FFI data pointers
    DUCKDB_API std::string GetInputLuaFFIType(idx_t col_idx) const;
    DUCKDB_API const LogicalType& GetInputLogicalType(idx_t col_idx) const;
    DUCKDB_API idx_t GetNumInputs() const;

private:
    std::vector<LogicalType> input_logical_types_;
    std::vector<std::string> input_lua_ffi_types_; // Derived from logical_types_
};

class LuaTranslator {
public:
    // Main function now takes duckdb::Expression
    DUCKDB_API static std::string TranslateExpressionToLuaRowLogic(const Expression& expr, LuaTranslatorContext& ctx);

private:
    // Recursive helper now takes duckdb::Expression
    // referenced_columns now stores the actual column_binding.column_index from BoundReferenceExpression
    static std::string GenerateValueExpression(const Expression& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns);

    // Overloads for different DuckDB BoundExpression types
    static std::string GenerateValue(const BoundConstantExpression& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns);
    static std::string GenerateValue(const BoundReferenceExpression& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns);
    static std::string GenerateValue(const BoundOperatorExpression& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns);
    // Add new overloads for other BoundExpression types as they are supported:
    // static std::string GenerateValue(const BoundFunctionExpression& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns);
    // static std::string GenerateValue(const BoundCaseExpression& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns);
    // static std::string GenerateValue(const BoundConjunctionExpression& expr, LuaTranslatorContext& ctx, std::vector<column_binding>& referenced_columns);
};

} // namespace duckdb
