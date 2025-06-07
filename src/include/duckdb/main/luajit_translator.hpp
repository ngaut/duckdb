#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/luajit_expression_nodes.hpp" // Our expression node definitions
#include <string>
#include <vector>
#include <sstream> // For std::stringstream

namespace duckdb {

// Context for translation (e.g., to manage input vector names, types, etc.)
class LuaTranslatorContext {
public:
    DUCKDB_API LuaTranslatorContext(idx_t num_input_vectors);

    // Gets the Lua variable name for the FFIVector representing an input column chunk.
    // Lua code will access data like: input_vectors[col_idx_1_based].data[i]
    // This method could just return "input_vectors" and Lua code uses it as a table.
    // Or it could map col_idx to specific variable names if that's preferred.
    // For now, let's assume a single table `input_vectors` indexed by `col_idx + 1`.
    DUCKDB_API std::string GetInputVectorsTable() const;

    // If we needed to pass type information for casting data pointers in Lua:
    // DUCKDB_API std::string GetInputVectorDataTypeLua(idx_t col_idx) const;
    // DUCKDB_API void SetInputVectorType(idx_t col_idx, const std::string& lua_type_name);

    // Manages a list of input FFIVector C structs that the generated Lua function will expect.
    // The generated Lua function signature might look like:
    // function(output_vector, count, input_vectors_table)
    // where input_vectors_table is a Lua table of FFIVector userdata objects.

private:
    idx_t num_input_vectors_;
    // std::vector<std::string> input_vector_lua_types_; // Example for type info
};

class LuaTranslator {
public:
    // Main function to translate an expression tree to a Lua code string.
    // This string is intended to be the *body* of a Lua function that operates
    // per row, using an implicit loop variable `i`.
    // The caller (e.g., JIT execution engine) would wrap this body in a loop:
    //   "for i = 0, count - 1 do\n"
    //   "  " .. generated_code_from_this_function .. "\n"
    //   "end\n"
    //
    // The generated code will expect:
    // - `i`: the current row index.
    // - `input_vectors`: a Lua table (1-indexed) of FFIVector userdata objects.
    // - `output_vector`: an FFIVector userdata object for the output.
    //
    // It will produce Lua code that performs:
    //   output_vector.nullmask[i] = ...
    //   output_vector.data[i] = ...
    //
    // Returns the Lua code string for the per-row operation.
    DUCKDB_API static std::string TranslateExpressionToLuaRowLogic(const BaseExpression& expr, LuaTranslatorContext& ctx);

private:
    // Recursive helper that generates the Lua code for an expression's value,
    // assuming inputs are non-null. It returns the Lua code snippet that computes the value.
    // e.g., "input_vectors[1].data[i] + input_vectors[2].data[i]"
    static std::string GenerateValueExpression(const BaseExpression& expr, LuaTranslatorContext& ctx, std::vector<idx_t>& referenced_columns);

    // Overloads for different expression types for GenerateValueExpression
    static std::string GenerateValue(const ConstantExpression& expr, LuaTranslatorContext& ctx, std::vector<idx_t>& referenced_columns);
    static std::string GenerateValue(const ColumnReferenceExpression& expr, LuaTranslatorContext& ctx, std::vector<idx_t>& referenced_columns);
    static std::string GenerateValue(const BinaryOperatorExpression& expr, LuaTranslatorContext& ctx, std::vector<idx_t>& referenced_columns);
    static std::string GenerateValue(const UnaryOperatorExpression& expr, LuaTranslatorContext& ctx, std::vector<idx_t>& referenced_columns); // Added
    static std::string GenerateValue(const CaseExpression& expr, LuaTranslatorContext& ctx, std::vector<idx_t>& referenced_columns);          // Added
};

} // namespace duckdb
