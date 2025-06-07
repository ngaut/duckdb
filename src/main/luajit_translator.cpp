#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
// Add includes for other BoundExpression subtypes as they are supported
#include <algorithm> // for std::find

namespace duckdb {

// --- LuaTranslatorContext ---
// Helper to get Lua FFI C type string from LogicalType
static std::string GetLuaFFITypeFromLogicalType(const LogicalType& type) {
    switch (type.id()) {
        case LogicalTypeId::INTEGER:    return "int32_t"; // Assuming FFIVector.data points to int32_t for INTEGER
        case LogicalTypeId::BIGINT:     return "int64_t";
        case LogicalTypeId::DOUBLE:     return "double";
        case LogicalTypeId::VARCHAR:    return "FFIString"; // FFIVector.data points to FFIString structs
        case LogicalTypeId::DATE:       return "int32_t"; // date_t
        case LogicalTypeId::TIMESTAMP:  return "int64_t"; // timestamp_t
        // Add other supported types
        default:
            throw NotImplementedException("LuaTranslatorContext: Unsupported logical type for FFI: " + type.ToString());
    }
}

LuaTranslatorContext::LuaTranslatorContext(const std::vector<LogicalType>& input_types)
    : input_logical_types_(input_types) {
    input_lua_ffi_types_.reserve(input_types.size());
    for (const auto& type : input_types) {
        input_lua_ffi_types_.push_back(GetLuaFFITypeFromLogicalType(type));
    }
}

std::string LuaTranslatorContext::GetInputVectorsTable() const {
    return "input_vectors";
}

std::string LuaTranslatorContext::GetInputLuaFFIType(idx_t col_idx) const {
    if (col_idx >= input_lua_ffi_types_.size()) {
        throw InternalException("LuaTranslatorContext: Column index out of bounds for GetInputLuaFFIType.");
    }
    return input_lua_ffi_types_[col_idx];
}
const LogicalType& LuaTranslatorContext::GetInputLogicalType(idx_t col_idx) const {
    if (col_idx >= input_logical_types_.size()) {
        throw InternalException("LuaTranslatorContext: Column index out of bounds for GetInputLogicalType.");
    }
    return input_logical_types_[col_idx];
}
idx_t LuaTranslatorContext::GetNumInputs() const {
    return input_logical_types_.size();
}


// --- LuaTranslator ---

// Helper to convert DuckDB ExpressionType (for binary operators) to Lua string
static std::string GetLuaOperatorFromExprType(ExpressionType op_type) {
    switch (op_type) {
        // Arithmetic
        case ExpressionType::OPERATOR_ADD:                return "+";
        case ExpressionType::OPERATOR_SUBTRACT:           return "-";
        case ExpressionType::OPERATOR_MULTIPLY:           return "*";
        case ExpressionType::OPERATOR_DIVIDE:             return "/";
        // Comparison (numeric and potentially others like strings, dates if inputs are prepared)
        case ExpressionType::COMPARE_EQUAL:               return "==";
        case ExpressionType::COMPARE_NOTEQUAL:            return "~=";
        case ExpressionType::COMPARE_LESSTHAN:            return "<";
        case ExpressionType::COMPARE_GREATERTHAN:         return ">";
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:   return "<=";
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:return ">=";
        // TODO: Add string CONCAT, LIKE, logical AND, OR, NOT if they are BoundOperatorExpression
        // Current PoC handles logical ops via specific expression types or separate logic.
        default:
            throw NotImplementedException("ExpressionType not yet supported as Lua binary operator: " + ExpressionTypeToString(op_type));
    }
}

// Helper to escape string literals for Lua
static std::string EscapeLuaString(const std::string& s) {
    std::string result = "\"";
    for (char c : s) {
        switch (c) {
        case '"': result += "\\\""; break;
        case '\\': result += "\\\\"; break;
        case '\n': result += "\\n"; break;
        case '\r': result += "\\r"; break;
        case '\t': result += "\\t"; break;
        default: result += c; break;
        }
    }
    result += "\"";
    return result;
}

// --- GenerateValue for specific BoundExpression types ---
std::string LuaTranslator::GenerateValue(const BoundConstantExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<column_binding>& referenced_columns) {
    const auto& val = expr.value;
    if (val.IsNull()) {
        return "nil"; // Represent SQL NULL as Lua nil for value expressions
    }
    switch (expr.return_type.id()) {
        case LogicalTypeId::INTEGER:  return std::to_string(val.GetValue<int32_t>());
        case LogicalTypeId::BIGINT:   return std::to_string(val.GetValue<int64_t>()); // Lua numbers are doubles
        case LogicalTypeId::DOUBLE:   return std::to_string(val.GetValue<double>());
        // For VARCHAR, DATE, TIMESTAMP, Lua representation of constant needs care
        // DATE/TIMESTAMP can be their integer representation.
        case LogicalTypeId::DATE:      return std::to_string(val.GetValue<date_t>().days);
        case LogicalTypeId::TIMESTAMP: return std::to_string(val.GetValue<timestamp_t>().micros);
        case LogicalTypeId::VARCHAR:   return EscapeLuaString(val.GetValue<string>());
        default:
            throw NotImplementedException("Unsupported constant type in LuaTranslator for BoundConstantExpression: " + expr.return_type.ToString());
    }
}

std::string LuaTranslator::GenerateValue(const BoundReferenceExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<column_binding>& referenced_columns) {
    column_binding binding(expr.index); // Assuming expr.index is the binding index for the DataChunk
                                        // This might need adjustment if it's a different kind of index.
                                        // For now, assume it maps to an input vector index.

    // Check if this binding is already recorded
    bool found = false;
    for(const auto& b : referenced_columns) {
        if (b.table_index == binding.table_index && b.column_index == binding.column_index) { // Simplistic check
            found = true;
            break;
        }
    }
    if (!found) {
        referenced_columns.push_back(binding);
    }

    // The actual column index for input_vectors table might be different from expr.index
    // if multiple tables are involved. For now, assume expr.index is the direct index
    // into the list of input vectors passed to the Lua function.
    // Lua tables are 1-indexed.
    idx_t input_vector_idx = expr.index + 1;

    const auto& type = ctx.GetInputLogicalType(expr.index);
    if (type.id() == LogicalTypeId::VARCHAR) {
        return StringUtil::Format("ffi.string(%s[%d].data[i].ptr, %s[%d].data[i].len)",
                                  ctx.GetInputVectorsTable(), input_vector_idx,
                                  ctx.GetInputVectorsTable(), input_vector_idx);
    }
    // For other types (numeric, date, timestamp as int) direct data access
    return StringUtil::Format("%s[%d].data[i]", ctx.GetInputVectorsTable(), input_vector_idx);
}

std::string LuaTranslator::GenerateValue(const BoundOperatorExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<column_binding>& referenced_columns) {
    if (expr.children.size() < 1 || expr.children.size() > 2) {
        throw NotImplementedException("LuaTranslator: BoundOperatorExpression with " + std::to_string(expr.children.size()) + " children not supported.");
    }

    std::string op_str = GetLuaOperatorFromExprType(expr.type); // Use DuckDB's ExpressionType

    std::string first_child_str = GenerateValueExpression(*expr.children[0], ctx, referenced_columns);

    if (expr.children.size() == 1) { // Unary operator (e.g. NOT, IS_NULL - though IS_NULL is different type)
        if (expr.type == ExpressionType::OPERATOR_IS_NULL) { // Special handling
             return StringUtil::Format("(%s[%d].nullmask[i])", ctx.GetInputVectorsTable(), static_cast<const BoundReferenceExpression&>(*expr.children[0]).index + 1); // Example direct nullmask access
        }
        // Assume other unary ops are prefix, e.g. "not (...)"
        // For "NOT", need to ensure child is boolean 0/1
        if (op_str == "not ") { // Assuming GetLuaOperatorFromExprType is extended for unary NOT
             return StringUtil::Format("(%s (%s == 1))", op_str, first_child_str);
        }
        return StringUtil::Format("(%s%s)", op_str, first_child_str);
    }

    // Binary operator
    std::string second_child_str = GenerateValueExpression(*expr.children[1], ctx, referenced_columns);

    // For logical AND/OR, ensure inputs are treated as booleans (0 or 1 from comparisons)
    if (expr.type == ExpressionType::CONJUNCTION_AND || expr.type == ExpressionType::CONJUNCTION_OR) { // These are actually different ExpressionClass
        // This block for BoundOperatorExpression might not hit for AND/OR if they are BoundConjunctionExpression
        // This logic is more for when AND/OR are passed as generic operators.
        return StringUtil::Format("((%s == 1) %s (%s == 1))", first_child_str, op_str, second_child_str);
    }

    // Handle string CONCAT
    if (expr.type == ExpressionType::OPERATOR_CONCAT) { // Assuming this is the type for '||'
        // Ensure children are treated as strings if they are FFIString columns
        // This needs type awareness from children expressions.
        // For now, assume first_child_str and second_child_str evaluate to Lua strings.
    }


    return StringUtil::Format("(%s %s %s)", first_child_str, op_str, second_child_str);
}


// Main recursive dispatch for GenerateValueExpression
std::string LuaTranslator::GenerateValueExpression(const Expression& expr, LuaTranslatorContext& ctx,
                                               std::vector<column_binding>& referenced_columns) {
    switch (expr.GetExpressionClass()) {
    case ExpressionClass::BOUND_CONSTANT:
        return GenerateValue(expr.Cast<BoundConstantExpression>(), ctx, referenced_columns);
    case ExpressionClass::BOUND_REF:
        return GenerateValue(expr.Cast<BoundReferenceExpression>(), ctx, referenced_columns);
    case ExpressionClass::BOUND_OPERATOR:
        return GenerateValue(expr.Cast<BoundOperatorExpression>(), ctx, referenced_columns);
    // TODO: Add cases for BoundFunctionExpression, BoundCaseExpression, BoundConjunctionExpression etc.
    // case ExpressionClass::BOUND_FUNCTION:
    // case ExpressionClass::BOUND_CASE:
    // case ExpressionClass::BOUND_CONJUNCTION:
    default:
        throw NotImplementedException("Unsupported BoundExpression class in LuaTranslator: " + ExpressionTypeToString(expr.type) + "[" + expr.GetExpressionClassString() + "]");
    }
}

// Main public method: TranslateExpressionToLuaRowLogic
std::string LuaTranslator::TranslateExpressionToLuaRowLogic(const Expression& expr, LuaTranslatorContext& ctx) {
    std::vector<column_binding> referenced_columns;
    std::string value_expr_str = GenerateValueExpression(expr, ctx, referenced_columns);

    // Sort referenced_columns by column_index for consistent null check generation
    std::sort(referenced_columns.begin(), referenced_columns.end(),
              [](const column_binding& a, const column_binding& b) {
                  return a.column_index < b.column_index;
              });
    // Remove duplicates that might arise if same column_binding used multiple times
    referenced_columns.erase(std::unique(referenced_columns.begin(), referenced_columns.end(),
                                       [](const column_binding& a, const column_binding& b) {
                                           return a.column_index == b.column_index; // Simple unique by index
                                       }), referenced_columns.end());


    std::stringstream ss;

    if (!referenced_columns.empty()) {
        ss << "if ";
        for (size_t k = 0; k < referenced_columns.size(); ++k) {
            // Here, referenced_columns[k].column_index is the original index from BoundReferenceExpression
            // This should map to the correct input vector in the Lua function.
            // If input vectors to Lua are ordered corresponding to their appearance in the query's projection
            // or input chunk, then this index is correct for GetInputVectorsTable()[idx+1].
            ss << ctx.GetInputVectorsTable() << "[" << referenced_columns[k].column_index + 1 << "].nullmask[i]";
            if (k < referenced_columns.size() - 1) {
                ss << " or ";
            }
        }
        ss << " then\n";
        ss << "    output_vector.nullmask[i] = true\n";
    } else {
        ss << "output_vector.nullmask[i] = false\n";
    }

    if (!referenced_columns.empty()) {
        ss << "else\n";
        ss << "    output_vector.nullmask[i] = false\n";
    }

    bool is_boolean_output_type = expr.return_type.id() == LogicalTypeId::BOOLEAN;
    // Some operators might also implicitly return boolean (e.g. LIKE if not a BoundOperatorExpression)
    // For BoundOperatorExpression, comparisons return BOOLEAN.
    // For this phase, we rely on expr.return_type.

    std::string indent = (!referenced_columns.empty()) ? "    " : "";
    if (is_boolean_output_type) {
        ss << indent << "if " << value_expr_str << " then\n"; // Lua true/false
        ss << indent << "    output_vector.data[i] = 1\n";    // C true (1)
        ss << indent << "else\n";
        ss << indent << "    output_vector.data[i] = 0\n";    // C false (0)
        ss << indent << "end\n";
    } else {
        // For numeric types, direct assignment.
        // For VARCHAR output, this is complex. Lua string needs to be written to FFIString.
        // This requires output_vector.data[i] to be FFIString, and then setting ptr and len.
        // Memory for ptr needs to be managed (e.g. from a string heap passed via FFI).
        // For Phase 1, we focus on numeric results or boolean-as-int.
        // If expr.return_type is VARCHAR, this line is conceptually incorrect for direct assignment.
        if (expr.return_type.id() == LogicalTypeId::VARCHAR && value_expr_str.rfind("ffi.string", 0) != 0 && value_expr_str != "nil") {
             // This is a Lua string, e.g. from CONCAT or string literal. Cannot assign directly to output_vector.data[i] if it's void* for FFIString array.
             // This part needs a robust solution for writing strings back.
             // For now, we'll comment out direct assignment for strings for clarity of the problem.
             // ss << indent << "-- String result: " << value_expr_str << " (needs FFIString output handling)\n";
             // As a placeholder, if it's a string, let's make it try to assign length, or error.
             // For the benchmark, string ops produced boolean. Here, if we allow string result:
             ss << indent << "-- Attempting to assign Lua string: " << value_expr_str << "\n";
             ss << indent << "-- This requires output_vector.data[i] to be FFIString and proper handling.\n";
             ss << indent << "-- For PoC, this will likely fail or be incorrect for string results.\n";
             // Let's assume for now, if a string expression gets here, it's an error or unhandled.
             // A proper way: call a C function to copy string to output FFIString.
             // e.g. output_vector.data[i].ptr, .len = copy_string_to_output_buffer(value_expr_str)
        } else {
             ss << indent << "output_vector.data[i] = " << value_expr_str << "\n";
        }
    }

    if (!referenced_columns.empty()) {
        ss << "end";
    }

    return ss.str();
}

} // namespace duckdb
