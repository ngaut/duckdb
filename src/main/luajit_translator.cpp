#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/common/exception.hpp" // For NotImplementedException
#include <algorithm> // for std::find

namespace duckdb {

// --- LuaTranslatorContext ---
LuaTranslatorContext::LuaTranslatorContext(idx_t num_input_vectors) : num_input_vectors_(num_input_vectors) {
    // Initialization if needed, e.g., for type information
    // input_vector_lua_types_.resize(num_input_vectors);
}

std::string LuaTranslatorContext::GetInputVectorsTable() const {
    return "input_vectors"; // Lua code will use input_vectors[1], input_vectors[2], ...
}

// --- LuaTranslator ---

// Helper to convert binary operator type to Lua string
static std::string GetLuaOperator(LuaJITBinaryOperatorType op_type) {
    switch (op_type) {
    // Arithmetic
    case LuaJITBinaryOperatorType::ADD:
        return "+";
    case LuaJITBinaryOperatorType::SUBTRACT:
        return "-";
    case LuaJITBinaryOperatorType::MULTIPLY:
        return "*";
    case LuaJITBinaryOperatorType::DIVIDE:
        return "/";
    // Comparison
    case LuaJITBinaryOperatorType::EQUALS:
        return "==";
    case LuaJITBinaryOperatorType::NOT_EQUALS:
        return "~=";
    case LuaJITBinaryOperatorType::GREATER_THAN:
        return ">";
    case LuaJITBinaryOperatorType::LESS_THAN:
        return "<";
    case LuaJITBinaryOperatorType::GREATER_THAN_OR_EQUALS:
        return ">=";
    case LuaJITBinaryOperatorType::LESS_THAN_OR_EQUALS:
        return "<=";
    // Logical
    case LuaJITBinaryOperatorType::AND:
        return "and"; // Note: requires operands to be Lua booleans (or 0/1 converted)
    case LuaJITBinaryOperatorType::OR:
        return "or";  // Note: requires operands to be Lua booleans (or 0/1 converted)
    // String
    case LuaJITBinaryOperatorType::CONCAT:
        return "..";
    case LuaJITBinaryOperatorType::LIKE:
        // LIKE is handled specially, not a simple operator
        throw InternalException("LIKE operator should be handled by a special function call in LuaTranslator");
    default:
        throw NotImplementedException("LuaJITBinaryOperatorType not yet supported in GetLuaOperator: " + std::to_string((int)op_type));
    }
}

// Helper to convert unary operator type to Lua string
static std::string GetLuaUnaryOperator(LuaJITUnaryOperatorType op_type) {
    switch (op_type) {
    case LuaJITUnaryOperatorType::NOT:
        return "not "; // Note: requires operand to be Lua boolean (or 0/1 converted)
    default:
        throw NotImplementedException("LuaJITUnaryOperatorType not yet supported in GetLuaUnaryOperator");
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
        // Add more escapes if needed
        default: result += c; break;
        }
    }
    result += "\"";
    return result;
}


// Implementation of GenerateValue for ConstantExpression
std::string LuaTranslator::GenerateValue(const ConstantExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<idx_t>& referenced_columns) {
    if (std::holds_alternative<int>(expr.value)) {
        return std::to_string(std::get<int>(expr.value));
    } else if (std::holds_alternative<double>(expr.value)) {
        return std::to_string(std::get<double>(expr.value));
    } else if (std::holds_alternative<std::string>(expr.value)) {
        return EscapeLuaString(std::get<std::string>(expr.value));
    } else {
        throw NotImplementedException("Unsupported constant type in LuaTranslator");
    }
}

// Implementation of GenerateValue for ColumnReferenceExpression
std::string LuaTranslator::GenerateValue(const ColumnReferenceExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<idx_t>& referenced_columns) {
    if (std::find(referenced_columns.begin(), referenced_columns.end(), expr.column_index) == referenced_columns.end()) {
        referenced_columns.push_back(expr.column_index);
    }
    // For VARCHAR, this needs to become ffi.string(ptr, len)
    // This requires type information. For now, assume numeric/boolean.
    // A type parameter or looking up type in context would be needed.
    // For PoC, let's assume numeric types are directly accessed via .data[i]
    // and string types will require specific handling in BinaryOperator for CONCAT/LIKE/compare
    // or a more complex GetValue that knows the type.
    // Let's assume for now this returns the raw data access string, and type-specific
    // wrapping (like ffi.string) happens in the operator logic if needed.
    return StringUtil::Format("%s[%d].data[i]", ctx.GetInputVectorsTable(), expr.column_index + 1);
}

// Implementation of GenerateValue for BinaryOperatorExpression
std::string LuaTranslator::GenerateValue(const BinaryOperatorExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<idx_t>& referenced_columns) {
    std::string left_str = GenerateValueExpression(*expr.left_child, ctx, referenced_columns);
    std::string right_str = GenerateValueExpression(*expr.right_child, ctx, referenced_columns);

    // Type assumptions:
    // For arithmetic/numeric comparisons: left_str, right_str are direct numeric values.
    // For string ops (CONCAT, LIKE, string compare): left_str, right_str should evaluate to Lua strings.
    // This might mean wrapping them with ffi.string if they are column refs to FFIString.
    // For logical ops (AND, OR): left_str, right_str should evaluate to Lua booleans (or 0/1 that we convert).

    // TODO: Add type-aware wrapping for string operations.
    // For example, if it's a string column_ref, left_str might be:
    // string.format("ffi.string(%s[%d].data[i].ptr, %s[%d].data[i].len)", table, idx+1, table, idx+1)
    // This is simplified for now. Assume inputs are already appropriate types or simple values.

    if (expr.operator_type == LuaJITBinaryOperatorType::LIKE) {
        // Simplified LIKE: pattern is right_str (constant string expected)
        // left_str is the string to check
        // Assuming right_str is already a Lua string literal like "\"%pattern%\""
        // And left_str evaluates to a Lua string (e.g. ffi.string(ptr,len) or another literal)

        // For this PoC, assume right_str is a simple pattern string literal from ConstantExpression
        std::string pattern_str_literal = GenerateValueExpression(*expr.right_child, ctx, referenced_columns);
        // Remove quotes for direct use in string.sub/find
        std::string pattern_val = std::get<std::string>(static_cast<const ConstantExpression&>(*expr.right_child).value);

        if (pattern_val.front() == '%' && pattern_val.back() == '%') { // %abc% -> contains
            std::string sub = pattern_val.substr(1, pattern_val.length() - 2);
            return StringUtil::Format("(string.find(%s, \"%s\", 1, true) ~= nil)", left_str, sub);
        } else if (pattern_val.front() == '%') { // %abc -> ends with
            std::string sub = pattern_val.substr(1);
            return StringUtil::Format("(string.sub(%s, -string.len(\"%s\")) == \"%s\")", left_str, sub, sub);
        } else if (pattern_val.back() == '%') { // abc% -> starts with
            std::string sub = pattern_val.substr(0, pattern_val.length() - 1);
            return StringUtil::Format("(string.sub(%s, 1, string.len(\"%s\")) == \"%s\")", left_str, sub, sub);
        } else { // exact match (or more complex pattern not handled by this simple version)
            return StringUtil::Format("(%s == %s)", left_str, pattern_str_literal);
        }
    }

    std::string op_str = GetLuaOperator(expr.operator_type);

    // For logical AND/OR, ensure inputs are treated as booleans (0 or 1 from comparisons)
    if (expr.operator_type == LuaJITBinaryOperatorType::AND || expr.operator_type == LuaJITBinaryOperatorType::OR) {
        return StringUtil::Format("((%s == 1) %s (%s == 1))", left_str, op_str, right_str);
    }

    return StringUtil::Format("(%s %s %s)", left_str, op_str, right_str);
}

// Implementation of GenerateValue for UnaryOperatorExpression
std::string LuaTranslator::GenerateValue(const UnaryOperatorExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<idx_t>& referenced_columns) {
    std::string child_str = GenerateValueExpression(*expr.child_expression, ctx, referenced_columns);
    std::string op_str = GetLuaUnaryOperator(expr.operator_type);

    if (expr.operator_type == LuaJITUnaryOperatorType::NOT) {
        // Assuming child_str evaluates to 0 or 1 (result of a comparison)
        return StringUtil::Format("(%s(%s == 1))", op_str, child_str);
    }
    return StringUtil::Format("(%s%s)", op_str, child_str); // Default prefix unary
}

// Implementation of GenerateValue for CaseExpression
std::string LuaTranslator::GenerateValue(const CaseExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<idx_t>& referenced_columns) {
    // Simplified: CASE WHEN condition THEN result ELSE else_result END
    // Assumes only one branch for this PoC.
    if (expr.case_branches.empty()) {
        throw InternalException("CASE expression without branches in LuaTranslator");
    }
    const auto& branch = expr.case_branches[0];
    std::string condition_str = GenerateValueExpression(*branch.condition, ctx, referenced_columns);
    std::string result_true_str = GenerateValueExpression(*branch.result_if_true, ctx, referenced_columns);
    std::string result_else_str = GenerateValueExpression(*expr.result_if_else, ctx, referenced_columns);

    // Condition is expected to be 0 or 1 (result of a comparison)
    return StringUtil::Format("(function() if (%s == 1) then return %s else return %s end end)()",
                              condition_str, result_true_str, result_else_str);
}


// Main recursive dispatch for GenerateValueExpression
std::string LuaTranslator::GenerateValueExpression(const BaseExpression& expr, LuaTranslatorContext& ctx,
                                               std::vector<idx_t>& referenced_columns) {
    switch (expr.type) {
    case LuaJITExpressionType::CONSTANT:
        return GenerateValue(static_cast<const ConstantExpression&>(expr), ctx, referenced_columns);
    case LuaJITExpressionType::COLUMN_REFERENCE:
        return GenerateValue(static_cast<const ColumnReferenceExpression&>(expr), ctx, referenced_columns);
    case LuaJITExpressionType::BINARY_OPERATOR:
        return GenerateValue(static_cast<const BinaryOperatorExpression&>(expr), ctx, referenced_columns);
    case LuaJITExpressionType::UNARY_OPERATOR:
        return GenerateValue(static_cast<const UnaryOperatorExpression&>(expr), ctx, referenced_columns);
    case LuaJITExpressionType::CASE_EXPRESSION:
        return GenerateValue(static_cast<const CaseExpression&>(expr), ctx, referenced_columns);
    default:
        throw NotImplementedException("Unsupported expression type in LuaTranslator::GenerateValueExpression");
    }
}

// Main public method: TranslateExpressionToLuaRowLogic
std::string LuaTranslator::TranslateExpressionToLuaRowLogic(const BaseExpression& expr, LuaTranslatorContext& ctx) {
    std::vector<idx_t> referenced_columns;
    std::string value_expr_str = GenerateValueExpression(expr, ctx, referenced_columns);

    std::sort(referenced_columns.begin(), referenced_columns.end()); // Ensure consistent order for null checks

    std::stringstream ss;

    if (!referenced_columns.empty()) {
        ss << "if ";
        for (size_t k = 0; k < referenced_columns.size(); ++k) {
            ss << ctx.GetInputVectorsTable() << "[" << referenced_columns[k] + 1 << "].nullmask[i]";
            if (k < referenced_columns.size() - 1) {
                ss << " or ";
            }
        }
        ss << " then\n";
        ss << "    output_vector.nullmask[i] = true\n";
        // For CASE, if condition is null, result is null. If condition met but result_expr is null, result is null.
        // This top-level null check based on inputs covers this for direct inputs.
        // If a CASE branch itself evaluates to NULL due to its own inputs, that specific branch's
        // value_expr_str will be 'nil' or it will propagate null through its own sub-logic.
        // The current GenerateValue for CASE doesn't explicitly return 'nil' but a value,
        // assuming its subexpressions handle their own nulls to produce a value or propagate null to this outer check.
        // This needs careful thought for full SQL null semantics for CASE.
        // For now, if any referenced column in the *entire* expression is null, the output is null.
    } else {
        // No column references, expression is based on constants only.
        ss << "output_vector.nullmask[i] = false\n";
    }

    // This 'else' block is for when all direct inputs are NOT NULL.
    // The expression itself (value_expr_str) might still evaluate to NULL in Lua (e.g. string op on nil)
    // or produce a value that needs to be stored.
    if (!referenced_columns.empty()) { // Only add 'else' if there was an 'if'
        ss << "else\n";
        ss << "    output_vector.nullmask[i] = false\n"; // Tentatively set to not null
    }

    // Determine if the expression's result type is boolean (from comparisons, logical ops)
    // to convert Lua true/false to 1/0 for C.
    bool is_boolean_result = false;
    if (expr.type == LuaJITExpressionType::BINARY_OPERATOR) {
        auto& bin_op = static_cast<const BinaryOperatorExpression&>(expr);
        is_boolean_result = (bin_op.operator_type == LuaJITBinaryOperatorType::EQUALS ||
                             bin_op.operator_type == LuaJITBinaryOperatorType::NOT_EQUALS ||
                             bin_op.operator_type == LuaJITBinaryOperatorType::GREATER_THAN ||
                             bin_op.operator_type == LuaJITBinaryOperatorType::LESS_THAN ||
                             bin_op.operator_type == LuaJITBinaryOperatorType::GREATER_THAN_OR_EQUALS ||
                             bin_op.operator_type == LuaJITBinaryOperatorType::LESS_THAN_OR_EQUALS ||
                             bin_op.operator_type == LuaJITBinaryOperatorType::AND || // AND/OR results are effectively boolean
                             bin_op.operator_type == LuaJITBinaryOperatorType::OR ||
                             bin_op.operator_type == LuaJITBinaryOperatorType::LIKE);
    } else if (expr.type == LuaJITExpressionType::UNARY_OPERATOR) {
        auto& un_op = static_cast<const UnaryOperatorExpression&>(expr);
        is_boolean_result = (un_op.operator_type == LuaJITUnaryOperatorType::NOT);
    }
    // CASE expressions can return boolean too, but this depends on the result expressions.
    // For now, assume CASE does not automatically mean boolean result unless its inner exprs are.
    // This simple flag might not be perfect for all nested cases.

    std::string indent = (!referenced_columns.empty()) ? "    " : "";
    if (is_boolean_result) {
        ss << indent << "if " << value_expr_str << " then\n";
        ss << indent << "    output_vector.data[i] = 1\n";
        ss << indent << "else\n";
        ss << indent << "    output_vector.data[i] = 0\n";
        ss << indent << "end\n";
    } else {
        // For non-boolean results (numeric, string from CONCAT, or CASE returning non-bool)
        // TODO: For string results, output_vector.data[i] needs to be an FFIString struct.
        // This requires the output FFIVector to be typed or to have a way to set ptr/len.
        // This PoC primarily handles numeric output or boolean-as-int. String output is complex.
        // If value_expr_str is a Lua string (e.g. from CONCAT), this assignment is wrong.
        // For now, we assume output is numeric/boolean-as-int.
        ss << indent << "output_vector.data[i] = " << value_expr_str << "\n";
    }

    if (!referenced_columns.empty()) { // Close the 'if/else' for null checks
        ss << "end";
    }

    return ss.str();
}

} // namespace duckdb
