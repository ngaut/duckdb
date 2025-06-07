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
        case LogicalTypeId::INTEGER:    return "int32_t";
        case LogicalTypeId::BIGINT:     return "int64_t";
        case LogicalTypeId::DOUBLE:     return "double";
        case LogicalTypeId::VARCHAR:    return "FFIString";
        case LogicalTypeId::DATE:       return "int32_t";
        case LogicalTypeId::TIMESTAMP:  return "int64_t";
        case LogicalTypeId::INTERVAL:   return "FFIInterval"; // Added
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
        // Comparison
        case ExpressionType::COMPARE_EQUAL:               return "==";
        case ExpressionType::COMPARE_NOTEQUAL:            return "~=";
        case ExpressionType::COMPARE_LESSTHAN:            return "<";
        case ExpressionType::COMPARE_GREATERTHAN:         return ">";
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:   return "<=";
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:return ">=";
        // String Concat Operator (if binder maps || to this)
        case ExpressionType::OPERATOR_CONCAT:             return "..";
        // Logical Operators (if represented as generic operator)
        // Note: DuckDB uses BoundConjunctionExpression for AND/OR, BoundOperatorExpression for NOT.
        case ExpressionType::OPERATOR_NOT:                return "not "; // Unary, handled in GenerateValue for BoundOperatorExpression
        default:
            // LIKE, AND, OR are typically specific expression classes or functions, not generic BoundOperatorExpression types.
            throw NotImplementedException("ExpressionType not directly mapped to a simple Lua binary operator: " + ExpressionTypeToString(op_type));
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
        case LogicalTypeId::INTERVAL: {
            auto& interval_val = val.GetValue<interval_t>();
            return StringUtil::Format("ffi.new(\"FFIInterval\", { months = %d, days = %d, micros = %lld })",
                                      interval_val.months, interval_val.days, interval_val.micros);
        }
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
    // For INTERVAL, this would return a pointer to FFIInterval, Lua needs to access fields.
    if (type.id() == LogicalTypeId::INTERVAL) {
         // This gives the FFIInterval struct itself (by value if it's an array of structs)
         // Lua would then do ffi_interval_val.months, .days, .micros
        return StringUtil::Format("%s[%d].data[i]", ctx.GetInputVectorsTable(), input_vector_idx);
    }
    return StringUtil::Format("%s[%d].data[i]", ctx.GetInputVectorsTable(), input_vector_idx);
}

#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/date_functions.hpp" // For DatePartSpecifier for EXTRACT

// Forward declaration for BoundFunctionExpression GenerateValue
static std::string GenerateValueBoundFunction(const BoundFunctionExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<column_binding>& referenced_columns);
// Forward declaration for BoundCaseExpression GenerateValue
static std::string GenerateValueBoundCase(const BoundCaseExpression& expr, LuaTranslatorContext& ctx,
                                        std::vector<column_binding>& referenced_columns);


std::string LuaTranslator::GenerateValue(const BoundOperatorExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<column_binding>& referenced_columns) {
    if (expr.children.empty()) { // Should not happen for operators we handle
        throw InternalException("LuaTranslator: BoundOperatorExpression with no children.");
    }

    std::string first_child_str = GenerateValueExpression(*expr.children[0], ctx, referenced_columns);

    if (expr.type == ExpressionType::OPERATOR_NOT) { // Unary NOT
        // DuckDB's NOT operator returns BOOLEAN. Input is also BOOLEAN.
        // Lua's 'not' works on Lua booleans. Our inputs from comparisons are 0 or 1.
        return StringUtil::Format("(not (%s == 1))", first_child_str);
    }
    if (expr.type == ExpressionType::OPERATOR_IS_NULL) { // Unary IS NULL
        // This needs to check the nullmask of the child's original column.
        // The child_str is the *value* if not null. We need the column binding of the child.
        // This requires a more complex way to get the nullmask for an arbitrary child expression.
        // For a simple BoundReference child:
        if (expr.children[0]->GetExpressionClass() == ExpressionClass::BOUND_REF) {
            auto& ref_expr = expr.children[0]->Cast<BoundReferenceExpression>();
            // Ensure this column is "visited" for the outer null checks if it wasn't already part of value_expr_str
            column_binding binding(ref_expr.index);
            bool found = false;
            for(const auto& b : referenced_columns) { if (b.column_index == binding.column_index) { found = true; break; } }
            if (!found) { referenced_columns.push_back(binding); }
            return StringUtil::Format("(%s[%d].nullmask[i])", ctx.GetInputVectorsTable(), ref_expr.index + 1);
        }
        throw NotImplementedException("LuaTranslator: IS NULL on non-BoundReference child not yet fully supported for JIT.");
    }
     if (expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
        if (expr.children[0]->GetExpressionClass() == ExpressionClass::BOUND_REF) {
            auto& ref_expr = expr.children[0]->Cast<BoundReferenceExpression>();
            column_binding binding(ref_expr.index);
            bool found = false;
            for(const auto& b : referenced_columns) { if (b.column_index == binding.column_index) { found = true; break; } }
            if (!found) { referenced_columns.push_back(binding); }
            return StringUtil::Format("(not %s[%d].nullmask[i])", ctx.GetInputVectorsTable(), ref_expr.index + 1);
        }
        throw NotImplementedException("LuaTranslator: IS NOT NULL on non-BoundReference child not yet fully supported for JIT.");
    }


    if (expr.children.size() != 2) { // Must be binary from here
        throw NotImplementedException("LuaTranslator: BoundOperatorExpression with " + std::to_string(expr.children.size()) + " children not supported for this op type.");
    }
    std::string op_str = GetLuaOperatorFromExprType(expr.type);
    std::string second_child_str = GenerateValueExpression(*expr.children[1], ctx, referenced_columns);

    // String CONCAT specific handling (type awareness)
    if (expr.type == ExpressionType::OPERATOR_CONCAT) {
        // Children might be string constants or string column references.
        // GenerateValue for BoundReference already wraps VARCHAR with ffi.string().
        // GenerateValue for BoundConstant already generates Lua string literals.
        // So, first_child_str and second_child_str should be Lua strings here.
    }
    // Other binary ops assume numeric/comparable inputs based on GetLuaOperatorFromExprType mapping
    return StringUtil::Format("(%s %s %s)", first_child_str, op_str, second_child_str);
}

// Placeholder for BoundFunctionExpression
// This would be a large switch on function name.
std::string GenerateValueBoundFunction(const BoundFunctionExpression& expr, LuaTranslatorContext& ctx,
                                         std::vector<column_binding>& referenced_columns) {
    std::string func_name_lower = StringUtil::Lower(expr.function.name);
    std::vector<std::string> arg_strs;
    for(const auto& child : expr.children) {
        arg_strs.push_back(GenerateValueExpression(*child, ctx, referenced_columns));
    }

    if (func_name_lower == "lower") {
        if (arg_strs.size() != 1) throw InternalException("LOWER expects 1 arg");
        return StringUtil::Format("string.lower(%s)", arg_strs[0]);
    } else if (func_name_lower == "upper") {
        if (arg_strs.size() != 1) throw InternalException("UPPER expects 1 arg");
        return StringUtil::Format("string.upper(%s)", arg_strs[0]);
    } else if (func_name_lower == "length" || func_name_lower == "strlen") {
        if (arg_strs.size() != 1) throw InternalException("LENGTH expects 1 arg");
        return StringUtil::Format("#(%s)", arg_strs[0]); // Lua string length operator
    } else if (func_name_lower == "substring" || func_name_lower == "substr") {
        if (arg_strs.size() != 3 && arg_strs.size() != 2) throw InternalException("SUBSTRING expects 2 or 3 args");
        if (arg_strs.size() == 3) { // string.sub(s, i [, j])
            // SQL SUBSTRING(str FROM pos FOR len) -> string.sub(str, pos, pos + len - 1)
            // Lua pos is 1-based. DuckDB pos is 1-based.
            // Lua end index is inclusive.
            return StringUtil::Format("string.sub(%s, %s, (%s + %s - 1))", arg_strs[0], arg_strs[1], arg_strs[1], arg_strs[2]);
        } else { // string.sub(s, i) -- from i to end
             return StringUtil::Format("string.sub(%s, %s)", arg_strs[0], arg_strs[1]);
        }
    } else if (func_name_lower == "concat") { // For explicit concat function
        std::string res = "";
        for(size_t i=0; i<arg_strs.size(); ++i) {
            res += arg_strs[i];
            if (i < arg_strs.size() -1) res += " .. ";
        }
        return "(" + res + ")";
    }
    // LIKE is often a BoundLikeExpression, but if it's a function:
    // else if (func_name_lower == "like") { ... }
    // Numeric functions
    else if (func_name_lower == "abs") {
        if (arg_strs.size() != 1) throw InternalException("ABS expects 1 arg");
        return StringUtil::Format("math.abs(%s)", arg_strs[0]);
    } else if (func_name_lower == "floor") {
        if (arg_strs.size() != 1) throw InternalException("FLOOR expects 1 arg");
        return StringUtil::Format("math.floor(%s)", arg_strs[0]);
    } else if (func_name_lower == "ceil") {
        if (arg_strs.size() != 1) throw InternalException("CEIL expects 1 arg");
        return StringUtil::Format("math.ceil(%s)", arg_strs[0]);
    } else if (func_name_lower == "round") {
        if (arg_strs.size() == 1) { // round(num)
            return StringUtil::Format("math.floor(%s + 0.5)", arg_strs[0]); // Lua 5.1 doesn't have math.round
        } else if (arg_strs.size() == 2) { // round(num, precision)
            // prec_arg is arg_strs[1]
            // local p = 10^prec_arg; return math.floor(num_arg*p+0.5)/p
            return StringUtil::Format("(function() local p = 10^(%s); return math.floor((%s)*p+0.5)/p end)()", arg_strs[1], arg_strs[0]);
        }
        throw InternalException("ROUND expects 1 or 2 args");
    }
    // Date/Timestamp EXTRACT
    else if (func_name_lower == "date_part" || func_name_lower == "extract") {
        if (arg_strs.size() != 2) throw InternalException("DATE_PART/EXTRACT expects 2 args");
        // arg_strs[0] is the part string (e.g. "year", "month")
        // arg_strs[1] is the date/timestamp value
        // The part string should be a constant for direct FFI call.
        // We assume the first argument (part) is a constant string.
        // This requires inspecting the actual child expression node, not just its Lua code string.
        const auto& part_expr = expr.children[0]->Cast<BoundConstantExpression>();
        std::string part_str = part_expr.value.GetValue<std::string>();

        const auto& date_ts_arg_expr = *expr.children[1];
        LogicalTypeId date_ts_type_id = date_ts_arg_expr.return_type.id();

        if (date_ts_type_id == LogicalTypeId::DATE) {
            return StringUtil::Format("duckdb_ffi_extract_from_date(%s, \"%s\")", arg_strs[1], part_str);
        } else if (date_ts_type_id == LogicalTypeId::TIMESTAMP) {
            return StringUtil::Format("duckdb_ffi_extract_from_timestamp(%s, \"%s\")", arg_strs[1], part_str);
        } else {
            throw NotImplementedException("EXTRACT/DATE_PART on non-date/timestamp type not supported by JIT: " + date_ts_arg_expr.return_type.ToString());
        }
    } else if (func_name_lower == "year") { // Specific extract like year(date_col)
         if (arg_strs.size() != 1) throw InternalException("YEAR expects 1 arg");
         // Determine if it's date or timestamp from child expression type
         const auto& date_ts_arg_expr = *expr.children[0];
         LogicalTypeId date_ts_type_id = date_ts_arg_expr.return_type.id();
         if (date_ts_type_id == LogicalTypeId::DATE) {
            return StringUtil::Format("duckdb_ffi_extract_from_date(%s, \"year\")", arg_strs[0]);
         } else if (date_ts_type_id == LogicalTypeId::TIMESTAMP) {
            return StringUtil::Format("duckdb_ffi_extract_from_timestamp(%s, \"year\")", arg_strs[0]);
         } else {
            throw NotImplementedException("YEAR on non-date/timestamp type not supported by JIT: " + date_ts_arg_expr.return_type.ToString());
         }
    }
    // Add similar for MONTH, DAY etc. if they are separate functions

    // New Math Functions
    else if (func_name_lower == "sqrt") {
        if (arg_strs.size() != 1) throw InternalException("SQRT expects 1 arg");
        return StringUtil::Format("(function(a) if a == nil or a < 0 then return nil else return math.sqrt(a) end end)(%s)", arg_strs[0]);
    } else if (func_name_lower == "pow" || func_name_lower == "power") {
        if (arg_strs.size() != 2) throw InternalException("POW/POWER expects 2 args");
        return StringUtil::Format("(function(b, e) if b == nil or e == nil then return nil else return math.pow(b, e) end end)(%s, %s)", arg_strs[0], arg_strs[1]);
    } else if (func_name_lower == "ln") {
        if (arg_strs.size() != 1) throw InternalException("LN expects 1 arg");
        return StringUtil::Format("(function(a) if a == nil or a <= 0 then return nil else return math.log(a) end end)(%s)", arg_strs[0]);
    } else if (func_name_lower == "log10") {
        if (arg_strs.size() != 1) throw InternalException("LOG10 expects 1 arg");
        return StringUtil::Format("(function(a) if a == nil or a <= 0 then return nil else return math.log10(a) end end)(%s)", arg_strs[0]);
    } else if (func_name_lower == "sin") {
        if (arg_strs.size() != 1) throw InternalException("SIN expects 1 arg");
        return StringUtil::Format("(function(a) if a == nil then return nil else return math.sin(a) end end)(%s)", arg_strs[0]);
    } else if (func_name_lower == "cos") {
        if (arg_strs.size() != 1) throw InternalException("COS expects 1 arg");
        return StringUtil::Format("(function(a) if a == nil then return nil else return math.cos(a) end end)(%s)", arg_strs[0]);
    } else if (func_name_lower == "tan") {
        if (arg_strs.size() != 1) throw InternalException("TAN expects 1 arg");
        return StringUtil::Format("(function(a) if a == nil then return nil else return math.tan(a) end end)(%s)", arg_strs[0]);
    }
    // New String Functions
    else if (func_name_lower == "replace") {
        if (arg_strs.size() != 3) throw InternalException("REPLACE expects 3 args");
        // Using a simple string.gsub for all occurrences. Lua patterns are not SQL LIKE patterns.
        // For plain string replacement, string.gsub is fine.
        // arg_strs[1] (pattern) needs escaping for magic chars if it's not meant to be a pattern.
        // For simple literal replace, we can use a helper or more complex string.find loop.
        // The prompt's simple_replace is for one-by-one, this is simpler:
        return StringUtil::Format(
            "(function(s, from_str, to_str) "
            "  if s == nil or from_str == nil or to_str == nil then return nil end; "
            // Implemented gsub-like replace for fixed patterns (magic_chars_escaped = false)
            // This is a simplified version of gsub that doesn't use Lua patterns from `from_str`
            "  local result = ''; local i = 1; "
            "  while true do "
            "    local find_start, find_end = string.find(s, from_str, i, true); " // true for plain text
            "    if not find_start then break end; "
            "    result = result .. string.sub(s, i, find_start - 1) .. to_str; "
            "    i = find_end + 1; "
            "  end; "
            "  result = result .. string.sub(s, i); "
            "  return result; "
            "end)(%s, %s, %s)", arg_strs[0], arg_strs[1], arg_strs[2]);
    } else if (func_name_lower == "lpad") {
        if (arg_strs.size() != 3) throw InternalException("LPAD expects 3 args");
        return StringUtil::Format(
            "(function(s, len, pad) "
            "  if s == nil or len == nil or pad == nil then return nil end; "
            "  local s_len = #s; local pad_char = string.sub(pad, 1, 1); " // Use first char of pad string
            "  if pad_char == '' then return string.sub(s, 1, len) end; " // If pad string is empty, behave like substring
            "  if s_len >= len then return string.sub(s, 1, len) "
            "  else return string.rep(pad_char, len - s_len) .. s end "
            "end)(%s, %s, %s)", arg_strs[0], arg_strs[1], arg_strs[2]);
    } else if (func_name_lower == "rpad") {
        if (arg_strs.size() != 3) throw InternalException("RPAD expects 3 args");
        return StringUtil::Format(
            "(function(s, len, pad) "
            "  if s == nil or len == nil or pad == nil then return nil end; "
            "  local s_len = #s; local pad_char = string.sub(pad, 1, 1); "
            "  if pad_char == '' then return string.sub(s, 1, len) end; "
            "  if s_len >= len then return string.sub(s, 1, len) "
            "  else return s .. string.rep(pad_char, len - s_len) end "
            "end)(%s, %s, %s)", arg_strs[0], arg_strs[1], arg_strs[2]);
    } else if (func_name_lower == "trim") {
        if (arg_strs.size() != 1) throw InternalException("TRIM expects 1 arg");
        // string.match returns nil if no match (e.g. empty string or all whitespace)
        // We want "" in that case.
        return StringUtil::Format("(function(s) if s == nil then return nil end; return string.match(s, '^%%s*(.-)%%s*$') or '' end)(%s)", arg_strs[0]);
    }

    throw NotImplementedException("Unsupported BoundFunctionExpression in LuaTranslator: " + func_name_lower);
}

std::string GenerateValueBoundCase(const BoundCaseExpression& expr, LuaTranslatorContext& ctx,
                                 std::vector<column_binding>& referenced_columns) {
    std::stringstream ss;
    ss << "(function()\n"; // Start IIFE

    for (size_t i = 0; i < expr.case_checks.size(); ++i) {
        const auto& check = expr.case_checks[i];
        std::string when_str = GenerateValueExpression(*check.when_expr, ctx, referenced_columns);
        std::string then_str = GenerateValueExpression(*check.then_expr, ctx, referenced_columns);

        // WHEN condition is expected to be boolean (0 or 1 from translator)
        if (i == 0) {
            ss << "  if (" << when_str << " == 1) then return " << then_str << "\n";
        } else {
            ss << "  elseif (" << when_str << " == 1) then return " << then_str << "\n";
        }
    }

    std::string else_str = GenerateValueExpression(*expr.else_expr, ctx, referenced_columns);
    ss << "  else return " << else_str << "\n";
    ss << "  end\n";
    ss << "end)()"; // End IIFE and call it

    return ss.str();
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
    case ExpressionClass::BOUND_FUNCTION:
        return GenerateValueBoundFunction(expr.Cast<BoundFunctionExpression>(), ctx, referenced_columns);
    case ExpressionClass::BOUND_CASE: // Added
        return GenerateValueBoundCase(expr.Cast<BoundCaseExpression>(), ctx, referenced_columns);
    // case ExpressionClass::BOUND_CONJUNCTION: // TODO
    default:
        throw NotImplementedException("Unsupported BoundExpression class in LuaTranslator: " + ExpressionTypeToString(expr.type) + "[" + expr.GetExpressionClassString() + "]");
    }
}

// Main public method: TranslateExpressionToLuaRowLogic
std::string LuaTranslator::TranslateExpressionToLuaRowLogic(const Expression& expr, LuaTranslatorContext& ctx) {
    std::vector<column_binding> referenced_columns;
    std::string value_expr_str = GenerateValueExpression(expr, ctx, referenced_columns);

    std::sort(referenced_columns.begin(), referenced_columns.end(),
              [](const column_binding& a, const column_binding& b) {
                  return a.column_index < b.column_index;
              });
    referenced_columns.erase(std::unique(referenced_columns.begin(), referenced_columns.end(),
                                       [](const column_binding& a, const column_binding& b) {
                                           return a.column_index == b.column_index;
                                       }), referenced_columns.end());

    std::stringstream ss;
    // Use "output_vec_ffi" as the standard name for the output FFIVector* in Lua,
    // matching ConstructFullLuaFunctionScript.
    const std::string output_ffi_arg_name = "output_vec_ffi";

    if (!referenced_columns.empty()) {
        ss << "if ";
        for (size_t k = 0; k < referenced_columns.size(); ++k) {
            ss << ctx.GetInputVectorsTable() << "[" << referenced_columns[k].column_index + 1 << "].nullmask[i]";
            if (k < referenced_columns.size() - 1) {
                ss << " or ";
            }
        }
        ss << " then\n";
        if (expr.return_type.id() == LogicalTypeId::VARCHAR) {
            ss << "    duckdb_ffi_set_string_output_null(" << output_ffi_arg_name << ", i)\n";
        } else {
            // For non-string types, nullmask is part of the FFIVector struct passed for output
            // This refers to the adapted name used in ConstructFullLuaFunctionScript for the output nullmask array.
            ss << "    output_nullmask[i] = true\n";
        }
    } else { // No column references, so expression is constant or only involves constants.
             // It can still be NULL (e.g. CAST(NULL AS INTEGER) or a function returning NULL).
             // The value_expr_str itself will be "nil" if it's a constant null.
        if (expr.return_type.id() == LogicalTypeId::VARCHAR) {
            // String output handled below by checking lua_str_result.
            // No explicit nullmask setting here if no column refs.
        } else {
             ss << "output_nullmask[i] = false\n"; // Tentatively false for non-string consts
        }
    }

    if (!referenced_columns.empty()) {
        ss << "else\n"; // All referenced inputs are NOT NULL
        if (expr.return_type.id() != LogicalTypeId::VARCHAR) {
            ss << "    output_nullmask[i] = false\n";
        }
        // For VARCHAR, nullness is determined by lua_str_result below.
    }

    bool is_boolean_output_type = expr.return_type.id() == LogicalTypeId::BOOLEAN;
    std::string indent = (!referenced_columns.empty()) ? "    " : "";

    if (expr.return_type.id() == LogicalTypeId::VARCHAR) {
        ss << indent << "local lua_str_result = " << value_expr_str << "\n";
        ss << indent << "if lua_str_result == nil then\n";
        ss << indent << "    duckdb_ffi_set_string_output_null(" << output_ffi_arg_name << ", i)\n";
        // If there were no column_refs, we still need to ensure output_nullmask is set if const is nil
        if (referenced_columns.empty()) {
            ss << indent << "    output_nullmask[i] = true\n";
        }
        ss << indent << "else\n";
        // Ensure nullmask is false if we are adding a string.
        ss << indent << "    output_nullmask[i] = false\n";
        ss << indent << "    duckdb_ffi_add_string_to_output_vector(" << output_ffi_arg_name << ", i, lua_str_result, #lua_str_result)\n";
        ss << indent << "end\n";
    } else if (is_boolean_output_type) {
        // If value_expr_str itself could be nil (e.g. CASE expr returning NULL boolean)
        // this needs to be handled before comparison.
        // Current GenerateValue for CASE returns a value or calls another GenerateValue,
        // which for constants returns "nil".
        ss << indent << "local bool_val = " << value_expr_str << "\n";
        ss << indent << "if bool_val == nil then\n"; // Handle if expression itself results in NULL
        ss << indent << "    output_nullmask[i] = true\n";
        ss << indent << "else\n";
        // If it's a constant expression, output_nullmask[i] was already set to false.
        // If it depended on columns, it was set to false in the 'else' block.
        // So, if bool_val is not nil, output_nullmask[i] should be false.
        if (!referenced_columns.empty()) { // Ensure it's false if it came through the non-null path
             ss << indent << "    output_nullmask[i] = false\n";
        }
        ss << indent << "    if bool_val then\n"; // Lua true/false
        ss << indent << "        output_data[i] = 1\n";
        ss << indent << "    else\n";
        ss << indent << "        output_data[i] = 0\n";
        ss << indent << "    end\n";
        ss << indent << "end\n";
    } else { // Numeric types
        ss << indent << "local num_val = " << value_expr_str << "\n";
        ss << indent << "if num_val == nil then\n"; // Handle if expression itself results in NULL (e.g. 1 / 0 -> nil in some Lua contexts, or CASE)
        ss << indent << "    output_nullmask[i] = true\n";
        ss << indent << "else\n";
        if (!referenced_columns.empty()) { // Ensure it's false if it came through the non-null path
             ss << indent << "    output_nullmask[i] = false\n";
        }
        ss << indent << "    output_data[i] = num_val\n";
        ss << indent << "end\n";
    }

    if (!referenced_columns.empty()) {
        ss << "end"; // Closes the main "if all inputs not null" block
    }

    return ss.str();
}

} // namespace duckdb
