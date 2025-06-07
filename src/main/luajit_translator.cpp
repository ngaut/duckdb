#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/function/scalar/date_functions.hpp" // For DatePartSpecifier, etc.
#include <algorithm> // for std::find

namespace duckdb {

// --- LuaTranslatorContext ---
static std::string GetLuaFFITypeFromLogicalType(const LogicalType& type) {
    switch (type.id()) {
        case LogicalTypeId::INTEGER:    return "int32_t";
        case LogicalTypeId::BIGINT:     return "int64_t";
        case LogicalTypeId::DOUBLE:     return "double";
        case LogicalTypeId::VARCHAR:    return "FFIString";
        case LogicalTypeId::DATE:       return "int32_t";
        case LogicalTypeId::TIMESTAMP:  return "int64_t";
        case LogicalTypeId::INTERVAL:   return "FFIInterval";
        case LogicalTypeId::BOOLEAN:    return "int8_t";
        default:
            throw NotImplementedException("LuaTranslatorContext: Unsupported logical type for FFI: " + type.ToString());
    }
}

LuaTranslatorContext::LuaTranslatorContext(const std::vector<LogicalType>& unique_input_types,
                                           const std::unordered_map<idx_t, idx_t>& col_idx_to_lua_arg_map)
    : unique_input_logical_types_(unique_input_types),
      chunk_col_to_lua_arg_map_(col_idx_to_lua_arg_map) {
    unique_input_lua_ffi_types_.reserve(unique_input_types.size());
    for (const auto& type : unique_input_types) {
        unique_input_lua_ffi_types_.push_back(GetLuaFFITypeFromLogicalType(type));
    }
}

std::string LuaTranslatorContext::GetInputLuaFFIType(idx_t lua_arg_idx) const {
    if (lua_arg_idx >= unique_input_lua_ffi_types_.size()) {
        throw InternalException("LuaTranslatorContext: Lua argument index out of bounds for GetInputLuaFFIType.");
    }
    return unique_input_lua_ffi_types_[lua_arg_idx];
}

const LogicalType& LuaTranslatorContext::GetInputLogicalType(idx_t lua_arg_idx) const {
    if (lua_arg_idx >= unique_input_logical_types_.size()) {
        throw InternalException("LuaTranslatorContext: Lua argument index out of bounds for GetInputLogicalType.");
    }
    return unique_input_logical_types_[lua_arg_idx];
}

idx_t LuaTranslatorContext::GetNumInputs() const {
    return unique_input_logical_types_.size();
}

std::string LuaTranslatorContext::GetOutputTypeLuaFFIType(const LogicalType& type) const {
    // For now, output type string generation is same as input, but separated for future flexibility
    return GetLuaFFITypeFromLogicalType(type);
}

idx_t LuaTranslatorContext::GetLuaArgIndex(idx_t original_chunk_col_idx) const {
    auto it = chunk_col_to_lua_arg_map_.find(original_chunk_col_idx);
    if (it == chunk_col_to_lua_arg_map_.end()) {
        throw InternalException("LuaTranslatorContext: Original chunk column index %d not found in map to Lua arguments. This map should be pre-populated by ExpressionExecutor.", original_chunk_col_idx);
    }
    return it->second;
}

// --- LuaTranslator ---

static std::string GenerateTempVarName(int& temp_var_idx) {
    return "tval" + std::to_string(temp_var_idx++);
}

static std::string GetLuaOperatorFromExprType(ExpressionType op_type) {
    switch (op_type) {
        case ExpressionType::OPERATOR_ADD: return "+";
        case ExpressionType::OPERATOR_SUBTRACT: return "-";
        case ExpressionType::OPERATOR_MULTIPLY: return "*";
        case ExpressionType::OPERATOR_DIVIDE: return "/";
        case ExpressionType::COMPARE_EQUAL: return "==";
        case ExpressionType::COMPARE_NOTEQUAL: return "~=";
        case ExpressionType::COMPARE_LESSTHAN: return "<";
        case ExpressionType::COMPARE_GREATERTHAN: return ">";
        case ExpressionType::COMPARE_LESSTHANOREQUALTO: return "<=";
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return ">=";
        case ExpressionType::OPERATOR_CONCAT: return "..";
        case ExpressionType::OPERATOR_NOT: return "not ";
        default:
            throw NotImplementedException("ExpressionType not mapped to Lua operator: " + ExpressionTypeToString(op_type));
    }
}

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

// Forward declarations for static helpers
static std::string GenerateValueBoundFunction(const BoundFunctionExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);
static std::string GenerateValueBoundCase(const BoundCaseExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx);


std::string LuaTranslator::GenerateValue(const BoundConstantExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx) {
    std::stringstream ss;
    ss << "local " << result_var_name << "_val\n";
    ss << "local " << result_var_name << "_is_null\n";
    const auto& val = expr.value;
    if (val.IsNull()) {
        ss << result_var_name << "_is_null = true\n";
    } else {
        ss << result_var_name << "_is_null = false\n";
        switch (expr.return_type.id()) {
            case LogicalTypeId::INTEGER:  ss << result_var_name << "_val = " << val.GetValue<int32_t>() << "\n"; break;
            case LogicalTypeId::BIGINT:   ss << result_var_name << "_val = " << val.GetValue<int64_t>() << "LL\n"; break;
            case LogicalTypeId::DOUBLE:   ss << result_var_name << "_val = " << val.GetValue<double>() << "\n"; break;
            case LogicalTypeId::DATE:     ss << result_var_name << "_val = " << val.GetValue<date_t>().days << "\n"; break;
            case LogicalTypeId::TIMESTAMP:ss << result_var_name << "_val = " << val.GetValue<timestamp_t>().micros << "LL\n"; break;
            case LogicalTypeId::VARCHAR:  ss << result_var_name << "_val = " << EscapeLuaString(val.GetValue<string>()) << "\n"; break;
            case LogicalTypeId::BOOLEAN:  ss << result_var_name << "_val = " << (val.GetValue<bool>() ? "true" : "false") << "\n"; break;
            case LogicalTypeId::INTERVAL: {
                auto& interval_val = val.GetValue<interval_t>();
                ss << result_var_name << "_val = ffi.new(\"FFIInterval\", { months = " << interval_val.months
                   << ", days = " << interval_val.days << ", micros = " << interval_val.micros << "LL })\n";
                break;
            }
            default:
                throw NotImplementedException("Unsupported constant type for JIT: " + expr.return_type.ToString());
        }
    }
    return ss.str();
}

std::string LuaTranslator::GenerateValue(const BoundReferenceExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx) {
    std::stringstream ss;
    ss << "local " << result_var_name << "_val\n";
    ss << "local " << result_var_name << "_is_null\n";

    // Get the correct Lua argument index (0, 1, ...) for this original chunk column index
    idx_t lua_arg_idx = ctx.GetLuaArgIndex(expr.index);
    // Lua function arguments in ConstructFullLuaFunctionScript are input0_ffi, input1_ffi, ...
    // Correspondingly, casted data/nullmask arrays are input0_data, input0_nullmask etc.
    std::string input_data_var = "input" + std::to_string(lua_arg_idx) + "_data";
    std::string input_nullmask_var = "input" + std::to_string(lua_arg_idx) + "_nullmask";

    ss << "if " << input_nullmask_var << "[i] then\n";
    ss << "  " << result_var_name << "_is_null = true\n";
    ss << "else\n";
    ss << "  " << result_var_name << "_is_null = false\n";
    const auto& type = expr.return_type;
    if (type.id() == LogicalTypeId::VARCHAR) {
        ss << "  " << result_var_name << "_val = ffi.string(" << input_data_var << "[i].ptr, " << input_data_var << "[i].len)\n";
    } else { // Numeric, Date, Timestamp, Interval, Boolean (as int8_t)
        ss << "  " << result_var_name << "_val = " << input_data_var << "[i]\n";
    }
    ss << "end\n";
    return ss.str();
}

std::string LuaTranslator::GenerateValue(const BoundOperatorExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx) {
    std::stringstream ss;
    ss << "local " << result_var_name << "_val\n";
    ss << "local " << result_var_name << "_is_null\n";

    if (expr.children.empty()) throw InternalException("Operator expression with no children.");

    std::string child0_res_name = GenerateTempVarName(temp_var_idx);
    ss << GenerateValueExpression(*expr.children[0], ctx, child0_res_name, temp_var_idx);

    if (expr.type == ExpressionType::OPERATOR_NOT) {
        ss << "if " << child0_res_name << "_is_null then " << result_var_name << "_is_null = true else "
           << result_var_name << "_is_null = false; "
           << result_var_name << "_val = not " << child0_res_name << "_val end\n";
    } else if (expr.type == ExpressionType::OPERATOR_IS_NULL) {
        ss << result_var_name << "_is_null = false\n"; // IS NULL itself is never NULL
        ss << result_var_name << "_val = " << child0_res_name << "_is_null\n";
    } else if (expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
        ss << result_var_name << "_is_null = false\n"; // IS NOT NULL itself is never NULL
        ss << result_var_name << "_val = not " << child0_res_name << "_is_null\n";
    } else { // Binary operators
        if (expr.children.size() != 2) throw InternalException("Binary operator with not 2 children.");
        std::string child1_res_name = GenerateTempVarName(temp_var_idx);
        ss << GenerateValueExpression(*expr.children[1], ctx, child1_res_name, temp_var_idx);

        ss << "if " << child0_res_name << "_is_null or " << child1_res_name << "_is_null then "
           << result_var_name << "_is_null = true else "
           << result_var_name << "_is_null = false; "
           << result_var_name << "_val = " << child0_res_name << "_val "
           << GetLuaOperatorFromExprType(expr.type) << " " << child1_res_name << "_val end\n";
    }
    return ss.str();
}

std::string GenerateValueBoundFunction(const BoundFunctionExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx) {
    std::stringstream ss;
    ss << "local " << result_var_name << "_val\n";
    ss << "local " << result_var_name << "_is_null\n";

    std::vector<std::string> child_val_vars;
    std::string children_null_check = "";

    for (size_t k = 0; k < expr.children.size(); ++k) {
        std::string child_prefix = GenerateTempVarName(temp_var_idx);
        ss << LuaTranslator::GenerateValueExpression(*expr.children[k], ctx, child_prefix, temp_var_idx);
        child_val_vars.push_back(child_prefix + "_val");
        if (k > 0) children_null_check += " or ";
        children_null_check += (child_prefix + "_is_null");
    }

    if (expr.children.empty()) { // Handle 0-arg functions if any (none in this list)
        children_null_check = "false"; // No inputs means not null due to inputs
    }

    ss << "if " << children_null_check << " then\n";
    ss << "  " << result_var_name << "_is_null = true\n";
    ss << "else\n";
    ss << "  " << result_var_name << "_is_null = false\n"; // Tentatively

    std::string func_name_lower = StringUtil::Lower(expr.function.name);
    std::string args_joined = StringUtil::Join(child_val_vars, ", ");

    // Math functions
    if (func_name_lower == "abs") ss << "  " << result_var_name << "_val = math.abs(" << args_joined << ")\n";
    else if (func_name_lower == "ceil") ss << "  " << result_var_name << "_val = math.ceil(" << args_joined << ")\n";
    else if (func_name_lower == "floor") ss << "  " << result_var_name << "_val = math.floor(" << args_joined << ")\n";
    else if (func_name_lower == "round") {
        if (child_val_vars.size() == 1) ss << "  " << result_var_name << "_val = math.floor(" << child_val_vars[0] << " + 0.5)\n";
        else ss << "  do local p = 10^(" << child_val_vars[1] << "); " << result_var_name << "_val = math.floor(" << child_val_vars[0] << "*p+0.5)/p end\n";
    } else if (func_name_lower == "sqrt") {
        ss << "  if " << child_val_vars[0] << " < 0 then " << result_var_name << "_is_null = true else " << result_var_name << "_val = math.sqrt(" << child_val_vars[0] << ") end\n";
    } else if (func_name_lower == "pow" || func_name_lower == "power") {
        ss << "  " << result_var_name << "_val = math.pow(" << args_joined << ")\n"; // Lua math.pow handles errors like pow(-1, 0.5) by returning nan
    } else if (func_name_lower == "ln") {
        ss << "  if " << child_val_vars[0] << " <= 0 then " << result_var_name << "_is_null = true else " << result_var_name << "_val = math.log(" << child_val_vars[0] << ") end\n";
    } else if (func_name_lower == "log10") {
        ss << "  if " << child_val_vars[0] << " <= 0 then " << result_var_name << "_is_null = true else " << result_var_name << "_val = math.log10(" << child_val_vars[0] << ") end\n";
    } else if (func_name_lower == "sin") ss << "  " << result_var_name << "_val = math.sin(" << args_joined << ")\n";
    else if (func_name_lower == "cos") ss << "  " << result_var_name << "_val = math.cos(" << args_joined << ")\n";
    else if (func_name_lower == "tan") ss << "  " << result_var_name << "_val = math.tan(" << args_joined << ")\n";
    // String functions
    else if (func_name_lower == "lower") ss << "  " << result_var_name << "_val = string.lower(" << args_joined << ")\n";
    else if (func_name_lower == "upper") ss << "  " << result_var_name << "_val = string.upper(" << args_joined << ")\n";
    else if (func_name_lower == "length" || func_name_lower == "strlen") {
        // Optimization: If input is a direct VARCHAR column reference, use .len from FFIString
        if (expr.children.size() == 1 && expr.children[0]->GetExpressionClass() == ExpressionClass::BOUND_REF &&
            expr.children[0]->return_type.id() == LogicalTypeId::VARCHAR) {
            // child_val_vars[0] holds the name of the Lua variable for the FFIString struct itself (e.g. tval0_val)
            // The actual FFIString is inputX_data[i]. We need to use the variable that holds the FFIString struct for the specific input.
            // This requires GenerateValue(BoundReference) for VARCHAR to provide the struct itself, not ffi.string().
            // Let's assume child_val_vars[0] is the direct Lua variable name for the ffi.string() result.
            // The current GenerateValue(BoundReference) for VARCHAR already does ffi.string().
            // So, for direct .len access, we need GenerateValue(BoundReference) to expose the struct.
            // This is a larger change. For now, stick to #lua_string.
            // Conceptual change if GenerateValue(BoundRef) returned the struct:
            // ss << "  " << result_var_name << "_val = " << child_val_vars[0] << ".len\n";
            // Fallback to current:
            ss << "  " << result_var_name << "_val = #(" << args_joined << ")\n";
        } else {
            ss << "  " << result_var_name << "_val = #(" << args_joined << ")\n";
        }
    } else if (func_name_lower == "concat") {
        std::string concat_expr_str = "";
        for(size_t k=0; k < child_val_vars.size(); ++k) { concat_expr_str += child_val_vars[k]; if (k < child_val_vars.size()-1) concat_expr_str += " .. ";}
        ss << "  " << result_var_name << "_val = " << concat_expr_str << "\n";
    } else if (func_name_lower == "substring" || func_name_lower == "substr") {
        if (child_val_vars.size() == 2) ss << "  " << result_var_name << "_val = string.sub(" << child_val_vars[0] << ", " << child_val_vars[1] << ")\n";
        else ss << "  " << result_var_name << "_val = string.sub(" << child_val_vars[0] << ", " << child_val_vars[1] << ", " << child_val_vars[1] << " + " << child_val_vars[2] << " - 1)\n";
    } else if (func_name_lower == "replace") {
        ss << "  do local s, from_str, to_str = " << child_val_vars[0] << ", " << child_val_vars[1] << ", " << child_val_vars[2] << "; "
           << "local res = ''; local i = 1; "
           << "while true do local fs, fe = string.find(s, from_str, i, true); "
           << "if not fs then break end; res = res .. string.sub(s, i, fs - 1) .. to_str; i = fe + 1; end; "
           << result_var_name << "_val = res .. string.sub(s, i) end\n";
    } else if (func_name_lower == "lpad") {
        ss << "  do local s, len, pad = " << child_val_vars[0] << ", " << child_val_vars[1] << ", " << child_val_vars[2] << "; "
           << "local slen = #s; local pad_char = string.sub(pad,1,1); if pad_char == '' then " << result_var_name << "_val = string.sub(s,1,len); "
           << "elseif slen >= len then " << result_var_name << "_val = string.sub(s,1,len); "
           << "else " << result_var_name << "_val = string.rep(pad_char, len-slen) .. s; end end\n";
    } else if (func_name_lower == "rpad") {
         ss << "  do local s, len, pad = " << child_val_vars[0] << ", " << child_val_vars[1] << ", " << child_val_vars[2] << "; "
           << "local slen = #s; local pad_char = string.sub(pad,1,1); if pad_char == '' then " << result_var_name << "_val = string.sub(s,1,len); "
           << "elseif slen >= len then " << result_var_name << "_val = string.sub(s,1,len); "
           << "else " << result_var_name << "_val = s .. string.rep(pad_char, len-slen); end end\n";
    } else if (func_name_lower == "trim") {
        ss << "  " << result_var_name << "_val = string.match(" << child_val_vars[0] << ", '^%%s*(.-)%%s*$') or ''\n";
    }
    // Date/Timestamp EXTRACT (using FFI C helpers)
    else if (func_name_lower == "date_part" || func_name_lower == "extract") {
        const auto& part_expr_node = expr.children[0]->Cast<BoundConstantExpression>();
        std::string part_str_val = EscapeLuaString(part_expr_node.value.GetValue<std::string>());
        LogicalTypeId temporal_type = expr.children[1]->return_type.id();
        if (temporal_type == LogicalTypeId::DATE) ss << "  " << result_var_name << "_val = duckdb_ffi_extract_from_date(" << child_val_vars[1] << ", " << part_str_val << ")\n";
        else if (temporal_type == LogicalTypeId::TIMESTAMP) ss << "  " << result_var_name << "_val = duckdb_ffi_extract_from_timestamp(" << child_val_vars[1] << ", " << part_str_val << ")\n";
        else { ss << "  " << result_var_name << "_is_null = true -- EXTRACT on non-temporal\n"; }
    } else if (func_name_lower == "year") { // Example specific extract
        LogicalTypeId temporal_type = expr.children[0]->return_type.id();
         if (temporal_type == LogicalTypeId::DATE) ss << "  " << result_var_name << "_val = duckdb_ffi_extract_from_date(" << child_val_vars[0] << ", \"year\")\n";
        else if (temporal_type == LogicalTypeId::TIMESTAMP) ss << "  " << result_var_name << "_val = duckdb_ffi_extract_from_timestamp(" << child_val_vars[0] << ", \"year\")\n";
        else { ss << "  " << result_var_name << "_is_null = true -- YEAR on non-temporal\n"; }
    }
    else { // Fallback for unhandled functions
        ss << "  -- Function '" << func_name_lower << "' not fully translated to new JIT style.\n";
        ss << "  " << result_var_name << "_is_null = true\n";
    }
    // If the Lua operation itself could return nil (e.g., some math functions on invalid inputs not caught by domain checks)
    ss << "  if " << result_var_name << "_val == nil and not " << result_var_name << "_is_null then " << result_var_name << "_is_null = true; end\n";
    ss << "end\n";
    return ss.str();
}

std::string GenerateValueBoundCase(const BoundCaseExpression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx) {
    std::stringstream ss;
    ss << "local " << result_var_name << "_val\n";
    ss << "local " << result_var_name << "_is_null\n";

    std::vector<std::string> when_val_vars;
    std::vector<std::string> when_is_null_vars;
    std::vector<std::string> then_val_vars;
    std::vector<std::string> then_is_null_vars;

    for (const auto& case_check : expr.case_checks) {
        std::string when_prefix = GenerateTempVarName(temp_var_idx);
        ss << LuaTranslator::GenerateValueExpression(*case_check.when_expr, ctx, when_prefix, temp_var_idx);
        when_val_vars.push_back(when_prefix + "_val");
        when_is_null_vars.push_back(when_prefix + "_is_null");

        std::string then_prefix = GenerateTempVarName(temp_var_idx);
        ss << LuaTranslator::GenerateValueExpression(*case_check.then_expr, ctx, then_prefix, temp_var_idx);
        then_val_vars.push_back(then_prefix + "_val");
        then_is_null_vars.push_back(then_prefix + "_is_null");
    }
    std::string else_prefix = GenerateTempVarName(temp_var_idx);
    ss << LuaTranslator::GenerateValueExpression(*expr.else_expr, ctx, else_prefix, temp_var_idx);

    for (size_t i = 0; i < expr.case_checks.size(); ++i) {
        if (i == 0) ss << "if not " << when_is_null_vars[i] << " and " << when_val_vars[i] << " then\n";
        else ss << "elseif not " << when_is_null_vars[i] << " and " << when_val_vars[i] << " then\n";
        ss << "  " << result_var_name << "_val = " << then_val_vars[i] << "\n";
        ss << "  " << result_var_name << "_is_null = " << then_is_null_vars[i] << "\n";
    }
    ss << "else\n";
    ss << "  " << result_var_name << "_val = " << else_prefix << "_val\n";
    ss << "  " << result_var_name << "_is_null = " << else_prefix << "_is_null\n";
    ss << "end\n";

    return ss.str();
}

std::string LuaTranslator::GenerateValueExpression(const Expression& expr, LuaTranslatorContext& ctx, const std::string& result_var_name, int& temp_var_idx) {
    switch (expr.GetExpressionClass()) {
    case ExpressionClass::BOUND_CONSTANT:
        return GenerateValue(expr.Cast<BoundConstantExpression>(), ctx, result_var_name, temp_var_idx);
    case ExpressionClass::BOUND_REF:
        return GenerateValue(expr.Cast<BoundReferenceExpression>(), ctx, result_var_name, temp_var_idx);
    case ExpressionClass::BOUND_OPERATOR:
        return GenerateValue(expr.Cast<BoundOperatorExpression>(), ctx, result_var_name, temp_var_idx);
    case ExpressionClass::BOUND_FUNCTION:
        return GenerateValueBoundFunction(expr.Cast<BoundFunctionExpression>(), ctx, result_var_name, temp_var_idx);
    case ExpressionClass::BOUND_CASE:
        return GenerateValueBoundCase(expr.Cast<BoundCaseExpression>(), ctx, result_var_name, temp_var_idx);
    default:
        throw NotImplementedException("Unsupported BoundExpression class for JIT: " + expr.GetExpressionClassString());
    }
}

std::string LuaTranslator::TranslateExpressionToLuaRowLogic(const Expression& expr, LuaTranslatorContext& ctx) {
    int temp_var_idx = 0;
    return GenerateValueExpression(expr, ctx, "current_row", temp_var_idx);
}

} // namespace duckdb
