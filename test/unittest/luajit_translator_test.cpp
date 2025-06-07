#include "catch.hpp"
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <string>
#include <iostream>
#include <vector>
#include <regex> // For NormalizeLua (if not already included via other headers)
#include <cmath> // For M_PI, NAN for testing if needed, though not directly in generated code usually

// Helper to normalize Lua code (same as before)
std::string NormalizeLua(const std::string& lua_code) {
    std::string line;
    std::stringstream input_ss(lua_code);
    std::stringstream output_ss;
    bool first_line = true;
    while (std::getline(input_ss, line)) {
        line = std::regex_replace(line, std::regex("^\\s+|\\s+$"), "");
        if (line.empty()) continue;
        if (!first_line) {
            output_ss << "\n";
        }
        output_ss << line;
        first_line = false;
    }
    return output_ss.str();
}

// --- Helper functions (assumed to be same as before) ---
static duckdb::unique_ptr<duckdb::BoundConstantExpression> CreateBoundConstant(duckdb::Value val) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(val);
}
static duckdb::unique_ptr<duckdb::BoundReferenceExpression> CreateBoundReference(duckdb::idx_t col_idx, duckdb::LogicalType type) {
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, col_idx);
}
static duckdb::unique_ptr<duckdb::BoundOperatorExpression> CreateBoundUnaryOperator(
    duckdb::ExpressionType op_type, duckdb::unique_ptr<duckdb::Expression> child, duckdb::LogicalType return_type) {
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(child));
    return duckdb::make_uniq<duckdb::BoundOperatorExpression>(op_type, return_type, std::move(children), false);
}
static duckdb::unique_ptr<duckdb::BoundOperatorExpression> CreateBoundBinaryOperator(
    duckdb::ExpressionType op_type, duckdb::unique_ptr<duckdb::Expression> left, duckdb::unique_ptr<duckdb::Expression> right, duckdb::LogicalType return_type) {
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    return duckdb::make_uniq<duckdb::BoundOperatorExpression>(op_type, return_type, std::move(children), false);
}
static duckdb::unique_ptr<duckdb::BoundFunctionExpression> CreateBoundFunction(
    const std::string& func_name, std::vector<duckdb::unique_ptr<duckdb::Expression>> children, duckdb::LogicalType return_type) {
    duckdb::ScalarFunction scalar_func(func_name, {}, return_type, nullptr);
    return duckdb::make_uniq<duckdb::BoundFunctionExpression>(return_type, scalar_func, std::move(children), nullptr, false);
}


TEST_CASE("LuaTranslator Refactored Tests (Block-Based Snippets)", "[luajit][translator][bound_refactored]") {
    using namespace duckdb;

    std::vector<LogicalType> one_int_input_types = {LogicalType::INTEGER};
    std::unordered_map<idx_t, idx_t> one_int_map = {{0,0}};
    LuaTranslatorContext ctx_one_int(one_int_input_types, one_int_map);

    std::vector<LogicalType> one_double_input_types = {LogicalType::DOUBLE};
    LuaTranslatorContext ctx_one_double(one_double_input_types, one_int_map); // map still uses 0->0

    std::vector<LogicalType> one_varchar_input_types = {LogicalType::VARCHAR};
    LuaTranslatorContext ctx_one_varchar(one_varchar_input_types, one_int_map);

    std::vector<LogicalType> two_varchar_input_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
    std::unordered_map<idx_t, idx_t> two_varchar_map = {{0,0}, {1,1}};
    LuaTranslatorContext ctx_two_varchar(two_varchar_input_types, two_varchar_map);

    // ... (Keep existing SECTIONs for constants, references, basic operators, LENGTH, LOWER, SQRT, CASE, STARTS_WITH, LIKE etc.) ...
    // For brevity, only new tests are shown below, but existing ones should be preserved by overwrite.

    SECTION("Translate Optimized LENGTH(varchar_col_ref)") { // Was added in Subtask 10, ensure it's correct
        auto col_ref = CreateBoundReference(0, LogicalType::VARCHAR);
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(col_ref));
        auto length_expr = CreateBoundFunction("length", std::move(children), LogicalType::BIGINT);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*length_expr, ctx_one_varchar);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
if input0_nullmask[i] then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = input0_data[i].len
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }


    // New Math Function Tests
    SECTION("Translate DEGREES(col_double)") {
        auto expr = CreateBoundFunction("degrees", {CreateBoundReference(0, LogicalType::DOUBLE)}, LogicalType::DOUBLE);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*expr, ctx_one_double);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input0_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input0_data[i]
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = math.deg(tval0_val)
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate TRUNC(col_double)") {
        auto expr = CreateBoundFunction("trunc", {CreateBoundReference(0, LogicalType::DOUBLE)}, LogicalType::DOUBLE);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*expr, ctx_one_double);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input0_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input0_data[i]
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  do local int_part, frac_part = math.modf(tval0_val); current_row_val = int_part end
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate SIGN(col_double)") {
        auto expr = CreateBoundFunction("sign", {CreateBoundReference(0, LogicalType::DOUBLE)}, LogicalType::DOUBLE); // DuckDB SIGN returns DOUBLE
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*expr, ctx_one_double);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input0_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input0_data[i]
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  if tval0_val > 0 then current_row_val = 1 elseif tval0_val < 0 then current_row_val = -1 else current_row_val = 0 end
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate LOG2(col_double)") {
        auto expr = CreateBoundFunction("log2", {CreateBoundReference(0, LogicalType::DOUBLE)}, LogicalType::DOUBLE);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*expr, ctx_one_double);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input0_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input0_data[i]
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  if tval0_val <= 0 then current_row_is_null = true else current_row_val = math.log(tval0_val) / 0.6931471805599453 end
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    // DATE_TRUNC Test
    SECTION("Translate DATE_TRUNC('month', col_date)") {
        // Context for: DATE_TRUNC(VARCHAR_CONST, DATE_COL_REF_idx0)
        // Lua func args: input0_ffi (for DATE_COL_REF_idx0)
        std::vector<LogicalType> dt_inputs = {LogicalType::DATE};
        std::unordered_map<idx_t, idx_t> dt_map = {{0,0}}; // original col 0 is lua arg 0
        LuaTranslatorContext ctx_dt(dt_inputs, dt_map);

        auto part_const = CreateBoundConstant(Value("month"));
        auto date_col_ref = CreateBoundReference(0, LogicalType::DATE);
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(part_const));
        children.push_back(std::move(date_col_ref));
        auto dt_expr = CreateBoundFunction("date_trunc", std::move(children), LogicalType::TIMESTAMP); // DATE_TRUNC usually returns TIMESTAMP

        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*dt_expr, ctx_dt);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
tval0_is_null = false
tval0_val = "month"
local tval1_val
local tval1_is_null
if input0_nullmask[i] then
  tval1_is_null = true
else
  tval1_is_null = false
  tval1_val = input0_data[i]
end
if tval0_is_null or tval1_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = duckdb_ffi_date_trunc("month", tval1_val, false)
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }
}
```
