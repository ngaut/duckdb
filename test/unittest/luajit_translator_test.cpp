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
#include <regex>

// Helper to normalize Lua code by removing extra newlines and leading/trailing whitespace
std::string NormalizeLua(const std::string& lua_code) {
    std::string result = lua_code;
    // Replace multiple newlines with a single one
    result = std::regex_replace(result, std::regex("\n+"), "\n");
    // Trim leading/trailing whitespace from the whole string
    result = std::regex_replace(result, std::regex("^\\s+|\\s+$"), "");
    return result;
}


// --- Helper functions to create DuckDB BoundExpression instances for testing ---
duckdb::unique_ptr<duckdb::BoundConstantExpression> CreateBoundConstant(duckdb::Value val) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(val);
}

duckdb::unique_ptr<duckdb::BoundReferenceExpression> CreateBoundReference(duckdb::idx_t col_idx, duckdb::LogicalType type) {
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, col_idx);
}

duckdb::unique_ptr<duckdb::BoundOperatorExpression> CreateBoundUnaryOperator(
    duckdb::ExpressionType op_type,
    duckdb::unique_ptr<duckdb::Expression> child,
    duckdb::LogicalType return_type) {
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(child));
    return duckdb::make_uniq<duckdb::BoundOperatorExpression>(op_type, return_type, std::move(children), false);
}

duckdb::unique_ptr<duckdb::BoundOperatorExpression> CreateBoundBinaryOperator(
    duckdb::ExpressionType op_type,
    duckdb::unique_ptr<duckdb::Expression> left,
    duckdb::unique_ptr<duckdb::Expression> right,
    duckdb::LogicalType return_type) {
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    return duckdb::make_uniq<duckdb::BoundOperatorExpression>(op_type, return_type, std::move(children), false);
}
duckdb::unique_ptr<duckdb::BoundFunctionExpression> CreateBoundFunction(
    const std::string& func_name,
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children,
    duckdb::LogicalType return_type) {
    duckdb::ScalarFunction scalar_func(func_name, {}, return_type, nullptr);
    return duckdb::make_uniq<duckdb::BoundFunctionExpression>(return_type, scalar_func, std::move(children), nullptr, false);
}


TEST_CASE("LuaTranslator Refactored Tests (Block-Based Snippets)", "[luajit][translator][bound_refactored]") {
    using namespace duckdb;

    std::vector<LogicalType> one_int_input = {LogicalType::INTEGER};
    LuaTranslatorContext ctx_one_int(one_int_input);
    std::vector<LogicalType> two_int_inputs = {LogicalType::INTEGER, LogicalType::INTEGER};
    LuaTranslatorContext ctx_two_ints(two_int_inputs);
    std::vector<LogicalType> one_varchar_input = {LogicalType::VARCHAR};
    LuaTranslatorContext ctx_one_varchar(one_varchar_input);


    SECTION("Translate BoundConstantExpression (Integer)") {
        auto const_expr = CreateBoundConstant(Value::INTEGER(42));
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*const_expr, ctx_one_int);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
current_row_is_null = false
current_row_val = 42
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundConstantExpression (NULL Integer)") {
        auto const_expr = CreateBoundConstant(Value(LogicalType::INTEGER)); // SQL NULL
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*const_expr, ctx_one_int);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
current_row_is_null = true
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundReferenceExpression (Integer Column 0)") {
        auto col_ref_expr = CreateBoundReference(0, LogicalType::INTEGER);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*col_ref_expr, ctx_one_int);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
if input1_nullmask[i] then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = input1_data[i]
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundReferenceExpression (VARCHAR Column 0)") {
        auto col_ref_expr = CreateBoundReference(0, LogicalType::VARCHAR);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*col_ref_expr, ctx_one_varchar);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
if input1_nullmask[i] then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = ffi.string(input1_data[i].ptr, input1_data[i].len)
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }


    SECTION("Translate BoundOperatorExpression: col_0 + 10 (Integer)") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto const_10 = CreateBoundConstant(Value::INTEGER(10));
        auto add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD, std::move(col0), std::move(const_10), LogicalType::INTEGER);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*add_expr, ctx_one_int);

        // Expected structure:
        // 1. Code for col0 (sets tval0_val, tval0_is_null)
        // 2. Code for const_10 (sets tval1_val, tval1_is_null)
        // 3. Code for operator + (uses tval0_*, tval1_*, sets current_row_val, current_row_is_null)
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input1_data[i]
end
local tval1_val
local tval1_is_null
tval1_is_null = false
tval1_val = 10
if tval0_is_null or tval1_is_null then current_row_is_null = true else current_row_is_null = false; current_row_val = tval0_val + tval1_val end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundOperatorExpression: col_0 + col_1 (Integers)") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto col1 = CreateBoundReference(1, LogicalType::INTEGER);
        auto add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD, std::move(col0), std::move(col1), LogicalType::INTEGER);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*add_expr, ctx_two_ints);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input1_data[i]
end
local tval1_val
local tval1_is_null
if input2_nullmask[i] then
  tval1_is_null = true
else
  tval1_is_null = false
  tval1_val = input2_data[i]
end
if tval0_is_null or tval1_is_null then current_row_is_null = true else current_row_is_null = false; current_row_val = tval0_val + tval1_val end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }


    SECTION("Translate BoundOperatorExpression (Comparison): col_0 < 10") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto const_10 = CreateBoundConstant(Value::INTEGER(10));
        auto lt_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_LESSTHAN, std::move(col0), std::move(const_10), LogicalType::BOOLEAN);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*lt_expr, ctx_one_int);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input1_data[i]
end
local tval1_val
local tval1_is_null
tval1_is_null = false
tval1_val = 10
if tval0_is_null or tval1_is_null then current_row_is_null = true else current_row_is_null = false; current_row_val = (tval0_val < tval1_val)  end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundOperatorExpression (IS_NULL): col_0 IS NULL") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto is_null_expr = CreateBoundUnaryOperator(ExpressionType::OPERATOR_IS_NULL, std::move(col0), LogicalType::BOOLEAN);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*is_null_expr, ctx_one_int);
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input1_data[i]
end
current_row_is_null = false
current_row_val = tval0_is_null
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    // Test for a function call, e.g. abs(col_0)
    SECTION("Translate BoundFunctionExpression: abs(col_0)") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(col0));
        auto abs_expr = CreateBoundFunction("abs", std::move(children), LogicalType::INTEGER);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*abs_expr, ctx_one_int);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input1_data[i]
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = math.abs(tval0_val)
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundFunctionExpression: lower(col_varchar)") {
        auto col_varchar_ref = CreateBoundReference(0, LogicalType::VARCHAR);
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(col_varchar_ref));
        auto lower_expr = CreateBoundFunction("lower", std::move(children), LogicalType::VARCHAR);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*lower_expr, ctx_one_varchar);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = ffi.string(input1_data[i].ptr, input1_data[i].len)
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = string.lower(tval0_val)
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundFunctionExpression: sqrt(col_int)") {
        auto col_int_ref = CreateBoundReference(0, LogicalType::INTEGER); // Input is INTEGER
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(col_int_ref));
        auto sqrt_expr = CreateBoundFunction("sqrt", std::move(children), LogicalType::DOUBLE); // Output is DOUBLE
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*sqrt_expr, ctx_one_int);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = input1_data[i]
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  if tval0_val < 0 then current_row_is_null = true else current_row_val = math.sqrt(tval0_val) end
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }


    SECTION("Translate BoundCaseExpression: CASE WHEN col0 > 10 THEN 100 ELSE 0 END") {
        auto col0_ref_when = CreateBoundReference(0, LogicalType::INTEGER);
        auto const_10_when = CreateBoundConstant(Value::INTEGER(10));
        auto when_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_GREATERTHAN, std::move(col0_ref_when), std::move(const_10_when), LogicalType::BOOLEAN);

        auto then_expr = CreateBoundConstant(Value::INTEGER(100));
        auto else_expr = CreateBoundConstant(Value::INTEGER(0));

        auto case_expr_obj = duckdb::make_uniq<duckdb::BoundCaseExpression>(LogicalType::INTEGER);
        case_expr_obj->case_checks.emplace_back(std::move(when_expr), std::move(then_expr));
        case_expr_obj->else_expr = std::move(else_expr);

        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*case_expr_obj, ctx_one_int);

        // This expected string is complex due to nested temp vars for each part of the CASE
        std::string expected_lua_fragment_for_when_val = R"(if tval0_is_null or tval1_is_null then tval2_is_null = true else tval2_is_null = false; tval2_val = (tval0_val > tval1_val)  end)";
        // This is just a part; the full string is longer. A full string match is more robust.
        // For brevity, only checking a key part. Ideally, match the whole normalized string.
        // A simplified check:
        REQUIRE(lua_snippet.find("local tval0_val") != std::string::npos); // col0_ref_when
        REQUIRE(lua_snippet.find("local tval1_val") != std::string::npos); // const_10_when
        REQUIRE(lua_snippet.find("local tval2_val") != std::string::npos); // result of when_expr (col0 > 10)
        REQUIRE(lua_snippet.find(NormalizeLua(expected_lua_fragment_for_when_val)) != std::string::npos);
        REQUIRE(lua_snippet.find("local tval3_val") != std::string::npos); // then_expr (100)
        REQUIRE(lua_snippet.find("local tval4_val") != std::string::npos); // else_expr (0)
        REQUIRE(lua_snippet.find(NormalizeLua("if not tval2_is_null and tval2_val then")) != std::string::npos); // Check if condition
        REQUIRE(lua_snippet.find(NormalizeLua("current_row_val = tval3_val")) != std::string::npos); // then assignment
        REQUIRE(lua_snippet.find(NormalizeLua("current_row_val = tval4_val")) != std::string::npos); // else assignment

        // A more complete expected string for this CASE:
        std::string full_expected_lua = R"(
            local current_row_val
            local current_row_is_null
            local tval0_val
            local tval0_is_null
            if input1_nullmask[i] then
              tval0_is_null = true
            else
              tval0_is_null = false
              tval0_val = input1_data[i]
            end
            local tval1_val
            local tval1_is_null
            tval1_is_null = false
            tval1_val = 10
            local tval2_val
            local tval2_is_null
            if tval0_is_null or tval1_is_null then tval2_is_null = true else tval2_is_null = false; tval2_val = (tval0_val > tval1_val)  end
            local tval3_val
            local tval3_is_null
            tval3_is_null = false
            tval3_val = 100
            local tval4_val
            local tval4_is_null
            tval4_is_null = false
            tval4_val = 0
            if not tval2_is_null and (tval2_val == true or tval2_val == 1) then
              current_row_val = tval3_val
              current_row_is_null = tval3_is_null
            else
              current_row_val = tval4_val
              current_row_is_null = tval4_is_null
            end
        )";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(full_expected_lua));
    }

    SECTION("Translate BoundFunctionExpression: replace(col_varchar, 'H', 'X')") {
        auto col_varchar_ref = CreateBoundReference(0, LogicalType::VARCHAR);
        auto const_h_ref = CreateBoundConstant(Value("H"));
        auto const_x_ref = CreateBoundConstant(Value("X"));
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(col_varchar_ref));
        children.push_back(std::move(const_h_ref));
        children.push_back(std::move(const_x_ref));
        auto replace_expr = CreateBoundFunction("replace", std::move(children), LogicalType::VARCHAR);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*replace_expr, ctx_one_varchar);

        // Simplified check, actual replace logic is complex in generated Lua
        REQUIRE(lua_snippet.find("string.find(s, from_str, i, true)") != std::string::npos);
        REQUIRE(lua_snippet.find("current_row_is_null = false") != std::string::npos); // Assuming non-null inputs for this check
        REQUIRE(lua_snippet.find("local tval0_val") != std::string::npos); // col_varchar_ref
        REQUIRE(lua_snippet.find("local tval1_val") != std::string::npos); // 'H'
        REQUIRE(lua_snippet.find("local tval2_val") != std::string::npos); // 'X'
        REQUIRE(lua_snippet.find("local s, from_str, to_str = tval0_val, tval1_val, tval2_val;") != std::string::npos);
    }

    SECTION("Translate BoundFunctionExpression: extract(year from col_date)") {
        auto col_date_ref = CreateBoundReference(0, LogicalType::DATE);
        auto const_year_str = CreateBoundConstant(Value("year"));
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(const_year_str));
        children.push_back(std::move(col_date_ref));
        auto extract_expr = CreateBoundFunction("extract", std::move(children), LogicalType::BIGINT);

        std::vector<LogicalType> date_input_ctx = {LogicalType::VARCHAR, LogicalType::DATE}; // 0: part_str, 1: date
        LuaTranslatorContext ctx_extract(date_input_ctx); // Context needs to reflect actual inputs to the function node
                                                          // However, GenerateValueExpression for children uses their own return types.
                                                          // The context passed to TranslateExpressionToLuaRowLogic should be for the *ultimate* data chunk columns.
                                                          // For this test, assuming col_date_ref is input1 (index 0) of the chunk.
                                                          // The part string is a constant.

        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*extract_expr, ctx_one_int); // Using ctx_one_int because col_date_ref is index 0

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
tval0_is_null = false
tval0_val = "year"
local tval1_val
local tval1_is_null
if input1_nullmask[i] then
  tval1_is_null = true
else
  tval1_is_null = false
  tval1_val = input1_data[i]
end
if tval0_is_null or tval1_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = duckdb_ffi_extract_from_date(tval1_val, "year")
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundFunctionExpression: length(col_varchar)") {
        auto col_varchar_ref = CreateBoundReference(0, LogicalType::VARCHAR);
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(col_varchar_ref));
        auto length_expr = CreateBoundFunction("length", std::move(children), LogicalType::BIGINT); // LENGTH returns BIGINT
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*length_expr, ctx_one_varchar);

        // Current expectation (no .len optimization implemented in this pass):
        // It will create a lua string then get its length.
        // Output is BIGINT, so no "results_table" involved for this one.
        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = ffi.string(input1_data[i].ptr, input1_data[i].len)
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = #(tval0_val)
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
)";
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
    }

    SECTION("Translate BoundFunctionExpression: lower(col_varchar) (VARCHAR output)") {
        auto col_varchar_ref = CreateBoundReference(0, LogicalType::VARCHAR);
        std::vector<unique_ptr<Expression>> children;
        children.push_back(std::move(col_varchar_ref));
        auto lower_expr = CreateBoundFunction("lower", std::move(children), LogicalType::VARCHAR);
        std::string lua_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(*lower_expr, ctx_one_varchar);

        std::string expected_lua = R"(local current_row_val
local current_row_is_null
local tval0_val
local tval0_is_null
if input1_nullmask[i] then
  tval0_is_null = true
else
  tval0_is_null = false
  tval0_val = ffi.string(input1_data[i].ptr, input1_data[i].len)
end
if tval0_is_null then
  current_row_is_null = true
else
  current_row_is_null = false
  current_row_val = string.lower(tval0_val)
  if current_row_val == nil and not current_row_is_null then current_row_is_null = true; end
end
results_table[i+1] = current_row_val
)"; // Note the added results_table line
        REQUIRE(NormalizeLua(lua_snippet) == NormalizeLua(expected_lua));
        REQUIRE(lua_snippet.find("duckdb_ffi_add_string_to_output_vector") == std::string::npos); // Ensure old call is gone
    }
}
```
