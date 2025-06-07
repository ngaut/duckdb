#include "catch.hpp"
#include "duckdb/main/luajit_translator.hpp"
// Replace PoC expression nodes with actual DuckDB BoundExpression headers
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/types.hpp" // For LogicalType, Value
#include <string>
#include <iostream>
#include <vector>

// Helper to create the full Lua function string for testing purposes.
// (This can remain largely the same, as it wraps the row_logic string)
std::string WrapInLuaFunction(const std::string& row_logic, int num_inputs) {
    std::stringstream ss;
    ss << "function generated_lua_expr_func(output_vector, count, input_vectors)\n";
    ss << "  for i = 0, count - 1 do\n";
    std::string line;
    std::stringstream row_logic_ss(row_logic);
    while (std::getline(row_logic_ss, line)) {
        ss << "    " << line << "\n";
    }
    ss << "  end\n";
    ss << "end\n";
    return ss.str();
}

// --- Helper functions to create DuckDB BoundExpression instances for testing ---
// These are simplified and might not cover all aspects of real bound expressions.
duckdb::unique_ptr<duckdb::BoundConstantExpression> CreateBoundConstant(duckdb::Value val) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(val);
}

duckdb::unique_ptr<duckdb::BoundReferenceExpression> CreateBoundReference(duckdb::idx_t col_idx, duckdb::LogicalType type) {
    // BoundReferenceExpression constructor: (type, col_idx) or (alias, type, col_idx)
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, col_idx);
}

duckdb::unique_ptr<duckdb::BoundOperatorExpression> CreateBoundBinaryOperator(
    duckdb::ExpressionType op_type,
    duckdb::unique_ptr<duckdb::Expression> left,
    duckdb::unique_ptr<duckdb::Expression> right,
    duckdb::LogicalType return_type) {

    std::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    return duckdb::make_uniq<duckdb::BoundOperatorExpression>(op_type, return_type, std::move(children), false /*is_operator_prefix - not relevant for binary*/);
}


TEST_CASE("LuaTranslator Tests with DuckDB BoundExpressions (Numeric)", "[luajit][translator][bound]") {
    using namespace duckdb;

    // Context for input types
    std::vector<LogicalType> two_int_inputs = {LogicalType::INTEGER, LogicalType::INTEGER};
    std::vector<LogicalType> one_int_input = {LogicalType::INTEGER};
    std::vector<LogicalType> three_int_inputs = {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER};

    LuaTranslatorContext ctx_two_ints(two_int_inputs);
    LuaTranslatorContext ctx_one_int(one_int_input);
    LuaTranslatorContext ctx_three_ints(three_int_inputs);

    SECTION("Translate BoundConstantExpression (Integer)") {
        auto const_expr = CreateBoundConstant(Value::INTEGER(42));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*const_expr, ctx_one_int); // Context not strictly needed for const

        std::string expected_lua =
R"(output_vector.nullmask[i] = false
output_vector.data[i] = 42)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for BoundConstant(42):\n" << WrapInLuaFunction(lua_row_logic, 0));
    }

    SECTION("Translate BoundConstantExpression (Double)") {
        auto const_expr = CreateBoundConstant(Value::DOUBLE(3.14));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*const_expr, ctx_one_int);
        std::string expected_val_str = std::to_string(3.14); // Default double to string
        std::string expected_lua =
R"(output_vector.nullmask[i] = false
output_vector.data[i] = )" + expected_val_str;
        REQUIRE(lua_row_logic == expected_lua);
    }

    SECTION("Translate BoundReferenceExpression (Integer Column)") {
        auto col_ref_expr = CreateBoundReference(0, LogicalType::INTEGER); // col_0, type INTEGER
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*col_ref_expr, ctx_one_int);

        // Expects: input_vectors[1] (col_idx 0 + 1)
        // Lua cast for data[i] will use context: ctx_one_int.GetInputLuaFFIType(0) -> "int32_t"
        // The generic GenerateFullLuaJitFunction in executor tests handles the actual casting of void*
        // based on type. Here, the translator generates the direct access.
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = input_vectors[1].data[i]
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for BoundReference(col_0, INT):\n" << WrapInLuaFunction(lua_row_logic, 1));
    }

    SECTION("Translate BoundOperatorExpression: col_0 + col_1 (Integers)") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto col1 = CreateBoundReference(1, LogicalType::INTEGER);
        auto add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD, std::move(col0), std::move(col1), LogicalType::INTEGER);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*add_expr, ctx_two_ints);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = (input_vectors[1].data[i] + input_vectors[2].data[i])
end)";
        REQUIRE(lua_row_logic == expected_lua);
    }

    SECTION("Translate BoundOperatorExpression: col_0 * 10 (Integer)") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto const_10 = CreateBoundConstant(Value::INTEGER(10));
        // Return type of col * INT_CONST is INTEGER
        auto mul_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_MULTIPLY, std::move(col0), std::move(const_10), LogicalType::INTEGER);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*mul_expr, ctx_one_int);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = (input_vectors[1].data[i] * 10)
end)";
        REQUIRE(lua_row_logic == expected_lua);
    }

    SECTION("Translate BoundOperatorExpression: (col_0 + col_1) > 5 (Integers, output BOOLEAN)") {
        auto col0_add = CreateBoundReference(0, LogicalType::INTEGER);
        auto col1_add = CreateBoundReference(1, LogicalType::INTEGER);
        auto add_sub_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD, std::move(col0_add), std::move(col1_add), LogicalType::INTEGER);

        auto const_5 = CreateBoundConstant(Value::INTEGER(5));
        // Return type of comparison is BOOLEAN
        auto gt_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_GREATERTHAN, std::move(add_sub_expr), std::move(const_5), LogicalType::BOOLEAN);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*gt_expr, ctx_two_ints);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    if ((input_vectors[1].data[i] + input_vectors[2].data[i]) > 5) then
        output_vector.data[i] = 1
    else
        output_vector.data[i] = 0
    end
end)";
        REQUIRE(lua_row_logic == expected_lua);
    }

    SECTION("Translate BoundOperatorExpression: col_0 + (col_1 * col_2) (Integers)") {
        auto col0_ref = CreateBoundReference(0, LogicalType::INTEGER);
        auto col1_ref_mul = CreateBoundReference(1, LogicalType::INTEGER);
        auto col2_ref_mul = CreateBoundReference(2, LogicalType::INTEGER);
        auto mul_sub_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_MULTIPLY, std::move(col1_ref_mul), std::move(col2_ref_mul), LogicalType::INTEGER);
        auto add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD, std::move(col0_ref), std::move(mul_sub_expr), LogicalType::INTEGER);

        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*add_expr, ctx_three_ints);
        // Note: referenced_columns are sorted by index in TranslateExpressionToLuaRowLogic
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] or input_vectors[3].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = (input_vectors[1].data[i] + (input_vectors[2].data[i] * input_vectors[3].data[i]))
end)";
        REQUIRE(lua_row_logic == expected_lua);
    }

    SECTION("Translate BoundConstantExpression (VARCHAR)") {
        auto const_expr = CreateBoundConstant(Value("hello lua")); // Value creates VARCHAR by default
        REQUIRE(const_expr->return_type.id() == LogicalTypeId::VARCHAR);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*const_expr, ctx_one_int); // Context not really used for const

        // String output assignment is complex (see translator notes).
        // For now, testing the value_expr_str part.
        // The current translator's output assignment for non-boolean, non-referenced columns is:
        // output_vector.nullmask[i] = false
        // output_vector.data[i] = "hello lua"
        // This is okay if output_vector.data is for a string that Lua itself manages (not FFIString).
        // For this test, we focus on the constant string representation.
        std::string expected_lua =
R"(output_vector.nullmask[i] = false
output_vector.data[i] = "hello lua")"; // This assignment is problematic if output is FFIString
        // The translator has a comment about this.
        // For now, let's assume the test is about the value part "hello lua".
        // The full assignment requires more FFI output logic.
        REQUIRE(lua_row_logic == expected_lua);
    }

    SECTION("Translate BoundReferenceExpression (VARCHAR Column)") {
        auto col_ref_expr = CreateBoundReference(0, LogicalType::VARCHAR);
        std::vector<LogicalType> one_varchar_input = {LogicalType::VARCHAR};
        LuaTranslatorContext ctx_one_varchar(one_varchar_input);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*col_ref_expr, ctx_one_varchar);

        // Expected for VARCHAR: ffi.string(input_vectors[1].data[i].ptr, input_vectors[1].data[i].len)
        // And this Lua string result needs to be handled if assigned to output_vector.data[i]
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = ffi.string(input_vectors[1].data[i].ptr, input_vectors[1].data[i].len)
end)"; // Again, direct assignment of Lua string to output_vector.data[i] is for numeric/bool.
        REQUIRE(lua_row_logic == expected_lua);
    }

}

// TODO: Add tests for actual extended expressions (LIKE, CONCAT, CASE, AND, OR, NOT) using BoundExpressions.
// This will require creating appropriate BoundOperatorExpression, BoundCaseExpression, etc.
// and carefully managing the expected Lua output, especially for string operations and FFI interactions.

    SECTION("Translate BoundOperatorExpression: col_0 < 10 (Integer, output BOOLEAN)") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto const_10 = CreateBoundConstant(Value::INTEGER(10));
        auto lt_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_LESSTHAN, std::move(col0), std::move(const_10), LogicalType::BOOLEAN);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*lt_expr, ctx_one_int);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    if ((input_vectors[1].data[i] < 10)) then
        output_vector.data[i] = 1
    else
        output_vector.data[i] = 0
    end
end)";
        REQUIRE(lua_row_logic == expected_lua);
    }

    SECTION("Translate BoundOperatorExpression: col_0 == col_1 (Integers, output BOOLEAN)") {
        auto col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto col1 = CreateBoundReference(1, LogicalType::INTEGER);
        auto eq_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_EQUAL, std::move(col0), std::move(col1), LogicalType::BOOLEAN);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*eq_expr, ctx_two_ints);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    if ((input_vectors[1].data[i] == input_vectors[2].data[i])) then
        output_vector.data[i] = 1
    else
        output_vector.data[i] = 0
    end
end)";
        REQUIRE(lua_row_logic == expected_lua);
    }

    // Note: Full support for AND, OR (BoundConjunctionExpression), NOT (BoundOperatorExpression with OPERATOR_NOT),
    // LIKE (BoundLikeExpression), CASE (BoundCaseExpression), CONCAT (BoundFunctionExpression)
    // requires adding specific GenerateValue overloads for these BoundExpression subtypes in LuaTranslator.
    // The current tests primarily verify what's handled by the existing BoundOperatorExpression translator logic.
}
```
