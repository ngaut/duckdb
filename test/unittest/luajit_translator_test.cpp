#include "catch.hpp"
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/planner/luajit_expression_nodes.hpp" // Using the PoC expression nodes
#include <string>
#include <iostream>

// Helper to create the full Lua function string for testing purposes.
// The LuaTranslator::TranslateExpressionToLuaRowLogic only generates the body for one row.
// This helper wraps it in a loop and function signature for standalone testing/validation.
std::string WrapInLuaFunction(const std::string& row_logic, int num_inputs) {
    std::stringstream ss;
    ss << "function generated_lua_expr_func(output_vector, count, input_vectors)\n";
    // It's good practice to make ffi local if not already global from a previous script
    // ss << "  local ffi = require('ffi')\n";
    ss << "  for i = 0, count - 1 do\n";
    // Indent row_logic for readability
    std::string line;
    std::stringstream row_logic_ss(row_logic);
    while (std::getline(row_logic_ss, line)) {
        ss << "    " << line << "\n";
    }
    ss << "  end\n";
    ss << "end\n";
    return ss.str();
}


TEST_CASE("LuaTranslator Tests for Basic Expressions", "[luajit][translator]") {
    using namespace duckdb;

    // Context for 2 input vectors
    LuaTranslatorContext ctx(2);
    LuaTranslatorContext ctx_one_input(1);
    LuaTranslatorContext ctx_three_inputs(3);


    SECTION("Translate Constant Integer") {
        auto const_expr = MakeLuaConstant(42);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*const_expr, ctx);

        // Expected: assigns constant, never null
        // output_vector.nullmask[i] = false
        // output_vector.data[i] = 42
        // Note: TranslateExpressionToLuaRowLogic handles the if/else for comparisons. For constants, it's simpler.
        // Let's check the output directly.
        std::string expected_lua =
R"(output_vector.nullmask[i] = false
output_vector.data[i] = 42)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for '42':\n" << WrapInLuaFunction(lua_row_logic, 0));
    }

    SECTION("Translate Constant Double") {
        auto const_expr = MakeLuaConstant(3.14);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*const_expr, ctx);
        // std::to_string for double can have trailing zeros. Be mindful in comparison.
        // For this test, we assume default std::to_string behavior is acceptable.
        std::string expected_value_str = std::to_string(3.14);
        std::string expected_lua =
R"(output_vector.nullmask[i] = false
output_vector.data[i] = )" + expected_value_str;
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for '3.14':\n" << WrapInLuaFunction(lua_row_logic, 0));
    }

    SECTION("Translate Column Reference") {
        auto col_ref_expr = MakeLuaColumnRef(0); // col_0
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*col_ref_expr, ctx_one_input);

        // Expected: reads from input_vectors[1] (0+1), handles its nullmask
        // if input_vectors[1].nullmask[i] then
        //     output_vector.nullmask[i] = true
        // else
        //     output_vector.nullmask[i] = false
        //     output_vector.data[i] = input_vectors[1].data[i]
        // end
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = input_vectors[1].data[i]
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for 'col_0':\n" << WrapInLuaFunction(lua_row_logic, 1));
    }

    SECTION("Translate col_0 + col_1") {
        auto col0 = MakeLuaColumnRef(0);
        auto col1 = MakeLuaColumnRef(1);
        auto add_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::ADD, std::move(col0), std::move(col1));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*add_expr, ctx);

        // Expected: checks nulls for input_vectors[1] and input_vectors[2]
        // if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] then
        //     output_vector.nullmask[i] = true
        // else
        //     output_vector.nullmask[i] = false
        //     output_vector.data[i] = (input_vectors[1].data[i] + input_vectors[2].data[i])
        // end
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = (input_vectors[1].data[i] + input_vectors[2].data[i])
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for 'col_0 + col_1':\n" << WrapInLuaFunction(lua_row_logic, 2));
    }

    SECTION("Translate col_0 * 10") {
        auto col0 = MakeLuaColumnRef(0);
        auto const_10 = MakeLuaConstant(10);
        auto mul_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::MULTIPLY, std::move(col0), std::move(const_10));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*mul_expr, ctx_one_input);

        // Expected: checks null for input_vectors[1] only (constant is not nullable by this logic)
        // if input_vectors[1].nullmask[i] then
        //     output_vector.nullmask[i] = true
        // else
        //     output_vector.nullmask[i] = false
        //     output_vector.data[i] = (input_vectors[1].data[i] * 10)
        // end
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = (input_vectors[1].data[i] * 10)
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for 'col_0 * 10':\n" << WrapInLuaFunction(lua_row_logic, 1));
    }

    SECTION("Translate (col_0 + col_1) > 5") {
        auto col0 = MakeLuaColumnRef(0);
        auto col1 = MakeLuaColumnRef(1);
        auto const_5 = MakeLuaConstant(5);
        auto add_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::ADD, std::move(col0), std::move(col1));
        auto gt_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::GREATER_THAN, std::move(add_expr), std::move(const_5));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*gt_expr, ctx);

        // Expected: null checks for col0 and col1. Result of comparison is 0 or 1.
        // if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] then
        //     output_vector.nullmask[i] = true
        // else
        //     output_vector.nullmask[i] = false
        //     if ((input_vectors[1].data[i] + input_vectors[2].data[i]) > 5) then
        //         output_vector.data[i] = 1
        //     else
        //         output_vector.data[i] = 0
        //     end
        // end
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
        INFO("Generated Lua for '(col_0 + col_1) > 5':\n" << WrapInLuaFunction(lua_row_logic, 2));
    }

    SECTION("Translate col_0 + (col_1 * col_2)") {
        auto col0 = MakeLuaColumnRef(0); // input_vectors[1]
        auto col1 = MakeLuaColumnRef(1); // input_vectors[2]
        auto col2 = MakeLuaColumnRef(2); // input_vectors[3]
        auto mul_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::MULTIPLY, std::move(col1), std::move(col2));
        auto add_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::ADD, std::move(col0), std::move(mul_expr));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*add_expr, ctx_three_inputs);

        // Expected: null checks for col0, col1, col2.
        // if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] or input_vectors[3].nullmask[i] then
        //     output_vector.nullmask[i] = true
        // else
        //     output_vector.nullmask[i] = false
        //     output_vector.data[i] = (input_vectors[1].data[i] + (input_vectors[2].data[i] * input_vectors[3].data[i]))
        // end
        // Order of referenced_columns in the if condition might vary depending on std::vector behavior,
        // so this test might need to be made more robust to ordering, or sort referenced_columns in translator.
        // For now, assume fixed order from traversal (0, 1, 2).
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] or input_vectors[3].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = (input_vectors[1].data[i] + (input_vectors[2].data[i] * input_vectors[3].data[i]))
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for 'col_0 + (col_1 * col_2)':\n" << WrapInLuaFunction(lua_row_logic, 3));
    }

    SECTION("Translate Constant Comparison: 10 > 5") {
        auto const_10 = MakeLuaConstant(10);
        auto const_5 = MakeLuaConstant(5);
        auto gt_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::GREATER_THAN, std::move(const_10), std::move(const_5));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*gt_expr, ctx);

        // Expected: No input null checks. Result is 0 or 1.
        // output_vector.nullmask[i] = false
        // if (10 > 5) then
        //     output_vector.data[i] = 1
        // else
        //     output_vector.data[i] = 0
        // end
        std::string expected_lua =
R"(output_vector.nullmask[i] = false
if (10 > 5) then
    output_vector.data[i] = 1
else
    output_vector.data[i] = 0
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for '10 > 5':\n" << WrapInLuaFunction(lua_row_logic, 0));
    }
}

TEST_CASE("LuaTranslator Tests for Extended Expressions", "[luajit][translator][extended]") {
    using namespace duckdb;
    LuaTranslatorContext ctx_two_inputs(2);
    LuaTranslatorContext ctx_one_input(1);

    SECTION("Translate String Constant") {
        auto str_const_expr = MakeLuaConstant(std::string("hello lua"));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*str_const_expr, ctx_one_input);
        std::string expected_lua =
R"(output_vector.nullmask[i] = false
output_vector.data[i] = "hello lua")"; // Assuming numeric/bool output for now. String output is complex.
        // If outputting a string, the above data assignment is wrong.
        // This test primarily verifies the constant representation.
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for '\"hello lua\"':\n" << WrapInLuaFunction(lua_row_logic, 0));
    }

    // NOTE on String Column References for following tests:
    // The LuaTranslator::GenerateValue for ColumnReference currently produces: input_vectors[X].data[i]
    // For string operations, this should ideally be: ffi.string(input_vectors[X].data[i].ptr, input_vectors[X].data[i].len)
    // This requires type information in LuaTranslatorContext. For these tests, we'll assume
    // that if a column reference is used in a string context (e.g., CONCAT's operand), the
    // translator would generate the ffi.string(...) wrapper. The expected Lua strings will reflect this.
    // This is a conceptual leap for the current translator code but necessary for testing string ops.

    SECTION("Translate CONCAT: 'hello' .. 'world'") {
        auto s1 = MakeLuaConstant(std::string("hello"));
        auto s2 = MakeLuaConstant(std::string("world"));
        auto concat_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::CONCAT, std::move(s1), std::move(s2));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*concat_expr, ctx_one_input);
        // Outputting a string to FFIVector.data[i] is problematic.
        // This test focuses on the value_expr_str part: ("hello" .. "world")
        // The TranslateExpressionToLuaRowLogic will try to assign this.
        // For a real string output, output_vector.data[i] would need to be an FFIString,
        // and a C helper or complex Lua would be needed to allocate/copy the result.
        // We test the generated value expression, assuming the output part is placeholder for strings.
        std::string expected_lua =
R"(output_vector.nullmask[i] = false
output_vector.data[i] = ("hello" .. "world"))"; // This assignment is conceptually problematic for strings.
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for CONCAT (literals):\n" << WrapInLuaFunction(lua_row_logic, 0));
    }

    SECTION("Translate LIKE: col_str LIKE '%pattern%'") {
        // Conceptual: Assume col_str (col0) is a string column.
        // The GenerateValue for ColumnReferenceExpression would need to output
        // ffi.string(input_vectors[1].data[i].ptr, input_vectors[1].data[i].len)
        // For this test, we use a string literal for the column for simplicity of value_expr_str.
        auto str_val = MakeLuaConstant(std::string("teststring")); // Simulates string column for value part
        auto pattern = MakeLuaConstant(std::string("%str%"));
        auto like_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::LIKE, std::move(str_val), std::move(pattern));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*like_expr, ctx_one_input);

        std::string expected_lua =
R"(output_vector.nullmask[i] = false
if (string.find("teststring", "str", 1, true) ~= nil) then
    output_vector.data[i] = 1
else
    output_vector.data[i] = 0
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for LIKE:\n" << WrapInLuaFunction(lua_row_logic, 1));
    }


    SECTION("Translate Logical AND: (col0 > 5) AND (col1 < 10)") {
        auto col0 = MakeLuaColumnRef(0);
        auto const5 = MakeLuaConstant(5);
        auto gt_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::GREATER_THAN, std::move(col0), std::move(const5));

        auto col1 = MakeLuaColumnRef(1);
        auto const10 = MakeLuaConstant(10);
        auto lt_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::LESS_THAN, std::move(col1), std::move(const10));

        auto and_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::AND, std::move(gt_expr), std::move(lt_expr));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*and_expr, ctx_two_inputs);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] or input_vectors[2].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    if (((input_vectors[1].data[i] > 5) == 1) and ((input_vectors[2].data[i] < 10) == 1)) then
        output_vector.data[i] = 1
    else
        output_vector.data[i] = 0
    end
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for AND:\n" << WrapInLuaFunction(lua_row_logic, 2));
    }

    SECTION("Translate Logical OR: (col0 == 0) OR (col0 == 1)") {
        auto col0_a = MakeLuaColumnRef(0);
        auto const0 = MakeLuaConstant(0);
        auto eq_expr1 = MakeLuaBinaryOp(LuaJITBinaryOperatorType::EQUALS, std::move(col0_a), std::move(const0));

        auto col0_b = MakeLuaColumnRef(0); // Needs separate instance for unique_ptr
        auto const1 = MakeLuaConstant(1);
        auto eq_expr2 = MakeLuaBinaryOp(LuaJITBinaryOperatorType::EQUALS, std::move(col0_b), std::move(const1));

        auto or_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::OR, std::move(eq_expr1), std::move(eq_expr2));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*or_expr, ctx_one_input);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    if (((input_vectors[1].data[i] == 0) == 1) or ((input_vectors[1].data[i] == 1) == 1)) then
        output_vector.data[i] = 1
    else
        output_vector.data[i] = 0
    end
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for OR:\n" << WrapInLuaFunction(lua_row_logic, 1));
    }

    SECTION("Translate Unary NOT: NOT (col0 > 5)") {
        auto col0 = MakeLuaColumnRef(0);
        auto const5 = MakeLuaConstant(5);
        auto gt_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::GREATER_THAN, std::move(col0), std::move(const5));
        auto not_expr = MakeLuaUnaryOp(LuaJITUnaryOperatorType::NOT, std::move(gt_expr));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*not_expr, ctx_one_input);

        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    if (not ((input_vectors[1].data[i] > 5) == 1)) then
        output_vector.data[i] = 1
    else
        output_vector.data[i] = 0
    end
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for NOT:\n" << WrapInLuaFunction(lua_row_logic, 1));
    }

    SECTION("Translate Simple CASE: CASE WHEN col0 > 0 THEN 10 ELSE 20 END") {
        auto col0 = MakeLuaColumnRef(0);
        auto const0 = MakeLuaConstant(0);
        auto condition = MakeLuaBinaryOp(LuaJITBinaryOperatorType::GREATER_THAN, std::move(col0), std::move(const0));

        auto result_true = MakeLuaConstant(10);
        auto result_false = MakeLuaConstant(20);

        std::vector<CaseBranch> branches;
        branches.emplace_back(CaseBranch{std::move(condition), std::move(result_true)});

        auto case_expr = MakeLuaCaseExpression(std::move(branches), std::move(result_false));
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*case_expr, ctx_one_input);

        // Note: The CASE translation uses an IIFE: (function() if (CONDITION==1) then return RES_TRUE else return RES_FALSE end)()
        // Null propagation for CASE: if col0 is null, the outer null check handles it.
        // If condition is not null, but result_true/result_false path involves other nullable columns
        // (not in this simple test), their nullness should be handled by their own sub-expressions.
        // The current CASE translation itself doesn't add extra null checks for its result branches.
        std::string expected_lua =
R"(if input_vectors[1].nullmask[i] then
    output_vector.nullmask[i] = true
else
    output_vector.nullmask[i] = false
    output_vector.data[i] = (function() if ((input_vectors[1].data[i] > 0) == 1) then return 10 else return 20 end end)()
end)";
        REQUIRE(lua_row_logic == expected_lua);
        INFO("Generated Lua for CASE:\n" << WrapInLuaFunction(lua_row_logic, 1));
    }
}
// TODO: Test that referenced_columns are correctly collected and sorted if order matters for the test.
```
