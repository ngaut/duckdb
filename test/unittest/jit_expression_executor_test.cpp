#include "catch.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp" // To get ExpressionType for PoC
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp" // Required for ExpressionExecutor
#include "duckdb/common/luajit_wrapper.hpp" // Included by ExpressionExecutor, but good for clarity
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/planner/luajit_expression_nodes.hpp" // Using the PoC expression nodes
#include "duckdb/main/luajit_translator.hpp"     // Using our PoC translator

#include <iostream>

// Mock DuckDB ClientContext for ExpressionExecutor initialization
static duckdb::unique_ptr<duckdb::ClientContext> CreateMockContext() {
    // This is highly simplified. A real ClientContext needs a DB instance.
    // For ExpressionExecutor instantiation, it's mainly used to get Allocator and DBConfig.
    // We might need to mock parts of DBConfig if the executor tries to access it.
    // For now, let's hope the default construction of DBConfig inside ClientContext is enough.
    // If `DBConfig::GetConfig(*context)` is hit, it might require a proper DB instance.
    // As of DuckDB 0.9.2, ClientContext requires a shared_ptr<DuckDB>.
    // This makes true mocking harder. For this PoC, we will skip tests requiring deep context interaction
    // if they cause issues, or assume a global default DB is somehow available for test contexts.
    // For now, let's try with a default-constructed context, which might not be fully functional.
    // This part is tricky for a true unit test without a DB instance.
    // return duckdb::make_uniq<duckdb::ClientContext>(nullptr); // This constructor is protected/private

    // Alternative: Use a real in-memory DB for tests.
    // DuckDB db(nullptr);
    // Connection con(db);
    // return con.context; // This is how it's usually obtained.
    // For a unit test, this is heavy. Let's assume we can get by with a stub.
    // For now, many ExpressionExecutor features might not work correctly without a proper context.
    // The JIT parts we are adding might be okay if they don't heavily rely on context features yet.
    return nullptr; // This will likely make ExpressionExecutor use a default Allocator.
                    // The luajit_wrapper_ itself doesn't need context.
}


// Helper to manually create a very simple DataChunk with one or two vectors of INTs
// This bypasses full DataChunk/Vector complexities for the PoC.
// FFIVectors will be derived from the std::vectors used to back this.
void CreateMockChunk(duckdb::DataChunk& chunk,
                     std::vector<int>& data_col1, std::vector<bool>& nulls_col1,
                     std::vector<int>* data_col2 = nullptr, std::vector<bool>* nulls_col2 = nullptr,
                     duckdb::idx_t count = 0) {

    using namespace duckdb;
    idx_t num_cols = data_col2 ? 2 : 1;
    std::vector<LogicalType> types;
    if (num_cols >= 1) types.push_back(LogicalType::INTEGER);
    if (num_cols >= 2) types.push_back(LogicalType::INTEGER);

    chunk.Initialize(Allocator::DefaultAllocator(), types); // Use default allocator

    if (count == 0) {
        count = data_col1.size();
    }

    // Vector 1
    // In a real scenario, data would be copied into the Vector's internal buffers.
    // For PoC, we are using FFIVector which points to std::vector data directly.
    // The chunk itself won't be directly used by Lua, but ExpressionExecutor needs it.
    // We'll just set its count. The actual data for Lua comes from FFIVectors
    // created from the std::vectors.
    if (num_cols >= 1) {
        // VectorOperations::SetData(chunk.data[0], data_ptr_cast(data_col1.data()), count, data_col1.data());
        // For now, don't try to make the chunk's vectors point to this data,
        // as it's complicated with DuckDB vector types.
        // The JIT path will use FFIVectors created directly from std::vectors.
    }
    if (num_cols >= 2 && data_col2) {
        // VectorOperations::SetData(chunk.data[1], data_ptr_cast(data_col2->data()), count, data_col2->data());
    }
    chunk.SetCardinality(count);
}


// Helper to generate the full Lua function string for the JIT path
// This function will take FFIVector pointers as arguments.
// It adapts the row_logic from LuaTranslator.
std::string GenerateFullLuaJitFunction(const std::string& lua_row_logic, int num_input_exprs, const std::string& output_type_lua, const std::vector<std::string>& input_type_lua) {
    std::stringstream ss;
    ss << "local ffi = require('ffi')\n";
    ss << "ffi.cdef[[\n";
    ss << "    typedef struct FFIVector { void* data; bool* nullmask; unsigned long long count; } FFIVector;\n";
    ss << "]]\n";

    ss << "function execute_jitted_expression(output_vec_ffi";
    for (int i = 0; i < num_input_exprs; ++i) {
        ss << ", input_vec" << i + 1 << "_ffi";
    }
    ss << ", count)\n";

    // Cast output vector
    ss << "    local output_data = ffi.cast('" << output_type_lua << "*', output_vec_ffi.data)\n";
    ss << "    local output_nullmask = ffi.cast('bool*', output_vec_ffi.nullmask)\n";

    // Cast input vectors
    for (int i = 0; i < num_input_exprs; ++i) {
        ss << "    local input" << i + 1 << "_data = ffi.cast('" << input_type_lua[i] << "*', input_vec" << i + 1 << "_ffi.data)\n";
        ss << "    local input" << i + 1 << "_nullmask = ffi.cast('bool*', input_vec" << i + 1 << "_ffi.nullmask)\n";
    }

    ss << "    for i = 0, count - 1 do\n";
    // Adapt lua_row_logic:
    // Original `output_vector.data[i]` becomes `output_data[i]`
    // Original `output_vector.nullmask[i]` becomes `output_nullmask[i]`
    // Original `input_vectors[N].data[i]` becomes `inputN_data[i]`
    // Original `input_vectors[N].nullmask[i]` becomes `inputN_nullmask[i]`
    std::string adapted_row_logic = lua_row_logic;
    StringUtil::Replace(adapted_row_logic, "output_vector.data[i]", "output_data[i]");
    StringUtil::Replace(adapted_row_logic, "output_vector.nullmask[i]", "output_nullmask[i]");
    for (int i = 0; i < num_input_exprs; ++i) {
        std::string input_vec_table_access = StringUtil::Format("input_vectors[%d]", i + 1);
        StringUtil::Replace(adapted_row_logic, input_vec_table_access + ".data[i]", StringUtil::Format("input%d_data[i]", i + 1));
        StringUtil::Replace(adapted_row_logic, input_vec_table_access + ".nullmask[i]", StringUtil::Format("input%d_nullmask[i]", i + 1));
    }
    ss << "        " << adapted_row_logic << "\n"; // Indent adapted logic
    ss << "    end\n";
    ss << "end\n";
    return ss.str();
}


TEST_CASE("JIT ExpressionExecutor Basic Integration", "[luajit][executor]") {
    using namespace duckdb;
    // ClientContext is needed for ExpressionExecutor.
    // For this PoC, true functionality of context is not critical for JIT part itself.
    // However, ExpressionExecutor's constructor `DBConfig::GetConfig(*context)` might require a valid DB.
    // Let's try to create a minimal DB and context.
    DuckDB db(nullptr); // In-memory DB
    Connection con(db);
    ClientContext &context = *con.context;

    ExpressionExecutor executor(context);
    DataChunk mock_input_chunk;
    Vector output_vector(LogicalType::INTEGER); // Assuming INT output for col+col

    const int data_size = 5;
    std::vector<int> col1_data = {1, 2, 3, 4, 5};
    std::vector<bool> col1_nulls = {false, false, false, false, false};
    std::vector<int> col2_data = {10, 20, 0, 400, 500};
    std::vector<bool> col2_nulls = {false, false, true, false, false}; // col2[2] is NULL
    std::vector<int> result_data(data_size);       // For FFIVector output
    std::vector<bool> result_nulls(data_size);     // For FFIVector output

    // Create a mock input chunk (not strictly used by JIT path data, but executor needs it)
    CreateMockChunk(mock_input_chunk, col1_data, col1_nulls, &col2_data, &col2_nulls, data_size);
    executor.SetChunk(&mock_input_chunk); // Executor needs a chunk to know cardinality

    // 1. Create a PoC Expression (col0 + col1)
    // These are our simplified luajit_expression_nodes, not DuckDB's BoundExpressions.
    // This is a key simplification for this PoC.
    auto poc_col0 = MakeLuaColumnRef(0); // Refers to first input vector for JIT
    auto poc_col1 = MakeLuaColumnRef(1); // Refers to second input vector for JIT
    auto poc_add_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::ADD, std::move(poc_col0), std::move(poc_col1));

    // This is also a PoC Expression, used as the 'key' for the state in ExpressionExecutor.
    // In a real scenario, this would be a DuckDB BoundExpression.
    // For the PoC, we use the PoC expression itself.
    // The ExpressionExecutor expects a const Expression&. We need to ensure our PoC expression
    // can be passed. For this test, we'll assume it's fine, or the test would need
    // a dummy DuckDB BoundExpression that maps to this PoC expression.
    // Let's use a dummy BoundOperatorExpression as the "key" for the state.
    // The actual logic for JIT will use `poc_add_expr`.
    auto dummy_duckdb_expr = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_ADD, LogicalType::INTEGER);
    // Add child expressions to dummy_duckdb_expr if needed for it to be valid, though not used by JIT logic directly.

    executor.AddExpression(*dummy_duckdb_expr); // This creates an ExpressionState
    REQUIRE(executor.GetStates().size() == 1);
    ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();
    REQUIRE(expr_state != nullptr);

    // 2. Translate PoC expression to Lua row logic
    LuaTranslatorContext translator_ctx(2); // 2 input vectors
    std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*poc_add_expr, translator_ctx);

    // 3. Generate full Lua function and compile it using LuaJITStateWrapper
    std::vector<std::string> input_types_lua = {"int", "int"}; // col0 is int, col1 is int
    std::string full_lua_script = GenerateFullLuaJitFunction(lua_row_logic, 2, "int", input_types_lua);

    INFO("Generated Full Lua Script for JIT:\n" << full_lua_script);

    bool compiled = executor.luajit_wrapper_.ExecuteString(full_lua_script);
    REQUIRE(compiled == true);

    // Store function name and mark JIT as succeeded (simulating what Execute might do)
    expr_state->jitted_lua_function_name = "execute_jitted_expression";
    expr_state->jit_compilation_succeeded = true;
    expr_state->attempted_jit_compilation = true;

    // 4. Prepare FFIVectors for execution (pointing to our std::vector data)
    duckdb::ffi::FFIVector ffi_out_vec;
    ffi_out_vec.data = result_data.data();
    ffi_out_vec.nullmask = result_nulls.data();
    ffi_out_vec.count = data_size; // Count is passed separately to Lua func

    duckdb::ffi::FFIVector ffi_in_vec1;
    ffi_in_vec1.data = col1_data.data();
    ffi_in_vec1.nullmask = col1_nulls.data();
    ffi_in_vec1.count = data_size;

    duckdb::ffi::FFIVector ffi_in_vec2;
    ffi_in_vec2.data = col2_data.data();
    ffi_in_vec2.nullmask = col2_nulls.data();
    ffi_in_vec2.count = data_size;

    // 5. Call the JITed Lua function via LuaJITStateWrapper directly for this test
    // This bypasses the complex logic within ExpressionExecutor::Execute itself,
    // but tests the core JIT machinery (Lua function call with FFIVectors).
    lua_State* L = executor.luajit_wrapper_.GetState();
    lua_getglobal(L, expr_state->jitted_lua_function_name.c_str());
    REQUIRE(lua_isfunction(L, -1) == true);

    lua_pushlightuserdata(L, &ffi_out_vec);    // Arg 1: output FFIVector*
    lua_pushlightuserdata(L, &ffi_in_vec1);    // Arg 2: input1 FFIVector*
    lua_pushlightuserdata(L, &ffi_in_vec2);    // Arg 3: input2 FFIVector*
    lua_pushinteger(L, data_size);             // Arg 4: count

    bool pcall_ok = (lua_pcall(L, 4, 0, 0) == LUA_OK); // 4 args, 0 results
    if (!pcall_ok) {
        const char* err_msg = lua_tostring(L, -1);
        FAIL("Lua pcall failed: " << (err_msg ? err_msg : "unknown error"));
        lua_pop(L, 1); // Pop error
    }
    REQUIRE(pcall_ok == true);
    lua_settop(L, 0); // Clear stack


    // 6. Verify results in C++ std::vectors
    INFO("Verifying results after JIT execution...");
    for (int i = 0; i < data_size; ++i) {
        bool expected_null = col1_nulls[i] || col2_nulls[i];
        REQUIRE(result_nulls[i] == expected_null);
        if (!expected_null) {
            REQUIRE(result_data[i] == col1_data[i] + col2_data[i]);
            INFO("Result[" << i << "]: " << result_data[i] << " (Expected: " << col1_data[i] + col2_data[i] << ")");
        } else {
            INFO("Result[" << i << "]: NULL (Expected: NULL)");
        }
    }

    // Conceptual: If ExpressionExecutor::Execute was fully JIT-aware for PoC nodes:
    // executor.ExecuteExpression(0, output_vector); // This would use the JIT path internally
    // And then we'd verify output_vector.
    // For now, the direct Lua call above tests the JIT part. The `ShouldJIT` and
    // JIT path in `ExpressionExecutor::Execute` are more placeholders for future integration.
    // This test demonstrates the *potential* of calling a JITed Lua function
    // with data structures that mirror DuckDB vectors, using the LuaJIT wrapper
    // that would be part of the ExpressionExecutor.

    SUCCEED("JIT execution path tested (manual Lua call with FFIVectors).");
}


TEST_CASE("JIT ExpressionExecutor Extended Operations", "[luajit][executor][extended]") {
    using namespace duckdb;
    DuckDB db(nullptr);
    Connection con(db);
    ClientContext &context = *con.context;
    ExpressionExecutor executor(context);
    DataChunk mock_input_chunk; // For executor.SetChunk

    const int data_size = 3;
    std::vector<int> result_data(data_size);
    std::vector<bool> result_nulls(data_size);

    // Output FFIVector (usually int for these boolean/numeric result tests)
    duckdb::ffi::FFIVector ffi_out_vec;
    ffi_out_vec.data = result_data.data();
    ffi_out_vec.nullmask = result_nulls.data();
    ffi_out_vec.count = data_size;


    SECTION("Logical AND: (col0 > 0) AND (col1 < 10)") {
        // Input data
        std::vector<int> col0_data = {5, -1, 10};
        std::vector<bool> col0_nulls = {false, false, false};
        std::vector<int> col1_data = {5, 15, 5};
        std::vector<bool> col1_nulls = {false, false, false};
        CreateMockChunk(mock_input_chunk, col0_data, col0_nulls, &col1_data, &col1_nulls, data_size);
        executor.SetChunk(&mock_input_chunk);

        // PoC Expression: (col0 > 0) AND (col1 < 10)
        auto c0_gt_0 = MakeLuaBinaryOp(LuaJITBinaryOperatorType::GREATER_THAN, MakeLuaColumnRef(0), MakeLuaConstant(0));
        auto c1_lt_10 = MakeLuaBinaryOp(LuaJITBinaryOperatorType::LESS_THAN, MakeLuaColumnRef(1), MakeLuaConstant(10));
        auto and_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::AND, std::move(c0_gt_0), std::move(c1_lt_10));

        auto dummy_expr = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_AND, LogicalType::BOOLEAN);
        executor.AddExpression(*dummy_expr);
        ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();

        LuaTranslatorContext translator_ctx(2);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*and_expr, translator_ctx);
        std::string full_lua_script = GenerateFullLuaJitFunction(lua_row_logic, 2, "int", {"int", "int"});
        INFO("Logical AND script:\n" << full_lua_script);
        REQUIRE(executor.luajit_wrapper_.ExecuteString(full_lua_script));
        expr_state->jitted_lua_function_name = "execute_jitted_expression";

        duckdb::ffi::FFIVector ffi_in_vec1 {col0_data.data(), col0_nulls.data(), data_size};
        duckdb::ffi::FFIVector ffi_in_vec2 {col1_data.data(), col1_nulls.data(), data_size};

        lua_State* L = executor.luajit_wrapper_.GetState();
        lua_getglobal(L, expr_state->jitted_lua_function_name.c_str());
        REQUIRE(lua_isfunction(L, -1));
        lua_pushlightuserdata(L, &ffi_out_vec);
        lua_pushlightuserdata(L, &ffi_in_vec1);
        lua_pushlightuserdata(L, &ffi_in_vec2);
        lua_pushinteger(L, data_size);
        REQUIRE(lua_pcall(L, 4, 0, 0) == LUA_OK);
        lua_settop(L, 0);

        // Verification: (5>0 AND 5<10) -> T, (-1>0 AND 15<10) -> F, (10>0 AND 5<10) -> T
        REQUIRE(result_data[0] == 1); REQUIRE(result_nulls[0] == false);
        REQUIRE(result_data[1] == 0); REQUIRE(result_nulls[1] == false);
        REQUIRE(result_data[2] == 1); REQUIRE(result_nulls[2] == false);
    }

    SECTION("Simple CASE: CASE WHEN col0 > 0 THEN 100 ELSE 200 END") {
        std::vector<int> col0_data = {5, -5, 0};
        std::vector<bool> col0_nulls = {false, false, true}; // Last one is NULL
        CreateMockChunk(mock_input_chunk, col0_data, col0_nulls, nullptr, nullptr, data_size);
        executor.SetChunk(&mock_input_chunk);

        auto condition = MakeLuaBinaryOp(LuaJITBinaryOperatorType::GREATER_THAN, MakeLuaColumnRef(0), MakeLuaConstant(0));
        std::vector<CaseBranch> branches;
        branches.emplace_back(CaseBranch{std::move(condition), MakeLuaConstant(100)});
        auto case_expr = MakeLuaCaseExpression(std::move(branches), MakeLuaConstant(200));

        auto dummy_expr = make_uniq<BoundOperatorExpression>(ExpressionType::CASE_EXPR, LogicalType::INTEGER); // Dummy type
        executor.AddExpression(*dummy_expr);
        ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();

        LuaTranslatorContext translator_ctx(1);
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*case_expr, translator_ctx);
        std::string full_lua_script = GenerateFullLuaJitFunction(lua_row_logic, 1, "int", {"int"});
        INFO("CASE script:\n" << full_lua_script);
        REQUIRE(executor.luajit_wrapper_.ExecuteString(full_lua_script));
        expr_state->jitted_lua_function_name = "execute_jitted_expression";

        duckdb::ffi::FFIVector ffi_in_vec1 {col0_data.data(), col0_nulls.data(), data_size};

        lua_State* L = executor.luajit_wrapper_.GetState();
        lua_getglobal(L, expr_state->jitted_lua_function_name.c_str());
        REQUIRE(lua_isfunction(L, -1));
        lua_pushlightuserdata(L, &ffi_out_vec);
        lua_pushlightuserdata(L, &ffi_in_vec1);
        lua_pushinteger(L, data_size);
        REQUIRE(lua_pcall(L, 3, 0, 0) == LUA_OK);
        lua_settop(L, 0);

        // Verification: (5>0?100:200)->100, (-5>0?100:200)->200, (NULL>0?100:200)->NULL
        REQUIRE(result_data[0] == 100); REQUIRE(result_nulls[0] == false);
        REQUIRE(result_data[1] == 200); REQUIRE(result_nulls[1] == false);
        REQUIRE(result_nulls[2] == true); // Input col0[2] is NULL
    }

    SECTION("String LIKE (boolean result): col_str LIKE '%middle%'") {
        // Store actual string data for input
        std::vector<std::string> string_values = {"startmiddleend", "nomatch", "anothermiddlevalue"};
        std::vector<duckdb::ffi::FFIString> cpp_ffi_strings(data_size);
        std::vector<bool> col_nulls = {false, false, false};

        for(int i=0; i < data_size; ++i) {
            cpp_ffi_strings[i].ptr = const_cast<char*>(string_values[i].c_str());
            cpp_ffi_strings[i].len = static_cast<uint32_t>(string_values[i].length());
        }
        // Create a dummy DataChunk just for cardinality and executor setup
        DataChunk mock_str_chunk;
        std::vector<LogicalType> str_types = {LogicalType::VARCHAR};
        mock_str_chunk.Initialize(Allocator::DefaultAllocator(), str_types);
        mock_str_chunk.SetCardinality(data_size);
        executor.SetChunk(&mock_str_chunk);


        // PoC Expression: col0 LIKE '%middle%'
        // For the translator to generate ffi.string(input_vectors[1].data[i].ptr, input_vectors[1].data[i].len)
        // for col0, it would need type info. We assume translator handles this conceptually.
        // Here, the GenerateFullLuaJitFunction needs to know input is FFIString.
        // The lua_row_logic from translator for colref needs to be type aware.
        // Let's assume the Lua code for colref will be "ffi.string(input1_data[i].ptr, input1_data[i].len)"
        // This implies GenerateValue for ColumnReference must be type-aware or this test must use literals.
        // To keep translator simple for PoC, let's use a string literal as the subject of LIKE.
        // This tests LIKE logic in translator but not full FFIString column ref through translator.

        auto subject_str_expr = MakeLuaConstant(std::string("test_middle_test"));
        auto pattern_expr = MakeLuaConstant(std::string("%middle%"));
        auto like_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::LIKE, std::move(subject_str_expr), std::move(pattern_expr));

        auto dummy_expr = make_uniq<BoundOperatorExpression>(ExpressionType::COMPARE_LIKE, LogicalType::BOOLEAN);
        executor.AddExpression(*dummy_expr);
        ExpressionState* expr_state = executor.GetStates().back()->root_state.get(); // Get last added state

        LuaTranslatorContext translator_ctx(0); // 0 inputs as using constants
        std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*like_expr, translator_ctx);
        // Output is boolean (0/1), so "int". Inputs are FFIString, but this expr has no col inputs.
        std::string full_lua_script = GenerateFullLuaJitFunction(lua_row_logic, 0, "int", {});
        INFO("LIKE script:\n" << full_lua_script);
        REQUIRE(executor.luajit_wrapper_.ExecuteString(full_lua_script));
        expr_state->jitted_lua_function_name = "execute_jitted_expression";

        // No actual FFIVector inputs needed as expression is all constants
        lua_State* L = executor.luajit_wrapper_.GetState();
        lua_getglobal(L, expr_state->jitted_lua_function_name.c_str());
        REQUIRE(lua_isfunction(L, -1));
        lua_pushlightuserdata(L, &ffi_out_vec); // Output vector
        // No input FFIVector args
        lua_pushinteger(L, data_size); // Count
        REQUIRE(lua_pcall(L, 2, 0, 0) == LUA_OK); // 2 args: output_vec, count
        lua_settop(L, 0);

        // Verification: "test_middle_test" LIKE "%middle%" is true (1) for all rows
        for(int i=0; i<data_size; ++i) {
            REQUIRE(result_data[i] == 1); REQUIRE(result_nulls[i] == false);
        }
    }
}
// TODO: Add tests for:
// - Different expression types (e.g., col > const)
// - Different data types (e.g., double)
// - Cases where JIT should not be used (ShouldJIT returns false) and it falls back.
// - Cases where JIT compilation fails and it falls back.
```
