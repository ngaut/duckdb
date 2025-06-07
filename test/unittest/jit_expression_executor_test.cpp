#include "catch.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/luajit_wrapper.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_t.hpp" // Added for string_t
#include "duckdb/common/exception.hpp" // Added for duckdb::RuntimeException

#include <iostream>
#include <vector>

// --- Helper functions to create DuckDB BoundExpression instances for testing ---
static duckdb::unique_ptr<duckdb::BoundConstantExpression> CreateBoundConstant(duckdb::Value val) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(val);
}

static duckdb::unique_ptr<duckdb::BoundReferenceExpression> CreateBoundReference(duckdb::idx_t col_idx, duckdb::LogicalType type) {
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, col_idx);
}

static duckdb::unique_ptr<duckdb::BoundOperatorExpression> CreateBoundBinaryOperator(
    duckdb::ExpressionType op_type,
    duckdb::unique_ptr<duckdb::Expression> left,
    duckdb::unique_ptr<duckdb::Expression> right,
    duckdb::LogicalType return_type) {
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    return duckdb::make_uniq<duckdb::BoundOperatorExpression>(op_type, return_type, std::move(children), false);
}

// Helper to create and setup a DataChunk with specific vector types
static void SetupDataChunk(duckdb::DataChunk& chunk, duckdb::ClientContext& context,
                    const std::vector<duckdb::LogicalType>& types, duckdb::idx_t count) {
    chunk.Initialize(duckdb::Allocator::Get(context), types);
    for(size_t i=0; i < types.size(); ++i) {
        chunk.data[i].SetVectorType(duckdb::VectorType::FLAT_VECTOR); // Start with flat
    }
    chunk.SetCardinality(count);
}

// Updated GenerateFullLuaJitFunction to use LuaTranslatorContext for FFI types
static std::string GenerateFullLuaJitFunctionExt(const std::string& function_name, // Added function_name param
                                        const std::string& lua_row_logic,
                                        duckdb::LuaTranslatorContext& translator_ctx,
                                        const duckdb::LogicalType& output_logical_type) {
    using namespace duckdb;
    std::stringstream ss;
    ss << "local ffi = require('ffi')\n";
    ss << "ffi.cdef[[\n";
    ss << "    typedef struct FFIVector { void* data; bool* nullmask; unsigned long long count; "
       << "int ffi_logical_type_id; int ffi_duckdb_vector_type; } FFIVector;\n";
    ss << "    typedef struct FFIString { char* ptr; unsigned int len; } FFIString;\n";
    ss << "    typedef signed char int8_t;\n";
    ss << "    typedef int int32_t;\n";
    ss << "    typedef long long int64_t;\n";
    ss << "]]\n";

    ss << function_name << " = function(output_vec_ffi"; // Use provided function_name
    for (idx_t i = 0; i < translator_ctx.GetNumInputs(); ++i) {
        ss << ", input_vec" << i + 1 << "_ffi";
    }
    ss << ", count)\n";

    std::string output_lua_ffi_type_str;
    switch(output_logical_type.id()) {
        case LogicalTypeId::INTEGER: output_lua_ffi_type_str = "int32_t"; break;
        case LogicalTypeId::BIGINT: output_lua_ffi_type_str = "int64_t"; break;
        case LogicalTypeId::DOUBLE: output_lua_ffi_type_str = "double"; break;
        case LogicalTypeId::VARCHAR: output_lua_ffi_type_str = "FFIString"; break;
        case LogicalTypeId::BOOLEAN: output_lua_ffi_type_str = "int8_t"; break;
        case LogicalTypeId::DATE: output_lua_ffi_type_str = "int32_t"; break;
        case LogicalTypeId::TIMESTAMP: output_lua_ffi_type_str = "int64_t"; break;
        default: throw NotImplementedException("[JIT Test] Output type for Lua FFI cast not defined: " + output_logical_type.ToString());
    }

    if (output_logical_type.id() == LogicalTypeId::VARCHAR) {
        ss << "    local output_data_ffi_str_array = ffi.cast('FFIString*', output_vec_ffi.data)\n";
    } else {
        ss << "    local output_data = ffi.cast('" << output_lua_ffi_type_str << "*', output_vec_ffi.data)\n";
    }
    ss << "    local output_nullmask = ffi.cast('bool*', output_vec_ffi.nullmask)\n";

    for (idx_t i = 0; i < translator_ctx.GetNumInputs(); ++i) {
        std::string lua_ffi_type_str = translator_ctx.GetInputLuaFFIType(i);
        if (translator_ctx.GetInputLogicalType(i).id() == LogicalTypeId::VARCHAR) {
             ss << "    local input" << i + 1 << "_data_ffi_str_array = ffi.cast('FFIString*', input_vec" << i + 1 << "_ffi.data)\n";
        } else {
            ss << "    local input" << i + 1 << "_data = ffi.cast('" << lua_ffi_type_str << "*', input_vec" << i + 1 << "_ffi.data)\n";
        }
        ss << "    local input" << i + 1 << "_nullmask = ffi.cast('bool*', input_vec" << i + 1 << "_ffi.nullmask)\n";
    }

    ss << "    for i = 0, count - 1 do\n";
    std::string adapted_row_logic = lua_row_logic;
    if (output_logical_type.id() == LogicalTypeId::VARCHAR) {
        StringUtil::Replace(adapted_row_logic, "output_vector.data[i]",
                            "--[[JIT_TODO_STRING_OUTPUT]] error('Direct string output to FFIVector not implemented in test harness GenerateFullLuaJitFunctionExt')");
    } else {
        StringUtil::Replace(adapted_row_logic, "output_vector.data[i]", "output_data[i]");
    }
    StringUtil::Replace(adapted_row_logic, "output_vector.nullmask[i]", "output_nullmask[i]");

    for (idx_t i = 0; i < translator_ctx.GetNumInputs(); ++i) {
        std::string input_vec_table_access = StringUtil::Format("input_vectors[%d]", i + 1);
        std::string lua_input_var_prefix = StringUtil::Format("input%d", i + 1);
        if (translator_ctx.GetInputLogicalType(i).id() == LogicalTypeId::VARCHAR) {
            std::string original_col_ref_str = StringUtil::Format("ffi.string(%s.data[i].ptr, %s.data[i].len)",
                                                                input_vec_table_access, input_vec_table_access);
            std::string new_col_ref_str = StringUtil::Format("ffi.string(%s_data_ffi_str_array[i].ptr, %s_data_ffi_str_array[i].len)",
                                                             lua_input_var_prefix, lua_input_var_prefix);
            StringUtil::Replace(adapted_row_logic, original_col_ref_str, new_col_ref_str);
        } else {
            StringUtil::Replace(adapted_row_logic, input_vec_table_access + ".data[i]", lua_input_var_prefix + "_data[i]");
        }
        StringUtil::Replace(adapted_row_logic, input_vec_table_access + ".nullmask[i]", lua_input_var_prefix + "_nullmask[i]");
    }
    ss << "        " << adapted_row_logic << "\n";
    ss << "    end\n";
    ss << "end\n";
    return ss.str();
}


TEST_CASE("JIT ExpressionExecutor with BoundExpressions (Numeric, Flat Vectors)", "[luajit][executor][bound]") {
    using namespace duckdb;
    DuckDB db(nullptr);
    Connection con(db);
    ClientContext &context = *con.context;
    ExpressionExecutor executor(context);

    idx_t data_size = 5;
    DataChunk input_chunk;
    std::vector<LogicalType> input_types = {LogicalType::INTEGER, LogicalType::INTEGER};
    SetupDataChunk(input_chunk, context, input_types, data_size);

    auto col1_ptr_input_chunk = FlatVector::GetData<int32_t>(input_chunk.data[0]);
    auto col2_ptr_input_chunk = FlatVector::GetData<int32_t>(input_chunk.data[1]);
    for(idx_t i=0; i<data_size; ++i) {
        col1_ptr_input_chunk[i] = i + 1;
        col2_ptr_input_chunk[i] = (i + 1) * 10;
        if (i == 2) FlatVector::SetNull(input_chunk.data[1], i, true);
    }
    input_chunk.Verify();
    executor.SetChunk(&input_chunk);

    Vector output_vector(LogicalType::INTEGER);
    output_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::Validity(output_vector).EnsureWritable();


    auto bound_col0 = CreateBoundReference(0, LogicalType::INTEGER);
    auto bound_col1 = CreateBoundReference(1, LogicalType::INTEGER);
    auto bound_add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD,
                                                  std::move(bound_col0), std::move(bound_col1),
                                                  LogicalType::INTEGER);

    executor.AddExpression(*bound_add_expr);
    REQUIRE(executor.GetStates().size() == 1);
    ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();

    // Simulate what ExpressionExecutor::Execute would do for JIT compilation (first time)
    // This part is now internal to ExpressionExecutor::Execute
    // We call ExecuteExpression, which calls Execute, which should trigger JIT.
    executor.ExecuteExpression(0, output_vector);

    REQUIRE(expr_state->attempted_jit_compilation == true);
    REQUIRE(expr_state->jit_compilation_succeeded == true);
    REQUIRE(expr_state->jitted_lua_function_name.empty() == false);

    // Verify results from the (simulated or actual internal) JIT call
    auto out_data_ptr = reinterpret_cast<int32_t*>(FlatVector::GetData(output_vector));
    for(idx_t i=0; i<data_size; ++i) {
        bool col1_is_null = FlatVector::IsNull(input_chunk.data[0],i);
        bool col2_is_null = FlatVector::IsNull(input_chunk.data[1],i);
        bool expected_null = col1_is_null || col2_is_null;
        REQUIRE(FlatVector::IsNull(output_vector, i) == expected_null);
        if (!expected_null) {
            REQUIRE(out_data_ptr[i] == FlatVector::GetData<int32_t>(input_chunk.data[0])[i] + FlatVector::GetData<int32_t>(input_chunk.data[1])[i]);
        }
    }
}

TEST_CASE("JIT ExpressionExecutor with Constant Vector", "[luajit][executor][bound][constant]") {
    using namespace duckdb;
    DuckDB db(nullptr); Connection con(db); ClientContext &context = *con.context;
    ExpressionExecutor executor(context);

    idx_t data_size = 5;
    DataChunk input_chunk;
    std::vector<LogicalType> chunk_input_types = {LogicalType::INTEGER};
    SetupDataChunk(input_chunk, context, chunk_input_types, data_size);

    auto col0_ptr_chunk = FlatVector::GetData<int32_t>(input_chunk.data[0]);
    for(idx_t i=0; i<data_size; ++i) col0_ptr_chunk[i] = i + 1;
    input_chunk.Verify();
    executor.SetChunk(&input_chunk);

    Vector output_vector(LogicalType::INTEGER);
    output_vector.SetVectorType(VectorType::FLAT_VECTOR);
    FlatVector::Validity(output_vector).EnsureWritable();

    std::vector<LogicalType> expr_input_types = {LogicalType::INTEGER};
    // LuaTranslatorContext translator_ctx(expr_input_types); // Context created inside Execute

    auto bound_col0 = CreateBoundReference(0, LogicalType::INTEGER);
    auto bound_const_10 = CreateBoundConstant(Value::INTEGER(10));
    auto bound_add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD,
                                                  std::move(bound_col0), std::move(bound_const_10),
                                                  LogicalType::INTEGER);
    executor.AddExpression(*bound_add_expr);
    ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();

    executor.ExecuteExpression(0, output_vector); // Triggers JIT path

    REQUIRE(expr_state->attempted_jit_compilation == true);
    REQUIRE(expr_state->jit_compilation_succeeded == true);
    REQUIRE(expr_state->jitted_lua_function_name.empty() == false);

    auto out_data_ptr = reinterpret_cast<int32_t*>(FlatVector::GetData(output_vector));
    for(idx_t i=0; i<data_size; ++i) {
        REQUIRE(FlatVector::IsNull(output_vector, i) == false);
        REQUIRE(out_data_ptr[i] == FlatVector::GetData<int32_t>(input_chunk.data[0])[i] + 10);
    }
}

TEST_CASE("JIT ExpressionExecutor Caching Logic", "[luajit][executor][bound][cache]") {
    using namespace duckdb;
    DuckDB db(nullptr); Connection con(db); ClientContext &context = *con.context;
    ExpressionExecutor executor(context);

    idx_t data_size = 5;

    auto bound_col0_ex1 = CreateBoundReference(0, LogicalType::INTEGER);
    auto bound_const_100 = CreateBoundConstant(Value::INTEGER(100));
    auto bound_add_expr1 = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD,
                                                  std::move(bound_col0_ex1), std::move(bound_const_100),
                                                  LogicalType::INTEGER);
    executor.AddExpression(*bound_add_expr1);
    ExpressionState* expr_state1 = executor.GetStates()[0]->root_state.get();

    auto bound_col1_ex2 = CreateBoundReference(1, LogicalType::INTEGER);
    auto bound_const_2 = CreateBoundConstant(Value::INTEGER(2));
    auto bound_mul_expr2 = CreateBoundBinaryOperator(ExpressionType::OPERATOR_MULTIPLY,
                                                   std::move(bound_col1_ex2), std::move(bound_const_2),
                                                   LogicalType::INTEGER);
    executor.AddExpression(*bound_mul_expr2);
    ExpressionState* expr_state2 = executor.GetStates()[1]->root_state.get();

    DataChunk input_chunk;
    std::vector<LogicalType> chunk_input_types = {LogicalType::INTEGER, LogicalType::INTEGER};
    SetupDataChunk(input_chunk, context, chunk_input_types, data_size);
    auto col0_ptr_chunk = FlatVector::GetData<int32_t>(input_chunk.data[0]);
    auto col1_ptr_chunk = FlatVector::GetData<int32_t>(input_chunk.data[1]);
    for(idx_t i=0; i<data_size; ++i) {
        col0_ptr_chunk[i] = i + 1;
        col1_ptr_chunk[i] = (i + 1) * 10;
    }
    input_chunk.Verify();
    executor.SetChunk(&input_chunk);

    Vector output_vector1(LogicalType::INTEGER);
    Vector output_vector2(LogicalType::INTEGER);
    output_vector1.SetVectorType(VectorType::FLAT_VECTOR); FlatVector::Validity(output_vector1).EnsureWritable();
    output_vector2.SetVectorType(VectorType::FLAT_VECTOR); FlatVector::Validity(output_vector2).EnsureWritable();

    INFO("First execution of Expression 1 (col0 + 100)");
    executor.ExecuteExpression(0, output_vector1);

    REQUIRE(expr_state1->attempted_jit_compilation == true);
    REQUIRE(expr_state1->jit_compilation_succeeded == true);
    REQUIRE(expr_state1->jitted_lua_function_name.empty() == false);
    std::string func_name1_call1 = expr_state1->jitted_lua_function_name;

    auto out_data_ptr1_c1 = reinterpret_cast<int32_t*>(FlatVector::GetData(output_vector1));
    for(idx_t i=0; i<data_size; ++i) {
        REQUIRE(FlatVector::IsNull(output_vector1,i) == false);
        REQUIRE(out_data_ptr1_c1[i] == col0_ptr_chunk[i] + 100);
    }

    INFO("Second execution of Expression 1 (col0 + 100) - should use cache");
    // For this test, we rely on ExecuteExpression to use the cached path.
    // We can't easily check *if* CompileStringAndSetGlobal was called again without instrumentation.
    // But we check if the function name is the same and results are correct.
    executor.ExecuteExpression(0, output_vector1);
    REQUIRE(expr_state1->jitted_lua_function_name == func_name1_call1);
    auto out_data_ptr1_c2 = reinterpret_cast<int32_t*>(FlatVector::GetData(output_vector1));
     for(idx_t i=0; i<data_size; ++i) {
        REQUIRE(FlatVector::IsNull(output_vector1,i) == false);
        REQUIRE(out_data_ptr1_c2[i] == col0_ptr_chunk[i] + 100);
    }

    INFO("First execution of Expression 2 (col1 * 2)");
    executor.ExecuteExpression(1, output_vector2);

    REQUIRE(expr_state2->attempted_jit_compilation == true);
    REQUIRE(expr_state2->jit_compilation_succeeded == true);
    REQUIRE(expr_state2->jitted_lua_function_name.empty() == false);
    std::string func_name2_call1 = expr_state2->jitted_lua_function_name;
    REQUIRE(func_name1_call1 != func_name2_call1);

    auto out_data_ptr2_c1 = reinterpret_cast<int32_t*>(FlatVector::GetData(output_vector2));
    for(idx_t i=0; i<data_size; ++i) {
        bool col1_is_null = FlatVector::IsNull(input_chunk.data[1],i);
        REQUIRE(FlatVector::IsNull(output_vector2,i) == col1_is_null);
        if(!col1_is_null) {
            REQUIRE(out_data_ptr2_c1[i] == col1_ptr_chunk[i] * 2);
        }
    }
}

TEST_CASE("JIT ExpressionExecutor with VARCHAR (Flat Vectors and Comparisons)", "[luajit][executor][bound][varchar]") {
    using namespace duckdb;
    DuckDB db(nullptr); Connection con(db); ClientContext &context = *con.context;
    ExpressionExecutor executor(context);

    idx_t data_size = 3;
    DataChunk input_chunk;
    std::vector<LogicalType> input_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
    SetupDataChunk(input_chunk, context, input_types, data_size);

    std::string s_arr_1[] = {"hello", "world", "hello"};
    std::string s_arr_2[] = {"ciao", "world", "duck"}; // Last one will be NULL in chunk

    for(idx_t i=0; i<data_size; ++i) {
        FlatVector::GetData<string_t>(input_chunk.data[0])[i] = StringVector::AddString(input_chunk.data[0], s_arr_1[i]);
        if (i == 2) { // Make last element of second vector NULL
             FlatVector::SetNull(input_chunk.data[1], i, true);
        } else {
             FlatVector::GetData<string_t>(input_chunk.data[1])[i] = StringVector::AddString(input_chunk.data[1], s_arr_2[i]);
        }
    }
    input_chunk.Verify();
    executor.SetChunk(&input_chunk);

    Vector output_vector(LogicalType::BOOLEAN);
    output_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::Validity(output_vector).EnsureWritable();

    auto bound_col0 = CreateBoundReference(0, LogicalType::VARCHAR);
    auto bound_col1 = CreateBoundReference(1, LogicalType::VARCHAR);
    auto bound_eq_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_EQUAL,
                                                  std::move(bound_col0), std::move(bound_col1),
                                                  LogicalType::BOOLEAN);
    executor.AddExpression(*bound_eq_expr);
    ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();

    // This call should trigger JIT compilation
    executor.ExecuteExpression(0, output_vector);
    REQUIRE(expr_state->attempted_jit_compilation == true);
    REQUIRE(expr_state->jit_compilation_succeeded == true);
    REQUIRE(expr_state->jitted_lua_function_name.empty() == false);

    auto out_data_ptr = reinterpret_cast<int8_t*>(FlatVector::GetData(output_vector));
    // Expected: "hello"=="ciao" (F=0), "world"=="world" (T=1), "hello"==NULL (NULL)
    REQUIRE(FlatVector::IsNull(output_vector,0) == false); REQUIRE(out_data_ptr[0] == 0);
    REQUIRE(FlatVector::IsNull(output_vector,1) == false); REQUIRE(out_data_ptr[1] == 1);
    REQUIRE(FlatVector::IsNull(output_vector,2) == true);
}

TEST_CASE("JIT ExpressionExecutor Error Handling and Fallback", "[luajit][executor][bound][error]") {
    using namespace duckdb;
    DuckDB db(nullptr); Connection con(db); ClientContext &context = *con.context;
    ExpressionExecutor executor(context);
    idx_t data_size = 3;

    DataChunk input_chunk;
    std::vector<LogicalType> input_types = {LogicalType::INTEGER};
    SetupDataChunk(input_chunk, context, input_types, data_size);
    auto col0_ptr = FlatVector::GetData<int32_t>(input_chunk.data[0]);
    for(idx_t i=0; i<data_size; ++i) col0_ptr[i] = i; // 0, 1, 2
    executor.SetChunk(&input_chunk);
    Vector output_vector(LogicalType::INTEGER); // For 1/0, result is INTEGER due to division

    SECTION("Lua Runtime Error (Division by Zero) and Fallback") {
        auto bound_col0 = CreateBoundReference(0, LogicalType::INTEGER);
        // Expression: 1 / col0. Will cause division by zero for the first element.
        auto bound_div_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_DIVIDE,
                                                       CreateBoundConstant(Value::INTEGER(1)),
                                                       std::move(bound_col0),
                                                       LogicalType::INTEGER);
        executor.AddExpression(*bound_div_expr);
        ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();

        // First execution: JIT path, should throw RuntimeException due to Lua error
        REQUIRE_THROWS_AS(executor.ExecuteExpression(0, output_vector), duckdb::RuntimeException);
        REQUIRE(expr_state->attempted_jit_compilation == true);
        REQUIRE(expr_state->jit_compilation_succeeded == false); // Marked as failed

        // Second execution: Should use C++ fallback path
        output_vector.SetVectorType(VectorType::FLAT_VECTOR); // Reset output vector
        FlatVector::Validity(output_vector).EnsureWritable();
        FlatVector::SetAllValid(output_vector, data_size);

        executor.ExecuteExpression(0, output_vector); // Should now use C++ path
        REQUIRE(expr_state->jit_compilation_succeeded == false); // Still marked as failed

        // Verify C++ path results (DuckDB's division by zero is NULL)
        REQUIRE(FlatVector::IsNull(output_vector, 0) == true); // 1/0 is NULL
        REQUIRE(FlatVector::IsNull(output_vector, 1) == false);
        REQUIRE(FlatVector::GetData<int32_t>(output_vector)[1] == 1); // 1/1
        REQUIRE(FlatVector::IsNull(output_vector, 2) == false);
        REQUIRE(FlatVector::GetData<int32_t>(output_vector)[2] == 0); // 1/2 (integer division)
    }

    // Note: Testing Lua compilation errors is harder as LuaTranslator should always produce valid syntax.
    // A syntax error would mean a bug in LuaTranslator itself.
    // If one were to manually inject bad Lua code into the CompileStringAndSetGlobal call,
    // that would test the compilation error path of LuaJITStateWrapper and ExpressionExecutor.
}
```
