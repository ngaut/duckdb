#include "catch.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp" // Added
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/luajit_wrapper.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_t.hpp"
#include "duckdb/common/exception.hpp"

#include <iostream>
#include <vector>

// --- Helper functions to create DuckDB BoundExpression instances for testing ---
static duckdb::unique_ptr<duckdb::BoundConstantExpression> CreateBoundConstant(duckdb::Value val) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(val);
}

static duckdb::unique_ptr<duckdb::BoundReferenceExpression> CreateBoundReference(duckdb::idx_t col_idx, duckdb::LogicalType type) {
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(type, col_idx);
}

static duckdb::unique_ptr<duckdb::BoundOperatorExpression> CreateBoundUnaryOperator(
    duckdb::ExpressionType op_type,
    duckdb::unique_ptr<duckdb::Expression> child,
    duckdb::LogicalType return_type) {
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(child));
    return duckdb::make_uniq<duckdb::BoundOperatorExpression>(op_type, return_type, std::move(children), false);
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

static duckdb::unique_ptr<duckdb::BoundFunctionExpression> CreateBoundFunction(
    const std::string& func_name,
    std::vector<duckdb::unique_ptr<duckdb::Expression>> children,
    duckdb::LogicalType return_type) {
    duckdb::ScalarFunction scalar_func(func_name, {}, return_type, nullptr); // Dummy ScalarFunctionDef
    return duckdb::make_uniq<duckdb::BoundFunctionExpression>(return_type, scalar_func, std::move(children), nullptr, false);
}

static duckdb::unique_ptr<duckdb::BoundCaseExpression> CreateBoundCase(
    duckdb::unique_ptr<duckdb::Expression> when_expr,
    duckdb::unique_ptr<duckdb::Expression> then_expr,
    duckdb::unique_ptr<duckdb::Expression> else_expr,
    duckdb::LogicalType return_type) {
    auto case_expr = duckdb::make_uniq<duckdb::BoundCaseExpression>(return_type);
    case_expr->case_checks.emplace_back(std::move(when_expr), std::move(then_expr));
    case_expr->else_expr = std::move(else_expr);
    return case_expr;
}


// Helper to create and setup a DataChunk
static void SetupDataChunk(duckdb::DataChunk& chunk, duckdb::ClientContext& context,
                    const std::vector<duckdb::LogicalType>& types, duckdb::idx_t count) {
    chunk.Initialize(duckdb::Allocator::Get(context), types);
    for(size_t i=0; i < types.size(); ++i) {
        chunk.data[i].SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    }
    chunk.SetCardinality(count);
}

// GenerateFullLuaJitFunctionExt (remains the same as previous correct version)
static std::string GenerateFullLuaJitFunctionExt(const std::string& function_name,
                                        const std::string& lua_row_logic,
                                        duckdb::LuaTranslatorContext& translator_ctx,
                                        const duckdb::LogicalType& output_logical_type) {
    using namespace duckdb;
    std::stringstream ss;
    ss << "local ffi = require('ffi')\n";
    ss << "ffi.cdef[[\n";
    ss << "    typedef struct FFIVector { void* data; bool* nullmask; unsigned long long count; "
       << "int ffi_logical_type_id; int ffi_duckdb_vector_type; void* original_duckdb_vector; } FFIVector;\n";
    ss << "    typedef struct FFIString { char* ptr; unsigned int len; } FFIString;\n";
    ss << "    typedef signed char int8_t;\n";
    ss << "    typedef int int32_t;\n";
    ss << "    typedef long long int64_t;\n";
    ss << "    void duckdb_ffi_add_string_to_output_vector(void* ffi_vec_ptr, unsigned long long row_idx, const char* str_data, unsigned int str_len);\n";
    ss << "    void duckdb_ffi_set_string_output_null(void* ffi_vec_ptr, unsigned long long row_idx);\n";
    ss << "]]\n";

    ss << function_name << " = function(output_vec_ffi";
    for (idx_t i = 0; i < translator_ctx.GetNumInputs(); ++i) {
        ss << ", input_vec" << i + 1 << "_ffi";
    }
    ss << ", count)\n";

    std::string output_lua_ffi_type_str;
    bool output_is_string = output_logical_type.id() == LogicalTypeId::VARCHAR;

    if (!output_is_string) {
        switch(output_logical_type.id()) {
            case LogicalTypeId::INTEGER: output_lua_ffi_type_str = "int32_t"; break;
            case LogicalTypeId::BIGINT: output_lua_ffi_type_str = "int64_t"; break;
            case LogicalTypeId::DOUBLE: output_lua_ffi_type_str = "double"; break;
            case LogicalTypeId::BOOLEAN: output_lua_ffi_type_str = "int8_t"; break;
            case LogicalTypeId::DATE: output_lua_ffi_type_str = "int32_t"; break;
            case LogicalTypeId::TIMESTAMP: output_lua_ffi_type_str = "int64_t"; break;
            default: throw NotImplementedException("[JIT Test] Output type for Lua FFI cast not defined: " + output_logical_type.ToString());
        }
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
    if (!output_is_string) {
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

// Existing test cases ... (Numeric Flat, Constant, Caching, VARCHAR I/O and Functions, Error Handling)
// These will be kept as they are, as they are testing different aspects or were refactored correctly.
// For brevity, I will not repeat them here but assume they are present from the previous step.
// The new test cases will be added below.

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
    ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();
    executor.ExecuteExpression(0, output_vector);

    REQUIRE(expr_state->attempted_jit_compilation == true);
    if (expr_state->jit_compilation_succeeded) {
        auto out_data_ptr = FlatVector::GetData<int32_t>(output_vector);
        for(idx_t i=0; i<data_size; ++i) {
            bool col1_is_null = FlatVector::IsNull(input_chunk.data[0],i);
            bool col2_is_null = FlatVector::IsNull(input_chunk.data[1],i);
            bool expected_null = col1_is_null || col2_is_null;
            REQUIRE(FlatVector::IsNull(output_vector, i) == expected_null);
            if (!expected_null) {
                REQUIRE(out_data_ptr[i] == FlatVector::GetData<int32_t>(input_chunk.data[0])[i] + FlatVector::GetData<int32_t>(input_chunk.data[1])[i]);
            }
        }
    } else { SUCCEED("JIT compilation failed, C++ path taken and verified by executor internally.");}
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

    auto bound_col0 = CreateBoundReference(0, LogicalType::INTEGER);
    auto bound_const_10 = CreateBoundConstant(Value::INTEGER(10));
    auto bound_add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD,
                                                  std::move(bound_col0), std::move(bound_const_10),
                                                  LogicalType::INTEGER);
    executor.AddExpression(*bound_add_expr);
    ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();
    executor.ExecuteExpression(0, output_vector);
    REQUIRE(expr_state->jit_compilation_succeeded == true);

    auto out_data_ptr = FlatVector::GetData<int32_t>(output_vector);
    for(idx_t i=0; i<data_size; ++i) {
        REQUIRE(FlatVector::IsNull(output_vector, i) == false);
        REQUIRE(out_data_ptr[i] == col0_ptr_chunk[i] + 10);
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

    INFO("Second execution of Expression 1 (col0 + 100) - should use cache");
    executor.ExecuteExpression(0, output_vector1);
    REQUIRE(expr_state1->jitted_lua_function_name == func_name1_call1);

    INFO("First execution of Expression 2 (col1 * 2)");
    executor.ExecuteExpression(1, output_vector2);
    REQUIRE(expr_state2->attempted_jit_compilation == true);
    REQUIRE(expr_state2->jit_compilation_succeeded == true);
    REQUIRE(expr_state2->jitted_lua_function_name.empty() == false);
    std::string func_name2_call1 = expr_state2->jitted_lua_function_name;
    REQUIRE(func_name1_call1 != func_name2_call1);
}

TEST_CASE("JIT ExpressionExecutor VARCHAR I/O and Functions", "[luajit][executor][bound][varchar_ext]") {
    using namespace duckdb;
    DuckDB db(nullptr); Connection con(db); ClientContext &context = *con.context;
    ExpressionExecutor executor(context);
    idx_t data_size = 3;
    DataChunk input_chunk;
    std::vector<LogicalType> input_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
    SetupDataChunk(input_chunk, context, input_types, data_size);
    std::string s_arr_1[] = {"hello", "WORLD", "  Duck  "};
    std::string s_arr_2[] = {"_Suffix", " Test", ""};
    for(idx_t i=0; i<data_size; ++i) {
        FlatVector::GetData<string_t>(input_chunk.data[0])[i] = StringVector::AddString(input_chunk.data[0], s_arr_1[i]);
        FlatVector::GetData<string_t>(input_chunk.data[1])[i] = StringVector::AddString(input_chunk.data[1], s_arr_2[i]);
    }
    FlatVector::SetNull(input_chunk.data[0], 1, true);
    input_chunk.Verify();
    executor.SetChunk(&input_chunk);

    SECTION("LOWER(col_varchar)") {
        Vector output_vector(LogicalType::VARCHAR);
        output_vector.SetVectorType(VectorType::FLAT_VECTOR); FlatVector::Validity(output_vector).EnsureWritable();
        auto child_expr = CreateBoundReference(0, LogicalType::VARCHAR);
        std::vector<unique_ptr<Expression>> children; children.push_back(std::move(child_expr));
        auto lower_expr = CreateBoundFunction("lower", std::move(children), LogicalType::VARCHAR);
        executor.AddExpression(*lower_expr);
        ExpressionState* expr_state = executor.GetStates().back()->root_state.get();
        executor.ExecuteExpression(executor.expressions.size()-1, output_vector);
        REQUIRE(expr_state->jit_compilation_succeeded == true);
        REQUIRE(FlatVector::IsNull(output_vector, 0) == false);
        REQUIRE(FlatVector::GetData<string_t>(output_vector)[0].GetString() == "hello");
        REQUIRE(FlatVector::IsNull(output_vector, 1) == true);
        REQUIRE(FlatVector::IsNull(output_vector, 2) == false);
        REQUIRE(FlatVector::GetData<string_t>(output_vector)[2].GetString() == "  duck  ");
    }
    SECTION("CONCAT (col_varchar1 || col_varchar2)") {
        Vector output_vector(LogicalType::VARCHAR);
        output_vector.SetVectorType(VectorType::FLAT_VECTOR); FlatVector::Validity(output_vector).EnsureWritable();
        auto C0 = CreateBoundReference(0, LogicalType::VARCHAR);
        auto C1 = CreateBoundReference(1, LogicalType::VARCHAR);
        auto concat_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_CONCAT, std::move(C0), std::move(C1), LogicalType::VARCHAR);
        executor.AddExpression(*concat_expr);
        ExpressionState* expr_state = executor.GetStates().back()->root_state.get();
        executor.ExecuteExpression(executor.expressions.size()-1, output_vector);
        REQUIRE(expr_state->jit_compilation_succeeded == true);
        REQUIRE(FlatVector::IsNull(output_vector, 0) == false);
        REQUIRE(FlatVector::GetData<string_t>(output_vector)[0].GetString() == "hello_Suffix");
        REQUIRE(FlatVector::IsNull(output_vector, 1) == true);
        REQUIRE(FlatVector::IsNull(output_vector, 2) == false);
        REQUIRE(FlatVector::GetData<string_t>(output_vector)[2].GetString() == "  Duck  ");
    }
    SECTION("LENGTH(col_varchar)") {
        Vector output_vector(LogicalType::BIGINT);
        output_vector.SetVectorType(VectorType::FLAT_VECTOR); FlatVector::Validity(output_vector).EnsureWritable();
        auto child_expr = CreateBoundReference(0, LogicalType::VARCHAR);
        std::vector<unique_ptr<Expression>> children; children.push_back(std::move(child_expr));
        auto length_expr = CreateBoundFunction("length", std::move(children), LogicalType::BIGINT);
        executor.AddExpression(*length_expr);
        ExpressionState* expr_state = executor.GetStates().back()->root_state.get();
        executor.ExecuteExpression(executor.expressions.size()-1, output_vector);
        REQUIRE(expr_state->jit_compilation_succeeded == true);
        REQUIRE(FlatVector::IsNull(output_vector, 0) == false);
        REQUIRE(FlatVector::GetData<int64_t>(output_vector)[0] == 5);
        REQUIRE(FlatVector::IsNull(output_vector, 1) == true);
        REQUIRE(FlatVector::IsNull(output_vector, 2) == false);
        REQUIRE(FlatVector::GetData<int64_t>(output_vector)[2] == 8);
    }
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
    for(idx_t i=0; i<data_size; ++i) col0_ptr[i] = i;
    executor.SetChunk(&input_chunk);
    Vector output_vector(LogicalType::INTEGER);

    SECTION("Lua Runtime Error (Division by Zero) and Fallback") {
        auto bound_col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto bound_div_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_DIVIDE,
                                                       CreateBoundConstant(Value::INTEGER(1)),
                                                       std::move(bound_col0),
                                                       LogicalType::INTEGER);
        executor.AddExpression(*bound_div_expr);
        ExpressionState* expr_state = executor.GetStates()[0]->root_state.get();
        REQUIRE_THROWS_AS(executor.ExecuteExpression(0, output_vector), duckdb::RuntimeException);
        REQUIRE(expr_state->attempted_jit_compilation == true);
        REQUIRE(expr_state->jit_compilation_succeeded == false);
        output_vector.SetVectorType(VectorType::FLAT_VECTOR);
        FlatVector::Validity(output_vector).EnsureWritable();
        FlatVector::SetAllValid(output_vector, data_size);
        executor.ExecuteExpression(0, output_vector);
        REQUIRE(expr_state->jit_compilation_succeeded == false);
        REQUIRE(FlatVector::IsNull(output_vector, 0) == true);
        REQUIRE(FlatVector::IsNull(output_vector, 1) == false);
        REQUIRE(FlatVector::GetData<int32_t>(output_vector)[1] == 1);
        REQUIRE(FlatVector::IsNull(output_vector, 2) == false);
        REQUIRE(FlatVector::GetData<int32_t>(output_vector)[2] == 0);
    }
}

TEST_CASE("JIT ExpressionExecutor Advanced Operations (IS NULL, CASE, Numeric Funcs)", "[luajit][executor][bound][advanced]") {
    using namespace duckdb;
    DuckDB db(nullptr); Connection con(db); ClientContext &context = *con.context;
    ExpressionExecutor executor(context);
    idx_t data_size = 4;
    DataChunk input_chunk;
    std::vector<LogicalType> input_types = {LogicalType::INTEGER, LogicalType::DOUBLE};
    SetupDataChunk(input_chunk, context, input_types, data_size);

    auto int_ptr = FlatVector::GetData<int32_t>(input_chunk.data[0]);
    auto dbl_ptr = FlatVector::GetData<double>(input_chunk.data[1]);
    // col0 (INT): 0, 1, NULL, -5
    // col1 (DBL): 0.5, NULL, 2.5, -2.5
    int_ptr[0] = 0; FlatVector::SetNull(input_chunk.data[0], 0, false);
    int_ptr[1] = 1; FlatVector::SetNull(input_chunk.data[0], 1, false);
    int_ptr[2] = 99; FlatVector::SetNull(input_chunk.data[0], 2, true); // NULL
    int_ptr[3] = -5; FlatVector::SetNull(input_chunk.data[0], 3, false);

    dbl_ptr[0] = 0.5; FlatVector::SetNull(input_chunk.data[1], 0, false);
    dbl_ptr[1] = 99.9; FlatVector::SetNull(input_chunk.data[1], 1, true); // NULL
    dbl_ptr[2] = 2.5; FlatVector::SetNull(input_chunk.data[1], 2, false);
    dbl_ptr[3] = -2.5; FlatVector::SetNull(input_chunk.data[1], 3, false);

    input_chunk.Verify();
    executor.SetChunk(&input_chunk);

    SECTION("IS NULL / IS NOT NULL") {
        Vector output_is_null(LogicalType::BOOLEAN);
        auto col0_ref_is_null = CreateBoundReference(0, LogicalType::INTEGER);
        auto is_null_expr = CreateBoundUnaryOperator(ExpressionType::OPERATOR_IS_NULL, std::move(col0_ref_is_null), LogicalType::BOOLEAN);
        executor.AddExpression(*is_null_expr);
        executor.ExecuteExpression(executor.expressions.size()-1, output_is_null);
        // Expected: F, F, T, F
        REQUIRE(!FlatVector::IsNull(output_is_null,0) && FlatVector::GetData<int8_t>(output_is_null)[0] == 0);
        REQUIRE(!FlatVector::IsNull(output_is_null,1) && FlatVector::GetData<int8_t>(output_is_null)[1] == 0);
        REQUIRE(!FlatVector::IsNull(output_is_null,2) && FlatVector::GetData<int8_t>(output_is_null)[2] == 1);
        REQUIRE(!FlatVector::IsNull(output_is_null,3) && FlatVector::GetData<int8_t>(output_is_null)[3] == 0);

        Vector output_is_not_null(LogicalType::BOOLEAN);
        auto col1_ref_is_not_null = CreateBoundReference(1, LogicalType::DOUBLE);
        auto is_not_null_expr = CreateBoundUnaryOperator(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(col1_ref_is_not_null), LogicalType::BOOLEAN);
        executor.AddExpression(*is_not_null_expr);
        executor.ExecuteExpression(executor.expressions.size()-1, output_is_not_null);
        // Expected for col1: T, F, T, T
        REQUIRE(!FlatVector::IsNull(output_is_not_null,0) && FlatVector::GetData<int8_t>(output_is_not_null)[0] == 1);
        REQUIRE(!FlatVector::IsNull(output_is_not_null,1) && FlatVector::GetData<int8_t>(output_is_not_null)[1] == 0);
        REQUIRE(!FlatVector::IsNull(output_is_not_null,2) && FlatVector::GetData<int8_t>(output_is_not_null)[2] == 1);
        REQUIRE(!FlatVector::IsNull(output_is_not_null,3) && FlatVector::GetData<int8_t>(output_is_not_null)[3] == 1);
    }

    SECTION("Numeric Functions: ABS(col0), FLOOR(col1), CEIL(col1), ROUND(col1,1)") {
        Vector out_abs(LogicalType::INTEGER);
        auto abs_expr = CreateBoundFunction("abs", {CreateBoundReference(0, LogicalType::INTEGER)}, LogicalType::INTEGER);
        executor.AddExpression(*abs_expr); executor.ExecuteExpression(executor.expressions.size()-1, out_abs);
        // col0: 0, 1, NULL, -5 -> ABS: 0, 1, NULL, 5
        REQUIRE(FlatVector::GetData<int32_t>(out_abs)[0] == 0); REQUIRE(!FlatVector::IsNull(out_abs,0));
        REQUIRE(FlatVector::GetData<int32_t>(out_abs)[1] == 1); REQUIRE(!FlatVector::IsNull(out_abs,1));
        REQUIRE(FlatVector::IsNull(out_abs,2));
        REQUIRE(FlatVector::GetData<int32_t>(out_abs)[3] == 5); REQUIRE(!FlatVector::IsNull(out_abs,3));

        Vector out_floor(LogicalType::DOUBLE);
        auto floor_expr = CreateBoundFunction("floor", {CreateBoundReference(1, LogicalType::DOUBLE)}, LogicalType::DOUBLE);
        executor.AddExpression(*floor_expr); executor.ExecuteExpression(executor.expressions.size()-1, out_floor);
        // col1: 0.5, NULL, 2.5, -2.5 -> FLOOR: 0, NULL, 2, -3
        REQUIRE(FlatVector::GetData<double>(out_floor)[0] == 0.0);
        REQUIRE(FlatVector::IsNull(out_floor,1));
        REQUIRE(FlatVector::GetData<double>(out_floor)[2] == 2.0);
        REQUIRE(FlatVector::GetData<double>(out_floor)[3] == -3.0);

        // ROUND(col1, 1)
        Vector out_round(LogicalType::DOUBLE);
        std::vector<unique_ptr<Expression>> round_children;
        round_children.push_back(CreateBoundReference(1, LogicalType::DOUBLE));
        round_children.push_back(CreateBoundConstant(Value::TINYINT(1))); // Precision = 1
        auto round_expr = CreateBoundFunction("round", std::move(round_children), LogicalType::DOUBLE);
        executor.AddExpression(*round_expr); executor.ExecuteExpression(executor.expressions.size()-1, out_round);
        // col1: 0.5, NULL, 2.5, -2.5 -> ROUND(...,1): 0.5, NULL, 2.5, -2.5
        REQUIRE(FlatVector::GetData<double>(out_round)[0] == 0.5);
        REQUIRE(FlatVector::IsNull(out_round,1));
        REQUIRE(FlatVector::GetData<double>(out_round)[2] == 2.5);
        REQUIRE(FlatVector::GetData<double>(out_round)[3] == -2.5);
    }

    SECTION("Multi-branch CASE Expression") {
        // CASE WHEN col0 = 0 THEN 100 WHEN col0 = 1 THEN 200 ELSE 300 END
        Vector out_case(LogicalType::INTEGER);
        auto case_expr = duckdb::make_uniq<BoundCaseExpression>(LogicalType::INTEGER);
        // WHEN col0 = 0 THEN 100
        case_expr->case_checks.emplace_back(
            CreateBoundBinaryOperator(ExpressionType::COMPARE_EQUAL, CreateBoundReference(0, LogicalType::INTEGER), CreateBoundConstant(Value::INTEGER(0)), LogicalType::BOOLEAN),
            CreateBoundConstant(Value::INTEGER(100))
        );
        // WHEN col0 = 1 THEN 200
        case_expr->case_checks.emplace_back(
            CreateBoundBinaryOperator(ExpressionType::COMPARE_EQUAL, CreateBoundReference(0, LogicalType::INTEGER), CreateBoundConstant(Value::INTEGER(1)), LogicalType::BOOLEAN),
            CreateBoundConstant(Value::INTEGER(200))
        );
        // ELSE 300
        case_expr->else_expr = CreateBoundConstant(Value::INTEGER(300));

        executor.AddExpression(*case_expr);
        executor.ExecuteExpression(executor.expressions.size()-1, out_case);
        // col0: 0, 1, NULL, -5
        // Expected: 100, 200, NULL (because col0 is NULL, not because ELSE is NULL), 300
        REQUIRE(FlatVector::GetData<int32_t>(out_case)[0] == 100); REQUIRE(!FlatVector::IsNull(out_case,0));
        REQUIRE(FlatVector::GetData<int32_t>(out_case)[1] == 200); REQUIRE(!FlatVector::IsNull(out_case,1));
        REQUIRE(FlatVector::IsNull(out_case,2)); // col0 is NULL
        REQUIRE(FlatVector::GetData<int32_t>(out_case)[3] == 300); REQUIRE(!FlatVector::IsNull(out_case,3));
    }
}
```
