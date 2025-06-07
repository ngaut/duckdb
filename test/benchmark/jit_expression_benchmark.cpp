#include "duckdb/common/luajit_wrapper.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp" // For DBConfig
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/string_t.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <vector>
#include <string>
#include <chrono>
#include <iostream>
#include <iomanip> // For std::fixed and std::setprecision
#include <random>  // For random data

// For lua_State and Lua API
extern "C" {
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
}

// --- Timer Utility ---
using namespace std::chrono;
static high_resolution_clock::time_point T_START;
static high_resolution_clock::time_point T_END;

#define START_TIMER() T_START = high_resolution_clock::now()
#define END_TIMER() T_END = high_resolution_clock::now()
#define GET_TIMER_MS() duration_cast<duration<double, std::milli>>(T_END - T_START).count()

// --- DuckDB Setup ---
static duckdb::DuckDB g_db(nullptr); // Global DB instance

// --- BoundExpression Creation Helpers ---
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
static duckdb::unique_ptr<duckdb::BoundCaseExpression> CreateBoundCase(
    duckdb::unique_ptr<duckdb::Expression> when_expr, duckdb::unique_ptr<duckdb::Expression> then_expr, duckdb::unique_ptr<duckdb::Expression> else_expr, duckdb::LogicalType return_type) {
    auto case_expr = duckdb::make_uniq<duckdb::BoundCaseExpression>(return_type);
    case_expr->case_checks.emplace_back(std::move(when_expr), std::move(then_expr));
    case_expr->else_expr = std::move(else_expr);
    return case_expr;
}

// --- Data Generation & Chunk Setup ---
template<typename T>
void FillNumericVector(duckdb::Vector& vec, size_t count, double null_pct) {
    vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto data_ptr = duckdb::FlatVector::GetData<T>(vec);
    auto& null_mask = duckdb::FlatVector::Validity(vec);
    std::mt19937 gen(0); // Fixed seed for reproducibility
    std::uniform_real_distribution<> dis(0.0, 1.0);
    for (size_t i = 0; i < count; ++i) {
        if (dis(gen) < null_pct) {
            null_mask.SetInvalid(i);
        } else {
            null_mask.SetValid(i);
            data_ptr[i] = static_cast<T>(i % 1000 - (i % 2000 == 0 ? 500 : 0) ); // some negatives
        }
    }
}
void FillStringVector(duckdb::Vector& vec, size_t count, double null_pct, int str_len = 10) {
    vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto data_ptr = duckdb::FlatVector::GetData<duckdb::string_t>(vec);
    auto& null_mask = duckdb::FlatVector::Validity(vec);
    std::mt19937 gen(0);
    std::uniform_real_distribution<> dis(0.0, 1.0);
    std::string base_str = "s_";
    for(int k=0; k < str_len - base_str.length(); ++k) base_str += "x";

    for (size_t i = 0; i < count; ++i) {
        if (dis(gen) < null_pct) {
            null_mask.SetInvalid(i);
        } else {
            null_mask.SetValid(i);
            // Predictable but varied strings
            std::string current_str = base_str + std::to_string(i % 500);
            data_ptr[i] = duckdb::StringVector::AddString(vec, current_str);
        }
    }
}
static void SetupInputChunk(duckdb::DataChunk& chunk, duckdb::ClientContext& context,
                            const std::vector<duckdb::LogicalType>& types, duckdb::idx_t count, double null_pct) {
    chunk.Initialize(duckdb::Allocator::Get(context), types);
    for(size_t c=0; c<types.size(); ++c) {
        if (types[c].id() == duckdb::LogicalTypeId::INTEGER) FillNumericVector<int32_t>(chunk.data[c], count, null_pct);
        else if (types[c].id() == duckdb::LogicalTypeId::BIGINT) FillNumericVector<int64_t>(chunk.data[c], count, null_pct);
        else if (types[c].id() == duckdb::LogicalTypeId::DOUBLE) FillNumericVector<double>(chunk.data[c], count, null_pct);
        else if (types[c].id() == duckdb::LogicalTypeId::VARCHAR) FillStringVector(chunk.data[c], count, null_pct);
        else if (types[c].id() == duckdb::LogicalTypeId::DATE) FillNumericVector<duckdb::date_t>(chunk.data[c], count, null_pct);
        else if (types[c].id() == duckdb::LogicalTypeId::TIMESTAMP) FillNumericVector<duckdb::timestamp_t>(chunk.data[c], count, null_pct);
    }
    chunk.SetCardinality(count);
    chunk.Verify();
}

// --- Benchmark Scenarios ---
struct BenchmarkResult {
    std::string scenario_name;
    std::string data_type_str; // Type of primary input column
    size_t data_size;
    double null_pct;
    double cpp_baseline_ms = 0;
    double jit_first_run_ms = 0;
    double jit_cached_run_ms = 0;
    // Translation and Compilation are part of First Run, but can be estimated if needed
    // For this PoC, we focus on total first run vs cached run vs C++
};
std::vector<BenchmarkResult> benchmark_results_list;

// Global static counter for unique JIT function names in benchmarks
// Reset this if running multiple benchmark main() calls in one C++ execution.
// static std::atomic<idx_t> benchmark_jit_func_idx{0};


void RunScenario(duckdb::ClientContext& ctx_cpp, duckdb::ClientContext& ctx_jit, // Separate contexts
                 const std::string& scenario_name_prefix,
                 duckdb::unique_ptr<duckdb::Expression> expr_to_test,
                 const std::vector<duckdb::LogicalType>& input_col_types,
                 size_t data_size, int iterations, double null_pct) {
    using namespace duckdb;

    BenchmarkResult current_result;
    current_result.scenario_name = scenario_name_prefix + "_" + expr_to_test->GetName();
    current_result.data_type_str = input_col_types.empty() ? "N/A" : input_col_types[0].ToString();
    current_result.data_size = data_size;
    current_result.null_pct = null_pct;

    DataChunk input_chunk;
    SetupInputChunk(input_chunk, ctx_cpp, input_col_types, data_size, null_pct);
    Vector output_vector_cpp(expr_to_test->return_type);
    Vector output_vector_jit(expr_to_test->return_type);

    // --- C++ Baseline Path ---
    ctx_cpp.config.enable_luajit_jit = false; // Force C++ path
    ExpressionExecutor cpp_executor(ctx_cpp);
    cpp_executor.AddExpression(*expr_to_test);
    cpp_executor.SetChunk(&input_chunk);

    START_TIMER();
    for (int iter = 0; iter < iterations; ++iter) {
        cpp_executor.ExecuteExpression(0, output_vector_cpp);
    }
    END_TIMER();
    current_result.cpp_baseline_ms = GET_TIMER_MS() / iterations;

    // --- LuaJIT Path ---
    ctx_jit.config.enable_luajit_jit = true;
    ctx_jit.config.luajit_jit_trigger_count = 0; // JIT on first call
    ctx_jit.config.luajit_jit_complexity_threshold = 0; // JIT any complexity

    ExpressionExecutor jit_executor(ctx_jit);
    // Must add a *copy* of the expression, as state is tied to expression object
    jit_executor.AddExpression(*expr_to_test->Copy());
    jit_executor.SetChunk(&input_chunk);
    ExpressionState* jit_expr_state = jit_executor.GetStates()[0]->root_state.get();

    // First Run (Compile + Exec)
    START_TIMER();
    jit_executor.ExecuteExpression(0, output_vector_jit);
    END_TIMER();
    current_result.jit_first_run_ms = GET_TIMER_MS();
    REQUIRE(jit_expr_state->attempted_jit_compilation == true);
    // We assume for benchmark that if attempted, it succeeded, or it would have thrown and test failed
    REQUIRE(jit_expr_state->jit_compilation_succeeded == true);

    // Verify JIT output against C++ output (only once for correctness check)
    // output_vector_cpp.Verify(data_size);
    // output_vector_jit.Verify(data_size);
    // for(idx_t i=0; i<data_size; ++i) {
    //     if (FlatVector::IsNull(output_vector_cpp, i) != FlatVector::IsNull(output_vector_jit, i)) {
    //         FAIL("Null mismatch at " << i << " for " << current_result.scenario_name);
    //     }
    //     if (!FlatVector::IsNull(output_vector_cpp, i)) {
    //         // TODO: Add Value::operator== or type-specific comparison
    //     }
    // }


    // Subsequent Runs (Cached Execution)
    START_TIMER();
    for (int iter = 0; iter < iterations; ++iter) {
        jit_executor.ExecuteExpression(0, output_vector_jit);
    }
    END_TIMER();
    current_result.jit_cached_run_ms = GET_TIMER_MS() / iterations;

    // Translation and Compilation times are now internal to ExpressionExecutor's first call.
    // We can't easily get them separately here without instrumenting ExpressionExecutor.
    // For this PoC, we report FirstRunTotal and CachedRun.
    current_result.translation_time_ms = -1; // Mark as part of FirstRun
    current_result.compilation_time_ms = -1; // Mark as part of FirstRun

    benchmark_results_list.push_back(current_result);
}

void RunAllBenchmarks() {
    using namespace duckdb;
    auto ctx_cpp = CreateBenchContext();
    auto ctx_jit = CreateBenchContext();
    REQUIRE(ctx_cpp != nullptr);
    REQUIRE(ctx_jit != nullptr);

    std::vector<size_t> data_sizes = {10000, 1000000};
    std::vector<double> null_percentages = {0.0, 0.5};
    int iterations = 10;

    for (size_t ds : data_sizes) {
        for (double null_pct : null_percentages) {
            iterations = (ds >= 1000000 ? 2 : 10);
            std::string null_tag = (null_pct > 0.0) ? "_Nulls" : "";

            // Numeric: col_int1 + col_int2
            RunScenario(*ctx_cpp, *ctx_jit, "A_AddInt" + null_tag,
                        CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD,
                            CreateBoundReference(0, LogicalType::INTEGER),
                            CreateBoundReference(1, LogicalType::INTEGER), LogicalType::INTEGER),
                        {LogicalType::INTEGER, LogicalType::INTEGER}, ds, iterations, null_pct);

            // Numeric: col_int1 * 10
            RunScenario(*ctx_cpp, *ctx_jit, "C_MulConstInt" + null_tag,
                        CreateBoundBinaryOperator(ExpressionType::OPERATOR_MULTIPLY,
                            CreateBoundReference(0, LogicalType::INTEGER),
                            CreateBoundConstant(Value::INTEGER(10)), LogicalType::INTEGER),
                        {LogicalType::INTEGER}, ds, iterations, null_pct);

            // String: col_str1 == col_str2
            RunScenario(*ctx_cpp, *ctx_jit, "D_StrEq" + null_tag,
                        CreateBoundBinaryOperator(ExpressionType::COMPARE_EQUAL,
                            CreateBoundReference(0, LogicalType::VARCHAR),
                            CreateBoundReference(1, LogicalType::VARCHAR), LogicalType::BOOLEAN),
                        {LogicalType::VARCHAR, LogicalType::VARCHAR}, ds, iterations, null_pct);

            // String: LOWER(col_str1)
            std::vector<unique_ptr<Expression>> lower_children;
            lower_children.push_back(CreateBoundReference(0, LogicalType::VARCHAR));
            RunScenario(*ctx_cpp, *ctx_jit, "E_LowerStr" + null_tag,
                        CreateBoundFunction("lower", std::move(lower_children), LogicalType::VARCHAR),
                        {LogicalType::VARCHAR}, ds, iterations, null_pct);

            // Logical: col_int1 IS NULL
            RunScenario(*ctx_cpp, *ctx_jit, "F_IsNotNullInt" + null_tag, // Note: IS NOT NULL for variety
                        CreateBoundUnaryOperator(ExpressionType::OPERATOR_IS_NOT_NULL,
                            CreateBoundReference(0, LogicalType::INTEGER), LogicalType::BOOLEAN),
                        {LogicalType::INTEGER}, ds, iterations, null_pct);

            // Conditional: CASE WHEN col_int1 > 10 THEN col_int2 ELSE col_int1 * 2 END
            // Output type of this CASE is INTEGER.
            auto case_expr = CreateBoundCase(
                CreateBoundBinaryOperator(ExpressionType::COMPARE_GREATERTHAN, CreateBoundReference(0, LogicalType::INTEGER), CreateBoundConstant(Value::INTEGER(10)), LogicalType::BOOLEAN),
                CreateBoundReference(1, LogicalType::INTEGER), // then col_int2
                CreateBoundBinaryOperator(ExpressionType::OPERATOR_MULTIPLY, CreateBoundReference(0, LogicalType::INTEGER), CreateBoundConstant(Value::INTEGER(2)), LogicalType::INTEGER), // else col_int1 * 2
                LogicalType::INTEGER);
            RunScenario(*ctx_cpp, *ctx_jit, "G_CaseInt" + null_tag,
                        std::move(case_expr),
                        {LogicalType::INTEGER, LogicalType::INTEGER}, ds, iterations, null_pct);
        }
    }

    std::cout << "\n--- LuaJIT Expression Benchmark Results (DuckDB ExpressionExecutor) ---\n";
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Scenario,DataType,DataSize,NullPct,CppBaseline_ms,JIT_FirstRun_ms,JIT_CachedExec_ms,TranslateOnce_ms,CompileOnce_ms\n";
    for (const auto& r : benchmark_results_list) {
        std::cout << r.scenario_name << ","
                  << r.data_type_str << ","
                  << r.data_size << ","
                  << r.null_pct << ","
                  << r.cpp_baseline_ms << ","
                  << r.jit_first_run_ms << ","
                  << r.jit_cached_run_ms << ","
                  << r.translation_time_ms << "," // Will be -1 for now
                  << r.compilation_time_ms << "\n"; // Will be -1 for now
    }
}

int main() {
    std::cout << "Running LuaJIT Expression Benchmarks with ExpressionExecutor..." << std::endl;
    RunAllBenchmarks();
    std::cout << "Benchmarks finished." << std::endl;
    return 0;
}

// Conceptual change for ExpressionExecutor::ShouldJIT for benchmarking:
// Add a global bool like `g_benchmark_force_cpp_path = false;`
// In ShouldJIT:
// if (g_benchmark_force_cpp_path) return false; // At the very top to force C++
// ... rest of ShouldJIT ...
// To set this from test:
// duckdb::ClientConfig::Get(context).options.enable_luajit_jit = false; // For baseline
// duckdb::ClientConfig::Get(context).options.enable_luajit_jit = true; // For JIT run
// duckdb::ClientConfig::Get(context).options.luajit_jit_trigger_count = 0;
// duckdb::ClientConfig::Get(context).options.luajit_jit_complexity_threshold = 0;
```
