#include "duckdb/common/luajit_wrapper.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector_operations.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/execution/expression_executor.hpp" // Main include for this refactor
#include "duckdb/common/types/string_t.hpp"


#include <vector>
#include <string>
#include <chrono>
#include <iostream>
#include <iomanip> // For std::fixed and std::setprecision

// For lua_State and Lua API, typically included via luajit_wrapper.hpp's inclusions
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

// --- DuckDB Setup Helpers ---
static duckdb::DuckDB g_db(nullptr); // Global DB instance for context

static duckdb::unique_ptr<duckdb::ClientContext> CreateBenchContext() {
    duckdb::Connection con(g_db);
    return std::move(con.context); // Return unique_ptr from shared_ptr
}

// --- BoundExpression Creation Helpers (from jit_expression_executor_test.cpp) ---
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

// Global flag to control JIT path in ExpressionExecutor for benchmarking
// This is a HACK for PoC. A proper config/pragma should be used.
bool g_force_jit_path = true;
// We need to modify ExpressionExecutor::ShouldJIT to check this flag.
// This modification is outside this file, assumed to be done conceptually.
// Example modification in ExpressionExecutor::ShouldJIT:
// if (g_force_jit_path_for_benchmark_is_set && !g_force_jit_path_value) return false; // If forcing CPP
// if (g_force_jit_path_for_benchmark_is_set && g_force_jit_path_value) return true; // If forcing JIT

// --- Data Generation & Chunk Setup ---
template<typename T>
void FillNumericVector(duckdb::Vector& vec, size_t count, bool with_nulls) {
    vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto data_ptr = duckdb::FlatVector::GetData<T>(vec);
    auto& null_mask = duckdb::FlatVector::Validity(vec);
    for (size_t i = 0; i < count; ++i) {
        data_ptr[i] = static_cast<T>(i % 1000);
        if (with_nulls && (i % 10 == 0)) {
            null_mask.SetInvalid(i);
        } else {
            null_mask.SetValid(i);
        }
    }
}

void FillStringVector(duckdb::Vector& vec, size_t count, bool with_nulls) {
    vec.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
    auto data_ptr = duckdb::FlatVector::GetData<duckdb::string_t>(vec);
    auto& null_mask = duckdb::FlatVector::Validity(vec);
    for (size_t i = 0; i < count; ++i) {
        std::string s = "s_" + std::to_string(i % 500);
        if (with_nulls && (i % 10 == 0)) {
            null_mask.SetInvalid(i);
        } else {
            null_mask.SetValid(i);
            data_ptr[i] = duckdb::StringVector::AddString(vec, s);
        }
    }
}

// --- Benchmark Scenarios ---
struct BenchmarkResult {
    std::string scenario_name;
    std::string data_type_str;
    size_t data_size;
    bool has_nulls;
    double cpp_baseline_time_ms = 0;
    double jit_first_run_total_time_ms = 0; // Trans+Compile+Exec
    double jit_cached_run_exec_time_ms = 0;
    double translation_time_ms = 0; // Measured during first run
    double compilation_time_ms = 0; // Measured during first run
};
std::vector<BenchmarkResult> benchmark_results_list;


void RunScenario(duckdb::ClientContext& context,
                 const std::string& scenario_name_prefix,
                 duckdb::unique_ptr<duckdb::Expression> expr_to_test, // Pass by value for unique_ptr
                 const std::vector<duckdb::LogicalType>& input_col_types,
                 size_t data_size, int iterations, bool with_nulls) {
    using namespace duckdb;

    BenchmarkResult current_result;
    current_result.scenario_name = scenario_name_prefix + "_" + expr_to_test->GetName();
    current_result.data_type_str = input_col_types[0].ToString(); // Assuming first col type is representative
    current_result.data_size = data_size;
    current_result.has_nulls = with_nulls;

    DataChunk input_chunk;
    input_chunk.Initialize(Allocator::Get(context), input_col_types);
    for(size_t c=0; c<input_col_types.size(); ++c) {
        if (input_col_types[c].id() == LogicalTypeId::INTEGER) FillNumericVector<int32_t>(input_chunk.data[c], data_size, with_nulls);
        else if (input_col_types[c].id() == LogicalTypeId::VARCHAR) FillStringVector(input_chunk.data[c], data_size, with_nulls);
        // Add other types if needed for more scenarios
    }
    input_chunk.SetCardinality(data_size);
    input_chunk.Verify();

    Vector output_vector(expr_to_test->return_type);

    // --- C++ Baseline Path ---
    // To force C++ path, we'd conceptually set g_force_jit_path = false;
    // For PoC, assume ExpressionExecutor has a way to disable JIT for baseline,
    // or we use a fresh executor with JIT disabled in its context.
    // For now, we assume the global flag works for this conceptual benchmark.
    ExpressionExecutor baseline_executor(context); // Fresh executor for baseline
    baseline_executor.AddExpression(*expr_to_test);
    baseline_executor.SetChunk(&input_chunk);
    // TODO: Modify ShouldJIT to check a global/context flag to force C++ path
    // For now, we time it, but it might take JIT path if not careful.
    // This requires `ExpressionExecutor::ShouldJIT` to be modifiable for benchmarks.
    // Let's assume `ShouldJIT` is currently hardcoded to return false for this run.
    // (This part is tricky without modifying ExpressionExecutor for benchmark mode)
    // As a workaround, if we can't modify ShouldJIT, we comment out baseline_executor.
    // For now, this baseline is "conceptual" if it can't be forced.
    // A true baseline would be `executor.ExecuteStandard(...)` if that were public,
    // or a manually coded C++ loop as in the previous benchmark version.
    // Let's revert to manual C++ loop for baseline for simplicity of this step as ShouldJIT not easily controlled.
    if (scenario_name_prefix == "A_AddInt" && input_col_types.size() == 2) {
        std::vector<int32_t> baseline_out_data(data_size);
        std::vector<bool> baseline_out_nulls(data_size);
        auto col1_data = FlatVector::GetData<int32_t>(input_chunk.data[0]);
        auto col2_data = FlatVector::GetData<int32_t>(input_chunk.data[1]);
        START_TIMER();
        for(int iter=0; iter < iterations; ++iter) {
            for(size_t i=0; i<data_size; ++i) {
                bool n1 = FlatVector::IsNull(input_chunk.data[0], i);
                bool n2 = FlatVector::IsNull(input_chunk.data[1], i);
                if (n1 || n2) baseline_out_nulls[i] = true;
                else { baseline_out_nulls[i] = false; baseline_out_data[i] = col1_data[i] + col2_data[i]; }
            }
        }
        END_TIMER();
        current_result.cpp_baseline_time_ms = GET_TIMER_MS() / iterations;
    } else {
        current_result.cpp_baseline_time_ms = -1.0; // Mark as not run for other scenarios for now
    }


    // --- LuaJIT Path ---
    ExpressionExecutor jit_executor(context); // Fresh executor for JIT
    jit_executor.AddExpression(*expr_to_test);
    jit_executor.SetChunk(&input_chunk);
    ExpressionState* jit_expr_state = jit_executor.GetStates()[0]->root_state.get();

    // First Run (Compile + Exec)
    // We need to capture translation/compilation times which happen inside ExecuteExpression first time.
    // This requires either instrumenting ExpressionExecutor or pre-compiling here.
    // For this PoC, let's pre-compile to measure times separately.

    LuaTranslatorContext translator_ctx(input_col_types);
    START_TIMER();
    std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*expr_to_test, translator_ctx);
    END_TIMER();
    current_result.translation_time_ms = GET_TIMER_MS();

    std::string func_name = "bench_func_" + expr_to_test->GetName() + "_" + std::to_string(jitted_function_counter.fetch_add(1));
    std::string full_lua_script = ConstructFullLuaFunctionScript( // Use the existing helper for this
        func_name, lua_row_logic, translator_ctx, expr_to_test->return_type);

    std::string compile_error;
    START_TIMER();
    bool compiled = jit_executor.luajit_wrapper_.CompileStringAndSetGlobal(full_lua_script, func_name, compile_error);
    END_TIMER();
    current_result.compilation_time_ms = GET_TIMER_MS();

    if (!compiled) {
        std::cerr << "JIT COMPILE ERROR for " << func_name << ": " << compile_error << std::endl;
        benchmark_results_list.push_back(current_result);
        return;
    }
    jit_expr_state->jitted_lua_function_name = func_name;
    jit_expr_state->jit_compilation_succeeded = true;
    jit_expr_state->attempted_jit_compilation = true; // Mark so ExecuteExpression uses this.

    // Time first execution (which uses the now-compiled function)
    // This is a bit redundant if CompileStringAndSetGlobal implies it's ready.
    // The real test is how ExpressionExecutor's Execute call performs.
    // For this benchmark, we will call the JITed function directly like in executor tests
    // to isolate JIT execution time after it's marked as compiled.

    std::vector<std::vector<char>> temp_buffers_owner;
    ffi::FFIVector ffi_out_vec;
    std::vector<ffi::FFIVector> ffi_input_vecs_storage(input_col_types.size());
    std::vector<ffi::FFIVector*> ffi_input_vecs_ptrs(input_col_types.size());

    CreateFFIVectorFromDuckDBVector(output_vector, data_size, ffi_out_vec, temp_buffers_owner);
    for(size_t i=0; i<input_col_types.size(); ++i) {
        CreateFFIVectorFromDuckDBVector(input_chunk.data[i], data_size, ffi_input_vecs_storage[i], temp_buffers_owner);
        ffi_input_vecs_ptrs[i] = &ffi_input_vecs_storage[i];
    }

    lua_State* L = jit_executor.luajit_wrapper_.GetState();
    std::string pcall_error;

    START_TIMER();
    for (int iter = 0; iter < iterations; ++iter) {
         // output_vector needs reset if data is written in place by Lua
        output_vector.SetVectorType(VectorType::FLAT_VECTOR);
        FlatVector::Validity(output_vector).EnsureWritable();
        FlatVector::SetAllValid(output_vector, data_size); // Or reset data too

        if (!jit_executor.luajit_wrapper_.PCallGlobal(func_name, ffi_input_vecs_ptrs, &ffi_out_vec, data_size, pcall_error)) {
             std::cerr << "JIT EXEC ERROR for " << func_name << " (iter " << iter << "): " << pcall_error << std::endl;
            benchmark_results_list.push_back(current_result);
            return;
        }
    }
    END_TIMER();
    current_result.jit_first_run_total_time_ms = current_result.translation_time_ms + current_result.compilation_time_ms + (GET_TIMER_MS() / iterations) ;
    current_result.jit_cached_run_exec_time_ms = GET_TIMER_MS() / iterations; // This is effectively the cached run

    benchmark_results_list.push_back(current_result);
}


void RunAllBenchmarks() {
    using namespace duckdb;
    auto context = CreateBenchContext();
    REQUIRE(context != nullptr); // Need a context for allocator in DataChunk

    std::vector<size_t> data_sizes = {1000, 100000}; //, 1000000};
    int iterations = 100;

    for (size_t ds : data_sizes) {
        iterations = (ds >= 100000 ? 10 : 100); // Fewer iterations for large data

        // Scenario A: col_int1 + col_int2
        auto чисел_col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto чисел_col1 = CreateBoundReference(1, LogicalType::INTEGER);
        auto add_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_ADD, std::move(чисел_col0), std::move(чисел_col1), LogicalType::INTEGER);
        RunScenario(*context, "A_AddInt", std::move(add_expr), {LogicalType::INTEGER, LogicalType::INTEGER}, ds, iterations, false);
        RunScenario(*context, "A_AddInt_Nulls", std::move(add_expr), {LogicalType::INTEGER, LogicalType::INTEGER}, ds, iterations, true);


        // Scenario B: col_int1 > col_int2
        auto gt_col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto gt_col1 = CreateBoundReference(1, LogicalType::INTEGER);
        auto gt_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_GREATERTHAN, std::move(gt_col0), std::move(gt_col1), LogicalType::BOOLEAN);
        RunScenario(*context, "B_GtInt", std::move(gt_expr), {LogicalType::INTEGER, LogicalType::INTEGER}, ds, iterations, false);

        // Scenario C: col_int1 * 10
        auto mul_col0 = CreateBoundReference(0, LogicalType::INTEGER);
        auto mul_const10 = CreateBoundConstant(Value::INTEGER(10));
        auto mul_expr = CreateBoundBinaryOperator(ExpressionType::OPERATOR_MULTIPLY, std::move(mul_col0), std::move(mul_const10), LogicalType::INTEGER);
        RunScenario(*context, "C_MulConstInt", std::move(mul_expr), {LogicalType::INTEGER}, ds, iterations, false);

        // Scenario D: String Comparison col_str1 == col_str2
        // Output is boolean. String inputs.
        auto str_eq_col0 = CreateBoundReference(0, LogicalType::VARCHAR);
        auto str_eq_col1 = CreateBoundReference(1, LogicalType::VARCHAR);
        auto str_eq_expr = CreateBoundBinaryOperator(ExpressionType::COMPARE_EQUAL, std::move(str_eq_col0), std::move(str_eq_col1), LogicalType::BOOLEAN);
        RunScenario(*context, "D_StrEq", std::move(str_eq_expr), {LogicalType::VARCHAR, LogicalType::VARCHAR}, ds, iterations, false);
        RunScenario(*context, "D_StrEq_Nulls", std::move(str_eq_expr), {LogicalType::VARCHAR, LogicalType::VARCHAR}, ds, iterations, true);

    }

    // Print results
    std::cout << "\n--- LuaJIT Expression Benchmark Results ---\n";
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Scenario,DataType,DataSize,HasNulls,CppBaseline_ms,JIT_FirstRunTotal_ms,JIT_CachedExec_ms,Translate_ms,Compile_ms\n";
    for (const auto& r : benchmark_results_list) {
        std::cout << r.scenario_name << ","
                  << r.data_type_str << ","
                  << r.data_size << ","
                  << (r.has_nulls ? "Y" : "N") << ","
                  << r.cpp_baseline_time_ms << ","
                  << r.jit_first_run_total_time_ms << ","
                  << r.jit_cached_run_exec_time_ms << ","
                  << r.translation_time_ms << ","
                  << r.compilation_time_ms << "\n";
    }
}

int main() {
    std::cout << "Running LuaJIT Expression Benchmarks (using ExpressionExecutor conceptually)..." << std::endl;
    RunAllBenchmarks();
    std::cout << "Benchmarks finished." << std::endl;
    return 0;
}
```
