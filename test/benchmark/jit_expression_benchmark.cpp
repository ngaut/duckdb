#include "duckdb/common/luajit_wrapper.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/planner/luajit_expression_nodes.hpp"
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/main/client_context.hpp" // For allocator, if needed, though benchmarks might use std::vector
#include "duckdb/common/types/value.hpp" // For StringUtil for Replace

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
#define GET_TIMER_US() duration_cast<duration<double, std::micro>>(T_END - T_START).count()


// --- Data Generation ---
template<typename T>
void GenerateNumericData(std::vector<T>& data, std::vector<bool>& nulls, size_t count, bool allow_nulls = true) {
    data.resize(count);
    nulls.resize(count);
    for (size_t i = 0; i < count; ++i) {
        data[i] = static_cast<T>(i % 1000); // Simple predictable data
        nulls[i] = allow_nulls && (i % 10 == 0); // ~10% nulls
    }
}

// For strings, FFIString.ptr needs to point to stable memory.
// std::vector<std::string> provides this if it outlives the FFIString structs.
void GenerateStringData(std::vector<std::string>& string_store,
                        std::vector<duckdb::ffi::FFIString>& ffi_strings,
                        std::vector<bool>& nulls, size_t count, bool allow_nulls = true) {
    string_store.resize(count);
    ffi_strings.resize(count);
    nulls.resize(count);
    for (size_t i = 0; i < count; ++i) {
        string_store[i] = "s_" + std::to_string(i % 500);
        if (allow_nulls && (i % 10 == 0)) {
            nulls[i] = true;
            ffi_strings[i].ptr = nullptr; // Or some convention
            ffi_strings[i].len = 0;
        } else {
            nulls[i] = false;
            ffi_strings[i].ptr = const_cast<char*>(string_store[i].c_str());
            ffi_strings[i].len = static_cast<uint32_t>(string_store[i].length());
        }
    }
}

// --- Lua Function Generation (similar to jit_expression_executor_test) ---
// output_type_lua: e.g. "int", "double", "FFIString" (if writing strings back)
// input_type_lua: vector of e.g. "int", "double", "FFIString"
std::string GenerateFullLuaBenchmarkFunction(const std::string& lua_row_logic,
                                           int num_input_exprs,
                                           const std::string& output_type_lua,
                                           const std::vector<std::string>& input_type_lua,
                                           bool output_is_ffi_string = false,
                                           const std::vector<bool>& input_is_ffi_string = {}) {
    std::stringstream ss;
    ss << "local ffi = require('ffi')\n";
    ss << "ffi.cdef[[\n";
    ss << "    typedef struct FFIVector { void* data; bool* nullmask; unsigned long long count; } FFIVector;\n";
    ss << "    typedef struct FFIString { char* ptr; unsigned int len; } FFIString;\n";
    ss << "]]\n";

    ss << "function benchmark_jitted_expression(output_vec_ffi";
    for (int i = 0; i < num_input_exprs; ++i) {
        ss << ", input_vec" << i + 1 << "_ffi";
    }
    ss << ", count)\n";

    // Cast output vector
    if (output_is_ffi_string) {
        ss << "    local output_data_ffi_str_array = ffi.cast('FFIString*', output_vec_ffi.data)\n";
    } else {
        ss << "    local output_data = ffi.cast('" << output_type_lua << "*', output_vec_ffi.data)\n";
    }
    ss << "    local output_nullmask = ffi.cast('bool*', output_vec_ffi.nullmask)\n";

    // Cast input vectors
    for (int i = 0; i < num_input_exprs; ++i) {
        if (i < input_is_ffi_string.size() && input_is_ffi_string[i]) {
             ss << "    local input" << i + 1 << "_data_ffi_str_array = ffi.cast('FFIString*', input_vec" << i + 1 << "_ffi.data)\n";
        } else {
            ss << "    local input" << i + 1 << "_data = ffi.cast('" << input_type_lua[i] << "*', input_vec" << i + 1 << "_ffi.data)\n";
        }
        ss << "    local input" << i + 1 << "_nullmask = ffi.cast('bool*', input_vec" << i + 1 << "_ffi.nullmask)\n";
    }

    ss << "    for i = 0, count - 1 do\n";
    std::string adapted_row_logic = lua_row_logic;
    // Adapt output assignment
    if (output_is_ffi_string) {
        // This case is complex: Lua needs to write ptr & len to output_data_ffi_str_array[i]
        // The generated `lua_row_logic` for string results (e.g. from CONCAT) produces a Lua string.
        // Writing this back to FFIString requires memory allocation or using pre-allocated buffers.
        // For benchmark simplicity, string output scenarios might focus on operations that
        // return boolean/numeric, or the benchmark measures up to Lua string creation only.
        // For now, let's assume lua_row_logic for strings will be handled by simpler output like bool/int.
        // If a string *result* is needed, the C++ side would need to provide output FFIString buffers.
        // The current translator doesn't generate code to populate output_data_ffi_str_array[i].ptr/len.
        StringUtil::Replace(adapted_row_logic, "output_vector.data[i]", "output_data[i]"); // Placeholder
    } else {
        StringUtil::Replace(adapted_row_logic, "output_vector.data[i]", "output_data[i]");
    }
    StringUtil::Replace(adapted_row_logic, "output_vector.nullmask[i]", "output_nullmask[i]");

    // Adapt input access
    for (int i = 0; i < num_input_exprs; ++i) {
        std::string input_vec_table_access = duckdb::StringUtil::Format("input_vectors[%d]", i + 1);
        std::string lua_input_var_prefix = duckdb::StringUtil::Format("input%d", i + 1);

        if (i < input_is_ffi_string.size() && input_is_ffi_string[i]) {
            // If input is FFIString, lua_row_logic should use ffi.string(ptr, len)
            // The translator should produce this. Here we adapt the placeholder.
            // e.g. input_vectors[1].data[i] -> ffi.string(input1_data_ffi_str_array[i].ptr, input1_data_ffi_str_array[i].len)
             std::string ffi_string_access = duckdb::StringUtil::Format("ffi.string(%s_data_ffi_str_array[i].ptr, %s_data_ffi_str_array[i].len)", lua_input_var_prefix, lua_input_var_prefix);
            duckdb::StringUtil::Replace(adapted_row_logic, input_vec_table_access + ".data[i]", ffi_string_access);
        } else {
            duckdb::StringUtil::Replace(adapted_row_logic, input_vec_table_access + ".data[i]", lua_input_var_prefix + "_data[i]");
        }
        duckdb::StringUtil::Replace(adapted_row_logic, input_vec_table_access + ".nullmask[i]", lua_input_var_prefix + "_nullmask[i]");
    }
    ss << "        " << adapted_row_logic << "\n";
    ss << "    end\n";
    ss << "end\n";
    return ss.str();
}


// --- Benchmark Scenarios ---
struct BenchmarkResult {
    std::string scenario_name;
    size_t data_size;
    double translation_time_ms = 0;
    double compilation_time_ms = 0;
    double jit_execution_time_ms = 0;
    double cpp_baseline_time_ms = 0;
    bool jit_success = true;
};

std::vector<BenchmarkResult> benchmark_results;

// Scenario A: col0 + col1 (int)
void Benchmark_A_AddInt(size_t data_size, int iterations) {
    using namespace duckdb;
    BenchmarkResult result{"A_AddInt", data_size};
    std::vector<int> col0_data, col1_data, out_data_jit(data_size), out_data_cpp(data_size);
    std::vector<bool> col0_nulls, col1_nulls, out_nulls_jit(data_size), out_nulls_cpp(data_size);

    GenerateNumericData(col0_data, col0_nulls, data_size);
    GenerateNumericData(col1_data, col1_nulls, data_size); // Can vary data for col1

    // C++ Baseline
    START_TIMER();
    for (int iter = 0; iter < iterations; ++iter) {
        for (size_t i = 0; i < data_size; ++i) {
            if (col0_nulls[i] || col1_nulls[i]) {
                out_nulls_cpp[i] = true;
            } else {
                out_nulls_cpp[i] = false;
                out_data_cpp[i] = col0_data[i] + col1_data[i];
            }
        }
    }
    END_TIMER();
    result.cpp_baseline_time_ms = GET_TIMER_MS() / iterations;

    // LuaJIT Path
    LuaJITStateWrapper lua_wrapper;
    auto expr_c0 = MakeLuaColumnRef(0);
    auto expr_c1 = MakeLuaColumnRef(1);
    auto add_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::ADD, std::move(expr_c0), std::move(expr_c1));
    LuaTranslatorContext translator_ctx(2);

    START_TIMER();
    std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(*add_expr, translator_ctx);
    END_TIMER();
    result.translation_time_ms = GET_TIMER_MS();

    std::string full_lua_script = GenerateFullLuaBenchmarkFunction(lua_row_logic, 2, "int", {"int", "int"});

    START_TIMER();
    bool compiled = lua_wrapper.ExecuteString(full_lua_script);
    END_TIMER();
    if (!compiled) { result.jit_success = false; benchmark_results.push_back(result); return; }
    result.compilation_time_ms = GET_TIMER_MS();

    ffi::FFIVector ffi_out{out_data_jit.data(), out_nulls_jit.data(), data_size};
    ffi::FFIVector ffi_in1{col0_data.data(), col0_nulls.data(), data_size};
    ffi::FFIVector ffi_in2{col1_data.data(), col1_nulls.data(), data_size};

    lua_State* L = lua_wrapper.GetState();
    lua_getglobal(L, "benchmark_jitted_expression");

    START_TIMER();
    for (int iter = 0; iter < iterations; ++iter) {
        lua_pushvalue(L, -1); // Duplicate function
        lua_pushlightuserdata(L, &ffi_out);
        lua_pushlightuserdata(L, &ffi_in1);
        lua_pushlightuserdata(L, &ffi_in2);
        lua_pushinteger(L, data_size);
        if (lua_pcall(L, 4, 0, 0) != LUA_OK) {
            result.jit_success = false;
            const char* err = lua_tostring(L, -1);
            std::cerr << "Lua error in A_AddInt: " << (err ? err : "unknown") << std::endl;
            lua_pop(L, 1); // pop error
            break;
        }
    }
    END_TIMER();
    lua_pop(L, 1); // Pop function
    if(result.jit_success) result.jit_execution_time_ms = GET_TIMER_MS() / iterations;
    benchmark_results.push_back(result);

    // Optional: Verify JIT output against C++ output
}


// Scenario D: String CONCAT (col_str || 'suffix') -> output boolean (e.g. length > X)
// This avoids complex string output FFI for benchmark, focuses on input string FFI and Lua processing.
void Benchmark_D_StringConcatAndLength(size_t data_size, int iterations) {
    using namespace duckdb;
    BenchmarkResult result{"D_StringConcatAndLength", data_size};

    std::vector<std::string> col0_string_store;
    std::vector<ffi::FFIString> col0_ffi_strings;
    std::vector<bool> col0_nulls;
    GenerateStringData(col0_string_store, col0_ffi_strings, col0_nulls, data_size);

    std::vector<int> out_data_jit(data_size); // Output is int (0 or 1)
    std::vector<bool> out_nulls_jit(data_size);
    std::vector<int> out_data_cpp(data_size);
    std::vector<bool> out_nulls_cpp(data_size);

    const std::string suffix = "_suffix";
    const int length_threshold = 10;

    // C++ Baseline
    START_TIMER();
    for (int iter = 0; iter < iterations; ++iter) {
        for (size_t i = 0; i < data_size; ++i) {
            if (col0_nulls[i]) {
                out_nulls_cpp[i] = true;
            } else {
                out_nulls_cpp[i] = false;
                std::string temp_str = col0_string_store[i] + suffix;
                out_data_cpp[i] = (temp_str.length() > length_threshold) ? 1 : 0;
            }
        }
    }
    END_TIMER();
    result.cpp_baseline_time_ms = GET_TIMER_MS() / iterations;

    // LuaJIT Path
    LuaJITStateWrapper lua_wrapper;
    // Expression: (length(col0 .. "suffix")) > 10
    // For PoC, assume col0 is string. LuaTranslator needs to handle this type.
    // The GenerateValue for ColumnReference should conceptually produce:
    // ffi.string(input_vectors[1].data[i].ptr, input_vectors[1].data[i].len)
    // This is a leap for the translator as written, but we test the concept.
    auto expr_c0 = MakeLuaColumnRef(0); // This will be our string col
    auto const_suffix = MakeLuaConstant(suffix);
    auto concat_expr = MakeLuaBinaryOp(LuaJITBinaryOperatorType::CONCAT, std::move(expr_c0), std::move(const_suffix));

    // This would need a LENGTH function, then a GT comparison.
    // Let's simplify for the benchmark: the Lua code will do this directly.
    // The translator would generate `(string.len(col0_str .. suffix_str) > 10)`.
    // The `lua_row_logic` will be manually crafted for this benchmark scenario a bit more.

    LuaTranslatorContext translator_ctx(1); // 1 input string vector
    // This translation is too simple for the complex operation. We override lua_row_logic.
    // std::string lua_row_logic_translated = LuaTranslator::TranslateExpressionToLuaRowLogic(*concat_expr, translator_ctx);

    // Manually craft the row logic for: (string.len(col0_str .. "suffix") > 10)
    // This assumes col0_str is already a Lua string (via ffi.string by GenerateFullLuaBenchmarkFunction)
    std::string lua_row_logic = duckdb::StringUtil::Format(
        R"(
    if input1_nullmask[i] then
        output_nullmask[i] = true
    else
        output_nullmask[i] = false
        local col0_str = ffi.string(input1_data_ffi_str_array[i].ptr, input1_data_ffi_str_array[i].len)
        local temp_str = col0_str .. "%s"
        if string.len(temp_str) > %d then
            output_data[i] = 1
        else
            output_data[i] = 0
        end
    end)", suffix, length_threshold);

    START_TIMER();
    // Translation time is minimal here as we manually crafted logic
    END_TIMER();
    result.translation_time_ms = GET_TIMER_MS(); // Effectively zero for this manual craft

    std::string full_lua_script = GenerateFullLuaBenchmarkFunction(lua_row_logic, 1, "int", {"FFIString"}, false, {true});

    START_TIMER();
    bool compiled = lua_wrapper.ExecuteString(full_lua_script);
    END_TIMER();
    if (!compiled) { result.jit_success = false; benchmark_results.push_back(result); return; }
    result.compilation_time_ms = GET_TIMER_MS();

    ffi::FFIVector ffi_out{out_data_jit.data(), out_nulls_jit.data(), data_size};
    // Input FFIVector's data points to an array of FFIString structs
    ffi::FFIVector ffi_in1{col0_ffi_strings.data(), col0_nulls.data(), data_size};

    lua_State* L = lua_wrapper.GetState();
    lua_getglobal(L, "benchmark_jitted_expression");

    START_TIMER();
    for (int iter = 0; iter < iterations; ++iter) {
        lua_pushvalue(L, -1); // Duplicate function
        lua_pushlightuserdata(L, &ffi_out);
        lua_pushlightuserdata(L, &ffi_in1);
        lua_pushinteger(L, data_size);
        if (lua_pcall(L, 3, 0, 0) != LUA_OK) { // 3 args: out, in1, count
            result.jit_success = false;
             const char* err = lua_tostring(L, -1);
            std::cerr << "Lua error in D_StringConcat: " << (err ? err : "unknown") << std::endl;
            lua_pop(L, 1);
            break;
        }
    }
    END_TIMER();
    lua_pop(L, 1); // Pop function
    if(result.jit_success) result.jit_execution_time_ms = GET_TIMER_MS() / iterations;
    benchmark_results.push_back(result);
}


void RunBenchmarks() {
    std::vector<size_t> data_sizes = {1000, 100000}; //, 1000000};
    int iterations = 100; // Iterations for execution timing, reduce for very large data

    for (size_t ds : data_sizes) {
        Benchmark_A_AddInt(ds, (ds > 100000 ? iterations/10 : iterations) );
        // TODO: Add calls for Scenario B (Comparison) and C (Constant)
        // These would be structured similarly to Benchmark_A_AddInt.
        Benchmark_D_StringConcatAndLength(ds, (ds > 100000 ? iterations/10 : iterations) );
    }

    // Print results
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Scenario,DataSize,TranslationTime_ms,CompilationTime_ms,JITExecutionTime_ms,CppBaselineTime_ms,JIT_Success\n";
    for (const auto& r : benchmark_results) {
        std::cout << r.scenario_name << ","
                  << r.data_size << ","
                  << r.translation_time_ms << ","
                  << r.compilation_time_ms << ","
                  << r.jit_execution_time_ms << ","
                  << r.cpp_baseline_time_ms << ","
                  << (r.jit_success ? "Yes" : "No") << "\n";
    }
}

int main() {
    // This benchmark executable would be standalone or integrated into DuckDB's test runner.
    // For PoC, it's a simple main.
    // DuckDB db(nullptr); // For full context, if benchmarks need deeper DuckDB integration.
    // Connection con(db);
    // auto context = con.context.get();
    // LuaTranslator::Initialize(context); // If translator needs context-specific init

    std::cout << "Running LuaJIT Expression Benchmarks..." << std::endl;
    RunBenchmarks();
    std::cout << "Benchmarks finished." << std::endl;

    return 0;
}

// To compile this (conceptual, assuming headers are in include paths and LuaJIT linked):
// g++ -std=c++11 jit_expression_benchmark.cpp ../../src/common/luajit_wrapper.cpp ../../src/main/luajit_translator.cpp -I../../src/include -lluajit-5.1 -o benchmark -O2
// (Plus any other .cpp files for expression nodes if not header only)
// This is highly simplified. Real DuckDB build is via CMake.

```
