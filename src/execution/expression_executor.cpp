#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/main/config.hpp" // Required for DBConfig options

// Includes for JIT
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/unmanaged_vector_data.hpp"
#include "duckdb/common/exception.hpp"

#include <atomic>

extern "C" {
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
}


namespace duckdb {

// --- ExpressionExecutor Implementation ---
ExpressionExecutor::ExpressionExecutor(ClientContext &context) : context(&context), luajit_wrapper_() {
	auto &config = DBConfig::GetConfig(context);
	debug_vector_verification = config.options.debug_verify_vector;
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const Expression *expression)
    : ExpressionExecutor(context) {
	D_ASSERT(expression);
	AddExpression(*expression);
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const Expression &expression)
    : ExpressionExecutor(context) {
	AddExpression(expression);
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const vector<unique_ptr<Expression>> &exprs)
    : ExpressionExecutor(context) {
	D_ASSERT(!exprs.empty());
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

ExpressionExecutor::ExpressionExecutor(const vector<unique_ptr<Expression>> &exprs) : context(nullptr), luajit_wrapper_() {
	D_ASSERT(!exprs.empty());
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

ExpressionExecutor::ExpressionExecutor() : context(nullptr), luajit_wrapper_() {
}

bool ExpressionExecutor::HasContext() {
	return context;
}

ClientContext &ExpressionExecutor::GetContext() {
	if (!context) {
		throw InternalException("Calling ExpressionExecutor::GetContext on an expression executor without a context");
	}
	return *context;
}

Allocator &ExpressionExecutor::GetAllocator() {
	return context ? Allocator::Get(*context) : Allocator::DefaultAllocator();
}

void ExpressionExecutor::AddExpression(const Expression &expr) {
	expressions.push_back(&expr);
	auto state_manager = make_uniq<ExpressionExecutorState>();
	Initialize(expr, *state_manager);
	state_manager->Verify();
	states.push_back(std::move(state_manager));
}

void ExpressionExecutor::ClearExpressions() {
	states.clear();
	expressions.clear();
}

void ExpressionExecutor::Initialize(const Expression &expression, ExpressionExecutorState &state_manager) {
	state_manager.executor = this;
	state_manager.root_state = InitializeState(expression, state_manager);
    state_manager.root_state->execution_count = 0; // Ensure initialized for JIT heuristics
}

void ExpressionExecutor::Execute(DataChunk *input, DataChunk &result) {
	SetChunk(input);
	D_ASSERT(expressions.size() == result.ColumnCount());
	D_ASSERT(!expressions.empty());

	for (idx_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(i, result.data[i]);
	}
	result.SetCardinality(input ? input->size() : 1);
	result.Verify();
}

void ExpressionExecutor::ExecuteExpression(DataChunk &input, Vector &result) {
	SetChunk(&input);
	ExecuteExpression(result);
}

idx_t ExpressionExecutor::SelectExpression(DataChunk &input, SelectionVector &sel) {
	return SelectExpression(input, sel, nullptr, input.size());
}

idx_t ExpressionExecutor::SelectExpression(DataChunk &input, SelectionVector &result_sel,
                                           optional_ptr<SelectionVector> current_sel, idx_t current_count) {
	D_ASSERT(expressions.size() == 1);
	D_ASSERT(current_count <= input.size());
	SetChunk(&input);
	idx_t selected_tuples =
	    Select(*expressions[0], states[0]->root_state.get(), current_sel.get(), current_count, &result_sel, nullptr);
	return selected_tuples;
}

void ExpressionExecutor::ExecuteExpression(Vector &result) {
	D_ASSERT(expressions.size() == 1);
	ExecuteExpression(0, result);
}

void ExpressionExecutor::ExecuteExpression(idx_t expr_idx, Vector &result) {
	D_ASSERT(expr_idx < expressions.size());
	D_ASSERT(result.GetType().id() == expressions[expr_idx]->return_type.id());
	Execute(*expressions[expr_idx], states[expr_idx]->root_state.get(), nullptr, chunk ? chunk->size() : 1, result);
}

Value ExpressionExecutor::EvaluateScalar(ClientContext &context, const Expression &expr, bool allow_unfoldable) {
	D_ASSERT(allow_unfoldable || expr.IsFoldable());
	D_ASSERT(expr.IsScalar());
	ExpressionExecutor executor(context, expr);
	Vector result_vector(expr.return_type);
	executor.ExecuteExpression(result_vector);
	D_ASSERT(allow_unfoldable || result_vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	auto result_value = result_vector.GetValue(0);
	D_ASSERT(result_value.type().InternalType() == expr.return_type.InternalType());
	return result_value;
}

bool ExpressionExecutor::TryEvaluateScalar(ClientContext &context, const Expression &expr, Value &result) {
	try {
		result = EvaluateScalar(context, expr);
		return true;
	} catch (InternalException &ex) {
		throw;
	} catch (...) {
		return false;
	}
}

void ExpressionExecutor::Verify(const Expression &expr, Vector &vector, idx_t count) {
	D_ASSERT(expr.return_type.id() == vector.GetType().id());
	vector.Verify(count);
	if (expr.verification_stats) {
		expr.verification_stats->Verify(vector, count);
	}
	if (debug_vector_verification == DebugVectorVerification::DICTIONARY_EXPRESSION) {
		Vector::DebugTransformToDictionary(vector, count);
	}
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const Expression &expr,
                                                                ExpressionExecutorState &state_manager) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
		return InitializeState(expr.Cast<BoundReferenceExpression>(), state_manager);
	case ExpressionClass::BOUND_BETWEEN:
		return InitializeState(expr.Cast<BoundBetweenExpression>(), state_manager);
	case ExpressionClass::BOUND_CASE:
		return InitializeState(expr.Cast<BoundCaseExpression>(), state_manager);
	case ExpressionClass::BOUND_CAST:
		return InitializeState(expr.Cast<BoundCastExpression>(), state_manager);
	case ExpressionClass::BOUND_COMPARISON:
		return InitializeState(expr.Cast<BoundComparisonExpression>(), state_manager);
	case ExpressionClass::BOUND_CONJUNCTION:
		return InitializeState(expr.Cast<BoundConjunctionExpression>(), state_manager);
	case ExpressionClass::BOUND_CONSTANT:
		return InitializeState(expr.Cast<BoundConstantExpression>(), state_manager);
	case ExpressionClass::BOUND_FUNCTION:
		return InitializeState(expr.Cast<BoundFunctionExpression>(), state_manager);
	case ExpressionClass::BOUND_OPERATOR:
		return InitializeState(expr.Cast<BoundOperatorExpression>(), state_manager);
	case ExpressionClass::BOUND_PARAMETER:
		return InitializeState(expr.Cast<BoundParameterExpression>(), state_manager);
	default:
		throw InternalException("Attempting to initialize state of expression of unknown type!");
	}
}


// --- JIT Helper Implementations ---
static idx_t GetExpressionComplexity(const Expression &expr) {
    idx_t complexity = 1; // Start with 1 for the root expression itself
    ExpressionIterator it(expr);
    it.Next(); // Skip root, already counted
    while(it.Next()) {
        complexity++;
    }
    return complexity;
}

static std::atomic<idx_t> jitted_function_counter(0);

static std::string GenerateUniqueJitFunctionName(const Expression& expr) {
    return "jitted_duckdb_expr_func_" + std::to_string(jitted_function_counter.fetch_add(1));
}

// Note: input_types in LuaTranslatorContext are the unique, ordered inputs to the Lua function
static std::string ConstructFullLuaFunctionScript(
    const std::string& jitted_function_name,
    const std::string& lua_row_logic_snippet,
    const LuaTranslatorContext& translator_ctx, // Contains unique input types and mapping
    const LogicalType& output_logical_type) {

    std::stringstream ss;
    ss << "local ffi = require('ffi')\n";
    // FFI CDEFs: Including struct definitions here makes each generated function more self-contained,
    // though ideally, these are globally defined once in the Lua state. For PoC, this is safer.
    ss << "ffi.cdef[[\n";
    ss << "    typedef unsigned long long uint64_t;\n"; // Ensure uint64_t is known for FFIVector.count
    ss << "    typedef unsigned int uint32_t;\n";   // Ensure uint32_t is known for FFIString.len
    ss << "    typedef signed char int8_t;\n";
    ss << "    typedef int int32_t;\n";
    ss << "    typedef long long int64_t;\n";
    ss << "    typedef struct FFIVector { void* data; bool* nullmask; uint64_t count; int32_t ffi_logical_type_id; int32_t ffi_duckdb_vector_type; void* original_duckdb_vector; } FFIVector;\n";
    ss << "    typedef struct FFIString { char* ptr; uint32_t len; } FFIString;\n";
    ss << "    typedef struct FFIInterval { int32_t months; int32_t days; int64_t micros; } FFIInterval;\n";
    ss << "    typedef signed char int8_t;\n";
    ss << "    typedef int int32_t;\n";
    ss << "    typedef long long int64_t;\n";
    ss << "    void duckdb_ffi_add_string_to_output_vector(void* ffi_vec_ptr, uint64_t row_idx, const char* str_data, uint32_t str_len);\n";
    ss << "    void duckdb_ffi_set_string_output_null(void* ffi_vec_ptr, uint64_t row_idx);\n";
    ss << "    int64_t duckdb_ffi_extract_from_date(int32_t date_val, const char* part_str);\n";
    ss << "    int64_t duckdb_ffi_extract_from_timestamp(int64_t ts_val, const char* part_str);\n";
    ss << "    int64_t duckdb_ffi_extract_year_from_date(int32_t date_val);\n";
    ss << "]]\n";

    ss << jitted_function_name << " = function(output_vec_ffi";
    for (idx_t i = 0; i < translator_ctx.GetNumInputs(); ++i) {
        // Lua function arguments are input0_ffi, input1_ffi, ...
        ss << ", input" << i << "_ffi";
    }
    ss << ", count)\n";

    // Cast output vector
    ss << "    local output_nullmask = ffi.cast('bool*', output_vec_ffi.nullmask)\n";
    std::string output_data_var_name = "output_data";
    if (output_logical_type.id() != LogicalTypeId::VARCHAR) { // Strings are handled by FFI call
        std::string output_ffi_type_str = translator_ctx.GetOutputTypeLuaFFIType(output_logical_type);
        ss << "    local " << output_data_var_name << " = ffi.cast('" << output_ffi_type_str << "*', output_vec_ffi.data)\n";
    }

    // Cast input vectors
    for (idx_t i = 0; i < translator_ctx.GetNumInputs(); ++i) {
        std::string lua_input_ffi_ptr_name = "input" + std::to_string(i) + "_ffi";
        std::string lua_input_data_var = "input" + std::to_string(i) + "_data";
        std::string lua_input_nullmask_var = "input" + std::to_string(i) + "_nullmask";

        std::string ffi_element_type_str = translator_ctx.GetInputLuaFFIType(i); // e.g. "int32_t", "FFIString"
        ss << "    local " << lua_input_data_var << " = ffi.cast('" << ffi_element_type_str << "*', " << lua_input_ffi_ptr_name << ".data)\n";
        ss << "    local " << lua_input_nullmask_var << " = ffi.cast('bool*', " << lua_input_ffi_ptr_name << ".nullmask)\n";
    }

    // Main processing loop
    ss << "    for i = 0, count - 1 do\n";
    ss << "        local current_row_value\n";
    ss << "        local current_row_is_null = false -- Default to not null\n";

    // Embed the row logic snippet from LuaTranslator
    // This snippet is expected to use input0_data, input0_nullmask, input1_data etc.
    // and set current_row_value and current_row_is_null.
    ss << "        " << lua_row_logic_snippet << "\n";

    // Output handling
    ss << "        if current_row_is_null then\n";
    ss << "            output_nullmask[i] = true\n";
    if (output_logical_type.id() == LogicalTypeId::VARCHAR) {
        ss << "            duckdb_ffi_set_string_output_null(output_vec_ffi, i)\n";
    }
    // Optional: zero out data for non-VARCHAR nulls if desired, e.g. output_data[i] = 0
    ss << "        else\n";
    ss << "            output_nullmask[i] = false\n";
    if (output_logical_type.id() == LogicalTypeId::VARCHAR) {
        ss << "            duckdb_ffi_add_string_to_output_vector(output_vec_ffi, i, current_row_value, #current_row_value)\n";
    } else if (output_logical_type.id() == LogicalTypeId::BOOLEAN) {
        ss << "            " << output_data_var_name << "[i] = current_row_value and 1 or 0 -- Lua bool to C int8_t\n";
    } else if (output_logical_type.id() == LogicalTypeId::INTERVAL) {
        ss << "            " << output_data_var_name << "[i].months = current_row_value.months\n";
        ss << "            " << output_data_var_name << "[i].days = current_row_value.days\n";
        ss << "            " << output_data_var_name << "[i].micros = current_row_value.micros\n";
    } else { // Other fixed-width types (numeric, date, timestamp)
        ss << "            " << output_data_var_name << "[i] = current_row_value\n";
    }
    ss << "        end\n";
    ss << "    end\n"; // End for loop
    ss << "end\n"; // End function
    return ss.str();
}


bool ExpressionExecutor::ShouldJIT(const Expression &expr, ExpressionState *state) {
	if (!context) {
		return false;
	}

    ClientConfig& config = ClientConfig::Get(*context);
    if (!config.enable_luajit_jit) { return false; }

	switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_CONSTANT:
		case ExpressionClass::BOUND_REF:
		case ExpressionClass::BOUND_OPERATOR:
        case ExpressionClass::BOUND_FUNCTION:
        case ExpressionClass::BOUND_CASE:
			break;
		default:
			return false;
	}
	if (state->attempted_jit_compilation && !state->jit_compilation_succeeded) {
		return false;
	}
    if (GetExpressionComplexity(expr) < config.luajit_jit_complexity_threshold) { return false; }
    if (state->execution_count < config.luajit_jit_trigger_count) { return false; }
	return true;
}

void ExpressionExecutor::ExecuteStandard(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                         idx_t count, Vector &result) {
    if (!(state->attempted_jit_compilation && state->jit_compilation_succeeded)) {
        state->execution_count++;
    }
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_BETWEEN:
		Execute(expr.Cast<BoundBetweenExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_REF:
		Execute(expr.Cast<BoundReferenceExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute(expr.Cast<BoundCaseExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute(expr.Cast<BoundCastExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute(expr.Cast<BoundComparisonExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute(expr.Cast<BoundConjunctionExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute(expr.Cast<BoundConstantExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute(expr.Cast<BoundFunctionExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute(expr.Cast<BoundOperatorExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		Execute(expr.Cast<BoundParameterExpression>(), state, sel, count, result);
		break;
	default:
		throw InternalException("Attempting to execute expression of unknown type!");
	}
}


void ExpressionExecutor::Execute(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	if (count == 0) {
		return;
	}
#ifdef DEBUG
	if (result.GetVectorType() == VectorType::FLAT_VECTOR) {
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF &&
		    expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT &&
		    expr.GetExpressionClass() != ExpressionClass::BOUND_PARAMETER) {
			// D_ASSERT(FlatVector::Validity(result).CheckAllValid(count));
		}
	}
#endif
	if (result.GetType().id() != expr.return_type.id()) {
		throw InternalException(
		    "ExpressionExecutor::Execute called with a result vector of type %s that does not match expression type %s",
		    result.GetType(), expr.return_type);
	}

    bool jit_path_taken = false;
    if (ShouldJIT(expr, state)) {
        std::string error_message;
        try {
            if (!state->jit_compilation_succeeded && !state->attempted_jit_compilation) {
                state->attempted_jit_compilation = true;

                // 1. Identify unique input columns and their types for the Lua function signature
                std::vector<LogicalType> unique_input_types;
                std::vector<idx_t> unique_input_original_indices; // Original chunk indices of these unique inputs
                std::unordered_map<idx_t, idx_t> original_col_idx_to_lua_arg_map; // Map original chunk idx to Lua arg idx (0,1,2..)

                ExpressionIterator it(expr);
                // Note: ExpressionIterator includes the root expression itself as the first element.
                // We are interested in the BoundReferenceExpressions within the expression tree.
                while(it.Next()) {
                    auto& child_exp = *it.Get();
                    if(child_exp.GetExpressionClass() == ExpressionClass::BOUND_REF) {
                        auto& ref_expr = child_exp.Cast<BoundReferenceExpression>();
                        bool found = false;
                        for(idx_t existing_original_idx : unique_input_original_indices) {
                            if (existing_original_idx == ref_expr.index) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            idx_t lua_arg_idx = unique_input_types.size(); // New Lua argument index is current size of unique types vector
                            unique_input_types.push_back(ref_expr.return_type);
                            unique_input_original_indices.push_back(ref_expr.index);
                            original_col_idx_to_lua_arg_map[ref_expr.index] = lua_arg_idx;
                        }
                    }
                }

                // 2. Create LuaTranslatorContext with these unique inputs and the mapping
                LuaTranslatorContext translator_ctx(unique_input_types, original_col_idx_to_lua_arg_map);

                // 3. Translate expression to Lua row logic snippet
                // LuaTranslator::GenerateValue(BoundReferenceExpression) will use ctx.GetLuaArgIndex()
                std::string lua_row_logic_snippet = LuaTranslator::TranslateExpressionToLuaRowLogic(expr, translator_ctx);

                // 4. Construct the full Lua function script
                state->jitted_lua_function_name = GenerateUniqueJitFunctionName(expr);
                std::string full_lua_script = ConstructFullLuaFunctionScript(
                    state->jitted_lua_function_name,
                    lua_row_logic_snippet,
                    translator_ctx, // This context now has info about unique inputs for the Lua function
                    expr.return_type);

                // 5. Compile the script
                if (luajit_wrapper_.CompileStringAndSetGlobal(full_lua_script, state->jitted_lua_function_name, error_message)) {
                    state->jit_compilation_succeeded = true;
                } else {
                    state->jit_compilation_succeeded = false;
                    throw RuntimeException("LuaJIT Compilation Error for expr '%s' (func %s): %s",
                                           expr.ToString(), state->jitted_lua_function_name, error_message);
                }
            }

            if (state->jit_compilation_succeeded) {
                std::vector<std::vector<char>> temp_buffers_owner;
                ffi::FFIVector ffi_output_vec;
                result.SetVectorType(VectorType::FLAT_VECTOR);
                FlatVector::Validity(result).EnsureWritable();
                ffi_output_vec.original_duckdb_vector = &result;
                CreateFFIVectorFromDuckDBVector(result, count, ffi_output_vec, temp_buffers_owner);

                std::vector<ffi::FFIVector*> ffi_input_args_ptrs; // Pointers to FFIVector objects
                std::vector<ffi::FFIVector> ffi_input_vecs_storage; // Actual storage for input FFIVectors

                // Prepare FFIVectors for each unique input column, in the order they appear in unique_input_original_indices
                ffi_input_vecs_storage.resize(unique_input_original_indices.size());
                for(size_t i=0; i < unique_input_original_indices.size(); ++i) {
                    idx_t original_chunk_col_idx = unique_input_original_indices[i];
                    if (!this->chunk || original_chunk_col_idx >= this->chunk->ColumnCount()) {
                         throw InternalException("JIT Execution: Input chunk error or column index %d out of bounds for expression '%s'",
                                                 original_chunk_col_idx, expr.ToString());
                    }
                    CreateFFIVectorFromDuckDBVector(this->chunk->data[original_chunk_col_idx], count, ffi_input_vecs_storage[i], temp_buffers_owner);
                    ffi_input_vecs_storage[i].original_duckdb_vector = &this->chunk->data[original_chunk_col_idx];
                    ffi_input_args_ptrs.push_back(&ffi_input_vecs_storage[i]);
                }

                // 7. Call the JITed Lua function
                if (luajit_wrapper_.PCallGlobal(state->jitted_lua_function_name, ffi_input_args_ptrs, &ffi_output_vec, count, error_message)) {
                    result.SetCount(count);
                    Verify(expr, result, count);
                    jit_path_taken = true;
                } else {
                    state->jit_compilation_succeeded = false;
                    throw RuntimeException("LuaJIT Runtime Error in expr '%s' (func %s): %s",
                                           expr.ToString(), state->jitted_lua_function_name, error_message);
                }
            }
        } catch (const duckdb::Exception& e) {
            if (context && context->client.logger) {
                 context->client.logger->Log(duckdb::LogLevel::DEBUG, StringUtil::Format("JIT Exception for expr '%s': %s", expr.ToString(), e.what()));
            }
            state->jit_compilation_succeeded = false;
        } catch (const std::exception& e) {
             if (context && context->client.logger) {
                 context->client.logger->Log(duckdb::LogLevel::DEBUG, StringUtil::Format("Standard JIT Exception for expr '%s': %s", expr.ToString(), e.what()));
            }
            state->jit_compilation_succeeded = false;
        } catch (...) {
             if (context && context->client.logger) {
                context->client.logger->Log(duckdb::LogLevel::DEBUG, StringUtil::Format("Unknown JIT Exception for expr '%s'", expr.ToString()));
            }
            state->jit_compilation_succeeded = false;
        }
    }

    if (!jit_path_taken) {
        ExecuteStandard(expr, state, sel, count, result);
        // Verify(expr, result, count); // Already called in ExecuteStandard if that path is taken
    }
}


idx_t ExpressionExecutor::Select(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (count == 0) {
		return 0;
	}
	D_ASSERT(true_sel || false_sel);
	D_ASSERT(expr.return_type.id() == LogicalTypeId::BOOLEAN);
	switch (expr.GetExpressionClass()) {
#ifndef DUCKDB_SMALLER_BINARY
	case ExpressionClass::BOUND_BETWEEN:
		return Select(expr.Cast<BoundBetweenExpression>(), state, sel, count, true_sel, false_sel);
#endif
	case ExpressionClass::BOUND_COMPARISON:
		return Select(expr.Cast<BoundComparisonExpression>(), state, sel, count, true_sel, false_sel);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Select(expr.Cast<BoundConjunctionExpression>(), state, sel, count, true_sel, false_sel);
	default:
		return DefaultSelect(expr, state, sel, count, true_sel, false_sel);
	}
}

template <bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DefaultSelectLoop(const SelectionVector *bsel, const uint8_t *__restrict bdata, ValidityMask &mask,
                                      const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                      SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto bidx = bsel->get_index(i);
		auto result_idx = sel->get_index(i);
		if ((NO_NULL || mask.RowIsValid(bidx)) && bdata[bidx] > 0) {
			if (HAS_TRUE_SEL) {
				true_sel->set_index(true_count++, result_idx);
			}
		} else {
			if (HAS_FALSE_SEL) {
				false_sel->set_index(false_count++, result_idx);
			}
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

template <bool NO_NULL>
static inline idx_t DefaultSelectSwitch(UnifiedVectorFormat &idata, const SelectionVector *sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DefaultSelectLoop<NO_NULL, true, true>(idata.sel, UnifiedVectorFormat::GetData<uint8_t>(idata),
		                                              idata.validity, sel, count, true_sel, false_sel);
	} else if (true_sel) {
		return DefaultSelectLoop<NO_NULL, true, false>(idata.sel, UnifiedVectorFormat::GetData<uint8_t>(idata),
		                                               idata.validity, sel, count, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DefaultSelectLoop<NO_NULL, false, true>(idata.sel, UnifiedVectorFormat::GetData<uint8_t>(idata),
		                                               idata.validity, sel, count, true_sel, false_sel);
	}
}

idx_t ExpressionExecutor::DefaultSelect(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                        idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	bool intermediate_bools[STANDARD_VECTOR_SIZE];
	Vector intermediate(LogicalType::BOOLEAN, data_ptr_cast(intermediate_bools));
	Execute(expr, state, sel, count, intermediate);

	UnifiedVectorFormat idata;
	intermediate.ToUnifiedFormat(count, idata);

	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}
	if (!idata.validity.AllValid()) {
		return DefaultSelectSwitch<false>(idata, sel, count, true_sel, false_sel);
	} else {
		return DefaultSelectSwitch<true>(idata, sel, count, true_sel, false_sel);
	}
}

vector<unique_ptr<ExpressionExecutorState>> &ExpressionExecutor::GetStates() {
	return states;
}

} // namespace duckdb
