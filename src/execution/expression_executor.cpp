#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/list.hpp"

// Includes for JIT
#include "duckdb/main/luajit_translator.hpp"
#include "duckdb/common/luajit_ffi_structs.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/unmanaged_vector_data.hpp"
#include "duckdb/common/exception.hpp" // For duckdb::Exception and duckdb::RuntimeException

#include <atomic> // For atomic counter for unique function names

// Required for lua_State and Lua API, typically included via luajit_wrapper.hpp's inclusions
extern "C" {
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
}


namespace duckdb {

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
	D_ASSERT(exprs.size() > 0);
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

ExpressionExecutor::ExpressionExecutor(const vector<unique_ptr<Expression>> &exprs) : context(nullptr), luajit_wrapper_() {
	D_ASSERT(exprs.size() > 0);
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
	auto state_manager = make_uniq<ExpressionExecutorState>(); // Renamed from 'state' to avoid conflict
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
	} catch (InternalException &ex) { // Keep DuckDB internal exceptions propagating
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

// Placeholder for expression complexity calculation
static idx_t GetExpressionComplexity(const Expression &expr) {
    // Simple complexity: count number of children (recursive) + 1 for self
    // This is a very basic heuristic.
    idx_t complexity = 1;
    for (const auto& child : expr.children) {
        complexity += GetExpressionComplexity(*child);
    }
    return complexity;
}

static std::atomic<idx_t> jitted_function_counter(0);

static std::string GenerateUniqueJitFunctionName(const Expression& expr) {
    return "jitted_duckdb_expr_func_" + std::to_string(jitted_function_counter.fetch_add(1));
}

static std::string ConstructFullLuaFunctionScript(
    const std::string& function_name,
    const std::string& lua_row_logic,
    LuaTranslatorContext& translator_ctx,
    const LogicalType& output_logical_type) {

    std::stringstream ss;
    ss << "local ffi = require('ffi')\n";
    ss << "ffi.cdef[[\n";
    ss << "    typedef struct FFIVector { void* data; bool* nullmask; unsigned long long count; "
       << "int ffi_logical_type_id; int ffi_duckdb_vector_type; } FFIVector;\n"; // Using int for enums for C
    ss << "    typedef struct FFIString { char* ptr; unsigned int len; } FFIString;\n";
    ss << "    typedef signed char int8_t;\n";
    ss << "    typedef int int32_t;\n";
    ss << "    typedef long long int64_t;\n";
    // TODO: Add C FFI declarations for duckdb_ffi_add_string_to_output etc. if used
    ss << "]]\n";

    ss << function_name << " = function(output_vec_ffi";
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
        default: throw NotImplementedException("[JIT] Output type for Lua FFI cast not defined: " + output_logical_type.ToString());
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
                            "--[[JIT_TODO_STRING_OUTPUT]] error('VARCHAR output not fully implemented in JIT script generation')");
    } else {
        StringUtil::Replace(adapted_row_logic, "output_vector.data[i]", "output_data[i]");
    }
    StringUtil::Replace(adapted_row_logic, "output_vector.nullmask[i]", "output_nullmask[i]");

    for (idx_t i = 0; i < translator_ctx.GetNumInputs(); ++i) {
        std::string input_vec_table_access = StringUtil::Format("input_vectors[%d]", i + 1);
        std::string lua_input_var_prefix = StringUtil::Format("input%d", i + 1);
        if (translator_ctx.GetInputLogicalType(i).id() == LogicalTypeId::VARCHAR) {
            std::string original_ffi_string_access = StringUtil::Format("ffi.string(%s.data[i].ptr, %s.data[i].len)",
                                                                input_vec_table_access, input_vec_table_access);
            std::string new_ffi_string_access = StringUtil::Format("ffi.string(%s_data_ffi_str_array[i].ptr, %s_data_ffi_str_array[i].len)",
                                                             lua_input_var_prefix, lua_input_var_prefix);
            StringUtil::Replace(adapted_row_logic, original_ffi_string_access, new_ffi_string_access);
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


bool ExpressionExecutor::ShouldJIT(const Expression &expr, ExpressionState *state) {
	if (!context) {
		return false;
	}
	switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_CONSTANT:
		case ExpressionClass::BOUND_REF:
		case ExpressionClass::BOUND_OPERATOR:
			break;
		default:
			return false;
	}
	if (state->attempted_jit_compilation && !state->jit_compilation_succeeded) {
		return false;
	}
	// Add global JIT enable/disable switch from DBConfig here if desired
	// e.g. if (!DBConfig::GetConfig(*context).options.enable_jit) return false;
	return true;
}

// Standard C++ execution path, refactored from original Execute
void ExpressionExecutor::ExecuteStandard(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                         idx_t count, Vector &result) {
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
			D_ASSERT(FlatVector::Validity(result).CheckAllValid(count));
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

                std::vector<LogicalType> input_types_for_translator;
                if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR ||
                    expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON || // Comparisons are often ops too
                    expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) { // Add other multi-child types
                    for(const auto& child : expr.children) {
                        input_types_for_translator.push_back(child->return_type);
                    }
                } else if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
                    input_types_for_translator.push_back(expr.return_type);
                }
                // Else: BoundConstant has no vector inputs for Lua function. Context can be empty.

                LuaTranslatorContext translator_ctx(input_types_for_translator);
                std::string lua_row_logic = LuaTranslator::TranslateExpressionToLuaRowLogic(expr, translator_ctx);
                state->jitted_lua_function_name = GenerateUniqueJitFunctionName(expr);
                std::string full_lua_script = ConstructFullLuaFunctionScript(
                    state->jitted_lua_function_name, lua_row_logic, translator_ctx, expr.return_type);

                if (luajit_wrapper_.CompileStringAndSetGlobal(full_lua_script, state->jitted_lua_function_name, error_message)) {
                    state->jit_compilation_succeeded = true;
                } else {
                    state->jit_compilation_succeeded = false;
                    // Throwing here means this specific Execute call fails, subsequent might try C++ path
                    throw RuntimeException("LuaJIT Compilation Error for expr '%s' (func %s): %s",
                                           expr.GetName(), state->jitted_lua_function_name, error_message);
                }
            }

            if (state->jit_compilation_succeeded) {
                std::vector<std::vector<char>> temp_buffers_owner;
                ffi::FFIVector ffi_output_vec;
                result.SetVectorType(VectorType::FLAT_VECTOR);
                FlatVector::Validity(result).EnsureWritable();
                CreateFFIVectorFromDuckDBVector(result, count, ffi_output_vec, temp_buffers_owner);

                std::vector<ffi::FFIVector*> ffi_input_args_ptrs;
                std::vector<ffi::FFIVector> ffi_input_vecs_storage; // To hold the actual structs

                // This input mapping is still very PoC specific
                idx_t num_lua_inputs = 0;
                if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR ||
                    expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON ||
                    expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
                     num_lua_inputs = expr.children.size();
                } else if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
                    num_lua_inputs = 1;
                }
                ffi_input_vecs_storage.resize(num_lua_inputs);
                for(idx_t i=0; i < num_lua_inputs; ++i) {
                    if (!this->chunk || i >= this->chunk->ColumnCount()) {
                         throw InternalException("JIT Execution: Input chunk error for expression " + expr.GetName());
                    }
                    CreateFFIVectorFromDuckDBVector(this->chunk->data[i], count, ffi_input_vecs_storage[i], temp_buffers_owner);
                    ffi_input_args_ptrs.push_back(&ffi_input_vecs_storage[i]);
                }

                if (luajit_wrapper_.PCallGlobal(state->jitted_lua_function_name, ffi_input_args_ptrs, &ffi_output_vec, count, error_message)) {
                    result.SetCount(count);
                    Verify(expr, result, count);
                    jit_path_taken = true;
                    // return; // Successfully executed JIT path
                } else {
                    state->jit_compilation_succeeded = false; // Runtime error, don't try JIT again for this state
                    throw RuntimeException("LuaJIT Runtime Error in expr '%s' (func %s): %s",
                                           expr.GetName(), state->jitted_lua_function_name, error_message);
                }
            }
        } catch (const duckdb::Exception& e) {
            // Log e.what() using context logger if available
            // std::cerr << "DuckDB JIT Exception: " << e.what() << std::endl;
            state->jit_compilation_succeeded = false; // Mark JIT as failed for this state
        } catch (const std::exception& e) {
            // std::cerr << "Standard JIT Exception: " << e.what() << std::endl;
            state->jit_compilation_succeeded = false;
        } catch (...) {
            // std::cerr << "Unknown JIT Exception." << std::endl;
            state->jit_compilation_succeeded = false;
        }
    }

    if (!jit_path_taken) {
        // Fallback to C++ interpreter path
        ExecuteStandard(expr, state, sel, count, result);
        Verify(expr, result, count); // Verify after standard execution too.
    }
}


idx_t ExpressionExecutor::Select(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (count == 0) {
		return 0;
	}
	D_ASSERT(true_sel || false_sel);
	D_ASSERT(expr.return_type.id() == LogicalTypeId::BOOLEAN);
	// JIT path for Select is not implemented in this PoC, falls back to standard.
	// A JITed Select would need Lua code that returns e.g. a boolean array,
	// then C++ would build the selection vector.
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
	Execute(expr, state, sel, count, intermediate); // This call will use the main Execute with try-catch

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
