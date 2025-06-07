#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/planner/expression/list.hpp"

// Includes for JIT
#include "duckdb/main/luajit_translator.hpp"       // Assuming this path for PoC
#include "duckdb/common/luajit_ffi_structs.hpp" // Assuming this path for PoC
#include "duckdb/planner/luajit_expression_nodes.hpp" // For casting Expression to PoC nodes

// Required for lua_State and Lua API, typically included via luajit_wrapper.hpp's inclusions
extern "C" {
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
}


namespace duckdb {

ExpressionExecutor::ExpressionExecutor(ClientContext &context) : context(&context), luajit_wrapper_() { // Initialize luajit_wrapper_
	auto &config = DBConfig::GetConfig(context);
	debug_vector_verification = config.options.debug_verify_vector;
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const Expression *expression)
    : ExpressionExecutor(context) { // luajit_wrapper_ initialized by delegating constructor
	D_ASSERT(expression);
	AddExpression(*expression);
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const Expression &expression)
    : ExpressionExecutor(context) { // luajit_wrapper_ initialized by delegating constructor
	AddExpression(expression);
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const vector<unique_ptr<Expression>> &exprs)
    : ExpressionExecutor(context) { // luajit_wrapper_ initialized by delegating constructor
	D_ASSERT(exprs.size() > 0);
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

// For constructors without ClientContext, luajit_wrapper_ will be default constructed.
// This is fine if JIT is only enabled when a context is present.
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
	auto state = make_uniq<ExpressionExecutorState>();
	Initialize(expr, *state);
	state->Verify();
	states.push_back(std::move(state));
}

void ExpressionExecutor::ClearExpressions() {
	states.clear();
	expressions.clear();
}

void ExpressionExecutor::Initialize(const Expression &expression, ExpressionExecutorState &state) {
	state.executor = this;
	state.root_state = InitializeState(expression, state);
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
	// use an ExpressionExecutor to execute the expression
	ExpressionExecutor executor(context, expr);

	Vector result(expr.return_type);
	executor.ExecuteExpression(result);

	D_ASSERT(allow_unfoldable || result.GetVectorType() == VectorType::CONSTANT_VECTOR);
	auto result_value = result.GetValue(0);
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
                                                                ExpressionExecutorState &state) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
		return InitializeState(expr.Cast<BoundReferenceExpression>(), state);
	case ExpressionClass::BOUND_BETWEEN:
		return InitializeState(expr.Cast<BoundBetweenExpression>(), state);
	case ExpressionClass::BOUND_CASE:
		return InitializeState(expr.Cast<BoundCaseExpression>(), state);
	case ExpressionClass::BOUND_CAST:
		return InitializeState(expr.Cast<BoundCastExpression>(), state);
	case ExpressionClass::BOUND_COMPARISON:
		return InitializeState(expr.Cast<BoundComparisonExpression>(), state);
	case ExpressionClass::BOUND_CONJUNCTION:
		return InitializeState(expr.Cast<BoundConjunctionExpression>(), state);
	case ExpressionClass::BOUND_CONSTANT:
		return InitializeState(expr.Cast<BoundConstantExpression>(), state);
	case ExpressionClass::BOUND_FUNCTION:
		return InitializeState(expr.Cast<BoundFunctionExpression>(), state);
	case ExpressionClass::BOUND_OPERATOR:
		return InitializeState(expr.Cast<BoundOperatorExpression>(), state);
	case ExpressionClass::BOUND_PARAMETER:
		return InitializeState(expr.Cast<BoundParameterExpression>(), state);
	default:
		throw InternalException("Attempting to initialize state of expression of unknown type!");
	}
}

// New ShouldJIT method implementation
bool ExpressionExecutor::ShouldJIT(const Expression &expr, ExpressionState *state) {
	if (!context) { // JIT needs a context for LuaJIT state and potentially configs
		return false;
	}
	// For PoC, enable JIT only for specific expression types (e.g., our simplified binary ops)
	// and if a global JIT switch is on (e.g., via PRAGMA or session setting).
	// This PoC assumes luajit_expression_nodes.hpp types are used or mapped.
	// A real implementation would check DuckDB's internal ExpressionType.
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR &&
	    expr.GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) { // Placeholder for actual check
		// This check is too generic for real DuckDB expressions.
		// We'd need to cast to our PoC BaseExpression type or map DuckDB expressions.
		// For now, let's assume the test will pass a compatible expression type
		// that we can identify or that this check is a placeholder.
		// A better check for PoC might be:
		// if (dynamic_cast<const BinaryOperatorExpression*>(&expr)) return true;
		// But that requires the input 'expr' to be of our PoC type, not DuckDB's real types.
		// Let's assume for the PoC test, this will pass through.
	}

	// Example: check a global enable flag from ClientContext
	auto &config = DBConfig::GetConfig(*context);
	// if (!config.options.enable_jit) return false; // Assuming such an option exists

	// Avoid JITing for very small counts (compile overhead might dominate)
	// if (count < 100) return false; // 'count' is not available here, but in Execute.

	// If already tried and failed, don't try again for this expression state
	if (state->attempted_jit_compilation && !state->jit_compilation_succeeded) {
		return false;
	}
	return true; // Default to attempting JIT for compatible expressions for PoC
}

void ExpressionExecutor::Execute(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// JIT Path
	if (ShouldJIT(expr, state)) {
		if (!state->attempted_jit_compilation) {
			state->attempted_jit_compilation = true;
			// This is a placeholder for translating DuckDB's Expression to PoC's BaseExpression
			// For the unit test, we will construct a PoC expression directly.
			// In a real scenario, this translation is complex.
			// We assume 'expr' can be conceptually cast or converted to 'lua_expr_node' for translation.
			// For this PoC, we will skip direct translation from 'expr' and assume
			// the test provides a translatable 'BaseExpression'.
			// The current 'expr' is DuckDB's native Expression.
			// We'll need to construct a luajit_expression_nodes::BaseExpression for the translator.
			// This part is highly conceptual for full DuckDB integration.
			// For testing, the test itself will create a luajit_expression_nodes::BaseExpression.
			// Here, we'd ideally have a function:
			// unique_ptr<BaseExpression> ConvertToLuaExr(const Expression& duckdb_expr);
			// For now, this path will only really work if the test calls Execute with a
			// luajit_expression_nodes::BaseExpression that's also a duckdb::Expression,
			// which is not the case.
			// Let's assume for the PoC that we bypass this for now and the test will
			// call a specific JIT execution path with the PoC expression type.
			//
			// OR, we make the test responsible for JIT compilation and storing the function name.
			// For this step, let's focus on the call to a pre-compiled Lua function.
			// The test will compile and register the function.
			//
			// If state->jitted_lua_function_name is set (by a test or a future JIT manager):
			if (state->jit_compilation_succeeded && !state->jitted_lua_function_name.empty()) {
				// Prepare FFIVectors for inputs and output. This is highly conceptual
				// as it needs to bridge DuckDB's Vector/DataChunk with FFIVector.
				// For the PoC test, we will create FFIVectors manually from std::vectors.

				// Example for one output, two inputs (hardcoded for col0 + col1 like expr)
				// This part would need to know the number of inputs for the specific expr.
				// This is very simplified and assumes flat int vectors.
				// Proper type handling and UnifiedVectorFormat are major tasks.

				duckdb::ffi::FFIVector ffi_output_vec;
				// This requires result vector to be setup for writing data, including its data buffer and nullmask
				// For PoC, assume result.GetData() and result.GetNullMask().GetData() are valid.
				// THIS IS DANGEROUS AND SIMPLIFIED - DuckDB Vectors need proper handling.
				// ffi_output_vec.data = FlatVector::GetData(result);
				// ffi_output_vec.nullmask = FlatVector::Validity(result).GetData(); // This is not bool*
				// ffi_output_vec.count = count;
				// For the PoC, we'll assume the test prepares FFIVectors and calls a different ExecuteJITed.
				// This direct integration is too complex for one step with current PoC structure.

				// ---> REVISION: The JIT path for this PoC will be mostly driven by the TEST.
				// The test will:
				// 1. Create PoC expression.
				// 2. Translate it to Lua, get full_lua_script.
				// 3. Use luajit_wrapper_.ExecuteString to compile and define the Lua function (e.g., "test_jit_func_1").
				// 4. Store "test_jit_func_1" in state->jitted_lua_function_name & set jit_compilation_succeeded.
				// 5. Then, this Execute method, when called, would find the function name and execute it.

				// Actual call to Lua function:
				lua_State* L = luajit_wrapper_.GetState();
				lua_getglobal(L, state->jitted_lua_function_name.c_str());
				if (lua_isfunction(L, -1)) {
					// Simplified: Assume 1 output vector, and inputs are from 'chunk' (DataChunk)
					// This part requires mapping DataChunk to FFIVector arguments.
					// For the PoC, this will be very hardcoded in the test.
					// The test will prepare FFIVector args and push them.
					// lua_pushlightuserdata(L, &ffi_output_vec_from_test);
					// lua_pushlightuserdata(L, &ffi_input1_vec_from_test);
					// lua_pushlightuserdata(L, &ffi_input2_vec_from_test);
					// lua_pushinteger(L, count);
					// if (lua_pcall(L, num_args, 0, 0) == LUA_OK) {
					//     result.SetCount(count); // Or whatever the Lua script sets as output count
					//     Verify(expr, result, count);
					//     return; // JIT execution successful
					// } else {
					//     std::cerr << "LuaJIT Runtime Error: " << lua_tostring(L, -1) << std::endl;
					//     lua_pop(L, 1);
					//     state->jit_compilation_succeeded = false; // Mark as failed for future
					// }
				} else {
					lua_pop(L, 1); // Pop non-function
					// std::cerr << "LuaJIT Error: Compiled function " << state->jitted_lua_function_name << " not found." << std::endl;
					state->jit_compilation_succeeded = false; // Function disappeared?
				}
				// If JIT call failed or function not found, fall through to interpreter.
				// For the PoC, the test will handle the direct call and verification.
				// This block here is more of a placeholder for where real integration would go.
				// The critical part is that if state->jit_compilation_succeeded is true AND
				// jitted_lua_function_name is set, a JIT path MIGHT be taken.
				// For this subtask, we are making this block very conceptual. The actual JIT call
				// will be demonstrated more directly in the unit test.
			}
		}
		// If JIT was attempted and succeeded, and the function was called, we would have returned.
		// If it falls through, it means JIT path was not taken or failed.
	}


#ifdef DEBUG
	// The result vector must be used for the first time, or must be reset.
	// Otherwise, the validity mask can contain previous (now incorrect) data.
	if (result.GetVectorType() == VectorType::FLAT_VECTOR) {

		// We do not initialize vector caches for these expressions.
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_REF &&
		    expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT &&
		    expr.GetExpressionClass() != ExpressionClass::BOUND_PARAMETER) {
			D_ASSERT(FlatVector::Validity(result).CheckAllValid(count));
		}
	}
#endif

	if (count == 0) {
		return;
	}
	if (result.GetType().id() != expr.return_type.id()) {
		throw InternalException(
		    "ExpressionExecutor::Execute called with a result vector of type %s that does not match expression type %s",
		    result.GetType(), expr.return_type);
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
	Verify(expr, result, count);
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
	// generic selection of boolean expression:
	// resolve the true/false expression first
	// then use that to generate the selection vector
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


// Helper function to get the number of arguments for a LuaJIT expression
// This is highly dependent on the expression structure.
// For PoC, we might inspect our simplified luajit_expression_nodes.
// This is a placeholder for where such logic might go.
// static idx_t GetNumberOfInputs(const BaseExpression& lua_expr_node) {
//    if (lua_expr_node.type == LuaJITExpressionType::COLUMN_REFERENCE) {
//        return 1; // Or rather, indicates max column index + 1
//    }
//    if (lua_expr_node.type == LuaJITExpressionType::BINARY_OPERATOR) {
//        auto& bin_op = static_cast<const BinaryOperatorExpression&>(lua_expr_node);
//        // This needs to find max column index referenced in children.
//        // For simplicity, if it's col0 + col1, it implies 2 inputs.
//        // This is not robust. A proper collection of referenced columns is needed.
//        idx_t left_inputs = GetNumberOfInputs(*bin_op.left_child);
//        idx_t right_inputs = GetNumberOfInputs(*bin_op.right_child);
//        return std::max(left_inputs, right_inputs); // This is not quite right.
//                                                  // It should be the count of distinct column refs.
//    }
//    return 0;
//}


} // namespace duckdb
