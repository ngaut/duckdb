#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_perfect_hash_codegen.hpp"
#include "sljit_aggregate_perfect_hash_update_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "sljitLir.h"

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups, const vector<SljitNativeRegionExpressionPlan> &group_expressions,
    const ExecutionRegionAggregateContract &contract, const vector<bool> &source_not_null,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	SljitPerfectHashFusedUpdatePlan update_plan;
	if (!TryBuildSljitPerfectHashFusedUpdatePlan(payloads, aggregates, groups, group_expressions, contract,
	                                             source_not_null, source_min_values, source_max_values, update_plan,
	                                             error)) {
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto &group_plans = update_plan.group_plans;
	const auto &codegen_plan = update_plan.codegen_plan;
	const auto &local_aggregate_plan = update_plan.local_aggregate_plan;
	const auto &deferred_flag_plan = update_plan.deferred_flag_plan;
	const auto &source_data_hoists = update_plan.source_data_hoists;
	const auto perfect_hash_group_count = update_plan.perfect_hash_group_count;
	const auto local_size = update_plan.local_size;
	const auto state_pointer_offset = update_plan.state_pointer_offset;
	const auto group_index_offset = update_plan.group_index_offset;
	const auto binary_shared_value_offset = update_plan.binary_shared_value_offset;
	const auto sparse_run_cached_group_offset = update_plan.sparse_run_cached_group_offset;
	const auto sparse_run_cached_pointer_offset = update_plan.sparse_run_cached_pointer_offset;
	const auto sparse_run_cached_start_offset = update_plan.sparse_run_cached_start_offset;
	const auto sparse_run_cache_enabled = update_plan.sparse_run_cache_enabled;
	const auto hoist_source_data_pointers = update_plan.hoist_source_data_pointers;
	const auto hoist_group_data_pointers = update_plan.hoist_group_data_pointers;
	const auto dedicated_state_register = update_plan.dedicated_state_register;
	const auto state_pointer_reg = update_plan.state_pointer_reg;
	const auto group_index_reg = update_plan.group_index_reg;
	const auto saved_register_count = update_plan.saved_register_count;

	vector<SljitExpressionTreeOverflowJumps> overflows;
	vector<sljit_jump *> group_out_of_range;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, saved_register_count, local_size);
	EmitZeroSljitLocalPerfectHashAggregateArrays(compiler, local_aggregate_plan);
	EmitZeroSljitDeferredPerfectHashFlagArray(compiler, deferred_flag_plan);
	if (sparse_run_cache_enabled) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_group_offset, SLJIT_IMM, -1);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_pointer_offset, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_start_offset, SLJIT_IMM, 0);
	}
	EmitInitSljitNativeVectorLoop(compiler);
	EmitInitSljitNativeVectorSourceArrays(compiler);
	if (hoist_source_data_pointers) {
		for (auto &hoist : source_data_hoists) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	}
	if (hoist_group_data_pointers) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
		for (idx_t group_idx = 0; group_idx < group_plans.size(); group_idx++) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SljitPerfectHashGroupDataPointerReg(group_idx), 0,
			               SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
		}
	}
	SljitPerfectHashFusedUpdateEmitContext emit_context {
	    compiler,
	    payloads,
	    aggregates,
	    group_plans,
	    contract,
	    codegen_plan,
	    local_aggregate_plan,
	    deferred_flag_plan,
	    overflows,
	    group_out_of_range,
	    perfect_hash_group_count,
	    state_pointer_offset,
	    group_index_offset,
	    binary_shared_value_offset,
	    hoist_group_data_pointers,
	    dedicated_state_register,
	    state_pointer_reg,
	    group_index_reg,
	};
	EmitSljitPerfectHashFusedUpdateLoops(emit_context, update_plan);
	EmitSljitLocalPerfectHashCommit(compiler, local_aggregate_plan, codegen_plan.payload_descriptors, contract, false);
	EmitSljitDeferredPerfectHashFlagsCommit(compiler, deferred_flag_plan, codegen_plan.payload_descriptors, contract);
	sljit_emit_return_void(compiler);

	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, overflow.op);
		sljit_emit_return_void(compiler);
	}

	auto range_error_label = sljit_emit_label(compiler);
	for (auto jump : group_out_of_range) {
		sljit_set_label(jump, range_error_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeRuntimeError));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
