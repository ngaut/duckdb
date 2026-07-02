//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_nested_loop_join_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_binding_runtime.hpp"
#include "sljit_projection_expression_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

static void SljitValidateNativeNestedLoopJoinProbeExecutable(SljitExecutableNestedLoopJoinProbe &executable) {
	if (!executable.function) {
		throw InternalException("SLJIT native nested loop join probe reached runtime without generated code");
	}
	if (executable.plan.conditions.size() != 1 || executable.lhs_conditions.size() != 1) {
		throw InternalException("SLJIT native nested loop join probe requires one executable condition");
	}
}

static const_data_ptr_t SljitNestedLoopJoinConditionSourceData(UnifiedVectorFormat &format,
                                                               SljitNativeNestedLoopJoinValueKind kind) {
	switch (kind) {
	case SljitNativeNestedLoopJoinValueKind::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeNestedLoopJoinValueKind::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	case SljitNativeNestedLoopJoinValueKind::INT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<hugeint_t>(format));
	case SljitNativeNestedLoopJoinValueKind::DOUBLE:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<double>(format));
	default:
		throw InternalException("Unknown SLJIT native nested loop join value kind");
	}
}

static void SljitPrepareNestedLoopJoinProbeInput(const SljitNativeNestedLoopJoinProbeConditionPlan &condition_plan,
                                                 DataChunk &left_condition, DataChunk &right_condition,
                                                 SelectionVector &left_selection, SelectionVector &right_selection,
                                                 SljitNestedLoopJoinProbeDrainState &state,
                                                 SljitNativeNestedLoopJoinProbeInput &native_input) {
	if (left_condition.ColumnCount() != 1 || right_condition.ColumnCount() != 1) {
		throw InternalException("SLJIT native nested loop join probe condition width mismatch");
	}
	UnifiedVectorFormat left_format;
	UnifiedVectorFormat right_format;
	left_condition.data[0].ToUnifiedFormat(left_format);
	right_condition.data[0].ToUnifiedFormat(right_format);

	native_input.left_data = SljitNestedLoopJoinConditionSourceData(left_format, condition_plan.value_kind);
	native_input.right_data = SljitNestedLoopJoinConditionSourceData(right_format, condition_plan.value_kind);
	native_input.left_sel = SljitNormalizedSourceSelectionData(left_format);
	native_input.right_sel = SljitNormalizedSourceSelectionData(right_format);
	native_input.left_validity = left_format.validity.CannotHaveNull() ? nullptr : left_format.validity.GetData();
	native_input.right_validity = right_format.validity.CannotHaveNull() ? nullptr : right_format.validity.GetData();
	native_input.left_count = left_condition.size();
	native_input.right_count = right_condition.size();
	native_input.left_offset = state.left_offset;
	native_input.right_offset = state.right_offset;
	native_input.output_capacity = STANDARD_VECTOR_SIZE;
	native_input.left_match_sel = left_selection.data();
	native_input.right_match_sel = right_selection.data();
	native_input.selected_count = 0;
	native_input.right_chunk_finished = false;
}

static ExecutionOperatorBindResult
SljitExecuteNativeNestedLoopJoinProbe(SljitExecutableNestedLoopJoinProbe &executable,
                                      const ExecutionNestedLoopJoinProbeBinding &probe, DataChunk &input,
                                      DataChunk &left_condition, DataChunk &output, SelectionVector &left_selection,
                                      SelectionVector &right_selection, SljitNestedLoopJoinProbeDrainState &state) {
	SljitValidateNativeNestedLoopJoinProbeExecutable(executable);
	if (probe.join_type != ExecutionRegionJoinType::INNER ||
	    executable.plan.join_type != ExecutionRegionJoinType::INNER) {
		throw InternalException("SLJIT native nested loop join probe currently requires INNER join");
	}
	if (probe.empty_build_side) {
		state.finished = true;
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}
	if (!state.started) {
		state.started = true;
		state.left_offset = 0;
		state.right_offset = 0;
		state.right_chunk_finished = false;
		if (!ExecutionNestedLoopJoinProbeStartInput(probe)) {
			state.finished = true;
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
	}
	while (!state.finished) {
		if (state.right_chunk_finished) {
			state.left_offset = 0;
			state.right_offset = 0;
			state.right_chunk_finished = false;
			if (!ExecutionNestedLoopJoinProbeAdvanceRight(probe)) {
				state.finished = true;
				output.Reset();
				return ExecutionOperatorBindResult::READY;
			}
		}
		if (!probe.right_condition || probe.right_condition->size() == 0) {
			state.right_chunk_finished = true;
			continue;
		}

		SljitNativeNestedLoopJoinProbeInput native_input;
		SljitPrepareNestedLoopJoinProbeInput(executable.plan.conditions[0], left_condition, *probe.right_condition,
		                                     left_selection, right_selection, state, native_input);
		executable.function(&native_input);
		state.left_offset = native_input.left_offset;
		state.right_offset = native_input.right_offset;
		state.right_chunk_finished = native_input.right_chunk_finished;
		if (probe.left_tuple) {
			*probe.left_tuple = state.left_offset;
		}
		if (probe.right_tuple) {
			*probe.right_tuple = state.right_offset;
		}
		if (native_input.selected_count == 0) {
			continue;
		}
		ExecutionMaterializeNestedLoopJoinProbe(probe, input, left_selection, right_selection,
		                                        native_input.selected_count, output);
		return ExecutionOperatorBindResult::READY;
	}
	output.Reset();
	return ExecutionOperatorBindResult::READY;
}

static ExecutionOperatorBindResult SljitExecuteNativeNestedLoopJoinProbeWithScratch(
    ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
    SljitExecutableRegionOp &op, DataChunk &input, DataChunk &left_condition, DataChunk &output,
    SelectionVector &left_selection, SelectionVector &right_selection, SljitNestedLoopJoinProbeDrainState &state,
    string &deferred_reason) {
	SljitValidateNativeNestedLoopJoinProbeExecutable(op.nested_loop_join_probe);
	ExecutionOperatorBinding *binding_ptr = nullptr;
	auto bind_result = SljitBindNativeOperator(
	    native_runtime, scratch, op_idx, op, input, op.nested_loop_join_probe.plan.operator_info,
	    "native-operator-runtime-deferred", "SLJIT native nested loop join probe", binding_ptr, deferred_reason);
	if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
		return bind_result;
	}
	auto &binding = *binding_ptr;
	if (!binding.ready || !binding.nested_loop_join_probe.ready) {
		throw InternalException("SLJIT native nested loop join probe received an incomplete operator binding");
	}
	auto &probe = binding.nested_loop_join_probe;
	if (probe.empty_build_side) {
		return SljitExecuteNativeNestedLoopJoinProbe(op.nested_loop_join_probe, probe, input, left_condition, output,
		                                             left_selection, right_selection, state);
	}
	if (!state.lhs_materialized) {
		left_condition.Reset();
		for (idx_t condition_idx = 0; condition_idx < op.nested_loop_join_probe.lhs_conditions.size();
		     condition_idx++) {
			SljitExecuteProjectionExpression(op.nested_loop_join_probe.lhs_conditions[condition_idx], input,
			                                 left_condition.data[condition_idx], nullptr, input.size(),
			                                 scratch.ExpressionAdapterScratch(op_idx, condition_idx));
		}
		left_condition.SetChildCardinality(input.size());
		state.lhs_materialized = true;
	}
	return SljitExecuteNativeNestedLoopJoinProbe(op.nested_loop_join_probe, probe, input, left_condition, output,
	                                             left_selection, right_selection, state);
}

} // namespace duckdb
