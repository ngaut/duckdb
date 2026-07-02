//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_between_join_sidecar_plan_common_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_between_join_sidecar_runtime.hpp"

namespace duckdb {

static bool SljitSecondJoinInputColIsProbeKey(SljitExecutableRegionOp &second_hash_join_op,
                                              const ExecutionHashJoinProbeBinding &second_binding, idx_t input_col) {
	for (auto key_input_idx : second_binding.probe_key_input_indices) {
		if (key_input_idx == input_col) {
			return true;
		}
	}
	for (auto &key : second_hash_join_op.hash_join_probe.plan.keys) {
		if (key.key_input_index == input_col) {
			return true;
		}
	}
	return false;
}

static bool SljitTryProjectionReferencesSecondJoinInputCol(const SljitExecutableRegionExpression &projection,
                                                           const ExecutionHashJoinProbeBinding &second_binding,
                                                           idx_t input_col, bool &references_input_col) {
	references_input_col = false;
	vector<uint8_t> referenced_sources;
	if (!SljitTryCollectHashJoinProjectionExpressionSources(projection, second_binding.output_types.size(),
	                                                        referenced_sources)) {
		return false;
	}
	for (idx_t source_idx = 0; source_idx < referenced_sources.size(); source_idx++) {
		if (!referenced_sources[source_idx] || source_idx >= second_binding.lhs_output_column_indices.size()) {
			continue;
		}
		if (second_binding.lhs_output_column_indices[source_idx] == input_col) {
			references_input_col = true;
			return true;
		}
	}
	return true;
}

static bool SljitProjectionMayReferenceSecondJoinInputCol(const SljitExecutableRegionExpression &projection,
                                                          const ExecutionHashJoinProbeBinding &second_binding,
                                                          idx_t input_col) {
	bool references_input_col;
	return !SljitTryProjectionReferencesSecondJoinInputCol(projection, second_binding, input_col,
	                                                       references_input_col) ||
	       references_input_col;
}

static bool SljitSecondJoinInputColUsedByLiveProjection(SljitExecutableRegionOp &second_join_projection_op,
                                                        const ExecutionHashJoinProbeBinding &second_binding,
                                                        idx_t input_col,
                                                        const vector<uint8_t> &second_projection_skip) {
	if (second_join_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    second_projection_skip.size() != second_join_projection_op.projections.size()) {
		return true;
	}
	for (idx_t projection_idx = 0; projection_idx < second_join_projection_op.projections.size(); projection_idx++) {
		if (second_projection_skip[projection_idx]) {
			continue;
		}
		if (SljitProjectionMayReferenceSecondJoinInputCol(second_join_projection_op.projections[projection_idx],
		                                                  second_binding, input_col)) {
			return true;
		}
	}
	return false;
}

static bool SljitEnsureNativeHashJoinProbeBinding(ExecutionOperatorRuntime &native_runtime,
                                                  SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
                                                  SljitExecutableRegionOp &hash_join_op, DataChunk &input) {
	if (scratch.HasOperatorBinding(hash_join_idx)) {
		return true;
	}
	ExecutionOperatorBinding *binding_ptr = nullptr;
	string deferred_reason;
	auto bind_result = SljitBindNativeOperator(
	    native_runtime, scratch, hash_join_idx, hash_join_op, input, hash_join_op.hash_join_probe.plan.operator_info,
	    "native-operator-runtime-deferred", "SLJIT native hash join probe", binding_ptr, deferred_reason);
	return bind_result == ExecutionOperatorBindResult::READY;
}

} // namespace duckdb
