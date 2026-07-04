//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_selection_primitive_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_native_tail_handoff_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"

namespace duckdb {

class SljitHashJoinProbeSelectionPrimitiveRuntime {
public:
	SljitHashJoinProbeSelectionPrimitiveRuntime(ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p,
	                                            vector<SljitExecutableRegionOp> &ops_p,
	                                            SljitRegionExecutionScratch &scratch_p,
	                                            SljitSelectedHashJoinInputRuntime &selected_hash_join_inputs_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p), scratch(scratch_p),
	      selected_hash_join_inputs(selected_hash_join_inputs_p) {
	}

	template <class EXECUTE_HASH_JOIN_PROBE, class EXECUTE_NEXT_STEP>
	bool Execute(const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe, EXECUTE_NEXT_STEP &&execute_next_step) {
		string deferred_reason;
		DataChunk *join_input_ptr = nullptr;
		if (input.HasHashJoinSelection()) {
			if (!selected_hash_join_inputs.TryPrepareHashProbeInput(step.Op(0), input, join_input_ptr,
			                                                        deferred_reason)) {
				if (!deferred_reason.empty()) {
					return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
				}
				throw InternalException("SLJIT hash probe could not prepare selected upstream hash-join input");
			}
		} else {
			join_input_ptr = &SljitBindNativeTailHandoffInput(input);
		}
		auto &join_input = *join_input_ptr;
		if (join_input.size() == 0) {
			return false;
		}
		auto &primitive = step.hash_join_probe_selection;
		const auto hash_join_idx = primitive.hash_join_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		const auto output_column_map = primitive.HasOutputColumnMap() ? &primitive.output_column_map : nullptr;
		auto handle_output = [&](DataChunk &output, SljitHashJoinProbeDrainState &state) {
			auto output_view = SljitRuntimeBatchViewFromHashJoinSelection(
			    join_input, scratch.FilterSelection(hash_join_idx), scratch.HashJoinBuildSelection(hash_join_idx),
			    scratch.HashJoinRowPointers(hash_join_idx), output.size(), hash_join_idx,
			    state.source_key0_int64_to_int32_matches_are_proven, output_column_map,
			    primitive.output_projection_idx);
			return execute_next_step(output_view);
		};
		auto handle_defer = [&](string &deferred_reason) {
			return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
		};
		return SljitDrainHashJoinProbeOutputsWithState(
		    scratch, hash_join_idx, hash_join_op, join_input, join_output, execute_hash_join_probe, handle_output,
		    handle_defer, primitive.source_key0_int64_to_int32_unchecked,
		    SljitHashJoinProbeOutputContract::SELECTED_VIEW,
		    optional_ptr<const SljitHashJoinProbeInputRemap>(&primitive.input_remap));
	}

private:
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	SljitSelectedHashJoinInputRuntime &selected_hash_join_inputs;
};

} // namespace duckdb
