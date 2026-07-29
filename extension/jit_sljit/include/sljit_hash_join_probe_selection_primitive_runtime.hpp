//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_selection_primitive_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_consumer_result.hpp"
#include "sljit_hash_join_probe_drain_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_runtime_batch_state.hpp"
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

	template <class EXECUTE_HASH_JOIN_PROBE, class EXECUTE_NEXT_STEP, class TRY_EXECUTE_DIRECT_CONSUMER>
	bool Execute(const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe, EXECUTE_NEXT_STEP &&execute_next_step,
	             optional_ptr<const SljitHashJoinDirectAggregateConsumerContract> direct_consumer_contract,
	             SljitHashJoinAggregateConsumerDispatch &direct_consumer_dispatch,
	             TRY_EXECUTE_DIRECT_CONSUMER &&try_execute_direct_consumer) {
		auto &primitive = step.hash_join_probe_selection;
		string deferred_reason;
		DataChunk *join_input_ptr = nullptr;
		if (input.HasHashJoinSelection()) {
			if (!selected_hash_join_inputs.TryPrepareHashProbeInput(primitive.hash_join_idx, input, join_input_ptr,
			                                                        deferred_reason)) {
				if (!deferred_reason.empty()) {
					return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
				}
				throw InternalException("SLJIT hash probe could not prepare selected upstream hash-join input");
			}
		} else if (input.selection) {
			auto &source = SljitBindRuntimeBatchInput(input, "SLJIT selected hash join probe input");
			selected_source_input.Ensure(runtime.GetAllocator(), source.GetTypes());
			auto &selected = selected_source_input.chunk;
			selected.Reset();
			selected.Slice(source, *input.selection, input.count);
			join_input_ptr = &selected;
			RecordSljitRegionRuntimePath(runtime, ops[primitive.hash_join_idx].kind, "selected_source_view",
			                             input.count);
		} else {
			join_input_ptr = &SljitBindMaterializedRuntimeBatchInput(input, "SLJIT hash join selection primitive");
		}
		auto &join_input = *join_input_ptr;
		if (join_input.size() == 0) {
			return false;
		}
		const auto hash_join_idx = primitive.hash_join_idx;
		auto &hash_join_op = ops[hash_join_idx];
		if (direct_consumer_contract &&
		    direct_consumer_dispatch != SljitHashJoinAggregateConsumerDispatch::MATERIALIZED) {
			auto direct_result =
			    try_execute_direct_consumer(*direct_consumer_contract, primitive, join_input, execute_hash_join_probe);
			if (direct_result.status == SljitHashJoinAggregateConsumerStatus::DEFERRED) {
				return SljitDeferFullPipelineResult(runtime, direct_result.deferred_reason, result);
			}
			if (direct_result.status == SljitHashJoinAggregateConsumerStatus::EMPTY) {
				return false;
			}
			if (direct_result.status == SljitHashJoinAggregateConsumerStatus::EXECUTED) {
				if (direct_consumer_dispatch == SljitHashJoinAggregateConsumerDispatch::UNBOUND) {
					direct_consumer_dispatch = SljitHashJoinAggregateConsumerDispatch::DIRECT;
				}
				return false;
			}
			if (direct_consumer_dispatch == SljitHashJoinAggregateConsumerDispatch::UNBOUND) {
				direct_consumer_dispatch = SljitHashJoinAggregateConsumerDispatch::MATERIALIZED;
				runtime.RecordJitRuntimePath("hash_join_probe.direct_aggregate_consumer.materialized_dispatch");
			} else if (direct_consumer_dispatch == SljitHashJoinAggregateConsumerDispatch::DIRECT) {
				direct_consumer_dispatch = SljitHashJoinAggregateConsumerDispatch::HYBRID;
				runtime.RecordJitRuntimePath("hash_join_probe.direct_aggregate_consumer.hybrid_dispatch");
			}
		}
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		const auto output_column_map = primitive.HasOutputColumnMap() ? &primitive.output_column_map : nullptr;
		auto handle_output = [&](DataChunk &output, SljitHashJoinProbeDrainState &state) {
			auto output_view = SljitRuntimeBatchViewFromHashJoinSelection(
			    join_input, scratch.FilterSelection(hash_join_idx), scratch.HashJoinBuildSelection(hash_join_idx),
			    scratch.HashJoinRowPointers(hash_join_idx), output.size(), hash_join_idx, state.output_proof,
			    output_column_map, primitive.output_projection_idx);
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
	SljitDataChunkBatch selected_source_input;
};

} // namespace duckdb
