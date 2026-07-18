//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_direct_aggregate_update_runtime_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_input_vector_update_runtime.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_grouped_primitive_aggregate_update_runtime.hpp"
#include "sljit_pending_preaggregated_group_batch_runtime.hpp"
#include "sljit_projected_input_grouped_aggregate_descriptor.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

class SljitGroupedDirectPrimitivePayloadUpdateRuntimeState {
public:
	bool Prepare(vector<SljitExecutableRegionOp> &ops, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		materialized_direct_descriptor_ready = false;
		materialized_direct_group_sources.clear();
		materialized_direct_payload_source_indices.clear();
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return true;
		}
		if (primitive.strategy != SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE) {
			return false;
		}
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			projected_direct_update = primitive.projected_direct_update;
			if (!projected_direct_update ||
			    !SljitProjectedInputGroupedAggregateCanUseSourceInput(*projected_direct_update)) {
				return false;
			}
			if (projected_direct_update->group_sources.size() == 1) {
				return direct_preaggregated_batch.ConfigureGroupOutputTransform(
				    projected_direct_update->group_sources[0]);
			}
			return direct_preaggregated_batch.ClearGroupOutputTransform();
		}
		if (primitive.aggregate_idx >= ops.size()) {
			return false;
		}
		if (!direct_preaggregated_batch.ClearGroupOutputTransform()) {
			return false;
		}
		materialized_direct_descriptor_ready = PrepareMaterializedDirectDescriptor(ops[primitive.aggregate_idx]);
		return true;
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		switch (primitive.strategy) {
		case SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE:
			return ExecuteDirectPrimitivePayloadUpdate(runtime, result, ops, scratch, primitive, input,
			                                           processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
			return ExecuteFilteredPrimitivePayloadUpdate(runtime, result, ops, scratch, primitive, input,
			                                             processed_batches);
		default:
			throw InternalException("SLJIT grouped direct update runtime received an unsupported strategy");
		}
	}

	bool Flush(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (!FlushDirectPreaggregatedBatch(runtime, ops, scratch, primitive)) {
			throw InternalException("SLJIT direct preaggregated grouped update flush failed");
		}
		return false;
	}

private:
	bool ExecuteDirectPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                         vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                         const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                         const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			RecordSljitRegionRuntimePath(runtime, ops[primitive.aggregate_idx].kind, "grouped_direct_projected_input",
			                             input.count);
			return ExecuteProjectedDirectPrimitivePayloadUpdate(runtime, ops, scratch, primitive, input,
			                                                    processed_batches);
		}
		RecordSljitRegionRuntimePath(runtime, ops[primitive.aggregate_idx].kind, "grouped_direct_materialized_input",
		                             input.count);
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT grouped aggregate direct update");
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		auto &bound_direct_update = scratch.AggregateBoundGroupedUpdate(primitive.aggregate_idx);
		SljitBindGroupedPrimitiveAggregateUpdate(native_runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                         input_chunk, bound_direct_update);
		SinkResultType sink_result;
		if (TryExecuteMaterializedDirectPendingPreaggregation(runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                                      bound_direct_update, input_chunk, input.selection,
		                                                      input.count)) {
			sink_result = SinkResultType::NEED_MORE_INPUT;
		} else {
			TransitionMaterializedDirectPendingToFallback(runtime, scratch, primitive.aggregate_idx, aggregate_op,
			                                              bound_direct_update);
			sink_result = SljitExecuteBoundGroupedPrimitiveAggregateUpdate(
			    runtime, scratch, bound_direct_update, input_chunk, input.selection, input.count, true);
		}
		sink_result = native_runtime.RecordSinkResult(input.count, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}

	bool PrepareMaterializedDirectDescriptor(SljitExecutableRegionOp &op) {
		if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			return false;
		}
		auto &sink_info = op.aggregate_update.plan.sink_info;
		auto &input_types = op.aggregate_update.plan.input_types;
		auto &payload_descriptors = op.aggregate_update.payload_descriptors;
		auto &payloads = op.aggregate_update.payloads;
		if (sink_info.groups.empty() || payload_descriptors.empty() || payload_descriptors.size() != payloads.size()) {
			return false;
		}
		materialized_direct_group_sources.reserve(sink_info.groups.size());
		for (auto &group : sink_info.groups) {
			if (group.input_index >= input_types.size() || input_types[group.input_index] != group.type) {
				materialized_direct_group_sources.clear();
				return false;
			}
			ExecutionRowPointerGroupKeySource group_source;
			SljitInitializeInputVectorGroupKeySource(group.input_index, group.type, group.type, group_source);
			group_source.ready = true;
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
			materialized_direct_group_sources.push_back(std::move(group_source));
		}

		materialized_direct_payload_source_indices.reserve(payload_descriptors.size());
		for (idx_t payload_idx = 0; payload_idx < payload_descriptors.size(); payload_idx++) {
			auto &descriptor = payload_descriptors[payload_idx];
			if (!descriptor.has_payload) {
				materialized_direct_payload_source_indices.push_back(DConstants::INVALID_INDEX);
				continue;
			}
			auto &payload = payloads[payload_idx].plan;
			if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
			    payload.source_index >= input_types.size() ||
			    input_types[payload.source_index] != payload.return_type ||
			    payload.return_type.InternalType() != descriptor.input_type) {
				return false;
			}
			materialized_direct_payload_source_indices.push_back(payload.source_index);
		}
		return true;
	}

	bool TryExecuteMaterializedDirectPendingPreaggregation(ExecutionRegionRuntime &runtime,
	                                                       SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                                       SljitExecutableRegionOp &op,
	                                                       SljitBoundGroupedPrimitiveAggregateUpdate &bound,
	                                                       DataChunk &input, const SelectionVector *selection,
	                                                       idx_t count) {
		if (!materialized_direct_descriptor_ready || selection != nullptr || count != input.size() ||
		    !bound.grouped_state || !bound.payload_lanes) {
			return false;
		}
		if (TryPreaggregateInputVectorPrimitiveGroupsIntoPending(
		        runtime, scratch, op_idx, op, input, materialized_direct_group_sources,
		        materialized_direct_payload_source_indices, *bound.payload_lanes, *bound.grouped_state,
		        direct_preaggregated_batch, false)) {
			RecordSljitRegionMaterializationElisionPath(
			    runtime, op.kind, "direct_materialized_pending_preaggregated_grouped_update", count);
			return true;
		}
		return false;
	}

	void TransitionMaterializedDirectPendingToFallback(ExecutionRegionRuntime &runtime,
	                                                   SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                                   SljitExecutableRegionOp &op,
	                                                   SljitBoundGroupedPrimitiveAggregateUpdate &bound) {
		if (!direct_preaggregated_batch.HasPending() && !direct_preaggregated_batch.proven_unique_append_active) {
			return;
		}
		if (!bound.grouped_state) {
			throw InternalException("SLJIT materialized direct pending grouped update lost its grouped state");
		}
		if (direct_preaggregated_batch.HasPending() &&
		    !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, direct_preaggregated_batch,
		                                                   *bound.grouped_state)) {
			throw InternalException("SLJIT materialized direct pending grouped update flush failed");
		}
		if (direct_preaggregated_batch.proven_unique_append_active) {
			SljitInvalidateProvenUniqueAppendContract(runtime, op, direct_preaggregated_batch, *bound.grouped_state);
		}
	}

	bool ExecuteFilteredPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                           vector<SljitExecutableRegionOp> &ops,
	                                           SljitRegionExecutionScratch &scratch,
	                                           const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                           const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		auto &input_chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT filtered grouped aggregate update");
		auto &aggregate_op = ops[primitive.aggregate_idx];
		if (aggregate_op.aggregate_update.filtered_update.kind ==
		    SljitFilteredAggregateKernelKind::PERFECT_HASH_GROUPED) {
			auto &native_runtime = runtime.ExecutionOperators();
			auto &bound = scratch.AggregateBoundGroupedUpdate(primitive.aggregate_idx);
			SljitBindRecordedGroupedPrimitiveAggregateUpdate(runtime, native_runtime, scratch, primitive.aggregate_idx,
			                                                 aggregate_op, input_chunk, bound);
			if (bound.strategy != SljitBoundGroupedAggregateStrategy::PERFECT_HASH_FUSED || !bound.grouped_state ||
			    !bound.payload_lanes || !bound.reduction_lanes || !bound.payload_scratch) {
				throw InternalException("SLJIT filtered perfect-hash aggregate binding is incomplete");
			}
			auto stage_start = SljitRegionStageStart(runtime);
			SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
			    aggregate_op.aggregate_update.filtered_update.payloads,
			    aggregate_op.aggregate_update.filtered_update.compiled.Function(),
			    aggregate_op.aggregate_update.plan.sink_info.groups,
			    aggregate_op.aggregate_update.plan.group_expressions,
			    aggregate_op.aggregate_update.group_source_not_null,
			    aggregate_op.aggregate_update.plan.sink_info.aggregate_contract,
			    aggregate_op.aggregate_update.payload_descriptors,
			    aggregate_op.aggregate_update.filtered_update.payload_source_layout,
			    aggregate_op.aggregate_update.filtered_update.input_source_indices,
			    aggregate_op.aggregate_update.filtered_update.input_source_not_null, *bound.payload_lanes,
			    *bound.reduction_lanes, bound.grouped_state->perfect_hash_layout, input_chunk, nullptr,
			    input_chunk.size(), *bound.payload_scratch);
			RecordSljitRegionStageRuntime(runtime, primitive.aggregate_idx, aggregate_op.kind,
			                              "filtered_perfect_hash_update", stage_start);
			RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind, "filtered_perfect_hash_update",
			                                            input_chunk.size());
			processed_batches++;
			return false;
		}
		auto &filter_op = ops[primitive.filter_idx];
		auto &filter_selection = scratch.FilterSelection(primitive.filter_idx);
		auto filter_stage_start = SljitRegionStageStart(runtime);
		auto selected_count = SljitSelectFilter(filter_op, input_chunk, filter_selection,
		                                        scratch.ExpressionAdapterScratch(primitive.filter_idx, 0));
		RecordSljitRegionStageRuntime(runtime, primitive.filter_idx, filter_op.kind, "selection", filter_stage_start);
		if (selected_count == 0) {
			processed_batches++;
			return false;
		}

		auto &native_runtime = runtime.ExecutionOperators();
		auto &bound_direct_update = scratch.AggregateBoundGroupedUpdate(primitive.aggregate_idx);
		SljitBindGroupedPrimitiveAggregateUpdate(native_runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                         input_chunk, bound_direct_update);
		auto sink_result = SljitExecuteBoundGroupedPrimitiveAggregateUpdate(
		    runtime, scratch, bound_direct_update, input_chunk, &filter_selection, selected_count, true);
		sink_result = native_runtime.RecordSinkResult(input_chunk, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}

	bool ExecuteProjectedDirectPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime,
	                                                  vector<SljitExecutableRegionOp> &ops,
	                                                  SljitRegionExecutionScratch &scratch,
	                                                  const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                                  const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		auto stage_start = SljitRegionStageStart(runtime);
		if (!projected_direct_update || !projected_direct_update->Ready()) {
			throw InternalException("SLJIT projected grouped aggregate direct update descriptor is not prepared");
		}
		if (input.HasHashJoinSelection()) {
			throw InternalException("SLJIT projected grouped aggregate direct update requires a source batch view");
		}
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT projected grouped aggregate direct update");
		if (!ProjectedDirectSourceInputMatches(input_chunk)) {
			throw InternalException("SLJIT projected grouped aggregate direct update input schema mismatch");
		}
		DataChunk *source_input = &input_chunk;
		if (input.selection) {
			projected_direct_selected_input.Ensure(runtime.GetAllocator(), input_chunk.GetTypes());
			auto &selected_input = projected_direct_selected_input.chunk;
			selected_input.Reset();
			selected_input.Slice(input_chunk, *input.selection, input.count);
			source_input = &selected_input;
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &group_sources = PrepareProjectedDirectGroupSources(aggregate_op, *source_input);
		if (TryExecuteProjectedDirectSourceInputPayloadUpdate(runtime, ops, scratch, primitive, *source_input,
		                                                      group_sources, processed_batches)) {
			RecordSljitRegionStageRuntime(runtime, primitive.aggregate_idx, aggregate_op.kind,
			                              "projected_source_input_grouped_update", stage_start);
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, primitive.aggregate_idx, aggregate_op.kind,
		                              "projected_source_input_grouped_update_miss", stage_start);
		const auto failure_reason =
		    projected_direct_failure_reason.empty() ? "unknown" : projected_direct_failure_reason.c_str();
		throw InternalException("SLJIT projected source-input grouped aggregate update rejected bound descriptor: %s",
		                        failure_reason);
	}

	bool ProjectedDirectSourceInputMatches(DataChunk &input) const {
		if (!projected_direct_update || input.ColumnCount() > projected_direct_update->input_types.size()) {
			return false;
		}
		for (idx_t column_idx = 0; column_idx < input.ColumnCount(); column_idx++) {
			if (input.data[column_idx].GetType() != projected_direct_update->input_types[column_idx]) {
				return false;
			}
		}
		for (auto &group_source : projected_direct_update->group_sources) {
			if (group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
			    group_source.input_vector_index >= input.ColumnCount() ||
			    input.data[group_source.input_vector_index].GetType() != group_source.source_type) {
				return false;
			}
		}
		for (auto source_idx : projected_direct_update->payload_source_indices) {
			if (source_idx == DConstants::INVALID_INDEX) {
				continue;
			}
			if (source_idx >= input.ColumnCount() ||
			    input.data[source_idx].GetType() != projected_direct_update->input_types[source_idx]) {
				return false;
			}
		}
		return true;
	}

	vector<ExecutionRowPointerGroupKeySource> &PrepareProjectedDirectGroupSources(SljitExecutableRegionOp &aggregate_op,
	                                                                              DataChunk &source_input) {
		projected_direct_group_sources = projected_direct_update->group_sources;
		SljitApplyExecutableIntegralGroupKeyRangeProofs(aggregate_op.aggregate_update.integral_group_key_ranges,
		                                                projected_direct_group_sources);
		SljitApplyInputVectorGroupBatchCastProofs(source_input, projected_direct_group_sources, source_input.size());
		return projected_direct_group_sources;
	}

	bool TryExecuteProjectedDirectSourceInputPayloadUpdate(
	    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	    const SljitGroupedAggregateUpdatePrimitive &primitive, DataChunk &source_input,
	    const vector<ExecutionRowPointerGroupKeySource> &group_sources, idx_t &processed_batches) {
		projected_direct_failure_reason.clear();
		if (!SljitProjectedInputGroupedAggregateCanUseSourceInput(*projected_direct_update)) {
			projected_direct_failure_reason = "descriptor";
			return false;
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
		if (aggregate_op.aggregate_update.dense_group_domain.ready) {
			dense_domain = &aggregate_op.aggregate_update.dense_group_domain;
		}
		string failure_reason;
		if (!SljitTryExecuteNativeInputVectorGroupedAggregateUpdate(
		        runtime, native_runtime, scratch, primitive.aggregate_idx, aggregate_op, source_input, group_sources,
		        projected_direct_update->payload_source_indices, projected_direct_update->payload_source_layout, true,
		        false, dense_domain, optional_ptr<string>(&failure_reason),
		        optional_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch>(&direct_preaggregated_batch),
		        optional_ptr<const vector<bool>>(&projected_direct_update->payload_source_not_null))) {
			auto unsupported = string("projected_source_input_grouped_update_unsupported.") +
			                   (failure_reason.empty() ? "unknown" : failure_reason);
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, unsupported.c_str(), source_input.size());
			projected_direct_failure_reason = failure_reason.empty() ? "unknown" : failure_reason;
			return false;
		}
		RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind, "projected_source_input_grouped_update",
		                                            source_input.size());
		processed_batches++;
		return true;
	}

	bool FlushDirectPreaggregatedBatch(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                   SljitRegionExecutionScratch &scratch,
	                                   const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (!direct_preaggregated_batch.HasPending()) {
			return true;
		}
		if (primitive.aggregate_idx >= ops.size()) {
			throw InternalException("SLJIT direct preaggregated grouped update has no aggregate operator");
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &binding = scratch.SinkBinding(primitive.aggregate_idx);
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
		    !binding.aggregate_update.grouped_state.state) {
			throw InternalException("SLJIT direct preaggregated grouped update has no grouped state");
		}
		return SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                                     direct_preaggregated_batch,
		                                                     binding.aggregate_update.grouped_state);
	}

private:
	shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update;
	SljitDataChunkBatch projected_direct_selected_input;
	SljitPendingPreaggregatedPrimitiveGroupBatch direct_preaggregated_batch;
	bool materialized_direct_descriptor_ready = false;
	vector<ExecutionRowPointerGroupKeySource> materialized_direct_group_sources;
	vector<idx_t> materialized_direct_payload_source_indices;
	vector<ExecutionRowPointerGroupKeySource> projected_direct_group_sources;
	string projected_direct_failure_reason;
};

} // namespace duckdb
