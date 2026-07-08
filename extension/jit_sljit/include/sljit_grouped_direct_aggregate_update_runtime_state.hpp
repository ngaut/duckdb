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
	bool Prepare(const SljitGroupedAggregateUpdatePrimitive &primitive) {
		deferred_grouped_finish = false;
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return true;
		}
		if (primitive.strategy != SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE) {
			return false;
		}
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			projected_direct_update = primitive.projected_direct_update;
			return projected_direct_update &&
			       SljitProjectedInputGroupedAggregateCanUseSourceInput(*projected_direct_update);
		}
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
		if (!FlushProjectedDirectPreaggregatedBatch(runtime, ops, scratch, primitive)) {
			throw InternalException("SLJIT projected direct preaggregated grouped update flush failed");
		}
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx, deferred_grouped_finish);
		return false;
	}

private:
	bool ExecuteDirectPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                         vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                         const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                         const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			return ExecuteProjectedDirectPrimitivePayloadUpdate(runtime, ops, scratch, primitive, input,
			                                                    processed_batches);
		}
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT grouped aggregate direct update");
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		SljitBindGroupedPrimitiveAggregateUpdate(native_runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                         input_chunk, bound_direct_update);
		auto sink_result = SljitExecuteBoundGroupedPrimitiveAggregateUpdate(
		    runtime, scratch, bound_direct_update, input_chunk, input.selection, input.count, true,
		    optional_ptr<bool>(&deferred_grouped_finish));
		sink_result = native_runtime.RecordSinkResult(input.count, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx,
			                                          deferred_grouped_finish);
			return true;
		}
		processed_batches++;
		return false;
	}

	bool ExecuteFilteredPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                           vector<SljitExecutableRegionOp> &ops,
	                                           SljitRegionExecutionScratch &scratch,
	                                           const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                           const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		auto &input_chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT filtered grouped aggregate update");
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

		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		SljitBindGroupedPrimitiveAggregateUpdate(native_runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                         input_chunk, bound_direct_update);
		auto sink_result = SljitExecuteBoundGroupedPrimitiveAggregateUpdate(
		    runtime, scratch, bound_direct_update, input_chunk, &filter_selection, selected_count, true,
		    optional_ptr<bool>(&deferred_grouped_finish));
		sink_result = native_runtime.RecordSinkResult(input_chunk, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx,
			                                          deferred_grouped_finish);
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
		auto &group_sources = PrepareProjectedDirectGroupSources(aggregate_op);
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

	bool ProjectedDenseDomainProvesGroupCast(SljitExecutableRegionOp &aggregate_op,
	                                         const ExecutionRowPointerGroupKeySource &group_source) {
		auto &domain = aggregate_op.aggregate_update.dense_group_domain;
		if (!domain.ready || domain.physical_type != group_source.target_physical_type) {
			return false;
		}
		return SljitGroupKeyNarrowingIntegralCast(group_source.cast_kind);
	}

	vector<ExecutionRowPointerGroupKeySource> &
	PrepareProjectedDirectGroupSources(SljitExecutableRegionOp &aggregate_op) {
		projected_direct_group_sources = projected_direct_update->group_sources;
		if (projected_direct_group_sources.size() == 1 &&
		    ProjectedDenseDomainProvesGroupCast(aggregate_op, projected_direct_group_sources[0])) {
			projected_direct_group_sources[0].unchecked_integral_cast = true;
		}
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
		        projected_direct_update->payload_source_indices, true, optional_ptr<bool>(&deferred_grouped_finish),
		        false, dense_domain, optional_ptr<string>(&failure_reason),
		        optional_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch>(&projected_direct_preaggregated_batch),
		        optional_ptr<const vector<bool>>(&projected_direct_update->payload_source_not_null))) {
			auto unsupported = string("projected_source_input_grouped_update_unsupported.") +
			                   (failure_reason.empty() ? "unknown" : failure_reason);
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, unsupported.c_str(), source_input.size());
			projected_direct_failure_reason = failure_reason.empty() ? "unknown" : failure_reason;
			return false;
		}
		RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
		                                            "projected_source_input_grouped_update", source_input.size());
		processed_batches++;
		return true;
	}

	bool FlushProjectedDirectPreaggregatedBatch(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                            SljitRegionExecutionScratch &scratch,
	                                            const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (projected_direct_preaggregated_batch.Empty()) {
			return true;
		}
		if (primitive.aggregate_idx >= ops.size()) {
			throw InternalException("SLJIT projected direct preaggregated grouped update has no aggregate operator");
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &binding = scratch.SinkBinding(primitive.aggregate_idx);
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
		    !binding.aggregate_update.grouped_state.state) {
			throw InternalException("SLJIT projected direct preaggregated grouped update has no grouped state");
		}
		return SljitFlushPendingPreaggregatedPrimitiveGroups(
		    runtime, scratch, primitive.aggregate_idx, aggregate_op, projected_direct_preaggregated_batch,
		    binding.aggregate_update.grouped_state, optional_ptr<bool>(&deferred_grouped_finish));
	}

private:
	bool deferred_grouped_finish = false;
	shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update;
	SljitDataChunkBatch projected_direct_selected_input;
	SljitPendingPreaggregatedPrimitiveGroupBatch projected_direct_preaggregated_batch;
	vector<ExecutionRowPointerGroupKeySource> projected_direct_group_sources;
	string projected_direct_failure_reason;
	SljitBoundGroupedPrimitiveAggregateUpdate bound_direct_update;
};

} // namespace duckdb
