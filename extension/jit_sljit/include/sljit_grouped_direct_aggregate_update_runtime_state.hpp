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
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_projection_expression_runtime.hpp"
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
			       SljitProjectedInputGroupedAggregateCanUseCompactInput(*projected_direct_update);
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

	bool Flush(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	           const SljitGroupedAggregateUpdatePrimitive &primitive) {
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx, deferred_grouped_finish);
		return false;
	}

private:
	bool ExecuteDirectPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                         vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                         const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                         const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			return ExecuteProjectedDirectPrimitivePayloadUpdate(runtime, result, ops, scratch, primitive, input,
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

	bool ExecuteProjectedDirectPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                                  vector<SljitExecutableRegionOp> &ops,
	                                                  SljitRegionExecutionScratch &scratch,
	                                                  const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                                  const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		(void)result;
		if (!projected_direct_update || !projected_direct_update->Ready()) {
			throw InternalException("SLJIT projected grouped aggregate direct update descriptor is not prepared");
		}
		if (input.HasHashJoinSelection()) {
			throw InternalException("SLJIT projected grouped aggregate direct update requires a source batch view");
		}
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT projected grouped aggregate direct update");
		if (input_chunk.ColumnCount() != projected_direct_update->input_types.size()) {
			throw InternalException("SLJIT projected grouped aggregate direct update input schema mismatch");
		}
		DataChunk *source_input = &input_chunk;
		if (input.selection) {
			projected_direct_selected_input.Ensure(runtime.GetAllocator(), projected_direct_update->input_types);
			auto &selected_input = projected_direct_selected_input.chunk;
			selected_input.Reset();
			selected_input.Slice(input_chunk, *input.selection, input.count);
			source_input = &selected_input;
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &group_sources = PrepareProjectedDirectGroupSources(aggregate_op);
		if (TryExecuteProjectedDirectSourceInputPayloadUpdate(runtime, result, ops, scratch, primitive, *source_input,
		                                                      group_sources, processed_batches)) {
			return false;
		}
		auto &aggregate_input =
		    BuildProjectedDirectAggregateInput(runtime, ops, primitive, *source_input, group_sources);
		if (aggregate_op.aggregate_update.dense_group_domain.ready) {
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_dense_group_domain",
			                             aggregate_input.size());
		}
		auto &native_runtime = runtime.ExecutionOperators();
		SljitBindGroupedPrimitiveAggregateUpdate(native_runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                         aggregate_input, bound_projected_direct_update);
		auto sink_result = SljitExecuteBoundGroupedPrimitiveAggregateUpdate(
		    runtime, scratch, bound_projected_direct_update, aggregate_input, nullptr, aggregate_input.size(), true,
		    optional_ptr<bool>(&deferred_grouped_finish));
		sink_result = runtime.ExecutionOperators().RecordSinkResult(aggregate_input, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx,
			                                          deferred_grouped_finish);
			return true;
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_input_vector_grouped_update",
		                             aggregate_input.size());
		processed_batches++;
		return false;
	}

	bool ProjectedDenseDomainProvesGroupCast(SljitExecutableRegionOp &aggregate_op,
	                                         const ExecutionRowPointerGroupKeySource &group_source) {
		auto &domain = aggregate_op.aggregate_update.dense_group_domain;
		if (!domain.ready || domain.physical_type != group_source.target_physical_type) {
			return false;
		}
		switch (group_source.cast_kind) {
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
			return true;
		default:
			return false;
		}
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
	    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	    SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive,
	    DataChunk &source_input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
	    idx_t &processed_batches) {
		(void)result;
		if (!SljitProjectedInputGroupedAggregateCanUseSourceInput(*projected_direct_update)) {
			return false;
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		if (!aggregate_op.aggregate_update.grouped_direct_update.DirectStateAddressPayloadOnly()) {
			return false;
		}
		if (aggregate_op.aggregate_update.dense_group_domain.ready) {
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_dense_group_domain",
			                             source_input.size());
		}
		auto &native_runtime = runtime.ExecutionOperators();
		optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
		if (aggregate_op.aggregate_update.dense_group_domain.ready) {
			dense_domain = &aggregate_op.aggregate_update.dense_group_domain;
		}
		string failure_reason;
		if (!SljitTryExecuteNativeInputVectorGroupedAggregateUpdate(
		        runtime, native_runtime, scratch, primitive.aggregate_idx, aggregate_op, source_input, group_sources,
		        projected_direct_update->payload_source_indices, true, optional_ptr<bool>(&deferred_grouped_finish),
		        false, dense_domain, optional_ptr<string>(&failure_reason))) {
			auto unsupported = string("direct_projected_source_input_grouped_update_unsupported.") +
			                   (failure_reason.empty() ? "unknown" : failure_reason);
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, unsupported.c_str(), source_input.size());
			return false;
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_source_input_grouped_update",
		                             source_input.size());
		processed_batches++;
		return true;
	}

	DataChunk &BuildProjectedDirectAggregateInput(ExecutionRegionRuntime &runtime,
	                                              const vector<SljitExecutableRegionOp> &ops,
	                                              const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                              DataChunk &source_input,
	                                              const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
		auto &projection = projected_direct_update->projection;
		projected_direct_aggregate_input.Ensure(runtime.GetAllocator(), projection.output_types);
		auto &aggregate_input = projected_direct_aggregate_input.chunk;
		aggregate_input.Reset();
		if (aggregate_input.ColumnCount() != projection.output_types.size()) {
			throw InternalException("SLJIT projected compact aggregate input schema mismatch");
		}
		for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
			auto &group = sink_info.groups[group_idx];
			if (group.input_index >= aggregate_input.ColumnCount() || group_idx >= group_sources.size()) {
				throw InternalException("SLJIT projected compact aggregate group source mismatch");
			}
			if (!SljitTryMaterializeInputVectorGroupSource(source_input, group_sources[group_idx],
			                                               aggregate_input.data[group.input_index], source_input.size(),
			                                               false)) {
				throw InternalException("SLJIT projected compact aggregate group materialization failed");
			}
		}
		if (projected_direct_update->payload_projection_indices.size() !=
		    projected_direct_update->payload_source_indices.size()) {
			throw InternalException("SLJIT projected compact aggregate payload mapping mismatch");
		}
		projected_direct_payload_scratch.resize(projection.projections.size());
		vector<uint8_t> initialized_columns(aggregate_input.ColumnCount(), 0);
		for (auto &group : sink_info.groups) {
			if (group.input_index < initialized_columns.size()) {
				initialized_columns[group.input_index] = 1;
			}
		}
		for (idx_t payload_idx = 0; payload_idx < projected_direct_update->payload_projection_indices.size();
		     payload_idx++) {
			const auto projection_idx = projected_direct_update->payload_projection_indices[payload_idx];
			const auto source_idx = projected_direct_update->payload_source_indices[payload_idx];
			if (projection_idx == DConstants::INVALID_INDEX) {
				continue;
			}
			if (projection_idx >= aggregate_input.ColumnCount() || projection_idx >= projection.projections.size()) {
				throw InternalException("SLJIT projected compact aggregate payload projection mismatch");
			}
			if (initialized_columns[projection_idx]) {
				continue;
			}
			if (source_idx != DConstants::INVALID_INDEX) {
				if (source_idx >= source_input.ColumnCount() ||
				    aggregate_input.data[projection_idx].GetType() != source_input.data[source_idx].GetType()) {
					throw InternalException("SLJIT projected compact aggregate payload source mismatch");
				}
				aggregate_input.data[projection_idx].Reference(source_input.data[source_idx]);
				initialized_columns[projection_idx] = 1;
				continue;
			}
			if (projection.output_types[projection_idx] != aggregate_input.data[projection_idx].GetType()) {
				throw InternalException("SLJIT projected compact aggregate payload source mismatch");
			}
			SljitExecuteProjectionExpression(projection.projections[projection_idx], source_input,
			                                 aggregate_input.data[projection_idx], nullptr, source_input.size(),
			                                 projected_direct_payload_scratch[projection_idx]);
			initialized_columns[projection_idx] = 1;
		}
		aggregate_input.SetChildCardinality(source_input.size());
		RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind, "projected_compact_aggregate_input",
		                                         source_input.size());
		return aggregate_input;
	}

private:
	bool deferred_grouped_finish = false;
	shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update;
	SljitDataChunkBatch projected_direct_selected_input;
	SljitDataChunkBatch projected_direct_aggregate_input;
	vector<ExecutionRowPointerGroupKeySource> projected_direct_group_sources;
	vector<SljitExpressionAdapterScratch> projected_direct_payload_scratch;
	SljitBoundGroupedPrimitiveAggregateUpdate bound_direct_update;
	SljitBoundGroupedPrimitiveAggregateUpdate bound_projected_direct_update;
};

} // namespace duckdb
