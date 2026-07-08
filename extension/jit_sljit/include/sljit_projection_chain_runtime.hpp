//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_chain_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_chain_primitive.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projection_reference_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"

#include "duckdb/common/types.hpp"

namespace duckdb {

struct SljitProjectionChainSyntheticProjectionScratch {
	void Ensure(const SljitExecutableRegionOp &projection_op) {
		expression_adapter_scratch.resize(projection_op.projections.size());
	}

	SljitProjectionAdapterScratch &ProjectionScratch(idx_t op_idx) {
		(void)op_idx;
		return projection_adapter_scratch;
	}

	SljitExpressionAdapterScratch &ExpressionAdapterScratch(idx_t op_idx, idx_t expression_idx) {
		(void)op_idx;
		if (expression_idx >= expression_adapter_scratch.size()) {
			throw InternalException("SLJIT synthetic projection expression has no adapter scratch");
		}
		return expression_adapter_scratch[expression_idx];
	}

	SljitProjectionAdapterScratch projection_adapter_scratch;
	vector<SljitExpressionAdapterScratch> expression_adapter_scratch;
};

static bool SljitResolveBoundProjectionChain(vector<SljitExecutableRegionOp> &ops,
                                             const SljitProjectionChainPrimitive &primitive,
                                             optional_ptr<SljitExecutableRegionOp> &projection_op) {
	if (primitive.HasBoundComposedProjection()) {
		projection_op = primitive.bound_composed_projection.get();
		return true;
	}
	if (primitive.first_projection_idx == primitive.final_projection_idx) {
		projection_op = &ops[primitive.final_projection_idx];
		return true;
	}
	projection_op = nullptr;
	return false;
}

template <class EXECUTE_OUTPUT_BATCH>
static bool SljitAppendProjectionChainOutputBatch(ExecutionRegionRuntime &runtime, DataChunk &batch, DataChunk &output,
                                                  idx_t trace_projection_idx,
                                                  const SljitExecutableRegionOp &trace_projection_op,
                                                  EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
	if (output.size() == 0) {
		return false;
	}
	auto flush_output_batch = [&]() -> bool {
		return SljitFlushDataChunkBatch(batch, execute_output_batch);
	};
	return SljitAppendChunkToInitializedBatch(runtime, batch, output, trace_projection_idx,
	                                          optional_ptr<const SljitExecutableRegionOp>(&trace_projection_op),
	                                          "projection_buffer_append", flush_output_batch, execute_output_batch);
}

static bool SljitProjectionHasVariableWidthOutput(const SljitExecutableRegionOp &projection_op) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	for (auto &type : projection_op.output_types) {
		if (!TypeIsConstantSize(type.InternalType())) {
			return true;
		}
	}
	return false;
}

struct SljitPreparedProjectionChainInput {
	DataChunk *source_chunk = nullptr;
	optional_ptr<SljitExecutableRegionOp> projection_op;
	unique_ptr<SljitExecutableRegionOp> mapped_projection;
	const SelectionVector *selection = nullptr;
	idx_t count = 0;
};

static bool SljitPrepareProjectionChainInput(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                             vector<SljitExecutableRegionOp> &ops, idx_t projection_idx,
                                             SljitExecutableRegionOp &semantic_projection,
                                             const SljitRuntimeBatchView &input,
                                             SljitDataChunkBatch &selected_hash_join_input,
                                             SljitPreparedProjectionChainInput &prepared) {
	prepared = SljitPreparedProjectionChainInput();
	prepared.selection = input.selection;
	prepared.count = input.count;
	if (input.HasHashJoinSelection()) {
		if (!SljitTryPrepareSelectedHashJoinProjectionInput(
		        runtime, scratch, ops, projection_idx, semantic_projection, input, selected_hash_join_input,
		        prepared.source_chunk, prepared.mapped_projection, prepared.projection_op)) {
			return false;
		}
		prepared.selection = nullptr;
		prepared.count = prepared.source_chunk->size();
		return true;
	}
	prepared.source_chunk = &SljitBindRuntimeBatchInput(input, "SLJIT projection-chain primitive");
	if (input.count == 0) {
		return false;
	}
	prepared.projection_op = &semantic_projection;
	return true;
}

static bool SljitMaterializeProjectionChainStep(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                vector<SljitExecutableRegionOp> &ops, idx_t projection_idx,
                                                const SljitRuntimeBatchView &input,
                                                SljitDataChunkBatch &selected_hash_join_input, DataChunk &output) {
	SljitPreparedProjectionChainInput prepared;
	if (!SljitPrepareProjectionChainInput(runtime, scratch, ops, projection_idx, ops[projection_idx], input,
	                                      selected_hash_join_input, prepared)) {
		return false;
	}
	output.Reset();
	SljitExecuteProjection(scratch, projection_idx, *prepared.projection_op, *prepared.source_chunk, output,
	                        prepared.selection, prepared.count);
	return output.size() > 0;
}

template <class EXECUTE_OUTPUT_BATCH>
static bool SljitExecuteProjectionChainPrimitiveSequential(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
    const SljitProjectionChainPrimitive &primitive, const SljitRuntimeBatchView &input,
    SljitDataChunkBatch &projection_chain_batch, SljitDataChunkBatch &selected_hash_join_input,
    bool direct_handoff, EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
	SljitRuntimeBatchView current_input = input;
	DataChunk *final_output = nullptr;
	for (idx_t projection_idx = primitive.first_projection_idx; projection_idx <= primitive.final_projection_idx;
	     projection_idx++) {
		auto &output = scratch.TemporaryChunk(projection_idx);
		if (!SljitMaterializeProjectionChainStep(runtime, scratch, ops, projection_idx, current_input,
		                                         selected_hash_join_input, output)) {
			return false;
		}
		final_output = &output;
		current_input = SljitRuntimeBatchViewFromChunk(output);
	}
	if (!final_output || final_output->size() == 0) {
		return false;
	}
	if (direct_handoff) {
		return execute_output_batch(*final_output);
	}
	projection_chain_batch.Ensure(runtime.GetAllocator(), ops[primitive.final_projection_idx].output_types);
	return SljitAppendProjectionChainOutputBatch(runtime, projection_chain_batch.chunk, *final_output,
	                                             primitive.final_projection_idx, ops[primitive.final_projection_idx],
	                                             execute_output_batch);
}

template <class EXECUTE_OUTPUT_BATCH>
static bool SljitExecuteProjectionChainPrimitiveDirectHandoff(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
    const SljitProjectionChainPrimitive &primitive, SljitExecutableRegionOp &bound_projection_op,
    DataChunk &source_chunk, const SelectionVector *selection, idx_t count,
    SljitDataChunkBatch &synthetic_projection_output,
    optional_ptr<SljitProjectionChainSyntheticProjectionScratch> synthetic_projection_scratch,
    EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
	DataChunk *output_ptr;
	if (synthetic_projection_scratch) {
		synthetic_projection_output.Ensure(runtime.GetAllocator(), bound_projection_op.output_types);
		output_ptr = &synthetic_projection_output.chunk;
	} else {
		output_ptr = &scratch.TemporaryChunk(primitive.final_projection_idx);
	}
	auto &output = *output_ptr;
	output.Reset();
	if (SljitProjectionHasVariableWidthOutput(bound_projection_op) &&
	    SljitTrySliceReferenceProjection(output, source_chunk, bound_projection_op, selection, count)) {
		auto handoff_stage_start = SljitRegionStageStart(runtime);
		RecordSljitRegionStageRuntime(runtime, primitive.final_projection_idx, bound_projection_op.kind,
		                              "reference_view_handoff", handoff_stage_start);
		return output.size() > 0 && execute_output_batch(output);
	}

	bool direct_materialized = false;
	if (!selection && count == source_chunk.size()) {
		if (synthetic_projection_scratch) {
			direct_materialized = SljitTryDirectMaterializeFixedProjectionToBatch(
			    runtime, *synthetic_projection_scratch, primitive.final_projection_idx, bound_projection_op,
			    source_chunk, output);
		} else {
			direct_materialized = SljitTryDirectMaterializeFixedProjectionToBatch(
			    runtime, scratch, primitive.final_projection_idx, bound_projection_op, source_chunk, output);
		}
	}
	if (!direct_materialized) {
		if (synthetic_projection_scratch) {
			SljitExecuteProjection(*synthetic_projection_scratch, primitive.final_projection_idx, bound_projection_op,
			                       source_chunk, output, selection, count);
		} else {
			SljitExecuteProjection(scratch, primitive.final_projection_idx, bound_projection_op, source_chunk, output,
			                       selection, count);
		}
	}
	return output.size() > 0 && execute_output_batch(output);
}

template <class EXECUTE_OUTPUT_BATCH>
static bool SljitExecuteProjectionChainPrimitive(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
    const SljitProjectionChainPrimitive &primitive, const SljitRuntimeBatchView &input,
    SljitDataChunkBatch &projection_chain_batch, SljitDataChunkBatch &selected_hash_join_input,
    SljitDataChunkBatch &synthetic_projection_output,
    optional_ptr<SljitProjectionChainSyntheticProjectionScratch> synthetic_projection_scratch,
    bool direct_handoff, EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
	optional_ptr<SljitExecutableRegionOp> projection_op;
	if (!SljitResolveBoundProjectionChain(ops, primitive, projection_op)) {
		return SljitExecuteProjectionChainPrimitiveSequential(runtime, scratch, ops, primitive, input,
		                                                      projection_chain_batch, selected_hash_join_input,
		                                                      direct_handoff,
		                                                      execute_output_batch);
	}
	SljitPreparedProjectionChainInput prepared;
	if (!SljitPrepareProjectionChainInput(runtime, scratch, ops, primitive.final_projection_idx, *projection_op, input,
	                                      selected_hash_join_input, prepared)) {
		return false;
	}
	auto &bound_projection_op = *prepared.projection_op;
	auto &source_chunk = *prepared.source_chunk;
	const auto selection = prepared.selection;
	const auto count = prepared.count;
	if (synthetic_projection_scratch) {
		synthetic_projection_scratch->Ensure(bound_projection_op);
	}
	if (direct_handoff) {
		return SljitExecuteProjectionChainPrimitiveDirectHandoff(
		    runtime, scratch, primitive, bound_projection_op, source_chunk, selection, count,
		    synthetic_projection_output, synthetic_projection_scratch, execute_output_batch);
	}
	projection_chain_batch.Ensure(runtime.GetAllocator(), bound_projection_op.output_types);
	auto &batch = projection_chain_batch.chunk;
	auto flush_output_batch = [&]() -> bool {
		return SljitFlushDataChunkBatch(batch, execute_output_batch);
	};

	if (SljitProjectionHasVariableWidthOutput(bound_projection_op)) {
		DataChunk *reference_output_ptr;
		if (synthetic_projection_scratch) {
			synthetic_projection_output.Ensure(runtime.GetAllocator(), bound_projection_op.output_types);
			reference_output_ptr = &synthetic_projection_output.chunk;
		} else {
			reference_output_ptr = &scratch.TemporaryChunk(primitive.final_projection_idx);
		}
		auto &reference_output = *reference_output_ptr;
		reference_output.Reset();
		if (SljitTrySliceReferenceProjection(reference_output, source_chunk, bound_projection_op, selection, count)) {
			if (batch.size() > 0 && flush_output_batch()) {
				return true;
			}
			auto handoff_stage_start = SljitRegionStageStart(runtime);
			RecordSljitRegionStageRuntime(runtime, primitive.final_projection_idx, bound_projection_op.kind,
			                              "reference_view_handoff", handoff_stage_start);
			return execute_output_batch(reference_output);
		}
	}

	if (batch.size() + count > STANDARD_VECTOR_SIZE && flush_output_batch()) {
		return true;
	}

	bool direct_materialized = false;
	if (!selection && count == source_chunk.size()) {
		if (synthetic_projection_scratch) {
			direct_materialized = SljitTryDirectMaterializeFixedProjectionToBatch(
			    runtime, *synthetic_projection_scratch, primitive.final_projection_idx, bound_projection_op,
			    source_chunk, batch);
		} else {
			direct_materialized = SljitTryDirectMaterializeFixedProjectionToBatch(
			    runtime, scratch, primitive.final_projection_idx, bound_projection_op, source_chunk, batch);
		}
	}
	if (!direct_materialized) {
		auto append_stage_start = SljitRegionStageStart(runtime);
		auto execute_selection = selection ? selection : FlatVector::IncrementalSelectionVector();
		if (!SljitTryAppendReferenceProjectionToBatch(batch, source_chunk, bound_projection_op, *execute_selection,
		                                              count)) {
			DataChunk *filtered_ptr;
			if (synthetic_projection_scratch) {
				synthetic_projection_output.Ensure(runtime.GetAllocator(), bound_projection_op.output_types);
				filtered_ptr = &synthetic_projection_output.chunk;
			} else {
				filtered_ptr = &scratch.TemporaryChunk(primitive.final_projection_idx);
			}
			auto &filtered = *filtered_ptr;
			filtered.Reset();
			if (synthetic_projection_scratch) {
				SljitExecuteProjection(*synthetic_projection_scratch, primitive.final_projection_idx,
				                       bound_projection_op, source_chunk, filtered, selection, count);
			} else {
				SljitExecuteProjection(scratch, primitive.final_projection_idx, bound_projection_op, source_chunk,
				                       filtered, selection, count);
			}
			if (!SljitTryFastAppendFixedAllValid(batch, filtered)) {
				batch.Append(filtered);
			}
		}
		RecordSljitRegionStageRuntime(runtime, primitive.final_projection_idx, bound_projection_op.kind,
		                              "projection_buffer_append", append_stage_start);
	}
	return batch.size() == STANDARD_VECTOR_SIZE && flush_output_batch();
}

} // namespace duckdb
