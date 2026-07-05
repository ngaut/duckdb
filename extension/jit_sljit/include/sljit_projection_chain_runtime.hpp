//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_chain_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_executable_expression_codegen.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_projection_composition.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projection_reference_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/common/types.hpp"

namespace duckdb {

struct SljitProjectionChainPrimitive {
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	shared_ptr<SljitExecutableRegionOp> bound_composed_projection;

	bool HasBoundComposedProjection() const {
		return bound_composed_projection != nullptr;
	}
};

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

static bool SljitBuildProjectionChainComposedProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker = nullptr);

static bool SljitCanBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx,
                                                 idx_t final_projection_idx) {
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx) {
		return false;
	}
	for (idx_t op_idx = first_projection_idx; op_idx <= final_projection_idx; op_idx++) {
		if (ops[op_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
	}
	return true;
}

static bool SljitCanBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t projection_idx) {
	return SljitCanBindProjectionChainPrimitive(ops, projection_idx, projection_idx);
}

static SljitProjectionChainPrimitive SljitBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                       idx_t first_projection_idx,
                                                                       idx_t final_projection_idx) {
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
		throw InternalException("SLJIT projection-chain primitive cannot bind requested operator");
	}
	SljitProjectionChainPrimitive primitive;
	primitive.first_projection_idx = first_projection_idx;
	primitive.final_projection_idx = final_projection_idx;
	if (first_projection_idx != final_projection_idx) {
		auto composed_projection = make_shared_ptr<SljitExecutableRegionOp>();
		if (SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
		                                                *composed_projection)) {
			primitive.bound_composed_projection = std::move(composed_projection);
		}
	}
	return primitive;
}

static SljitProjectionChainPrimitive
SljitBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t projection_idx,
                                  shared_ptr<SljitExecutableRegionOp> bound_projection) {
	if (!SljitCanBindProjectionChainPrimitive(ops, projection_idx) || !bound_projection ||
	    bound_projection->kind != SljitNativeRegionOpKind::PROJECTION) {
		throw InternalException("SLJIT projection-chain primitive cannot bind requested projection override");
	}
	SljitProjectionChainPrimitive primitive;
	primitive.first_projection_idx = projection_idx;
	primitive.final_projection_idx = projection_idx;
	primitive.bound_composed_projection = std::move(bound_projection);
	return primitive;
}

static SljitProjectionChainPrimitive SljitBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                       idx_t projection_idx) {
	return SljitBindProjectionChainPrimitive(ops, projection_idx, projection_idx);
}

static bool SljitTryPrepareSelectedHashJoinProjectionChainInput(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
    idx_t projection_idx, SljitExecutableRegionOp &semantic_projection, const SljitRuntimeBatchView &input,
    SljitDataChunkBatch &selected_hash_join_input, DataChunk *&input_chunk,
    unique_ptr<SljitExecutableRegionOp> &mapped_projection, optional_ptr<SljitExecutableRegionOp> &projection_op) {
	if (!input.HasHashJoinSelection()) {
		return false;
	}
	if (input.hash_join_idx >= ops.size() || projection_idx >= ops.size()) {
		throw InternalException("SLJIT selected projection-chain input has invalid operator indexes");
	}
	if (!scratch.HasOperatorBinding(input.hash_join_idx)) {
		throw InternalException("SLJIT selected projection-chain input has no hash-join binding");
	}
	auto &binding = scratch.OperatorBinding(input.hash_join_idx).hash_join_probe;
	if (!binding.ready || binding.output_types.empty()) {
		throw InternalException("SLJIT selected projection-chain input has an incomplete hash-join binding");
	}

	projection_op = &semantic_projection;
	if (input.hash_join_output_column_map) {
		string blocker;
		mapped_projection = make_uniq<SljitExecutableRegionOp>();
		if (!SljitTryBuildHashJoinMappedProjection(*input.hash_join_output_column_map, binding, semantic_projection,
		                                           *mapped_projection, optional_ptr<string>(&blocker))) {
			if (blocker.empty()) {
				blocker = "mapped_projection";
			}
			throw InternalException("SLJIT selected projection-chain input could not map projection sources: %s",
			                        blocker.c_str());
		}
		projection_op = mapped_projection.get();
	}

	vector<uint8_t> referenced_columns;
	if (!SljitBuildProjectionSourceColumnSet(*projection_op, binding.output_types.size(), nullptr, nullptr,
	                                         referenced_columns)) {
		throw InternalException("SLJIT selected projection-chain input could not collect projection sources");
	}

	selected_hash_join_input.Ensure(runtime.GetAllocator(), binding.output_types);
	auto &materialized_input = selected_hash_join_input.chunk;
	materialized_input.Reset();
	if (!SljitTryMaterializeSelectedHashJoinOutputColumns(binding, input, referenced_columns, materialized_input)) {
		throw InternalException("SLJIT selected projection-chain input could not materialize hash-join output columns");
	}
	input_chunk = &materialized_input;
	return materialized_input.size() > 0;
}

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
	if (!primitive.HasBoundComposedProjection()) {
		projection_op = nullptr;
		return false;
	}
	projection_op = primitive.bound_composed_projection.get();
	return true;
}

template <class EXECUTE_OUTPUT_BATCH>
static bool SljitAppendProjectionChainOutputBatch(ExecutionRegionRuntime &runtime, DataChunk &batch,
                                                  DataChunk &output, idx_t trace_projection_idx,
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
	                                          "batch_append", nullptr, flush_output_batch, execute_output_batch);
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

static bool SljitMaterializeProjectionChainStep(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                vector<SljitExecutableRegionOp> &ops, idx_t projection_idx,
                                                const SljitRuntimeBatchView &input,
                                                SljitDataChunkBatch &selected_hash_join_input, DataChunk &output) {
	DataChunk *source_chunk;
	unique_ptr<SljitExecutableRegionOp> mapped_projection;
	optional_ptr<SljitExecutableRegionOp> projection_op;
	const SelectionVector *selection = input.selection;
	idx_t count = input.count;
	if (input.HasHashJoinSelection()) {
		auto &semantic_projection = ops[projection_idx];
		if (!SljitTryPrepareSelectedHashJoinProjectionChainInput(runtime, scratch, ops, projection_idx,
		                                                        semantic_projection, input, selected_hash_join_input,
		                                                        source_chunk, mapped_projection, projection_op)) {
			return false;
		}
		selection = nullptr;
		count = source_chunk->size();
	} else {
		source_chunk = &SljitBindRuntimeBatchInput(input, "SLJIT projection-chain primitive");
		if (input.count == 0) {
			return false;
		}
		projection_op = &ops[projection_idx];
	}
	output.Reset();
	SljitExecuteProjection(scratch, projection_idx, *projection_op, *source_chunk, output, selection, count);
	return output.size() > 0;
}

template <class EXECUTE_OUTPUT_BATCH>
static bool SljitExecuteProjectionChainPrimitiveSequential(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
    const SljitProjectionChainPrimitive &primitive, const SljitRuntimeBatchView &input,
    SljitDataChunkBatch &projection_chain_batch, SljitDataChunkBatch &selected_hash_join_input,
    EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
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
	projection_chain_batch.Ensure(runtime.GetAllocator(), ops[primitive.final_projection_idx].output_types);
	return SljitAppendProjectionChainOutputBatch(runtime, projection_chain_batch.chunk, *final_output,
	                                             primitive.final_projection_idx, ops[primitive.final_projection_idx],
	                                             execute_output_batch);
}

template <class EXECUTE_OUTPUT_BATCH>
static bool SljitExecuteProjectionChainPrimitive(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                 vector<SljitExecutableRegionOp> &ops,
                                                 const SljitProjectionChainPrimitive &primitive,
                                                 const SljitRuntimeBatchView &input,
                                                 SljitDataChunkBatch &projection_chain_batch,
                                                 SljitDataChunkBatch &selected_hash_join_input,
                                                 SljitDataChunkBatch &synthetic_projection_output,
                                                 optional_ptr<SljitProjectionChainSyntheticProjectionScratch>
                                                     synthetic_projection_scratch,
                                                 EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
	DataChunk *source_chunk;
	unique_ptr<SljitExecutableRegionOp> mapped_projection;
	optional_ptr<SljitExecutableRegionOp> projection_op;
	const SelectionVector *selection = input.selection;
	idx_t count = input.count;
	if (!SljitResolveBoundProjectionChain(ops, primitive, projection_op)) {
		return SljitExecuteProjectionChainPrimitiveSequential(runtime, scratch, ops, primitive, input,
		                                                      projection_chain_batch, selected_hash_join_input,
		                                                      execute_output_batch);
	}
	if (input.HasHashJoinSelection()) {
		if (!SljitTryPrepareSelectedHashJoinProjectionChainInput(runtime, scratch, ops, primitive.final_projection_idx,
		                                                        *projection_op, input, selected_hash_join_input,
		                                                        source_chunk, mapped_projection, projection_op)) {
			return false;
		}
		selection = nullptr;
		count = source_chunk->size();
	} else {
		source_chunk = &SljitBindRuntimeBatchInput(input, "SLJIT projection-chain primitive");
		if (input.count == 0) {
			return false;
		}
	}
	if (synthetic_projection_scratch) {
		synthetic_projection_scratch->Ensure(*projection_op);
	}
	projection_chain_batch.Ensure(runtime.GetAllocator(), projection_op->output_types);
	auto &batch = projection_chain_batch.chunk;
	auto flush_output_batch = [&]() -> bool {
		return SljitFlushDataChunkBatch(batch, execute_output_batch);
	};

	if (SljitProjectionHasVariableWidthOutput(*projection_op)) {
		DataChunk *reference_output_ptr;
		if (synthetic_projection_scratch) {
			synthetic_projection_output.Ensure(runtime.GetAllocator(), projection_op->output_types);
			reference_output_ptr = &synthetic_projection_output.chunk;
		} else {
			reference_output_ptr = &scratch.TemporaryChunk(primitive.final_projection_idx);
		}
		auto &reference_output = *reference_output_ptr;
		reference_output.Reset();
		if (SljitTrySliceReferenceProjection(reference_output, *source_chunk, *projection_op, selection, count)) {
			if (batch.size() > 0 && flush_output_batch()) {
				return true;
			}
			auto handoff_stage_start = SljitRegionStageStart(runtime);
			RecordSljitRegionStageRuntime(runtime, primitive.final_projection_idx, projection_op->kind,
			                              "reference_view_handoff", handoff_stage_start);
			return execute_output_batch(reference_output);
		}
	}

	if (batch.size() + count > STANDARD_VECTOR_SIZE && flush_output_batch()) {
		return true;
	}

	bool direct_materialized = false;
	if (!selection && count == source_chunk->size()) {
		if (synthetic_projection_scratch) {
			direct_materialized = SljitTryDirectMaterializeFixedProjectionToBatch(
			    runtime, *synthetic_projection_scratch, primitive.final_projection_idx, *projection_op, *source_chunk,
			    batch);
		} else {
			direct_materialized = SljitTryDirectMaterializeFixedProjectionToBatch(
			    runtime, scratch, primitive.final_projection_idx, *projection_op, *source_chunk, batch);
		}
	}
	if (!direct_materialized) {
		auto append_stage_start = SljitRegionStageStart(runtime);
		auto execute_selection = selection ? selection : FlatVector::IncrementalSelectionVector();
		if (!SljitTryAppendReferenceProjectionToBatch(batch, *source_chunk, *projection_op, *execute_selection,
		                                              count)) {
			DataChunk *filtered_ptr;
			if (synthetic_projection_scratch) {
				synthetic_projection_output.Ensure(runtime.GetAllocator(), projection_op->output_types);
				filtered_ptr = &synthetic_projection_output.chunk;
			} else {
				filtered_ptr = &scratch.TemporaryChunk(primitive.final_projection_idx);
			}
			auto &filtered = *filtered_ptr;
			filtered.Reset();
			if (synthetic_projection_scratch) {
				SljitExecuteProjection(*synthetic_projection_scratch, primitive.final_projection_idx, *projection_op,
				                       *source_chunk, filtered, selection, count);
			} else {
				SljitExecuteProjection(scratch, primitive.final_projection_idx, *projection_op, *source_chunk, filtered,
				                       selection, count);
			}
			if (!SljitTryFastAppendFixedFlatAllValid(batch, filtered)) {
				batch.Append(filtered);
			}
		}
		RecordSljitRegionStageRuntime(runtime, primitive.final_projection_idx, projection_op->kind, "batch_append",
		                              append_stage_start);
	}
	return batch.size() == STANDARD_VECTOR_SIZE && flush_output_batch();
}

static bool SljitOutputTypesAreFixedWidth(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (!TypeIsConstantSize(type.InternalType())) {
			return false;
		}
	}
	return true;
}

static bool SljitProjectionOutputsAreFixedWidth(const SljitExecutableRegionOp &projection_op) {
	return projection_op.kind == SljitNativeRegionOpKind::PROJECTION &&
	       SljitOutputTypesAreFixedWidth(projection_op.output_types);
}

static bool SljitBuildReferenceProjectionOutputMap(const SljitExecutableRegionOp &projection_op,
                                                   const SljitExecutableRegionOp &reference_projection_op,
                                                   vector<idx_t> &output_to_projection) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    reference_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    !SljitProjectionOutputsAreFixedWidth(reference_projection_op)) {
		return false;
	}
	output_to_projection.clear();
	output_to_projection.reserve(reference_projection_op.projections.size());
	for (auto &reference_expr : reference_projection_op.projections) {
		auto &reference = reference_expr.plan;
		if (reference.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    reference.source_index >= projection_op.projections.size() ||
		    reference.return_type != projection_op.projections[reference.source_index].plan.return_type) {
			output_to_projection.clear();
			return false;
		}
		output_to_projection.push_back(reference.source_index);
	}
	return true;
}

static bool SljitBuildProjectionChainComposedProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker) {
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx ||
	    ops[first_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    ops[final_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
		if (blocker) {
			*blocker = "shape";
		}
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> current_projection;
	auto &first_projection = ops[first_projection_idx];
	current_projection.reserve(first_projection.projections.size());
	for (auto &projection : first_projection.projections) {
		current_projection.push_back(projection.plan.Copy(true, false));
	}

	for (idx_t projection_idx = first_projection_idx + 1; projection_idx <= final_projection_idx; projection_idx++) {
		auto &projection_op = ops[projection_idx];
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
			if (blocker) {
				*blocker = "chain_shape";
			}
			return false;
		}
		vector<SljitNativeRegionExpressionPlan> next_projection;
		next_projection.reserve(projection_op.projections.size());
		for (idx_t output_idx = 0; output_idx < projection_op.projections.size(); output_idx++) {
			auto &projection = projection_op.projections[output_idx];
			SljitNativeRegionExpressionPlan composed;
			if (!TryComposeNativeProjection(current_projection, projection.plan, composed, false)) {
				if (blocker) {
					*blocker = "compose_output_" + to_string(output_idx);
				}
				return false;
			}
			if (output_idx >= projection_op.output_types.size() ||
			    composed.return_type != projection_op.output_types[output_idx]) {
				if (blocker) {
					*blocker = "return_type";
				}
				return false;
			}
			next_projection.push_back(std::move(composed));
		}
		current_projection = std::move(next_projection);
	}

	auto &final_projection = ops[final_projection_idx];
	composed_projection = SljitExecutableRegionOp();
	composed_projection.kind = SljitNativeRegionOpKind::PROJECTION;
	composed_projection.operator_index = final_projection.operator_index;
	composed_projection.input_types = ops[first_projection_idx].input_types;
	composed_projection.output_types = final_projection.output_types;
	composed_projection.output_not_null = final_projection.output_not_null;
		composed_projection.projections.reserve(current_projection.size());
		for (idx_t output_idx = 0; output_idx < current_projection.size(); output_idx++) {
			auto &projection_plan = current_projection[output_idx];
			SljitExecutableRegionExpression projection;
			SljitPrepareExecutableRegionExpression(projection_plan, projection, nullptr, true);
			string compile_error;
			if (!SljitCompilePreparedExecutableRegionExpression(projection, false, compile_error)) {
				if (blocker) {
					*blocker = "compile_output_" + to_string(output_idx);
				}
				return false;
			}
			composed_projection.projections.push_back(std::move(projection));
		}
	return composed_projection.projections.size() == final_projection.projections.size();
}

static bool SljitTryResolveReferenceThroughProjectionChain(const vector<SljitExecutableRegionOp> &ops,
                                                           idx_t first_projection_idx, idx_t aggregate_idx,
                                                           const SljitExecutableRegionExpression &source_expr,
                                                           idx_t &join_output_source_idx, LogicalType &source_type) {
	SljitExecutableRegionExpression remapped_source;
	idx_t source_idx;
	if (!SljitTryBuildSingleSourceProjectionExpression(source_expr, remapped_source, source_idx) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_source.plan)) {
		return false;
	}
	source_type = remapped_source.plan.return_type;
	for (idx_t op_idx = aggregate_idx; op_idx > first_projection_idx; op_idx--) {
		auto &projection_op = ops[op_idx - 1];
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    source_idx >= projection_op.projections.size() || source_idx >= projection_op.output_types.size() ||
		    projection_op.output_types[source_idx] != source_type) {
			return false;
		}
		SljitExecutableRegionExpression remapped_projection;
		idx_t next_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[source_idx], remapped_projection,
		                                                   next_source_idx) ||
		    !SljitProjectionIsSingleSourceReferenceLike(remapped_projection.plan) ||
		    remapped_projection.plan.return_type != source_type) {
			return false;
		}
		source_idx = next_source_idx;
	}
	join_output_source_idx = source_idx;
	return true;
}

static bool SljitTryBuildRemappedPayloadReference(const SljitExecutableRegionExpression &payload,
                                                  idx_t payload_input_idx,
                                                  SljitExecutableRegionExpression &remapped_payload,
                                                  idx_t &join_output_source_idx) {
	if (!SljitTryBuildSingleSourceProjectionExpression(payload, remapped_payload, join_output_source_idx) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_payload.plan)) {
		return false;
	}
	remapped_payload.input_source_indices.clear();
	remapped_payload.input_source_not_null.clear();
	if (remapped_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		remapped_payload.plan.source_index = payload_input_idx;
		remapped_payload.plan.expression_tree_source_indices.clear();
		return true;
	}
	remapped_payload.plan.expression_tree_source_indices.clear();
	remapped_payload.plan.expression_tree_source_indices.push_back(payload_input_idx);
	return true;
}

} // namespace duckdb
