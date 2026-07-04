//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_chain_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

static SljitProjectionChainPrimitive SljitBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                       idx_t projection_idx) {
	return SljitBindProjectionChainPrimitive(ops, projection_idx, projection_idx);
}

static bool SljitBindProjectionChainInput(const SljitRuntimeBatchView &input, DataChunk *&input_chunk) {
	if (!input.HasChunk()) {
		throw InternalException("SLJIT projection-chain primitive requires an input chunk");
	}
	input_chunk = &input.Chunk();
	if (input.count > input_chunk->size()) {
		throw InternalException("SLJIT projection-chain primitive count exceeds input chunk cardinality");
	}
	if (!input.selection && input.count != input_chunk->size()) {
		throw InternalException("SLJIT projection-chain primitive requires a selection for partial chunk input");
	}
	return input.count > 0;
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
		if (!SljitBindProjectionChainInput(input, source_chunk)) {
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
		if (!SljitBindProjectionChainInput(input, source_chunk)) {
			return false;
		}
	}
	projection_chain_batch.Ensure(runtime.GetAllocator(), projection_op->output_types);
	auto &batch = projection_chain_batch.chunk;
	auto flush_output_batch = [&]() -> bool {
		return SljitFlushDataChunkBatch(batch, execute_output_batch);
	};

	if (SljitProjectionHasVariableWidthOutput(*projection_op)) {
		auto &reference_output = scratch.TemporaryChunk(primitive.final_projection_idx);
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

	const bool direct_materialized =
	    !selection && count == source_chunk->size() &&
	    SljitTryDirectMaterializeFixedProjectionToBatch(runtime, scratch, primitive.final_projection_idx,
	                                                    *projection_op, *source_chunk, batch);
	if (!direct_materialized) {
		auto append_stage_start = SljitRegionStageStart(runtime);
		auto execute_selection = selection ? selection : FlatVector::IncrementalSelectionVector();
		if (!SljitTryAppendReferenceProjectionToBatch(batch, *source_chunk, *projection_op, *execute_selection,
		                                              count)) {
			auto &filtered = scratch.TemporaryChunk(primitive.final_projection_idx);
			filtered.Reset();
			SljitExecuteProjection(scratch, primitive.final_projection_idx, *projection_op, *source_chunk, filtered,
			                       selection, count);
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

static bool SljitTryResolveProjectionChainReferenceSource(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t first_projection_idx, idx_t projection_idx,
                                                          idx_t output_idx, idx_t &join_output_source_idx,
                                                          LogicalType &source_type) {
	if (first_projection_idx >= ops.size() || projection_idx >= ops.size() || first_projection_idx > projection_idx ||
	    ops[projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    output_idx >= ops[projection_idx].projections.size()) {
		return false;
	}

	SljitExecutableRegionExpression remapped_reference;
	idx_t previous_source_idx;
	auto &projection_expr = ops[projection_idx].projections[output_idx];
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_expr, remapped_reference, previous_source_idx) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_reference.plan)) {
		return false;
	}
	if (projection_idx == first_projection_idx) {
		join_output_source_idx = previous_source_idx;
		source_type = remapped_reference.plan.return_type;
		return true;
	}

	LogicalType resolved_type;
	if (!SljitTryResolveProjectionChainReferenceSource(ops, first_projection_idx, projection_idx - 1,
	                                                   previous_source_idx, join_output_source_idx, resolved_type) ||
	    resolved_type != remapped_reference.plan.return_type) {
		return false;
	}
	source_type = std::move(resolved_type);
	return true;
}

static bool SljitBuildProjectionChainReferenceSourceMap(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t projection_idx,
                                                        vector<idx_t> &source_map) {
	if (first_projection_idx >= ops.size() || projection_idx >= ops.size() || first_projection_idx > projection_idx ||
	    ops[projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	auto &projection_op = ops[projection_idx];
	source_map.assign(projection_op.projections.size(), DConstants::INVALID_INDEX);
	for (idx_t output_idx = 0; output_idx < projection_op.projections.size(); output_idx++) {
		idx_t join_output_source_idx;
		LogicalType source_type;
		if (!SljitTryResolveProjectionChainReferenceSource(ops, first_projection_idx, projection_idx, output_idx,
		                                                   join_output_source_idx, source_type)) {
			continue;
		}
		if (source_type != projection_op.projections[output_idx].plan.return_type) {
			return false;
		}
		source_map[output_idx] = join_output_source_idx;
	}
	return true;
}

static bool SljitTryBuildProjectionChainExpression(const vector<SljitExecutableRegionOp> &ops,
                                                   idx_t first_projection_idx, idx_t projection_idx, idx_t output_idx,
                                                   SljitExecutableRegionExpression &target,
                                                   optional_ptr<string> blocker = nullptr) {
	if (first_projection_idx >= ops.size() || projection_idx >= ops.size() || first_projection_idx > projection_idx ||
	    ops[projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    output_idx >= ops[projection_idx].projections.size()) {
		if (blocker) {
			*blocker = "expression_bounds";
		}
		return false;
	}
	auto &projection_expr = ops[projection_idx].projections[output_idx];
	if (projection_idx == first_projection_idx) {
		SljitBuildBorrowedProjectionExpression(projection_expr, target);
		return true;
	}

	SljitExecutableRegionExpression remapped_reference;
	idx_t previous_source_idx;
	if (SljitTryBuildSingleSourceProjectionExpression(projection_expr, remapped_reference, previous_source_idx) &&
	    SljitProjectionIsSingleSourceReferenceLike(remapped_reference.plan)) {
		return SljitTryBuildProjectionChainExpression(ops, first_projection_idx, projection_idx - 1,
		                                              previous_source_idx, target, blocker);
	}

	vector<idx_t> source_map;
	if (!SljitBuildProjectionChainReferenceSourceMap(ops, first_projection_idx, projection_idx - 1, source_map)) {
		if (blocker) {
			*blocker = "reference_source_map";
		}
		return false;
	}
	SljitBuildBorrowedProjectionExpression(projection_expr, target);
	if (!SljitTryRemapHashJoinProjectionPlanSources(source_map, target.plan)) {
		if (blocker) {
			*blocker = "remap_plan_sources";
		}
		return false;
	}
	if (!SljitTryRemapHashJoinProjectionExpressionInputSources(source_map, target)) {
		if (blocker) {
			*blocker = "remap_input_sources";
		}
		return false;
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
	auto &final_projection = ops[final_projection_idx];
	composed_projection = SljitExecutableRegionOp();
	composed_projection.kind = SljitNativeRegionOpKind::PROJECTION;
	composed_projection.operator_index = final_projection.operator_index;
	composed_projection.input_types = ops[first_projection_idx].input_types;
	composed_projection.output_types = final_projection.output_types;
	composed_projection.output_not_null = final_projection.output_not_null;
	composed_projection.projections.reserve(final_projection.projections.size());
	for (idx_t output_idx = 0; output_idx < final_projection.projections.size(); output_idx++) {
		SljitExecutableRegionExpression expression;
		if (!SljitTryBuildProjectionChainExpression(ops, first_projection_idx, final_projection_idx, output_idx,
		                                            expression, blocker)) {
			composed_projection.projections.clear();
			return false;
		}
		if (expression.plan.return_type != final_projection.projections[output_idx].plan.return_type) {
			if (blocker) {
				*blocker = "return_type";
			}
			composed_projection.projections.clear();
			return false;
		}
		composed_projection.projections.push_back(std::move(expression));
	}
	return composed_projection.projections.size() == final_projection.projections.size();
}

static bool SljitBuildProjectionChainSemanticProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker = nullptr) {
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
	for (auto &projection_plan : current_projection) {
		SljitExecutableRegionExpression projection;
		projection.plan = projection_plan.Copy(true, false);
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
