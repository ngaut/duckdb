//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_generated_filter_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"

namespace duckdb {

struct SljitGeneratedFilterPrimitive {
	idx_t filter_idx = 0;
};

struct SljitSelectedHashJoinFilterCache {
	vector<idx_t> source_map;
	unique_ptr<SljitExecutableRegionOp> mapped_filter;
};

static bool SljitCanBindGeneratedFilterPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t filter_idx) {
	return filter_idx < ops.size() && ops[filter_idx].kind == SljitNativeRegionOpKind::FILTER && ops[filter_idx].filter;
}

static bool SljitTryBindGeneratedFilterPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t filter_idx,
                                                 SljitGeneratedFilterPrimitive &primitive) {
	if (!SljitCanBindGeneratedFilterPrimitive(ops, filter_idx)) {
		return false;
	}
	SljitGeneratedFilterPrimitive candidate;
	candidate.filter_idx = filter_idx;
	primitive = candidate;
	return true;
}

static SljitGeneratedFilterPrimitive SljitBindGeneratedFilterPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                       idx_t filter_idx) {
	SljitGeneratedFilterPrimitive primitive;
	if (!SljitTryBindGeneratedFilterPrimitive(ops, filter_idx, primitive)) {
		throw InternalException("SLJIT generated filter primitive cannot bind requested operator");
	}
	return primitive;
}

static bool SljitExecuteGeneratedFilterPrimitive(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                 vector<SljitExecutableRegionOp> &ops,
                                                 const SljitGeneratedFilterPrimitive &primitive,
                                                 const SljitRuntimeBatchView &input, SljitRuntimeBatchView &output) {
	auto &source_chunk = SljitBindRuntimeBatchInput(input, "SLJIT generated filter");
	if (input.count == 0) {
		return false;
	}

	auto &filter_op = ops[primitive.filter_idx];
	D_ASSERT(filter_op.filter && filter_op.filter->expression.HasSelectionKernel());
	auto &filter_selection = scratch.FilterSelection(primitive.filter_idx);
	auto filter_stage_start = SljitRegionStageStart(runtime);
	auto selected_count =
	    SljitSelectFilter(filter_op, source_chunk, filter_selection,
	                      scratch.ExpressionAdapterScratch(primitive.filter_idx, 0), input.selection, input.count);
	RecordSljitRegionStageRuntime(runtime, primitive.filter_idx, filter_op.kind, "selection", filter_stage_start);
	// Rows-weighted SIMD coverage: traces answer "how much filter input ran
	// through the packed prefix" without a dedicated benchmark.
	RecordSljitRegionRuntimePath(runtime, filter_op.kind,
	                             filter_op.filter->expression.predicate_partial_simd ? "selection.partial_simd"
	                                                                                 : "selection.scalar",
	                             input.count);
	if (selected_count == 0) {
		return false;
	}
	if (selected_count == input.count) {
		output = input;
		return true;
	}
	const auto selected = &filter_selection;
	output = SljitRuntimeBatchViewFromChunk(source_chunk, selected, selected_count);
	return true;
}

static SljitExecutableRegionOp &SljitBindSelectedHashJoinFilterOp(const SljitRuntimeHashJoinSelection &selected,
                                                                  ExecutionHashJoinProbeBinding &source_binding,
                                                                  SljitExecutableRegionOp &filter_op,
                                                                  SljitSelectedHashJoinFilterCache &cache) {
	auto output_column_map = selected.OutputColumnMap();
	if (!output_column_map) {
		return filter_op;
	}
	if (!cache.mapped_filter || cache.source_map != *output_column_map) {
		string blocker;
		auto mapped_filter = make_uniq<SljitExecutableRegionOp>();
		if (!SljitTryBuildHashJoinMappedFilter(*output_column_map, source_binding, filter_op, *mapped_filter,
		                                       optional_ptr<string>(&blocker))) {
			if (blocker.empty()) {
				blocker = "mapped_filter";
			}
			throw InternalException("SLJIT selected hash-join filter could not map filter sources: %s",
			                        blocker.c_str());
		}
		cache.source_map = *output_column_map;
		cache.mapped_filter = std::move(mapped_filter);
	}
	return *cache.mapped_filter;
}

static void SljitCompactSelectedHashJoinViewInPlace(SljitRegionExecutionScratch &scratch,
                                                    const SljitRuntimeHashJoinSelection &selected,
                                                    const SelectionVector &filter_selection, idx_t selected_count) {
	if (selected_count == selected.count) {
		return;
	}
	auto &match_selection = scratch.FilterSelection(selected.hash_join_idx);
	if (selected.ExactSourceFilterMatches()) {
		auto &source_match_selection = selected.MatchSelection();
		for (idx_t row_idx = 0; row_idx < selected_count; row_idx++) {
			match_selection.set_index(row_idx, source_match_selection.get_index(filter_selection.get_index(row_idx)));
		}
		return;
	}
	auto &build_selection = scratch.HashJoinBuildSelection(selected.hash_join_idx);
	auto &row_pointers = scratch.HashJoinRowPointers(selected.hash_join_idx);
	row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	auto row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(row_pointers);
	for (idx_t row_idx = 0; row_idx < selected_count; row_idx++) {
		const auto source_idx = filter_selection.get_index(row_idx);
		match_selection.set_index(row_idx, match_selection.get_index(source_idx));
		build_selection.set_index(row_idx, build_selection.get_index(source_idx));
		row_pointer_data[row_idx] = row_pointer_data[source_idx];
	}
	FlatVector::SetSize(row_pointers, count_t(selected_count));
}

static bool SljitExecuteSelectedHashJoinGeneratedFilterPrimitive(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
    const SljitGeneratedFilterPrimitive &primitive, const SljitRuntimeBatchView &input,
    SljitDataChunkBatch &selected_hash_join_filter_input, SljitSelectedHashJoinFilterCache &cache,
    SljitRuntimeBatchView &output) {
	auto selected = input.BindHashJoinSelection("SLJIT selected hash-join generated filter");
	if (selected.count == 0) {
		return false;
	}
	auto source_binding =
	    SljitTryGetSelectedHashJoinSourceBinding(input, ops.size(), scratch, "SLJIT selected hash-join filter");
	if (!source_binding) {
		throw InternalException("SLJIT selected hash-join filter has no source binding");
	}
	auto &filter_op = ops[primitive.filter_idx];
	auto &mapped_filter = SljitBindSelectedHashJoinFilterOp(selected, *source_binding, filter_op, cache);

	vector<uint8_t> referenced_columns;
	D_ASSERT(mapped_filter.filter);
	if (!SljitTryCollectHashJoinProjectionExpressionSources(mapped_filter.filter->expression,
	                                                        source_binding->output_types.size(), referenced_columns)) {
		throw InternalException("SLJIT selected hash-join filter could not collect filter sources");
	}
	selected_hash_join_filter_input.Ensure(runtime.GetAllocator(), source_binding->output_types);
	auto &filter_input = selected_hash_join_filter_input.chunk;
	filter_input.Reset();
	auto materialize_stage_start = SljitRegionStageStart(runtime);
	auto materialized = ExecuteSljitRegionRecordedOperation(
	    runtime, selected.hash_join_idx, ops[selected.hash_join_idx].kind, "selected_view_materialization",
	    materialize_stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    (void)recorder;
		    return SljitTryMaterializeSelectedHashJoinOutputColumns(*source_binding, input, referenced_columns,
		                                                            filter_input);
	    });
	if (!materialized) {
		RecordSljitRegionStageRuntime(runtime, selected.hash_join_idx, ops[selected.hash_join_idx].kind,
		                              "selected_view_materialization_miss", materialize_stage_start);
		throw InternalException("SLJIT selected hash-join filter could not materialize filter input");
	}
	RecordSljitRegionStageRuntime(runtime, selected.hash_join_idx, ops[selected.hash_join_idx].kind,
	                              "selected_view_materialization", materialize_stage_start);
	RecordSljitRegionRuntimeDelegation(runtime, ops[selected.hash_join_idx].kind, "selected_view_materialization",
	                                   filter_input.size());

	auto &filter_selection = scratch.FilterSelection(primitive.filter_idx);
	auto selection_stage_start = SljitRegionStageStart(runtime);
	auto selected_count = SljitSelectFilter(mapped_filter, filter_input, filter_selection,
	                                        scratch.ExpressionAdapterScratch(primitive.filter_idx, 0));
	RecordSljitRegionStageRuntime(runtime, primitive.filter_idx, filter_op.kind, "selected_hash_join_selection",
	                              selection_stage_start);
	if (selected_count == 0) {
		return false;
	}
	const bool selection_compacted = selected_count != selected.count;
	SljitCompactSelectedHashJoinViewInPlace(scratch, selected, filter_selection, selected_count);
	auto output_proof = selected.output_proof;
	if (selection_compacted) {
		output_proof.SetExplicitMatchSelection();
	}
	output = SljitRuntimeBatchViewFromHashJoinSelection(
	    selected.Input(), scratch.FilterSelection(selected.hash_join_idx),
	    scratch.HashJoinBuildSelection(selected.hash_join_idx), scratch.HashJoinRowPointers(selected.hash_join_idx),
	    selected_count, selected.hash_join_idx, output_proof, selected.output_column_map,
	    selected.output_projection_idx);
	return true;
}

} // namespace duckdb
