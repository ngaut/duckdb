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

namespace duckdb {

struct SljitGeneratedFilterPrimitive {
	idx_t filter_idx = 0;
};

static bool SljitCanBindGeneratedFilterPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t filter_idx) {
	return filter_idx < ops.size() && ops[filter_idx].kind == SljitNativeRegionOpKind::FILTER;
}

static SljitGeneratedFilterPrimitive SljitBindGeneratedFilterPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                       idx_t filter_idx) {
	if (!SljitCanBindGeneratedFilterPrimitive(ops, filter_idx)) {
		throw InternalException("SLJIT generated filter primitive cannot bind requested operator");
	}
	SljitGeneratedFilterPrimitive primitive;
	primitive.filter_idx = filter_idx;
	return primitive;
}

static bool SljitExecuteGeneratedFilterPrimitive(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                 vector<SljitExecutableRegionOp> &ops,
                                                 const SljitGeneratedFilterPrimitive &primitive,
                                                 const SljitRuntimeBatchView &input, SljitRuntimeBatchView &output) {
	auto &source_chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT generated filter");
	if (source_chunk.size() == 0) {
		return false;
	}

	auto &filter_op = ops[primitive.filter_idx];
	auto &filter_selection = scratch.FilterSelection(primitive.filter_idx);
	auto filter_stage_start = SljitRegionStageStart(runtime);
	auto selected_count = SljitSelectFilter(filter_op, source_chunk, filter_selection,
	                                        scratch.ExpressionAdapterScratch(primitive.filter_idx, 0));
	RecordSljitRegionStageRuntime(runtime, primitive.filter_idx, filter_op.kind, "selection", filter_stage_start);
	if (selected_count == 0) {
		return false;
	}
	const auto selected = selected_count == source_chunk.size() ? nullptr : &filter_selection;
	output = SljitRuntimeBatchViewFromChunk(source_chunk, selected, selected_count);
	return true;
}

} // namespace duckdb
