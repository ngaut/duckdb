//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_input_filter_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_state.hpp"

namespace duckdb {

enum class SljitHashJoinProbeInputFilterStatus : uint8_t { NOT_APPLICABLE, EMPTY, READY };

struct SljitHashJoinProbeInputFilterResult {
	SljitHashJoinProbeInputFilterStatus status = SljitHashJoinProbeInputFilterStatus::NOT_APPLICABLE;
	DataChunk *input = nullptr;
	const char *blocker = nullptr;
};

// A post-join filter can run before an inner probe when every referenced join
// output is a left-hand-side output. The cache owns the remapped predicate and
// the zero-copy selected input independently from the regular post-join filter
// path, whose input schema is the complete join output.
struct SljitHashJoinProbeInputFilterCache {
	bool initialized = false;
	bool ready = false;
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	vector<idx_t> lhs_output_column_indices;
	vector<LogicalType> input_types;
	unique_ptr<SljitExecutableRegionOp> mapped_filter;
	SljitDataChunkBatch selected_input;
	string blocker;

	bool Matches(idx_t hash_join_idx_p, idx_t filter_idx_p, const ExecutionHashJoinProbeBinding &binding,
	             const vector<LogicalType> &input_types_p) const {
		return initialized && hash_join_idx == hash_join_idx_p && filter_idx == filter_idx_p &&
		       lhs_output_column_indices == binding.lhs_output_column_indices && input_types == input_types_p;
	}

	void Reset(idx_t hash_join_idx_p, idx_t filter_idx_p, const ExecutionHashJoinProbeBinding &binding,
	           const vector<LogicalType> &input_types_p) {
		initialized = true;
		ready = false;
		hash_join_idx = hash_join_idx_p;
		filter_idx = filter_idx_p;
		lhs_output_column_indices = binding.lhs_output_column_indices;
		input_types = input_types_p;
		mapped_filter.reset();
		blocker.clear();
	}
};

static bool SljitTryBuildHashJoinProbeInputMappedFilter(const ExecutionHashJoinProbeBinding &binding,
                                                        const vector<LogicalType> &input_types,
                                                        SljitExecutableRegionOp &filter_op,
                                                        SljitExecutableRegionOp &mapped_filter, string &blocker) {
	if (filter_op.kind != SljitNativeRegionOpKind::FILTER ||
	    binding.output_types.size() < binding.lhs_output_column_indices.size()) {
		blocker = "filter_shape";
		return false;
	}
	vector<idx_t> source_map(binding.output_types.size(), DConstants::INVALID_INDEX);
	for (idx_t output_idx = 0; output_idx < binding.lhs_output_column_indices.size(); output_idx++) {
		const auto input_idx = binding.lhs_output_column_indices[output_idx];
		if (input_idx >= input_types.size() || binding.output_types[output_idx] != input_types[input_idx]) {
			blocker = "lhs_output_mapping";
			return false;
		}
		source_map[output_idx] = input_idx;
	}

	D_ASSERT(filter_op.filter);
	auto mapped_plan = filter_op.filter->expression.plan.Copy(true, false);
	idx_t failed_source_index = DConstants::INVALID_INDEX;
	if (!SljitTryRemapHashJoinProjectionPlanSources(source_map, mapped_plan,
	                                                optional_ptr<idx_t>(&failed_source_index))) {
		blocker = "non_probe_source_" + std::to_string(failed_source_index);
		return false;
	}
	mapped_filter = SljitExecutableRegionOp();
	mapped_filter.kind = SljitNativeRegionOpKind::FILTER;
	mapped_filter.input_types = input_types;
	mapped_filter.output_types = input_types;
	string compile_error;
	if (!SljitPrepareAndCompileExecutableFilter(mapped_plan, mapped_filter, compile_error)) {
		blocker = "compile_" + compile_error;
		mapped_filter = SljitExecutableRegionOp();
		return false;
	}
	return true;
}

static SljitHashJoinProbeInputFilterResult SljitTryExecuteHashJoinProbeInputFilter(

    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
    idx_t hash_join_idx, idx_t filter_idx, const ExecutionHashJoinProbeBinding &binding,
    const vector<LogicalType> &input_types, DataChunk &input, SljitHashJoinProbeInputFilterCache &cache) {
	SljitHashJoinProbeInputFilterResult result;
	if (filter_idx == DConstants::INVALID_INDEX) {
		result.status = SljitHashJoinProbeInputFilterStatus::READY;
		result.input = &input;
		return result;
	}
	if (filter_idx >= ops.size() || hash_join_idx >= ops.size()) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.filter_index";
		return result;
	}
	if (!cache.Matches(hash_join_idx, filter_idx, binding, input_types)) {
		cache.Reset(hash_join_idx, filter_idx, binding, input_types);
		auto mapped_filter = make_uniq<SljitExecutableRegionOp>();
		if (!SljitTryBuildHashJoinProbeInputMappedFilter(binding, input_types, ops[filter_idx], *mapped_filter,
		                                                 cache.blocker)) {
			result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.filter_mapping";
			return result;
		}
		cache.mapped_filter = std::move(mapped_filter);
		cache.ready = true;
	}
	if (!cache.ready || !cache.mapped_filter) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.filter_mapping";
		return result;
	}

	auto &filter_selection = scratch.FilterSelection(filter_idx);
	auto filter_stage_start = SljitRegionStageStart(runtime);
	const auto selected_count = SljitSelectFilter(*cache.mapped_filter, input, filter_selection,
	                                              scratch.ExpressionAdapterScratch(filter_idx, 0));
	RecordSljitRegionStageRuntime(runtime, filter_idx, ops[filter_idx].kind, "probe_input_selection",
	                              filter_stage_start);
	if (selected_count == 0) {
		result.status = SljitHashJoinProbeInputFilterStatus::EMPTY;
		return result;
	}
	if (selected_count == input.size()) {
		result.status = SljitHashJoinProbeInputFilterStatus::READY;
		result.input = &input;
		return result;
	}
	cache.selected_input.Ensure(runtime.GetAllocator(), input_types);
	auto &selected_input = cache.selected_input.chunk;
	selected_input.Reset();
	selected_input.Slice(input, filter_selection, selected_count);
	result.status = SljitHashJoinProbeInputFilterStatus::READY;
	result.input = &selected_input;
	return result;
}

} // namespace duckdb
