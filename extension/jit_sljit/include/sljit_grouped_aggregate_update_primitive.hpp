//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_contract_utils.hpp"
#include "sljit_region_executable.hpp"

namespace duckdb {

struct SljitProjectedInputGroupedAggregateDescriptor;

enum class SljitGroupedAggregateUpdateStrategyKind : uint8_t {
	INVALID,
	COUNT_STAR_PREAGGREGATION,
	DIRECT_PRIMITIVE_PAYLOAD_UPDATE,
	FILTERED_PRIMITIVE_PAYLOAD_UPDATE
};

enum class SljitGroupedAggregateUpdateInputKind : uint8_t { MATERIALIZED, PROJECTED_INPUT };

struct SljitGroupedAggregateUpdatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	SljitGroupedAggregateUpdateStrategyKind strategy = SljitGroupedAggregateUpdateStrategyKind::INVALID;
	SljitGroupedAggregateUpdateInputKind input_kind = SljitGroupedAggregateUpdateInputKind::MATERIALIZED;
	shared_ptr<SljitExecutableRegionOp> projected_count_star_group_projection;
	shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update;
};

//! The grouped aggregate's fused perfect-hash kernel evaluates the filter in the pass that updates the groups
static bool SljitGroupedAggregateHasFusedFilteredKernel(const SljitExecutableRegionOp &aggregate) {
	return aggregate.Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	       aggregate.AggregateUpdate().plan.sink_info.Kind() ==
	           ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	       aggregate.AggregateUpdate().filtered_update.IsExecutable() &&
	       aggregate.AggregateUpdate().filtered_update.kind == SljitFilteredAggregateKernelKind::PERFECT_HASH_GROUPED;
}

static bool SljitCanBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t aggregate_idx) {
	if (aggregate_idx >= ops.size() || ops[aggregate_idx].Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return ops[aggregate_idx].AggregateUpdate().plan.sink_info.Kind() ==
	           ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	       SljitGroupedAggregateHasFusedFilteredKernel(ops[aggregate_idx]);
}

static bool SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(const SljitExecutableRegionOp &op) {
	if (op.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    op.AggregateUpdate().plan.sink_info.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.AggregateUpdate().plan.UsesPrimitivePayloads() || !op.AggregateUpdate().plan.use_grouped_state_addresses) {
		return false;
	}
	auto &sink_info = op.AggregateUpdate().plan.sink_info;
	if (SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	if (sink_info.groups.size() != 1 || sink_info.aggregates.size() != 1 || op.AggregateUpdate().payloads.size() != 1) {
		return false;
	}
	auto &aggregate = sink_info.aggregates[0];
	return aggregate.child_count == 0 && aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR;
}

static bool SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(const SljitExecutableRegionOp &op) {
	if (op.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    op.AggregateUpdate().plan.sink_info.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.AggregateUpdate().plan.UsesPrimitivePayloads()) {
		return false;
	}
	auto &sink_info = op.AggregateUpdate().plan.sink_info;
	if (SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	return !sink_info.groups.empty() && sink_info.aggregates.size() == op.AggregateUpdate().payloads.size();
}

//! A filtered grouped update executes through the fused kernel, or selects rows with the filter's compiled
//! selection and updates the selected rows directly. A recipe step that owns its filter leaves it without a
//! standalone selector, so the second way needs one compiled already (a filter whose codegen deferred to the
//! fused kernel has none).
static bool SljitCanBindFilteredGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                idx_t filter_idx, idx_t aggregate_idx) {
	if (filter_idx >= ops.size() || ops[filter_idx].Kind() != SljitNativeRegionOpKind::FILTER ||
	    aggregate_idx >= ops.size()) {
		return false;
	}
	auto &aggregate = ops[aggregate_idx];
	auto &filter = ops[filter_idx].Filter();
	return SljitGroupedAggregateHasFusedFilteredKernel(aggregate) ||
	       (filter && filter->expression.HasSelectionKernel() &&
	        SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(aggregate));
}

static SljitGroupedAggregateUpdateStrategyKind
SljitChooseGroupedAggregateUpdateStrategy(const SljitExecutableRegionOp &op) {
	if (SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
	}
	if (SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
	}
	return SljitGroupedAggregateUpdateStrategyKind::INVALID;
}

static bool SljitTryBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx,
                                                        SljitGroupedAggregateUpdatePrimitive &primitive) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		return false;
	}
	auto strategy = SljitChooseGroupedAggregateUpdateStrategy(ops[aggregate_idx]);
	if (strategy == SljitGroupedAggregateUpdateStrategyKind::INVALID) {
		return false;
	}
	SljitGroupedAggregateUpdatePrimitive candidate;
	candidate.aggregate_idx = aggregate_idx;
	candidate.strategy = strategy;
	primitive = std::move(candidate);
	return true;
}

//! A grouped update fed selected rows (a generated filter's view) updates them directly: count-star
//! preaggregation reads a materialized chunk
static bool SljitTryBindSelectedInputGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                     idx_t aggregate_idx,
                                                                     SljitGroupedAggregateUpdatePrimitive &primitive) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx) ||
	    !SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(ops[aggregate_idx])) {
		return false;
	}
	SljitGroupedAggregateUpdatePrimitive candidate;
	candidate.aggregate_idx = aggregate_idx;
	candidate.strategy = SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
	primitive = std::move(candidate);
	return true;
}

static bool SljitTryBindFilteredGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                idx_t filter_idx, idx_t aggregate_idx,
                                                                SljitGroupedAggregateUpdatePrimitive &primitive) {
	if (!SljitCanBindFilteredGroupedAggregateUpdatePrimitive(ops, filter_idx, aggregate_idx)) {
		return false;
	}
	SljitGroupedAggregateUpdatePrimitive candidate;
	candidate.aggregate_idx = aggregate_idx;
	candidate.filter_idx = filter_idx;
	candidate.strategy = SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE;
	primitive = std::move(candidate);
	return true;
}

} // namespace duckdb
