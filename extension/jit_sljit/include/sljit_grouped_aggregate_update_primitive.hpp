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
	FILTERED_PRIMITIVE_PAYLOAD_UPDATE,
	DISTINCT_KEY_SINK
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
	shared_ptr<SljitExecutableRegionOp> projected_distinct_key_input_projection;
	shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update;
};

static bool SljitCanBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t aggregate_idx) {
	if (aggregate_idx >= ops.size() || ops[aggregate_idx].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &aggregate = ops[aggregate_idx].aggregate_update;
	if (aggregate.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
		return true;
	}
	return aggregate.plan.sink_info.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	       aggregate.filtered_update.IsExecutable() &&
	       aggregate.filtered_update.kind == SljitFilteredAggregateKernelKind::PERFECT_HASH_GROUPED;
}

static bool SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.UsesPrimitivePayloads() || !op.aggregate_update.plan.use_grouped_state_addresses) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	if (sink_info.groups.size() != 1 || sink_info.aggregates.size() != 1 || op.aggregate_update.payloads.size() != 1) {
		return false;
	}
	auto &aggregate = sink_info.aggregates[0];
	return aggregate.child_count == 0 && aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR;
}

static bool SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.UsesPrimitivePayloads()) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	return !sink_info.groups.empty() && sink_info.aggregates.size() == op.aggregate_update.payloads.size();
}

static bool SljitGroupedAggregateUpdateCanUseDistinctKeySink(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
		return false;
	}
	return SljitAggregateSinkCanUseDistinctKeySink(op.aggregate_update.plan.sink_info);
}

static bool SljitCanBindFilteredGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                idx_t filter_idx, idx_t aggregate_idx) {
	if (filter_idx >= ops.size() || ops[filter_idx].kind != SljitNativeRegionOpKind::FILTER ||
	    aggregate_idx >= ops.size()) {
		return false;
	}
	auto &aggregate = ops[aggregate_idx];
	return SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(aggregate) ||
	       (aggregate.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	        aggregate.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	        aggregate.aggregate_update.filtered_update.IsExecutable() &&
	        aggregate.aggregate_update.filtered_update.kind == SljitFilteredAggregateKernelKind::PERFECT_HASH_GROUPED);
}

static SljitGroupedAggregateUpdateStrategyKind
SljitChooseGroupedAggregateUpdateStrategy(const SljitExecutableRegionOp &op) {
	if (SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
	}
	if (SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
	}
	if (SljitGroupedAggregateUpdateCanUseDistinctKeySink(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK;
	}
	return SljitGroupedAggregateUpdateStrategyKind::INVALID;
}

static bool SljitGroupedAggregateUpdateHasDedicatedBackend(const vector<SljitExecutableRegionOp> &ops,
                                                           idx_t aggregate_idx) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		return false;
	}
	auto &op = ops[aggregate_idx];
	return SljitChooseGroupedAggregateUpdateStrategy(op) != SljitGroupedAggregateUpdateStrategyKind::INVALID;
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

static SljitGroupedAggregateUpdatePrimitive
SljitBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx) {
	SljitGroupedAggregateUpdatePrimitive primitive;
	if (!SljitTryBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx, primitive)) {
		throw InternalException("SLJIT grouped aggregate update primitive has no dedicated backend");
	}
	return primitive;
}

static SljitGroupedAggregateUpdatePrimitive
SljitBindFilteredGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t filter_idx,
                                                 idx_t aggregate_idx) {
	SljitGroupedAggregateUpdatePrimitive primitive;
	if (!SljitTryBindFilteredGroupedAggregateUpdatePrimitive(ops, filter_idx, aggregate_idx, primitive)) {
		throw InternalException("SLJIT filtered grouped aggregate update primitive cannot bind requested operators");
	}
	return primitive;
}

} // namespace duckdb
