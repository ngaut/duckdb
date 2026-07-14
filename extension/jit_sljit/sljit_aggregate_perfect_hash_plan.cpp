//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

static bool SljitExpressionTreeResultNotNull(const ExecutionExpressionIR &node, const vector<bool> &source_not_null) {
	switch (node.validity) {
	case ExecutionExpressionValidityKind::CONSTANT_NULL:
		return false;
	case ExecutionExpressionValidityKind::CONSTANT_VALID:
	case ExecutionExpressionValidityKind::NOT_NULL:
		return true;
	case ExecutionExpressionValidityKind::SOURCE:
		return node.kind == ExecutionExpressionIRKind::REFERENCE && node.ref_index < source_not_null.size() &&
		       source_not_null[node.ref_index];
	case ExecutionExpressionValidityKind::CHILD:
		return node.left && SljitExpressionTreeResultNotNull(*node.left, source_not_null);
	case ExecutionExpressionValidityKind::CHILDREN_NULL_PROPAGATING:
		if (node.left && !SljitExpressionTreeResultNotNull(*node.left, source_not_null)) {
			return false;
		}
		if (node.right && !SljitExpressionTreeResultNotNull(*node.right, source_not_null)) {
			return false;
		}
		if (node.else_node && !SljitExpressionTreeResultNotNull(*node.else_node, source_not_null)) {
			return false;
		}
		for (auto &child : node.children) {
			if (child && !SljitExpressionTreeResultNotNull(*child, source_not_null)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static bool SljitAggregatePayloadResultNotNull(const SljitNativeRegionExpressionPlan &payload,
                                               const vector<bool> &source_not_null) {
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return !payload.constant_value.IsNull();
	case SljitNativeRegionExpressionKind::REFERENCE:
		return payload.source_index < source_not_null.size() && source_not_null[payload.source_index];
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return payload.expression_tree && SljitExpressionTreeResultNotNull(*payload.expression_tree, source_not_null);
	default:
		return false;
	}
}

vector<bool> BuildSljitAggregatePayloadNotNull(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                               const vector<ExecutionRegionAggregateInput> &aggregates,
                                               const vector<bool> &source_not_null) {
	vector<bool> result;
	result.reserve(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			result.push_back(true);
			continue;
		}
		result.push_back(SljitAggregatePayloadResultNotNull(payloads[payload_idx], source_not_null));
	}
	return result;
}

static sljit_sw AllocateSljitLocalPerfectHashArray(sljit_sw &local_size, idx_t group_count) {
	auto result = local_size;
	local_size += NumericCast<sljit_sw>(group_count * sizeof(sljit_sw));
	return result;
}

bool TryBuildSljitDensePerfectHashAggregateReductionPlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                                         const ExecutionRegionAggregateContract &contract,
                                                         const vector<bool> &payloads_not_null,
                                                         const vector<bool> &batch_lower_never_overflows,
                                                         sljit_sw &local_size,
                                                         SljitDensePerfectHashAggregateReductionPlan &result) {
	if (contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		return false;
	}
	const idx_t group_count = idx_t(1) << contract.perfect_required_bits_total;
	// This strategy performs one compact batch reduction followed by one commit
	// per seen group. It is deliberately limited to genuinely dense domains;
	// fitting a sparse scratch layout is not evidence that it reduces work.
	if (group_count == 0 || group_count > SLJIT_LOCAL_PERFECT_HASH_MAX_GROUPS) {
		return false;
	}
	result.group_count = group_count;
	idx_t count_star_count = 0;
	idx_t count_star_lane = DConstants::INVALID_INDEX;
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		if (aggregates[aggregate_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			count_star_count++;
			count_star_lane = aggregate_idx;
		}
	}
	if (count_star_count == 1) {
		result.count_seen_lane = count_star_lane;
	}
	if (result.count_seen_lane == DConstants::INVALID_INDEX) {
		result.group_seen_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
	}
	result.lanes.resize(aggregates.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		auto &lane = result.lanes[aggregate_idx];
		lane.value_always_seen = aggregate_idx < payloads_not_null.size() && payloads_not_null[aggregate_idx];
		lane.batch_lower_never_overflows =
		    aggregate_idx < batch_lower_never_overflows.size() && batch_lower_never_overflows[aggregate_idx];
		switch (aggregates[aggregate_idx].primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			lane.count_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			lane.lower_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			if (!lane.value_always_seen) {
				lane.saw_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			lane.lower_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			if (!lane.batch_lower_never_overflows) {
				lane.upper_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			}
			if (!lane.value_always_seen) {
				lane.saw_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			}
			break;
		default:
			return false;
		}
	}
	return true;
}

bool TryBuildSljitDeferredPerfectHashFlagPlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                              const ExecutionRegionAggregateContract &contract, sljit_sw &local_size,
                                              SljitDeferredPerfectHashFlagPlan &result) {
	if (contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		return false;
	}
	const idx_t group_count = idx_t(1) << contract.perfect_required_bits_total;
	if (group_count == 0 || group_count > SLJIT_DEFERRED_PERFECT_HASH_FLAG_MAX_GROUPS) {
		return false;
	}
	for (auto &aggregate : aggregates) {
		switch (aggregate.primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			return false;
		}
	}
	result.enabled = true;
	result.group_count = group_count;
	result.group_seen_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
	return true;
}

} // namespace duckdb
