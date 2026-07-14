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

static sljit_sw AlignSljitLocalSize(sljit_sw local_size, sljit_sw alignment) {
	D_ASSERT(alignment > 0);
	return (local_size + alignment - 1) & ~(alignment - 1);
}

static sljit_sw AllocateSljitLocalPerfectHashByteArray(sljit_sw &local_size, idx_t group_count) {
	auto result = local_size;
	local_size += NumericCast<sljit_sw>(group_count);
	local_size = AlignSljitLocalSize(local_size, NumericCast<sljit_sw>(sizeof(sljit_sw)));
	return result;
}

static bool TryBuildSljitLocalPerfectHashAggregatePlanCandidate(
    const vector<ExecutionRegionAggregateInput> &aggregates, const ExecutionRegionAggregateContract &contract,
    const vector<bool> &payloads_not_null, sljit_sw &local_size, SljitLocalPerfectHashAggregatePlan &result) {
	if (contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		return false;
	}
	const idx_t group_count = idx_t(1) << contract.perfect_required_bits_total;
	if (group_count == 0 || group_count > SLJIT_SPARSE_LOCAL_PERFECT_HASH_MAX_GROUPS) {
		return false;
	}
	result.enabled = true;
	result.sparse = group_count > SLJIT_LOCAL_PERFECT_HASH_MAX_GROUPS;
	if (result.sparse && !SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG) {
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
	if (result.sparse) {
		if (result.count_seen_lane == DConstants::INVALID_INDEX) {
			result.group_seen_is_byte = true;
			result.group_seen_offset = AllocateSljitLocalPerfectHashByteArray(local_size, group_count);
		}
		result.active_groups_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
		result.active_count_offset = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	} else if (result.count_seen_lane == DConstants::INVALID_INDEX) {
		result.group_seen_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
	}
	result.lanes.resize(aggregates.size());
	if (result.sparse) {
		for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
			auto &lane = result.lanes[aggregate_idx];
			lane.value_always_seen = aggregate_idx < payloads_not_null.size() && payloads_not_null[aggregate_idx];
			switch (aggregates[aggregate_idx].primitive_update_kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
				lane.count_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				break;
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				lane.lower_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				if (!lane.value_always_seen) {
					lane.saw_offset = result.group_payload_stride;
					result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				}
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				lane.lower_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				lane.upper_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				if (!lane.value_always_seen) {
					lane.saw_offset = result.group_payload_stride;
					result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				}
				break;
			default:
				return false;
			}
		}
		sljit_sw padded_stride = 1;
		while (padded_stride < result.group_payload_stride) {
			padded_stride <<= 1;
		}
		result.group_payload_stride = padded_stride;
		result.group_payload_offset = local_size;
		local_size += NumericCast<sljit_sw>(group_count) * result.group_payload_stride;
		bool payloads_always_seen = result.count_seen_lane != DConstants::INVALID_INDEX;
		for (auto &lane : result.lanes) {
			payloads_always_seen = payloads_always_seen && lane.saw_offset < 0;
		}
		result.sparse_eager_zero = payloads_always_seen && group_count <= SLJIT_EAGER_ZERO_SPARSE_LOCAL_MAX_GROUPS;
		return true;
	}
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		auto &lane = result.lanes[aggregate_idx];
		lane.value_always_seen = aggregate_idx < payloads_not_null.size() && payloads_not_null[aggregate_idx];
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
			lane.upper_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
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

bool TryBuildSljitLocalPerfectHashAggregatePlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                                const ExecutionRegionAggregateContract &contract,
                                                const vector<bool> &payloads_not_null, sljit_sw &local_size,
                                                SljitLocalPerfectHashAggregatePlan &result) {
	// Sparse perfect-hash domains can arise from compact physical encodings whose
	// values are not dense (for example, one-byte string prefixes). The local
	// reducer is profitable only while its complete state remains cache-resident;
	// otherwise the direct perfect-hash state path avoids an extra sparse table
	// and its per-row bookkeeping.
	auto candidate_local_size = local_size;
	SljitLocalPerfectHashAggregatePlan candidate;
	if (!TryBuildSljitLocalPerfectHashAggregatePlanCandidate(aggregates, contract, payloads_not_null,
	                                                         candidate_local_size, candidate)) {
		return false;
	}
	if (candidate.sparse && candidate_local_size - local_size > SLJIT_SPARSE_LOCAL_PERFECT_HASH_MAX_BYTES) {
		return false;
	}
	local_size = candidate_local_size;
	result = std::move(candidate);
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
