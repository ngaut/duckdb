//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_group_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

static bool TryGetSljitPerfectHashGroupIntegerKind(const LogicalType &type, SljitNativeIntegerKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		kind = SljitNativeIntegerKind::INT8;
		return true;
	case PhysicalType::UINT8:
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	case PhysicalType::INT32:
		kind = SljitNativeIntegerKind::INT32;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool TryGetSljitPerfectHashGroupMinimum(const LogicalType &type, const Value &minimum, int64_t &result) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::UINT8:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<uint8_t>());
		return true;
	case PhysicalType::INT32:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = minimum.GetValueUnsafe<int64_t>();
		return true;
	default:
		return false;
	}
}

static bool SljitPerfectHashGroupExpressionSupported(const SljitNativeRegionExpressionPlan &expr,
                                                     const ExecutionRegionGroupInput &group) {
	if (expr.return_type.InternalType() != group.type.InternalType()) {
		return false;
	}
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return true;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		return group.type.InternalType() == PhysicalType::UINT8 && expr.string_compress_target_size == sizeof(uint8_t);
	default:
		return false;
	}
}

bool TryBuildSljitPerfectHashGroupPlans(const vector<ExecutionRegionGroupInput> &groups,
                                        const vector<SljitNativeRegionExpressionPlan> &group_expressions,
                                        const ExecutionRegionAggregateContract &contract,
                                        vector<SljitPerfectHashGroupPlan> &result) {
	if (contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH ||
	    contract.perfect_required_bits.size() != groups.size() ||
	    contract.perfect_group_minima.size() != groups.size() ||
	    (!group_expressions.empty() && group_expressions.size() != groups.size())) {
		return false;
	}
	result.reserve(groups.size());
	idx_t shift = contract.perfect_required_bits_total;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		if (!group.supported_reference) {
			return false;
		}
		if (shift < contract.perfect_required_bits[group_idx]) {
			return false;
		}
		shift -= contract.perfect_required_bits[group_idx];
		SljitPerfectHashGroupPlan plan;
		if (!TryGetSljitPerfectHashGroupIntegerKind(group.type, plan.integer_kind) ||
		    !TryGetSljitPerfectHashGroupMinimum(group.type, contract.perfect_group_minima[group_idx], plan.minimum)) {
			return false;
		}
		if (group_expressions.empty()) {
			plan.expression_kind = SljitNativeRegionExpressionKind::REFERENCE;
			plan.source_index = group.input_index;
		} else {
			auto &group_expression = group_expressions[group_idx];
			if (!SljitPerfectHashGroupExpressionSupported(group_expression, group)) {
				return false;
			}
			plan.expression_kind = group_expression.kind;
			plan.source_index = group_expression.source_index;
			plan.string_compress_target_size = group_expression.string_compress_target_size;
		}
		plan.shift = shift;
		result.push_back(plan);
	}
	return true;
}

bool SljitCanPrecomputePerfectHashStringGroupOffset(const vector<SljitPerfectHashGroupPlan> &groups) {
	if (string_t::PREFIX_LENGTH == 0) {
		return false;
	}
	idx_t string_group_count = 0;
	for (auto &group : groups) {
		if (group.expression_kind == SljitNativeRegionExpressionKind::STRING_COMPRESS) {
			string_group_count++;
		}
	}
	return string_group_count > 1;
}

} // namespace duckdb
