//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_range_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "sljit_executable_range_analysis.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

static bool SljitRangeFromSource(idx_t source_index, const LogicalType &expected_type,
                                 const vector<Value> &source_min_values, const vector<Value> &source_max_values,
                                 SljitExecutableInt128Range &result) {
	if (source_index >= source_min_values.size() || source_index >= source_max_values.size()) {
		return false;
	}
	result.type = expected_type;
	return SljitExecutableValueToHugeint(source_min_values[source_index], expected_type, result.min) &&
	       SljitExecutableValueToHugeint(source_max_values[source_index], expected_type, result.max) &&
	       result.min <= result.max;
}

static bool SljitRangeAbsFitsLocalInt64Sum(const SljitExecutableInt128Range &range) {
	auto max_abs = range.max;
	if (max_abs < 0) {
		if (!Hugeint::TryNegate(max_abs, max_abs)) {
			return false;
		}
	}
	auto min_abs = range.min;
	if (min_abs < 0) {
		if (!Hugeint::TryNegate(min_abs, min_abs)) {
			return false;
		}
	}
	max_abs = MaxValue(max_abs, min_abs);
	auto limit = Hugeint::Convert(NumericLimits<int64_t>::Maximum() / NumericCast<int64_t>(STANDARD_VECTOR_SIZE));
	return max_abs <= limit;
}

static bool SljitPayloadRangeLocalSumLowerNeverOverflows(const SljitNativeRegionExpressionPlan &payload,
                                                         const vector<Value> &source_min_values,
                                                         const vector<Value> &source_max_values) {
	SljitExecutableInt128Range range;
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		range.type = payload.return_type;
		if (!SljitExecutableValueToHugeint(payload.constant_value, payload.return_type, range.min) ||
		    !SljitExecutableValueToHugeint(payload.constant_value, payload.return_type, range.max)) {
			return false;
		}
		break;
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (!SljitRangeFromSource(payload.source_index, payload.return_type, source_min_values, source_max_values,
		                          range)) {
			return false;
		}
		break;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		if (!payload.expression_tree ||
		    !SljitExecutableExpressionTreeRange(*payload.expression_tree, source_min_values, source_max_values,
		                                        range)) {
			return false;
		}
		break;
	default:
		return false;
	}
	return SljitRangeAbsFitsLocalInt64Sum(range);
}

static bool SljitLocalSumLowerNeverOverflows(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return true;
	default:
		break;
	}
	if (type.id() != LogicalTypeId::DECIMAL || type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	const auto width = DecimalType::GetWidth(type);
	if (width == 0 || width >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	const auto max_abs = NumericHelper::POWERS_OF_TEN[width] - 1;
	return max_abs <= NumericLimits<int64_t>::Maximum() / NumericCast<int64_t>(STANDARD_VECTOR_SIZE);
}

void AnnotateSljitLocalPerfectHashAggregatePlan(SljitLocalPerfectHashAggregatePlan &plan,
                                                const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                const vector<ExecutionRegionAggregateInput> &aggregates,
                                                const vector<Value> &source_min_values,
                                                const vector<Value> &source_max_values) {
	if (!plan.enabled) {
		return;
	}
	for (idx_t payload_idx = 0;
	     payload_idx < payloads.size() && payload_idx < aggregates.size() && payload_idx < plan.lanes.size();
	     payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			continue;
		}
		plan.lanes[payload_idx].local_lower_never_overflows =
		    SljitLocalSumLowerNeverOverflows(payloads[payload_idx].return_type) ||
		    SljitPayloadRangeLocalSumLowerNeverOverflows(payloads[payload_idx], source_min_values, source_max_values);
	}
}

} // namespace duckdb
