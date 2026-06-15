//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_executable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"
#include "sljit_region_plan.hpp"

namespace duckdb {

struct SljitExecutableRegionExpression {
	SljitNativeRegionExpressionPlan plan;
	unique_ptr<JitCodeHandle> code;
	SljitNativeVectorFunction function = nullptr;
	unique_ptr<JitCodeHandle> select_code;
	SljitNativeVectorFunction select_function = nullptr;
	unique_ptr<JitCodeHandle> predicate_code;
	SljitNativePredicateFunction predicate_function = nullptr;
	unique_ptr<JitCodeHandle> predicate_select_code;
	SljitNativePredicateFunction predicate_select_function = nullptr;
	string overflow_message;

	idx_t CodeSize() const {
		idx_t result = 0;
		if (code) {
			result += code->CodeSize();
		}
		if (select_code) {
			result += select_code->CodeSize();
		}
		if (predicate_code) {
			result += predicate_code->CodeSize();
		}
		if (predicate_select_code) {
			result += predicate_select_code->CodeSize();
		}
		return result;
	}
};

struct SljitExecutableUngroupedAggregateUpdate {
	SljitNativeUngroupedAggregateUpdatePlan plan;
	unique_ptr<JitCodeHandle> code;
	SljitNativeUngroupedAggregateFunction function = nullptr;

	idx_t CodeSize() const {
		return code ? code->CodeSize() : 0;
	}
};

struct SljitExecutableGroupedAggregateUpdate {
	SljitNativeGroupedAggregateUpdatePlan plan;
	unique_ptr<JitCodeHandle> code;
	SljitNativeGroupedAggregateFunction function = nullptr;

	idx_t CodeSize() const {
		return code ? code->CodeSize() : 0;
	}
};

struct SljitExecutableHashJoinProbe {
	SljitNativeHashJoinProbePlan plan;
	unique_ptr<JitCodeHandle> code;
	SljitNativeHashJoinProbeFunction function = nullptr;

	idx_t CodeSize() const {
		return code ? code->CodeSize() : 0;
	}
};

struct SljitExecutableHashJoinBuild {
	SljitNativeHashJoinBuildPlan plan;
};

struct SljitExecutableRegionOp {
	SljitNativeRegionOpKind kind;
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<LogicalType> output_types;
	SljitExecutableRegionExpression filter;
	SljitExecutableHashJoinProbe hash_join_probe;
	SljitExecutableHashJoinBuild hash_join_build;
	vector<SljitExecutableRegionExpression> projections;
	vector<JitUngroupedAggregatePayloadBinding> aggregate_payloads;
	vector<SljitExecutableUngroupedAggregateUpdate> native_ungrouped_aggregate_updates;
	vector<SljitExecutableGroupedAggregateUpdate> native_grouped_aggregate_updates;
	vector<JitGroupedAggregatePayloadBinding> grouped_aggregate_payloads;
	vector<JitGroupedAggregateGroupBinding> grouped_aggregate_groups;

	idx_t CodeSize() const {
		idx_t result = 0;
		if (kind == SljitNativeRegionOpKind::FILTER) {
			result += filter.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			result += hash_join_probe.CodeSize();
		}
		for (auto &projection : projections) {
			result += projection.CodeSize();
		}
		for (auto &update : native_ungrouped_aggregate_updates) {
			result += update.CodeSize();
		}
		for (auto &update : native_grouped_aggregate_updates) {
			result += update.CodeSize();
		}
		return result;
	}
};

struct SljitExecutableRegion {
	vector<SljitExecutableRegionOp> ops;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &op : ops) {
			result += op.CodeSize();
		}
		return result;
	}
};

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region,
                                        SljitExecutableRegion &executable, string &error);

} // namespace duckdb
