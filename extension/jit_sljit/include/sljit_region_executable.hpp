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

#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

struct SljitExecutableRegionExpression {
	SljitNativeRegionExpressionPlan plan;
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativeVectorFunction function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> select_code;
	SljitNativeVectorFunction select_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> predicate_code;
	SljitNativePredicateFunction predicate_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> predicate_select_code;
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

struct SljitExecutableHashJoinProbe {
	SljitNativeHashJoinProbePlan plan;
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativeHashJoinProbeFunction function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> perfect_code;
	SljitNativeHashJoinProbeFunction perfect_function = nullptr;
	SljitExecutableRegionExpression residual_filter;

	idx_t CodeSize() const {
		idx_t result = code ? code->CodeSize() : 0;
		result += perfect_code ? perfect_code->CodeSize() : 0;
		result += residual_filter.CodeSize();
		return result;
	}
};

struct SljitExecutableNestedLoopJoinProbe {
	SljitNativeNestedLoopJoinProbePlan plan;
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativeNestedLoopJoinProbeFunction function = nullptr;
	vector<SljitExecutableRegionExpression> lhs_conditions;

	idx_t CodeSize() const {
		idx_t result = code ? code->CodeSize() : 0;
		for (auto &condition : lhs_conditions) {
			result += condition.CodeSize();
		}
		return result;
	}
};

struct SljitExecutableHashJoinBuild {
	SljitNativeHashJoinBuildPlan plan;
};

struct SljitExecutableNestedLoopJoinBuild {
	SljitNativeNestedLoopJoinBuildPlan plan;
	vector<SljitExecutableRegionExpression> rhs_conditions;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &condition : rhs_conditions) {
			result += condition.CodeSize();
		}
		return result;
	}
};

struct SljitExecutableAppendSink {
	SljitNativeAppendSinkPlan plan;
};

struct SljitExecutableDelimJoinSink {
	SljitNativeDelimJoinSinkPlan plan;
};

struct SljitExecutableOrderSink {
	SljitNativeOrderSinkPlan plan;
	vector<SljitExecutableRegionExpression> order_keys;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &order_key : order_keys) {
			result += order_key.CodeSize();
		}
		return result;
	}
};

struct SljitExecutableAggregateUpdate {
	SljitNativeAggregateUpdatePlan plan;
	vector<SljitExecutableRegionExpression> payloads;
	vector<unique_ptr<ExecutionRegionCodeHandle>> payload_update_code;
	vector<SljitNativeAggregateUpdateFunction> payload_update_functions;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &payload : payloads) {
			result += payload.CodeSize();
		}
		for (auto &code : payload_update_code) {
			result += code ? code->CodeSize() : 0;
		}
		return result;
	}
};

struct SljitExecutableRegionOp {
	SljitNativeRegionOpKind kind;
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<LogicalType> output_types;
	SljitExecutableRegionExpression filter;
	SljitExecutableHashJoinProbe hash_join_probe;
	SljitExecutableNestedLoopJoinProbe nested_loop_join_probe;
	SljitExecutableHashJoinBuild hash_join_build;
	SljitExecutableNestedLoopJoinBuild nested_loop_join_build;
	SljitExecutableAppendSink append_sink;
	SljitExecutableDelimJoinSink delim_join_sink;
	SljitExecutableOrderSink order_sink;
	SljitExecutableAggregateUpdate aggregate_update;
	vector<SljitExecutableRegionExpression> projections;
	bool use_vectorized_projection = false;

	idx_t CodeSize() const {
		idx_t result = 0;
		if (kind == SljitNativeRegionOpKind::FILTER) {
			result += filter.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			result += hash_join_probe.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE) {
			result += nested_loop_join_probe.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD) {
			result += nested_loop_join_build.CodeSize();
		}
			if (kind == SljitNativeRegionOpKind::ORDER_SINK) {
				result += order_sink.CodeSize();
			}
			if (kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				result += aggregate_update.CodeSize();
			}
			for (auto &projection : projections) {
			result += projection.CodeSize();
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

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable, string &error);

} // namespace duckdb
