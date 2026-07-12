//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_payload_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_aggregate_codegen.hpp"

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_native_codegen.hpp"

namespace duckdb {

bool SljitBuildExecutableAggregateUpdateFallbackPayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                            SljitExecutableAggregateUpdate &executable, string &error) {
	executable.payload_updates.reserve(op.payloads.size());
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &payload = op.payloads[payload_idx];
		if (payload_idx >= op.sink_info.aggregates.size()) {
			error = "SLJIT aggregate update payload has no aggregate contract";
			return false;
		}
		SljitAggregatePayloadDescriptor descriptor;
		if (!SljitTryBindAggregatePayloadDescriptor(payload, op.sink_info.aggregates[payload_idx], descriptor)) {
			error = "SLJIT aggregate update payload descriptor is invalid";
			return false;
		}
		auto primitive_kind = descriptor.primitive_kind;
		SljitNativeAggregateUpdateFunction function = nullptr;
		unique_ptr<ExecutionRegionCodeHandle> code;
		if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (op.use_grouped_state_addresses) {
				code = BuildSljitNativeGroupedCountStar(function, error);
			} else {
				code = BuildSljitNativeUngroupedCountStar(function, error);
			}
			if (!code || !function) {
				if (error.empty()) {
					error = "SLJIT count-star aggregate update has no native primitive reducer";
				}
				return false;
			}
			executable.payload_updates.emplace_back(std::move(code), function);
			continue;
		}
		switch (payload.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			if (op.use_grouped_state_addresses) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeGroupedSumInt64Reference(payload.integer_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = descriptor.IsDoubleWord()
					           ? BuildSljitNativeGroupedSumHugeint128Reference(function, error)
					           : BuildSljitNativeGroupedSumHugeintReference(payload.integer_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
					code = BuildSljitNativeGroupedSumDoubleReference(payload.double_source_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
					code = BuildSljitNativeGroupedCountReference(function, error);
				} else {
					error = "SLJIT grouped aggregate reference reducer has no primitive state kind";
					return false;
				}
			} else {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64Reference(payload.integer_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT && descriptor.IsDoubleWord()) {
					code = BuildSljitNativeUngroupedSumHugeint128Reference(function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
					code = BuildSljitNativeUngroupedSumDoubleReference(payload.double_source_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
					code = BuildSljitNativeUngroupedCountReference(function, error);
				} else {
					error = "SLJIT aggregate reference reducer has no primitive state kind";
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate binary-constant reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
				error = "SLJIT aggregate binary-constant reducer only supports SUM_INT64";
				return false;
			}
			code = BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
			    payload.integer_kind, payload.binary_op, payload.constant_on_left, function, error,
			    payload.check_arithmetic_overflow, payload.check_result_range, payload.result_min, payload.result_max);
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate binary-reference reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
				error = "SLJIT aggregate binary-reference reducer only supports SUM_INT64";
				return false;
			}
			code = BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(
			    payload.integer_kind, payload.binary_op, function, error, payload.check_arithmetic_overflow,
			    payload.check_result_range, payload.result_min, payload.result_max);
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate double binary-constant reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				error = "SLJIT aggregate double binary-constant reducer only supports SUM_DOUBLE";
				return false;
			}
			code = BuildSljitNativeUngroupedSumDoubleDoubleBinaryConstant(
			    payload.double_binary_op, payload.double_source_kind, payload.constant_on_left, function, error);
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate double binary-reference reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				error = "SLJIT aggregate double binary-reference reducer only supports SUM_DOUBLE";
				return false;
			}
			code = BuildSljitNativeUngroupedSumDoubleDoubleBinaryReferences(
			    payload.double_binary_op, payload.double_source_kind, payload.double_right_source_kind, function,
			    error);
			break;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate expression-tree reducer is not supported";
				return false;
			}
			if (payload.expression_tree) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64ExpressionTree(*payload.expression_tree, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeUngroupedSumHugeintExpressionTree(*payload.expression_tree, function, error);
				} else {
					error = "SLJIT aggregate expression-tree reducer has no primitive state kind";
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate typed expression-tree reducer is not supported";
				return false;
			}
			if (payload.expression_tree) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64TypedExpressionTree(
					    *payload.expression_tree, function, error, payload.emit_flat_nullable_fast_path);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeUngroupedSumHugeintTypedExpressionTree(
					    *payload.expression_tree, function, error, payload.emit_flat_nullable_fast_path);
				} else {
					error = "SLJIT aggregate typed expression-tree reducer has no primitive state kind";
					return false;
				}
			}
			break;
		default:
			break;
		}
		if (!code || !function) {
			if (error.empty()) {
				error = "SLJIT aggregate update payload has no native primitive reducer";
			}
			return false;
		}
		executable.payload_updates.emplace_back(std::move(code), function);
	}
	return true;
}

} // namespace duckdb
