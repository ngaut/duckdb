#include "sljit_region_executable.hpp"

#include "sljit_native_codegen.hpp"
#include "sljit_region_codegen.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

static bool BuildExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan, bool require_boolean,
                                              SljitExecutableRegionExpression &expr, string &error) {
	expr.plan = CopySljitNativeRegionExpression(plan);
	auto &semantic = expr.plan;
	switch (semantic.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return true;
	case SljitNativeRegionExpressionKind::CONSTANT:
		if (require_boolean) {
			error = "SLJIT constant projection cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		expr.overflow_message = NativeIntegerBinaryOverflowMessage(semantic.binary_op);
		expr.code = BuildSljitNativeIntegerBinaryConstant(semantic.integer_kind, semantic.binary_op,
		                                                  semantic.constant_on_left, expr.function, error,
		                                                  semantic.check_result_range, semantic.result_min,
		                                                  semantic.result_max);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		expr.overflow_message = NativeIntegerBinaryOverflowMessage(semantic.binary_op);
		expr.code = BuildSljitNativeIntegerBinaryReferences(semantic.integer_kind, semantic.binary_op, expr.function,
		                                                    error, semantic.check_result_range, semantic.result_min,
		                                                    semantic.result_max);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		expr.code = BuildSljitNativeDoubleBinaryConstant(semantic.double_binary_op, semantic.constant_on_left,
		                                                 expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		expr.code = BuildSljitNativeDoubleBinaryReferences(semantic.double_binary_op, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerCompareConstant(semantic.integer_kind, semantic.compare_op,
			                                                   semantic.constant_on_left, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeIntegerSelectConstant(semantic.integer_kind, semantic.compare_op,
		                                                         semantic.constant_on_left, expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerCompareReferences(semantic.integer_kind, semantic.compare_op,
			                                                     expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeIntegerSelectReferences(semantic.integer_kind, semantic.compare_op,
		                                                           expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		expr.overflow_message = NativeIntegerCastOverflowMessage(semantic.cast_source_width, semantic.cast_target_width);
		expr.code = BuildSljitNativeIntegerCast(semantic.cast_source_width, semantic.cast_target_width,
		                                        semantic.try_cast, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		expr.overflow_message = NativeSignedToUnsignedIntegerCastOverflowMessage(semantic.cast_source_width,
		                                                                         semantic.unsigned_cast_target_width);
		expr.code = BuildSljitNativeSignedToUnsignedIntegerCast(
		    semantic.cast_source_width, semantic.unsigned_cast_target_width, semantic.try_cast, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		expr.code = BuildSljitNativeIntegerCoalesce(semantic.signed_integer_width, semantic.coalesce_rhs_kind,
		                                            semantic.coalesce_constant_is_null, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerInList(semantic.integer_kind, semantic.constants.size(),
			                                          semantic.list_has_null, semantic.not_in, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeIntegerInListSelect(
		    semantic.integer_kind, semantic.constants.size(), semantic.list_has_null, semantic.not_in,
		    expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerBetween(semantic.integer_kind, semantic.lower, semantic.upper,
			                                           semantic.lower_inclusive, semantic.upper_inclusive,
			                                           semantic.not_between, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeIntegerBetweenSelect(
		    semantic.integer_kind, semantic.lower, semantic.upper, semantic.lower_inclusive, semantic.upper_inclusive,
		    semantic.not_between, expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		if (require_boolean) {
			error = "SLJIT constant_or_null cannot lower as a predicate";
			return false;
		}
		expr.predicate_code = BuildSljitNativeConstantOrNull(semantic.constant_or_null.guard_source_indices,
		                                                     expr.predicate_function, error);
		return expr.predicate_code != nullptr;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8:
		if (require_boolean) {
			error = "SLJIT string compression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeStringCompressUInt8(expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		if (require_boolean) {
			error = "SLJIT integral compression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeIntegralCompress(semantic.cast_source_width, semantic.unsigned_cast_target_width,
		                                             expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT integral decompression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeIntegralDecompress(semantic.unsigned_source_width, semantic.cast_target_width,
		                                               expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		if (require_boolean) {
			error = "SLJIT date-year cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeDateYear(expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (!require_boolean) {
			expr.code = BuildSljitNativeNullCheck(semantic.null_check_op, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeNullCheckSelect(semantic.null_check_op, expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (!require_boolean) {
			expr.predicate_code = BuildSljitNativePredicate(*semantic.predicate, true, expr.predicate_function, error);
			if (!expr.predicate_code) {
				return false;
			}
		}
		expr.predicate_select_code =
		    BuildSljitNativePredicate(*semantic.predicate, false, expr.predicate_select_function, error);
		return expr.predicate_select_code != nullptr;
	default:
		throw InternalException("Unknown SLJIT native region expression kind");
	}
}

static bool SljitAggregateSumPayloadIsInt64(const LogicalType &payload_type) {
	return payload_type.InternalType() == PhysicalType::INT64;
}

static unique_ptr<JitCodeHandle> BuildExecutableGroupedSumUpdate(
    SljitExecutableGroupedAggregateUpdate &executable_update, string &error) {
	auto &plan = executable_update.plan;
	if (!SljitAggregateSumPayloadIsInt64(plan.payload_type)) {
		error = "SLJIT native grouped sum update requires one INT64 payload child";
		return nullptr;
	}
	if (plan.state_type == LogicalType::BIGINT) {
		return BuildSljitGroupedSumInt64Update(executable_update.function, error);
	}
	if (plan.state_type == LogicalType::HUGEINT) {
		return BuildSljitGroupedSumHugeintInt64Update(executable_update.function, error);
	}
	error = "SLJIT native grouped sum update has unsupported state type " + plan.state_type.ToString();
	return nullptr;
}

static unique_ptr<JitCodeHandle> BuildExecutableUngroupedSumUpdate(
    SljitExecutableUngroupedAggregateUpdate &executable_update, string &error) {
	auto &plan = executable_update.plan;
	if (!SljitAggregateSumPayloadIsInt64(plan.payload_type)) {
		error = "SLJIT native ungrouped sum update requires one INT64 payload child";
		return nullptr;
	}
	if (plan.state_type == LogicalType::BIGINT) {
		return BuildSljitUngroupedSumInt64Update(executable_update.function, error);
	}
	if (plan.state_type == LogicalType::HUGEINT) {
		return BuildSljitUngroupedSumHugeintInt64Update(executable_update.function, error);
	}
	error = "SLJIT native ungrouped sum update has unsupported state type " + plan.state_type.ToString();
	return nullptr;
}

static bool BuildExecutableGroupedAggregateUpdates(const vector<SljitNativeGroupedAggregateUpdatePlan> &updates,
                                                   vector<SljitExecutableGroupedAggregateUpdate> &executable_updates,
                                                   string &error) {
	executable_updates.reserve(updates.size());
	for (auto &update : updates) {
		SljitExecutableGroupedAggregateUpdate executable_update;
		executable_update.plan = update;
		switch (update.update_kind) {
		case JitAggregateUpdateKind::COUNT_STAR:
			executable_update.code = BuildSljitGroupedCountStarUpdate(executable_update.function, error);
			break;
		case JitAggregateUpdateKind::COUNT:
			executable_update.code = BuildSljitGroupedCountUpdate(executable_update.function, error);
			break;
		case JitAggregateUpdateKind::SUM:
			executable_update.code = BuildExecutableGroupedSumUpdate(executable_update, error);
			break;
		default:
			error = "SLJIT native grouped aggregate update kind is not executable";
			return false;
		}
		if (!executable_update.code || !executable_update.function) {
			return false;
		}
		executable_updates.push_back(std::move(executable_update));
	}
	return true;
}

static bool BuildExecutableRegionOp(const SljitNativeRegionOpPlan &op, SljitExecutableRegionOp &executable,
                                    string &error) {
	executable.kind = op.kind;
	executable.operator_index = op.operator_index;
	executable.output_types = op.output_types;
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return BuildExecutableRegionExpression(op.filter, true, executable.filter, error);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		executable.hash_join_probe.plan = op.hash_join_probe;
		executable.hash_join_probe.code = BuildSljitHashJoinProbe(
		    op.hash_join_probe.keys, op.hash_join_probe.equality_key_count, op.hash_join_probe.mark_build_match,
		    op.hash_join_probe.found_match_offset, op.hash_join_probe.pointer_offset,
		    op.hash_join_probe.output_mode, executable.hash_join_probe.function, error);
		return executable.hash_join_probe.code != nullptr && executable.hash_join_probe.function != nullptr;
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		executable.hash_join_build.plan = op.hash_join_build;
		return true;
	case SljitNativeRegionOpKind::RESULT_COLLECTOR_APPEND:
		return true;
	case SljitNativeRegionOpKind::PROJECTION:
		executable.projections.reserve(op.projections.size());
		for (auto &projection : op.projections) {
			SljitExecutableRegionExpression executable_projection;
			if (!BuildExecutableRegionExpression(projection, false, executable_projection, error)) {
				return false;
			}
			executable.projections.push_back(std::move(executable_projection));
		}
		return true;
	case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
	case SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE:
		executable.grouped_aggregate_groups = op.grouped_aggregate_groups;
		executable.grouped_aggregate_payloads = op.grouped_aggregate_payloads;
		return BuildExecutableGroupedAggregateUpdates(op.native_grouped_aggregate_updates,
		                                             executable.native_grouped_aggregate_updates, error);
	case SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE:
		executable.aggregate_payloads = op.aggregate_payloads;
		executable.native_ungrouped_aggregate_updates.reserve(op.native_ungrouped_aggregate_updates.size());
		for (auto &update : op.native_ungrouped_aggregate_updates) {
			SljitExecutableUngroupedAggregateUpdate executable_update;
			executable_update.plan = update;
			switch (update.update_kind) {
			case JitAggregateUpdateKind::COUNT_STAR:
				executable_update.code =
				    BuildSljitUngroupedCountStarUpdate(executable_update.function, error);
				break;
			case JitAggregateUpdateKind::COUNT:
				executable_update.code = BuildSljitUngroupedCountUpdate(executable_update.function, error);
				break;
			case JitAggregateUpdateKind::SUM:
				executable_update.code = BuildExecutableUngroupedSumUpdate(executable_update, error);
				break;
			default:
				error = "SLJIT native ungrouped aggregate update kind is not executable";
				return false;
			}
			if (!executable_update.code || !executable_update.function) {
				return false;
			}
			executable.native_ungrouped_aggregate_updates.push_back(std::move(executable_update));
		}
		return true;
	default:
		throw InternalException("Unknown SLJIT native region operator kind");
	}
}

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region,
                                        SljitExecutableRegion &executable, string &error) {
	executable.ops.reserve(region.ops.size());
	for (auto &op : region.ops) {
		SljitExecutableRegionOp executable_op;
		if (!BuildExecutableRegionOp(op, executable_op, error)) {
			return false;
		}
		executable.ops.push_back(std::move(executable_op));
	}
	return true;
}

} // namespace duckdb
