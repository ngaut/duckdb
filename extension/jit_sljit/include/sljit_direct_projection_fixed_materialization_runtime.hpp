//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_projection_fixed_materialization_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_fixed_source_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/exception.hpp"

#include <cstring>

namespace duckdb {

static bool IsFixedDirectProjectionGeneratedExpression(const SljitExecutableRegionExpression &expr) {
	auto &plan = expr.plan;
	if (!expr.function || !DirectAppendSupportsFixedSizeType(plan.return_type)) {
		return false;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		return !plan.try_cast;
	default:
		return false;
	}
}

static bool IsSourceAppendDirectProjection(const SljitExecutableRegionExpression &expr, DataChunk &input) {
	auto &plan = expr.plan;
	return plan.kind == SljitNativeRegionExpressionKind::REFERENCE && plan.source_index < input.ColumnCount() &&
	       input.data[plan.source_index].GetType() == plan.return_type &&
	       DirectAppendSupportsSourceAppendType(plan.return_type);
}

static bool TryBindDirectAppendSourceProjection(const SljitExecutableRegionExpression &expr, DataChunk &input,
                                                optional_ptr<DirectAppendSlice> slice, idx_t projection_idx,
                                                idx_t source_offset, idx_t count, bool execute) {
	if (!IsSourceAppendDirectProjection(expr, input)) {
		return false;
	}
	if (!execute) {
		return true;
	}
	if (!slice) {
		throw InternalException("SLJIT source direct append projection missing reservation slice");
	}
	if (slice->sources.size() != slice->targets.size() || projection_idx >= slice->sources.size()) {
		throw InternalException("SLJIT source direct append source count mismatch");
	}
	auto &plan = expr.plan;
	auto &source_vector = input.data[plan.source_index];
	if (source_offset + count > source_vector.size()) {
		throw InternalException("SLJIT source direct append source slice out of range");
	}
	slice->sources[projection_idx].vector = &source_vector;
	slice->sources[projection_idx].offset = source_offset;
	return true;
}

static const char *SljitFixedProjectionExpressionTracePhase(const SljitNativeRegionExpressionPlan &plan) {
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return "direct_batch_expression.reference";
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		return "direct_batch_expression.string_compress";
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		return "direct_batch_expression.integral_compress";
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		return "direct_batch_expression.integral_decompress";
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		return "direct_batch_expression.date_year";
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		return "direct_batch_expression.integer_cast";
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return "direct_batch_expression.integer_binary";
	default:
		return "direct_batch_expression.generated";
	}
}

static bool TryDirectMaterializeFixedReference(const SljitExecutableRegionExpression &expr, DataChunk &input,
                                               data_ptr_t target, idx_t source_offset, idx_t count, bool execute,
                                               optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache) {
	auto &plan = expr.plan;
	if (plan.source_index >= input.ColumnCount() || input.data[plan.source_index].GetType() != plan.return_type ||
	    !DirectAppendSupportsFixedSizeType(plan.return_type)) {
		return false;
	}

	UnifiedVectorFormat local_source_format;
	UnifiedVectorFormat *source_format;
	if (!PrepareFixedDirectProjectionSource(input, plan.source_index, source_offset, count, source_cache,
	                                        local_source_format, source_format)) {
		return false;
	}
	if (!execute) {
		return true;
	}
	if (!target) {
		throw InternalException("SLJIT fixed direct projection reference target is null");
	}
	auto source_data = OffsetFixedSizeData(source_format->data, plan.return_type, source_offset);
	memcpy(target, source_data, count * GetTypeIdSize(plan.return_type.InternalType()));
	return true;
}

static bool TryDirectMaterializeFixedGenerated(const SljitExecutableRegionExpression &expr, DataChunk &input,
                                               data_ptr_t target, idx_t source_offset, idx_t count, bool execute,
                                               optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache) {
	if (!IsFixedDirectProjectionGeneratedExpression(expr)) {
		return false;
	}
	auto &plan = expr.plan;
	UnifiedVectorFormat local_source_format;
	UnifiedVectorFormat *source_format;
	if (!PrepareFixedDirectProjectionSource(input, plan.source_index, source_offset, count, source_cache,
	                                        local_source_format, source_format)) {
		return false;
	}
	const bool has_right_source = plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
	UnifiedVectorFormat local_right_source_format;
	UnifiedVectorFormat *right_source_format = nullptr;
	if (has_right_source) {
		if (!PrepareFixedDirectProjectionSource(input, plan.right_source_index, source_offset, count, source_cache,
		                                        local_right_source_format, right_source_format)) {
			return false;
		}
	}
	if (!execute) {
		return true;
	}
	if (!target) {
		throw InternalException("SLJIT fixed direct projection generated target is null");
	}

	SljitNativeVectorInput native_input;
	if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS) {
		native_input.source_data = source_format->data;
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
		native_input.source_data = NativeUnsignedIntegerSourceData(*source_format, plan.unsigned_source_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
	           plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
	           plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
		native_input.source_data = NativeSignedIntegerSourceData(*source_format, plan.cast_source_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
		native_input.source_data = NativeIntegerSourceData(*source_format, SljitNativeIntegerKind::INT32);
	} else {
		native_input.source_data = NativeIntegerSourceData(*source_format, plan.integer_kind);
	}
	native_input.source_data =
	    OffsetFixedSizeData(native_input.source_data, input.data[plan.source_index].GetType(), source_offset);
	if (has_right_source) {
		native_input.right_source_data = NativeIntegerSourceData(*right_source_format, plan.integer_kind);
		native_input.right_source_data = OffsetFixedSizeData(
		    native_input.right_source_data, input.data[plan.right_source_index].GetType(), source_offset);
	}
	native_input.constants = plan.constants.data();
	native_input.constant = plan.constant;
	native_input.result_data = target;
	native_input.overflow_message =
	    plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
	            plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
	            plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
	            plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST
	        ? expr.overflow_message.c_str()
	        : nullptr;
	native_input.query_location = plan.query_location;
	native_input.count = count;
	native_input.has_error = false;
	auto function = expr.flat_function ? expr.flat_function : expr.function;
	SljitExecuteNativeFunction(function, native_input);
	return true;
}

static bool TryDirectMaterializeFixedExpression(const SljitExecutableRegionExpression &expr, DataChunk &input,
                                                data_ptr_t target, idx_t source_offset, idx_t count, bool execute,
                                                optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache) {
	if (expr.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		return TryDirectMaterializeFixedReference(expr, input, target, source_offset, count, execute, source_cache);
	}
	return TryDirectMaterializeFixedGenerated(expr, input, target, source_offset, count, execute, source_cache);
}

static bool SljitTryDirectMaterializeFixedProjection(
    SljitExecutableRegionOp &op, DataChunk &input, optional_ptr<DirectAppendSlice> slice,
    optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache = nullptr,
    optional_ptr<vector<uint8_t>> skip_projection = nullptr,
    optional_ptr<ExecutionRegionRuntime> trace_runtime = nullptr, idx_t trace_op_idx = DConstants::INVALID_INDEX) {
	if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.empty()) {
		return false;
	}
	const bool execute = slice != nullptr;
	const auto source_offset = execute ? slice->source_offset : 0;
	const auto count = execute ? slice->count : input.size();
	const bool trace_expressions =
	    execute && trace_runtime && trace_runtime->TraceRuntime() && trace_op_idx != DConstants::INVALID_INDEX;
	auto record_expression_runtime = [&](const SljitExecutableRegionExpression &projection,
	                                     std::chrono::steady_clock::time_point expression_stage_start) {
		if (!trace_expressions) {
			return;
		}
		RecordSljitRegionStageRuntime(*trace_runtime, trace_op_idx, op.kind,
		                              SljitFixedProjectionExpressionTracePhase(projection.plan),
		                              expression_stage_start);
	};
	for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
		if (skip_projection && projection_idx < skip_projection->size() && (*skip_projection)[projection_idx]) {
			continue;
		}
		auto &projection = op.projections[projection_idx];
		auto expression_stage_start =
		    trace_expressions ? SljitRegionStageStart(*trace_runtime) : std::chrono::steady_clock::time_point();
		if (TryBindDirectAppendSourceProjection(projection, input, slice, projection_idx, source_offset, count,
		                                        execute)) {
			record_expression_runtime(projection, expression_stage_start);
			continue;
		}
		data_ptr_t target = nullptr;
		if (execute) {
			if (slice->targets.size() != op.projections.size()) {
				throw InternalException("SLJIT fixed direct projection target count mismatch");
			}
			target = slice->targets[projection_idx];
		}
		if (!TryDirectMaterializeFixedExpression(projection, input, target, source_offset, count, execute,
		                                         source_cache)) {
			return false;
		}
		record_expression_runtime(projection, expression_stage_start);
	}
	return true;
}

} // namespace duckdb
