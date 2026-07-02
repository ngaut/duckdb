//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_projection_fixed_fused_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_fixed_source_runtime.hpp"

#include "duckdb/common/exception.hpp"

#include <exception>

namespace duckdb {

static bool HasDirectProjectionSourceRef(const SljitDirectProjectionPlan &direct_plan, idx_t source_index) {
	for (auto &source : direct_plan.sources) {
		if (source.input_index == source_index) {
			return true;
		}
	}
	return false;
}

template <class PROJECTION_SCRATCH>
static bool TryPrepareFlatFusedFixedProjectionSources(SljitExecutableRegionOp &op, DataChunk &input,
                                                      idx_t source_offset, idx_t count,
                                                      optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache,
                                                      PROJECTION_SCRATCH &adapter_scratch) {
	if (op.flat_fused_fixed_projection_plans.empty() ||
	    op.flat_fused_fixed_projection_plans.size() != op.flat_fused_fixed_projection_functions.size()) {
		return false;
	}
	adapter_scratch.Prepare(op.projections.size(), true);
	for (idx_t plan_idx = 0; plan_idx < op.flat_fused_fixed_projection_plans.size(); plan_idx++) {
		if (!op.flat_fused_fixed_projection_functions[plan_idx]) {
			return false;
		}
		auto &direct_plan = op.flat_fused_fixed_projection_plans[plan_idx];
		if (direct_plan.sources.empty()) {
			return false;
		}
		for (auto &source : direct_plan.sources) {
			if (source.projection_index >= op.projections.size()) {
				throw InternalException("SLJIT fixed fused direct projection source is out of range");
			}
			auto &plan = op.projections[source.projection_index].plan;
			UnifiedVectorFormat local_source_format;
			UnifiedVectorFormat *source_format;
			if (!PrepareFixedDirectProjectionSource(input, source.input_index, source_offset, count, source_cache,
			                                        local_source_format, source_format)) {
				return false;
			}
			auto source_data = NativeIntegerSourceData(*source_format, plan.integer_kind);
			auto source_pointer =
			    OffsetFixedSizeData(source_data, input.data[source.input_index].GetType(), source_offset);
			adapter_scratch.SetSourceData(source.projection_index, source.right_source, source_pointer);
		}
		for (auto projection_idx : direct_plan.projection_indices) {
			auto &expr = op.projections[projection_idx];
			auto &plan = expr.plan;
			if (plan.kind != SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT &&
			    plan.kind != SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
				return false;
			}
			if (!HasDirectProjectionSourceRef(direct_plan, plan.source_index)) {
				return false;
			}
			if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES &&
			    !HasDirectProjectionSourceRef(direct_plan, plan.right_source_index)) {
				return false;
			}
			adapter_scratch.integer_constants[projection_idx] = plan.constant;
			adapter_scratch.overflow_messages[projection_idx] = expr.overflow_message.c_str();
			adapter_scratch.fused[projection_idx] = 1;
		}
	}
	return true;
}

template <class PROJECTION_SCRATCH>
static void BindFlatFusedFixedProjectionTargets(SljitExecutableRegionOp &op, DirectAppendSlice &slice,
                                                PROJECTION_SCRATCH &adapter_scratch) {
	if (slice.targets.size() != op.projections.size()) {
		throw InternalException("SLJIT fixed fused direct projection target count mismatch");
	}
	for (auto &direct_plan : op.flat_fused_fixed_projection_plans) {
		for (auto projection_idx : direct_plan.projection_indices) {
			auto target = slice.targets[projection_idx];
			if (!target) {
				throw InternalException("SLJIT fixed fused direct projection target pointer is null");
			}
			adapter_scratch.result_data[projection_idx] = target;
		}
	}
}

template <class PROJECTION_SCRATCH>
static void RunFlatFusedFixedProjection(SljitExecutableRegionOp &op, idx_t count, PROJECTION_SCRATCH &adapter_scratch) {
	SljitNativeVectorInput native_input;
	native_input.source_data_array = adapter_scratch.SourceDataArray();
	native_input.right_source_data_array = adapter_scratch.RightSourceDataArray();
	native_input.result_data_array = adapter_scratch.result_data.data();
	native_input.constants = adapter_scratch.integer_constants.data();
	native_input.overflow_messages = adapter_scratch.overflow_messages.data();
	native_input.count = count;
	native_input.has_error = false;
	for (auto function : op.flat_fused_fixed_projection_functions) {
		native_input.error = nullptr;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}
}

} // namespace duckdb
