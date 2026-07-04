//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_projection_floating_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

template <class PROJECTION_SCRATCH>
static bool PrepareFlatFusedFloatingSource(DataChunk &input, idx_t source_index, idx_t projection_idx,
                                           bool right_source, idx_t count, PROJECTION_SCRATCH &adapter_scratch,
                                           idx_t source_offset = 0) {
	if (source_index >= input.ColumnCount()) {
		throw InternalException("SLJIT fused projection source is out of range");
	}
	const_data_ptr_t source_data;
	if (adapter_scratch.TryGetPreparedInput(source_index, source_data)) {
		adapter_scratch.SetSourceData(projection_idx, right_source, source_data);
		return true;
	}

	UnifiedVectorFormat source_format;
	input.data[source_index].ToUnifiedFormat(source_format);
	if (!SljitUnifiedFormatHasIdentitySelection(source_format) ||
	    (source_format.validity.CanHaveNull() &&
	     !source_format.validity.CheckAllValid(source_offset + count, source_offset))) {
		return false;
	}
	source_data = source_format.data + source_offset * GetTypeIdSize(input.data[source_index].GetType().InternalType());
	adapter_scratch.AddPreparedInput(source_index, source_data);
	adapter_scratch.SetSourceData(projection_idx, right_source, source_data);
	return true;
}

template <class PROJECTION_SCRATCH>
static bool PrepareFlatFusedFloatingProjectionSources(SljitExecutableRegionOp &op, DataChunk &input,
                                                      const SelectionVector *execute_sel, idx_t count,
                                                      PROJECTION_SCRATCH &adapter_scratch, bool track_fused,
                                                      idx_t source_offset = 0) {
	if (!op.flat_fused_floating_projection_function || execute_sel) {
		return false;
	}
	adapter_scratch.Prepare(op.projections.size(), track_fused);
	for (auto projection_idx : op.flat_fused_floating_projection_plan.projection_indices) {
		auto &plan = op.projections[projection_idx].plan;
		D_ASSERT(plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES);
		if (!PrepareFlatFusedFloatingSource(input, plan.source_index, projection_idx, false, count, adapter_scratch,
		                                    source_offset)) {
			return false;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			if (!PrepareFlatFusedFloatingSource(input, plan.right_source_index, projection_idx, true, count,
			                                    adapter_scratch, source_offset)) {
				return false;
			}
		}
	}
	return true;
}

template <class PROJECTION_SCRATCH>
static void RunFlatFusedFloatingProjection(SljitExecutableRegionOp &op, idx_t count,
                                           PROJECTION_SCRATCH &adapter_scratch) {
	SljitNativeVectorInput native_input;
	native_input.source_data_array = adapter_scratch.SourceDataArray();
	native_input.right_source_data_array = adapter_scratch.RightSourceDataArray();
	native_input.result_data_array = adapter_scratch.result_data.data();
	native_input.floating_constants = op.flat_fused_floating_projection_plan.SinglePrecision()
	                                      ? reinterpret_cast<const_data_ptr_t>(adapter_scratch.float_constants.data())
	                                      : reinterpret_cast<const_data_ptr_t>(adapter_scratch.double_constants.data());
	if (adapter_scratch.collect_floating_stats) {
		native_input.floating_stats_min = op.flat_fused_floating_projection_plan.SinglePrecision()
		                                      ? reinterpret_cast<data_ptr_t>(adapter_scratch.float_stats_min.data())
		                                      : reinterpret_cast<data_ptr_t>(adapter_scratch.double_stats_min.data());
		native_input.floating_stats_max = op.flat_fused_floating_projection_plan.SinglePrecision()
		                                      ? reinterpret_cast<data_ptr_t>(adapter_scratch.float_stats_max.data())
		                                      : reinterpret_cast<data_ptr_t>(adapter_scratch.double_stats_max.data());
	}
	native_input.count = count;
	native_input.has_error = false;
	SljitExecuteNativeFunction(op.flat_fused_floating_projection_function, native_input);
}

template <class PROJECTION_SCRATCH>
static bool ExecuteFlatFusedFloatingProjection(SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
                                               const SelectionVector *execute_sel, idx_t count,
                                               PROJECTION_SCRATCH &adapter_scratch) {
	auto all_projections_fused = op.flat_fused_floating_projection_plan.covers_all_projections;
	if (!PrepareFlatFusedFloatingProjectionSources(op, input, execute_sel, count, adapter_scratch,
	                                               !all_projections_fused)) {
		return false;
	}

	for (auto projection_idx : op.flat_fused_floating_projection_plan.projection_indices) {
		auto &plan = op.projections[projection_idx].plan;
		auto &result = output.data[projection_idx];
		result.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::ValidityMutable(result).Reset(count);
		if (plan.return_type.InternalType() == PhysicalType::FLOAT) {
			adapter_scratch.result_data[projection_idx] =
			    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<float>(result));
			adapter_scratch.float_constants[projection_idx] = static_cast<float>(plan.double_constant);
		} else {
			adapter_scratch.result_data[projection_idx] =
			    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<double>(result));
			adapter_scratch.double_constants[projection_idx] = plan.double_constant;
		}
		FlatVector::SetSize(result, count_t(count));
		if (!all_projections_fused) {
			adapter_scratch.fused[projection_idx] = 1;
		}
	}

	RunFlatFusedFloatingProjection(op, count, adapter_scratch);
	return true;
}

} // namespace duckdb
