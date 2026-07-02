//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_primitive_payload_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static void SljitExecutePrimitiveAggregatePayloadUpdate(SljitExecutableRegionExpression &payload,
                                                        SljitNativeAggregateUpdateFunction function,
                                                        const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                        DataChunk &input, const SelectionVector *execute_sel,
                                                        idx_t count, SljitExpressionAdapterScratch &adapter_scratch,
                                                        optional_ptr<Vector> grouped_state_addresses = nullptr) {
	if (!function) {
		throw InternalException("SLJIT aggregate primitive payload update is missing generated code");
	}
	if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		if (grouped_state_addresses) {
			if (!lane.ready || lane.state_size == 0) {
				auto blocker = lane.blocker.empty() ? "aggregate-count-star-grouped-lane-incomplete" : lane.blocker;
				throw InternalException("SLJIT grouped aggregate count-star lane is incomplete: %s", blocker.c_str());
			}
		} else if (!lane.ready || !lane.sum_int64_value || !lane.row_count) {
			auto blocker = lane.blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane.blocker;
			throw InternalException("SLJIT aggregate count-star lane is incomplete: %s", blocker.c_str());
		}

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.count = count;
		native_input.aggregate_int64_value = lane.sum_int64_value;
		native_input.aggregate_row_count = lane.row_count;
		if (grouped_state_addresses) {
			grouped_state_addresses->Flatten();
			native_input.aggregate_state_addresses = FlatVector::GetData<uintptr_t>(*grouped_state_addresses);
			native_input.aggregate_state_offset = lane.state_offset;
			native_input.aggregate_state_value_offset = lane.state_value_offset;
		}
		native_input.has_error = false;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		return;
	}
	if (grouped_state_addresses) {
		if (!lane.ready || lane.state_size == 0) {
			auto blocker = lane.blocker.empty() ? "aggregate-primitive-grouped-lane-incomplete" : lane.blocker;
			throw InternalException("SLJIT grouped aggregate primitive lane is incomplete: %s", blocker.c_str());
		}
	} else {
		auto has_sum_state = (AggregatePrimitiveUpdateUsesInt64State(lane.kind) && lane.sum_int64_value) ||
		                     (AggregatePrimitiveUpdateUsesHugeintState(lane.kind) && lane.sum_hugeint_value) ||
		                     (AggregatePrimitiveUpdateUsesDoubleState(lane.kind) && lane.sum_double_value);
		const auto needs_state_is_set = AggregatePrimitiveUpdateHasStateIsSet(lane.kind);
		if (!lane.ready || !has_sum_state || (needs_state_is_set && !lane.state_is_set) || !lane.row_count) {
			auto blocker = lane.blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane.blocker;
			throw InternalException("SLJIT aggregate primitive lane is incomplete: %s", blocker.c_str());
		}
	}
	auto &plan = payload.plan;
	if (plan.return_type.InternalType() != lane.payload_type) {
		throw InternalException("SLJIT aggregate primitive payload type mismatch");
	}

	SljitNativeVectorInput native_input;
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.count = count;
	native_input.aggregate_int64_value = lane.sum_int64_value;
	native_input.aggregate_hugeint_value = lane.sum_hugeint_value;
	native_input.aggregate_double_value = lane.sum_double_value;
	native_input.aggregate_state_is_set = lane.state_is_set;
	native_input.aggregate_row_count = lane.row_count;
	if (grouped_state_addresses) {
		grouped_state_addresses->Flatten();
		native_input.aggregate_state_addresses = FlatVector::GetData<uintptr_t>(*grouped_state_addresses);
		native_input.aggregate_state_offset = lane.state_offset;
		native_input.aggregate_state_value_offset = lane.state_value_offset;
		native_input.aggregate_state_is_set_offset = lane.state_is_set_offset;
	}
	native_input.has_error = false;

	UnifiedVectorFormat source_format;
	UnifiedVectorFormat right_source_format;

	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (plan.source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT aggregate primitive reference source is out of range");
		}
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		if (lane.kind == AggregatePrimitiveUpdateKind::COUNT) {
			native_input.source_data = nullptr;
		} else {
			native_input.source_data = plan.return_type.InternalType() == PhysicalType::DOUBLE
			                               ? source_format.data
			                               : NativeIntegerSourceData(source_format, plan.integer_kind);
		}
		native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
		native_input.source_validity = source_format.validity.GetData();
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		if (plan.source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT aggregate primitive binary source is out of range");
		}
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
		native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
		native_input.source_validity = source_format.validity.GetData();
		native_input.constant = plan.constant;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		if (plan.source_index >= input.ColumnCount() || plan.right_source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT aggregate primitive binary source is out of range");
		}
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		input.data[plan.right_source_index].ToUnifiedFormat(right_source_format);
		native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
		native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
		native_input.source_validity = source_format.validity.GetData();
		native_input.right_source_data = NativeIntegerSourceData(right_source_format, plan.integer_kind);
		native_input.right_source_sel = SljitNormalizedSourceSelectionData(right_source_format);
		native_input.right_source_validity = right_source_format.validity.GetData();
		break;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		if (plan.source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT aggregate primitive double binary source is out of range");
		}
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		native_input.source_data = source_format.data;
		native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
		native_input.source_validity = source_format.validity.GetData();
		native_input.double_constant = plan.double_constant;
		native_input.source_double_scale = plan.double_source_scale;
		break;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		if (plan.source_index >= input.ColumnCount() || plan.right_source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT aggregate primitive double binary source is out of range");
		}
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		input.data[plan.right_source_index].ToUnifiedFormat(right_source_format);
		native_input.source_data = source_format.data;
		native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
		native_input.source_validity = source_format.validity.GetData();
		native_input.right_source_data = right_source_format.data;
		native_input.right_source_sel = SljitNormalizedSourceSelectionData(right_source_format);
		native_input.right_source_validity = right_source_format.validity.GetData();
		native_input.source_double_scale = plan.double_source_scale;
		native_input.right_source_double_scale = plan.double_right_source_scale;
		break;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE: {
		if (!plan.expression_tree) {
			throw InternalException("SLJIT aggregate primitive expression-tree payload is missing IR");
		}
		adapter_scratch.PrepareExpressionTree(input, payload, native_input, execute_sel, count);
		break;
	}
	default:
		throw InternalException("SLJIT aggregate primitive payload has no runtime input adapter");
	}

	function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
}

} // namespace duckdb
