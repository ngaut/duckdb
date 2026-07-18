//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_preaggregation_common_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregation.hpp"
#include "sljit_executable_aggregate_codegen.hpp"

namespace duckdb {

static constexpr idx_t SLJIT_PRIMITIVE_RUN_MIN_COMPRESSION = 3;

template <class TARGET_TYPE, class LOAD_KEY>
static bool SljitTryInputVectorHasProfitablePrimitiveRuns(idx_t count, LOAD_KEY &&load_key, bool &profitable) {
	profitable = false;
	if (count < 2) {
		return true;
	}
	TARGET_TYPE previous_key;
	if (!load_key(0, previous_key)) {
		return false;
	}
	const auto sample_count = MinValue<idx_t>(count, 64);
	idx_t transition_count = 0;
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		TARGET_TYPE key;
		if (!load_key(row_idx, key)) {
			return false;
		}
		if (!(key == previous_key)) {
			transition_count++;
		}
		previous_key = key;
	}
	// Transitions measure the distance between run starts without charging the
	// sample's partial leading and trailing runs as complete groups.
	profitable = transition_count == 0 || transition_count * SLJIT_PRIMITIVE_RUN_MIN_COMPRESSION <= sample_count - 1;
	return true;
}

static bool
SljitPreaggregatedInputVectorGroupRowsAllValid(const SljitPreaggregatedInputVectorGroupKeySource &group_source) {
	return group_source.rows_all_valid;
}

static bool
SljitTryBindGeneratedPrimitiveRunSource(ExecutionRegionRuntime &runtime, SljitExecutablePrimitiveRunUpdate &run_update,
                                        SljitPreaggregatedInputVectorGroupKeySource &group_source,
                                        SljitPreaggregatedPrimitivePayloadSources &payload_sources,
                                        const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                        idx_t count, vector<SljitNativePrimitiveRunLaneInput> &lane_inputs,
                                        SljitNativePrimitiveRunInput &native_input,
                                        SljitNativePrimitiveRunFunction &function, const char *&blocker) {
	blocker = nullptr;
	function = nullptr;
	if (!run_update.HasDeferredCodegen()) {
		blocker = "code";
		return false;
	}
	if (count == 0 || count > STANDARD_VECTOR_SIZE || payload_lanes.empty() ||
	    payload_lanes.size() != run_update.primitive_kinds.size() ||
	    payload_lanes.size() != run_update.payload_types.size() || lane_inputs.size() != payload_lanes.size() ||
	    !group_source.source) {
		blocker = "shape";
		return false;
	}
	auto &source = *group_source.source;
	const auto group_output_type = SljitGroupKeyEquivalencePhysicalType(source);
	const bool exact_group_type = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
	                              source.source_physical_type == group_output_type;
	const bool proven_narrowing_group_cast =
	    ExecutionGroupKeyCastIsNarrowingIntegral(source.cast_kind) && source.unchecked_integral_cast;
	const bool integral_compression = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS;
	if (!exact_group_type && !proven_narrowing_group_cast && !integral_compression) {
		blocker = "group_cast";
		return false;
	}
	if (source.target_physical_type != run_update.group_type) {
		blocker = "group_type";
		return false;
	}
	if (group_source.format.sel->IsSet()) {
		blocker = "group_selection";
		return false;
	}
	if (!SljitPreaggregatedInputVectorGroupRowsAllValid(group_source)) {
		blocker = "group_null";
		return false;
	}
	native_input = SljitNativePrimitiveRunInput();
	for (idx_t lane_idx = 0; lane_idx < payload_lanes.size(); lane_idx++) {
		auto lane = payload_lanes[lane_idx];
		if (!lane || lane->kind != run_update.primitive_kinds[lane_idx]) {
			blocker = "specialization";
			return false;
		}
		auto &lane_input = lane_inputs[lane_idx];
		lane_input = SljitNativePrimitiveRunLaneInput();
		PhysicalType payload_type = PhysicalType::INVALID;
		if (lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
			auto payload_source = payload_sources.GetSource(lane_idx);
			if (!payload_source) {
				blocker = "payload_source";
				return false;
			}
			if (payload_source->format.sel->IsSet()) {
				blocker = "payload_selection";
				return false;
			}
			lane_input.payload_validity =
			    payload_source->rows_all_valid ? nullptr : payload_source->format.validity.GetData();
			if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
			    lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				payload_type = payload_source->type;
				lane_input.payload_data = payload_source->format.data;
			} else if (lane->kind != AggregatePrimitiveUpdateKind::COUNT) {
				blocker = "payload_kind";
				return false;
			}
		}
		if (run_update.payload_types[lane_idx] != payload_type) {
			blocker = "specialization";
			return false;
		}
	}
	const bool single_lane_nullable = lane_inputs.size() == 1 && lane_inputs[0].payload_validity != nullptr;
	function = SljitEnsureExecutablePrimitiveRunUpdate(runtime, run_update, source.source_physical_type,
	                                                   group_output_type, source.cast_kind, single_lane_nullable);
	if (!function) {
		blocker = "specialization";
		return false;
	}
	native_input.group_data = group_source.format.data;
	native_input.lane_inputs = lane_inputs.data();
	if (lane_inputs.size() == 1) {
		native_input.payload_data = lane_inputs[0].payload_data;
		native_input.payload_validity = lane_inputs[0].payload_validity;
	}
	native_input.group_cast_constant = source.cast_constant;
	native_input.input_count = count;
	return true;
}

static bool SljitTryBindGeneratedFusedAffinePrimitiveRunSource(
    ExecutionRegionRuntime &runtime, SljitExecutableRegionOp &op, DataChunk &input,
    SljitPreaggregatedInputVectorGroupKeySource &group_source, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, idx_t count,
    SljitPreaggregatedPrimitivePayloadSource &payload_source, SljitNativePrimitiveRunInput &native_input,
    SljitNativePrimitiveRunFunction &function, const char *&blocker) {
	blocker = nullptr;
	function = nullptr;
	auto &primitive_run_update = op.aggregate_update.primitive_run_update;
	auto &affine_run_update = op.aggregate_update.fused_affine_run_update;
	if (!affine_run_update.Ready() || affine_run_update.primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
		blocker = "code";
		return false;
	}
	if (count == 0 || count > STANDARD_VECTOR_SIZE || payload_lanes.size() != affine_run_update.lanes.size() ||
	    affine_run_update.source_position >= payload_source_indices.size() || !group_source.source) {
		blocker = "shape";
		return false;
	}
	auto &source = *group_source.source;
	const auto group_output_type = SljitGroupKeyEquivalencePhysicalType(source);
	const bool exact_group_type = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
	                              source.source_physical_type == group_output_type;
	const bool proven_narrowing_group_cast =
	    ExecutionGroupKeyCastIsNarrowingIntegral(source.cast_kind) && source.unchecked_integral_cast;
	const bool integral_compression = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS;
	if ((!exact_group_type && !proven_narrowing_group_cast && !integral_compression) ||
	    source.target_physical_type != primitive_run_update.group_type) {
		blocker = "group_cast";
		return false;
	}
	if (group_source.format.sel->IsSet()) {
		blocker = "group_selection";
		return false;
	}
	if (!SljitPreaggregatedInputVectorGroupRowsAllValid(group_source)) {
		blocker = "group_null";
		return false;
	}
	const auto payload_source_idx = payload_source_indices[affine_run_update.source_position];
	if (!PrepareSljitPreaggregatedPrimitivePayloadSource(input, payload_lanes[0], payload_source_idx, true,
	                                                     payload_source) ||
	    payload_source.type != affine_run_update.source_type) {
		blocker = "payload_source";
		return false;
	}
	if (payload_source.format.sel->IsSet()) {
		blocker = "payload_selection";
		return false;
	}
	for (auto lane : payload_lanes) {
		if (!lane || lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
			blocker = "specialization";
			return false;
		}
	}
	function = SljitEnsureExecutableFusedAffineRunUpdate(runtime, primitive_run_update, affine_run_update,
	                                                     source.source_physical_type, group_output_type,
	                                                     source.cast_kind, !payload_source.rows_all_valid);
	if (!function) {
		blocker = "specialization";
		return false;
	}
	native_input = SljitNativePrimitiveRunInput();
	native_input.group_data = group_source.format.data;
	native_input.payload_data = payload_source.format.data;
	native_input.payload_validity = payload_source.rows_all_valid ? nullptr : payload_source.format.validity.GetData();
	native_input.group_cast_constant = source.cast_constant;
	native_input.input_count = count;
	return true;
}

template <class TARGET_TYPE>
static bool
SljitBindGeneratedPrimitiveRunOutput(DataChunk &groups, SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                     const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                     vector<SljitNativePrimitiveRunLaneInput> &lane_inputs, idx_t output_count,
                                     idx_t output_capacity, SljitNativePrimitiveRunInput &native_input) {
	if (groups.ColumnCount() != 1 || output_count > output_capacity || payload_lanes.empty() ||
	    scratch.payloads.size() != payload_lanes.size() || lane_inputs.size() != payload_lanes.size() ||
	    !scratch.HasFixedCapacity(payload_lanes, output_capacity)) {
		return false;
	}
	auto &group_vector = groups.data[0];
	group_vector.SetVectorType(VectorType::FLAT_VECTOR);
	if (FlatVector::GetCapacity(group_vector) < output_capacity) {
		return false;
	}
	native_input.output_group_data =
	    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<TARGET_TYPE>(group_vector));
	native_input.output_int64_values = nullptr;
	native_input.output_hugeint_values = nullptr;
	native_input.output_value_is_set = nullptr;
	native_input.output_row_counts = scratch.group_row_counts.data();
	for (idx_t lane_idx = 0; lane_idx < payload_lanes.size(); lane_idx++) {
		auto lane = payload_lanes[lane_idx];
		if (!lane || scratch.payloads[lane_idx].kind != lane->kind) {
			return false;
		}
		auto &lane_input = lane_inputs[lane_idx];
		lane_input.output_int64_values = nullptr;
		lane_input.output_hugeint_values = nullptr;
		lane_input.output_value_is_set = nullptr;
		auto &payload = scratch.payloads[lane_idx];
		switch (lane->kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			lane_input.output_int64_values = payload.int64_values.data();
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			lane_input.output_hugeint_values = payload.hugeint_values.data();
			break;
		default:
			return false;
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
		    lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			lane_input.output_value_is_set = payload.value_is_set.data();
		}
	}
	if (lane_inputs.size() == 1) {
		native_input.output_int64_values = lane_inputs[0].output_int64_values;
		native_input.output_hugeint_values = lane_inputs[0].output_hugeint_values;
		native_input.output_value_is_set = lane_inputs[0].output_value_is_set;
	}
	native_input.lane_inputs = lane_inputs.data();
	native_input.output_count = output_count;
	native_input.output_capacity = output_capacity;
	return true;
}

template <class TARGET_TYPE>
static bool SljitBindGeneratedFusedAffinePrimitiveRunOutput(DataChunk &groups,
                                                            SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                                            idx_t output_count, idx_t output_capacity,
                                                            SljitNativePrimitiveRunInput &native_input) {
	if (groups.ColumnCount() != 1 || output_count > output_capacity ||
	    scratch.payload_layout != SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE ||
	    scratch.group_row_counts.size() != output_capacity || scratch.shared_int64_values.size() != output_capacity ||
	    scratch.shared_hugeint_values.size() != output_capacity ||
	    scratch.shared_value_is_wide.size() != output_capacity ||
	    scratch.shared_valid_counts.size() != output_capacity) {
		return false;
	}
	auto &group_vector = groups.data[0];
	group_vector.SetVectorType(VectorType::FLAT_VECTOR);
	if (FlatVector::GetCapacity(group_vector) < output_capacity) {
		return false;
	}
	native_input.output_group_data =
	    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<TARGET_TYPE>(group_vector));
	native_input.output_shared_int64_values = scratch.shared_int64_values.data();
	native_input.output_shared_valid_counts = scratch.shared_valid_counts.data();
	native_input.output_row_counts = scratch.group_row_counts.data();
	native_input.output_count = output_count;
	native_input.output_capacity = output_capacity;
	return true;
}

static bool
SljitPrimitiveAggregateLanesReplayable(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		auto lane = payload_lanes[payload_idx];
		if (!lane) {
			return false;
		}
		switch (lane->kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			return false;
		}
	}
	return true;
}

static bool
SljitPreaggregatedInputVectorGroupKeyCastReplayable(SljitPreaggregatedInputVectorGroupKeySource &group_source) {
	auto &source = *group_source.source;
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		return source.HasOutputTransform() || source.source_physical_type == source.target_physical_type;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		return source.unchecked_integral_cast;
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
		return SljitPreaggregationIntegralCompressionSourceType(source.source_physical_type) &&
		       SljitPreaggregationCompressedUnsignedTargetType(source.target_physical_type);
	default:
		return false;
	}
}

static bool SljitPreaggregatedInputVectorGroupKeyReplayable(SljitPreaggregatedInputVectorGroupKeySource &group_source) {
	return SljitPreaggregatedInputVectorGroupRowsAllValid(group_source) &&
	       SljitPreaggregatedInputVectorGroupKeyCastReplayable(group_source);
}

template <class PAYLOAD_DISPATCH>
static bool SljitDispatchPreaggregatedInt64PayloadType(PhysicalType payload_type, PAYLOAD_DISPATCH &dispatch) {
	switch (payload_type) {
	case PhysicalType::INT8:
		return dispatch.template Execute<int8_t>();
	case PhysicalType::INT16:
		return dispatch.template Execute<int16_t>();
	case PhysicalType::INT32:
		return dispatch.template Execute<int32_t>();
	case PhysicalType::INT64:
		return dispatch.template Execute<int64_t>();
	case PhysicalType::UINT8:
		return dispatch.template Execute<uint8_t>();
	case PhysicalType::UINT16:
		return dispatch.template Execute<uint16_t>();
	case PhysicalType::UINT32:
		return dispatch.template Execute<uint32_t>();
	default:
		return false;
	}
}

template <class PAYLOAD_DISPATCH>
static bool SljitDispatchPreaggregatedHugeintPayloadType(PhysicalType payload_type, PAYLOAD_DISPATCH &dispatch) {
	switch (payload_type) {
	case PhysicalType::INT8:
		return dispatch.template Execute<int8_t>();
	case PhysicalType::INT16:
		return dispatch.template Execute<int16_t>();
	case PhysicalType::INT32:
		return dispatch.template Execute<int32_t>();
	case PhysicalType::INT64:
		return dispatch.template Execute<int64_t>();
	case PhysicalType::INT128:
		return dispatch.template Execute<hugeint_t>();
	case PhysicalType::UINT8:
		return dispatch.template Execute<uint8_t>();
	case PhysicalType::UINT16:
		return dispatch.template Execute<uint16_t>();
	case PhysicalType::UINT32:
		return dispatch.template Execute<uint32_t>();
	default:
		return false;
	}
}

template <template <class, bool> class SUM_ACCUMULATOR, class SINK>
struct SljitPreaggregatedSingleLanePayloadAccumulatorDispatch {
	const SljitPreaggregatedPrimitivePayloadSource &source;
	SINK &sink;

	template <class PAYLOAD_TYPE>
	bool Execute() {
		auto data = UnifiedVectorFormat::GetData<PAYLOAD_TYPE>(source.format);
		auto selection = source.format.sel;
		if (selection->IsSet()) {
			SUM_ACCUMULATOR<PAYLOAD_TYPE, true> accumulator {data, selection};
			return sink(accumulator);
		}
		SUM_ACCUMULATOR<PAYLOAD_TYPE, false> accumulator {data, selection};
		return sink(accumulator);
	}
};

// One authority for typed single-lane accumulator selection, shared by the run and
// pending preaggregation paths: (lane kind, payload physical type, payload selection)
// choose the accumulator; the sink owns the continuation it feeds.
template <template <class, bool> class INT64_SUM_ACCUMULATOR, template <class, bool> class HUGEINT_SUM_ACCUMULATOR,
          class COUNT_ACCUMULATOR, class SINK>
static bool SljitSelectPreaggregatedSingleLaneAccumulator(
    SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, SINK sink) {
	if (payload_lanes.size() != 1 || !payload_lanes[0]) {
		return false;
	}
	auto &lane = *payload_lanes[0];
	if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    (lane.kind == AggregatePrimitiveUpdateKind::COUNT && !payload_sources.SourceCanHaveNull(0))) {
		COUNT_ACCUMULATOR accumulator;
		return sink(accumulator);
	}
	auto source = payload_sources.GetSource(0);
	if (!source || payload_sources.SourceCanHaveNull(0)) {
		return false;
	}
	switch (lane.kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64: {
		SljitPreaggregatedSingleLanePayloadAccumulatorDispatch<INT64_SUM_ACCUMULATOR, SINK> dispatch {*source, sink};
		return SljitDispatchPreaggregatedInt64PayloadType(source->type, dispatch);
	}
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
		SljitPreaggregatedSingleLanePayloadAccumulatorDispatch<HUGEINT_SUM_ACCUMULATOR, SINK> dispatch {*source, sink};
		return SljitDispatchPreaggregatedHugeintPayloadType(source->type, dispatch);
	}
	default:
		return false;
	}
}

template <class TARGET_TYPE, class LOAD_KEY>
static bool SljitTryInputVectorHasConsecutiveRepeat(idx_t count, LOAD_KEY &&load_key, bool &has_consecutive_repeat) {
	has_consecutive_repeat = false;
	if (count < 2) {
		return true;
	}
	TARGET_TYPE previous_key;
	if (!load_key(0, previous_key)) {
		return false;
	}
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		TARGET_TYPE key;
		if (!load_key(row_idx, key)) {
			return false;
		}
		if (key == previous_key) {
			has_consecutive_repeat = true;
			return true;
		}
		previous_key = key;
	}
	return true;
}

template <class LOAD_KEY>
static bool SljitInputVectorHasConsecutiveRepeat(idx_t count, LOAD_KEY &&load_key) {
	if (count < 2) {
		return false;
	}
	auto previous_key = load_key(0);
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		auto key = load_key(row_idx);
		if (key == previous_key) {
			return true;
		}
		previous_key = key;
	}
	return false;
}

template <class PAYLOAD_TYPE>
static int64_t SljitPreaggregatedPayloadAsInt64(PAYLOAD_TYPE value) {
	return NumericCast<int64_t>(value);
}

static hugeint_t SljitPreaggregatedPayloadAsHugeint(hugeint_t value) {
	return value;
}

template <class PAYLOAD_TYPE>
static hugeint_t SljitPreaggregatedPayloadAsHugeint(PAYLOAD_TYPE value) {
	return hugeint_t(NumericCast<int64_t>(value));
}

} // namespace duckdb
