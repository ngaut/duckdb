//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_input_row_pointer_complementary_sum_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_batch_runtime.hpp"
#include "sljit_grouped_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_hash_join_projection_aggregate_input_runtime.hpp"

namespace duckdb {

template <class T>
struct SljitJoinInputComplementaryGroupEntry {
	bool occupied = false;
	bool valid = false;
	T key {};
	idx_t group_idx = DConstants::INVALID_INDEX;
};

static bool SljitTryResolveComplementarySumRHSField(const ExecutionHashJoinProbeBinding &binding,
                                                    SljitJoinProjectionAggregateDescriptor &descriptor,
                                                    const SljitStringSetComplementarySumDescriptor &classification,
                                                    SljitComplementarySumRHSField &field, string &blocker) {
	if (classification.predicate_source_idx >= descriptor.input_sources.size()) {
		blocker = "source_bounds_" + to_string(classification.predicate_source_idx) + "_" +
		          to_string(descriptor.input_sources.size());
		return false;
	}
	auto &input_source = descriptor.input_sources[classification.predicate_source_idx];
	if (input_source.kind != SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT ||
	    input_source.projection_idx >= descriptor.Projection().projections.size()) {
		blocker = "source_kind_" + to_string(static_cast<int>(input_source.kind)) + "_projection_" +
		          to_string(input_source.projection_idx) + "_count_" +
		          to_string(descriptor.Projection().projections.size());
		return false;
	}
	SljitExecutableRegionExpression predicate_projection;
	idx_t join_output_source_idx;
	if (!SljitTryBuildSingleSourceProjectionExpression(descriptor.Projection().projections[input_source.projection_idx],
	                                                   predicate_projection, join_output_source_idx)) {
		blocker =
		    "projection_kind_" +
		    to_string(static_cast<int>(descriptor.Projection().projections[input_source.projection_idx].plan.kind));
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_idx < lhs_column_count) {
		blocker = "lhs_source_" + to_string(join_output_source_idx) + "_lhs_count_" + to_string(lhs_column_count);
		return false;
	}
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, join_output_source_idx - lhs_column_count, field.source)) {
		blocker = "rhs_source_" + to_string(join_output_source_idx - lhs_column_count);
		return false;
	}
	if (field.source.layout_offset == DConstants::INVALID_INDEX) {
		blocker = "rhs_layout";
		return false;
	}
	if (SljitProjectionIsSingleSourceReferenceLike(predicate_projection.plan)) {
		if (field.source.type.id() != LogicalTypeId::VARCHAR || field.source.physical_type != PhysicalType::VARCHAR) {
			blocker = "rhs_reference_type_" + field.source.type.ToString();
			return false;
		}
		return true;
	}
	if (predicate_projection.plan.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
	    predicate_projection.plan.source_index != 0 ||
	    predicate_projection.plan.return_type.id() != LogicalTypeId::VARCHAR) {
		blocker = "projection_kind_" + to_string(static_cast<int>(predicate_projection.plan.kind));
		return false;
	}
	field.compressed_size = predicate_projection.plan.string_decompress_source_size;
	if ((field.compressed_size != sizeof(uint8_t) && field.compressed_size != sizeof(uint16_t) &&
	     field.compressed_size != sizeof(uint32_t) && field.compressed_size != sizeof(uint64_t) &&
	     field.compressed_size != sizeof(uhugeint_t)) ||
	    GetTypeIdSize(field.source.physical_type) != field.compressed_size) {
		blocker = "compressed_size_" + to_string(field.compressed_size) + "_physical_" +
		          to_string(static_cast<int>(field.source.physical_type));
		return false;
	}
	for (idx_t constant_idx = 0; constant_idx < classification.constants.size(); constant_idx++) {
		auto constant = string_t(classification.constants[constant_idx]);
		auto target = field.compressed_constants[constant_idx].data();
		switch (field.compressed_size) {
		case sizeof(uint8_t): {
			uint8_t value;
			if (!TryStringCompressValue(constant, value)) {
				blocker = "constant_compression_" + to_string(constant_idx);
				return false;
			}
			Store<uint8_t>(value, target);
			break;
		}
		case sizeof(uint16_t): {
			uint16_t value;
			if (!TryStringCompressValue(constant, value)) {
				blocker = "constant_compression_" + to_string(constant_idx);
				return false;
			}
			Store<uint16_t>(value, target);
			break;
		}
		case sizeof(uint32_t): {
			uint32_t value;
			if (!TryStringCompressValue(constant, value)) {
				blocker = "constant_compression_" + to_string(constant_idx);
				return false;
			}
			Store<uint32_t>(value, target);
			break;
		}
		case sizeof(uint64_t): {
			uint64_t value;
			if (!TryStringCompressValue(constant, value)) {
				blocker = "constant_compression_" + to_string(constant_idx);
				return false;
			}
			Store<uint64_t>(value, target);
			break;
		}
		case sizeof(uhugeint_t): {
			uhugeint_t value;
			if (!TryStringCompressValue(constant, value)) {
				blocker = "constant_compression_" + to_string(constant_idx);
				return false;
			}
			Store<uhugeint_t>(value, target);
			break;
		}
		default:
			return false;
		}
	}
	return true;
}

static bool SljitTryBuildJoinInputRowPointerComplementarySumPlan(const ExecutionHashJoinProbeBinding &binding,
                                                                 SljitExecutableRegionOp &aggregate_op,
                                                                 SljitJoinProjectionAggregateDescriptor &descriptor,
                                                                 SljitJoinInputRowPointerComplementarySumPlan &plan) {
	if (plan.build_state.Built()) {
		return plan.build_state.Ready();
	}
	auto block = [&](const string &reason) {
		return plan.build_state.Block(reason.c_str());
	};
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return block("join_binding");
	}
	if (descriptor.group_sources.size() != 1) {
		return block("group_shape");
	}
	if (!SljitTryBindStringSetComplementarySumDescriptor(aggregate_op, descriptor.payload_source_indices,
	                                                     plan.classification)) {
		return block("classification");
	}
	string predicate_source_blocker;
	if (!SljitTryResolveComplementarySumRHSField(binding, descriptor, plan.classification, plan.predicate_field,
	                                             predicate_source_blocker)) {
		return block("predicate_rhs_field_" + predicate_source_blocker);
	}
	auto &group_source = descriptor.group_sources[0];
	if (group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    group_source.input_vector_index >= descriptor.input_sources.size() ||
	    !SljitInputVectorGroupKeySourceSupportsMaterialization(group_source)) {
		return block("group_source");
	}
	auto &group_input_source = descriptor.input_sources[group_source.input_vector_index];
	if (group_input_source.kind != SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT) {
		return block("group_input_source");
	}
	plan.group_input_vector_idx = group_source.input_vector_index;
	plan.join_input_group_column_idx = group_input_source.input_idx;
	plan.join_input_group_type = group_input_source.type;
	plan.build_state.MarkReady();
	return true;
}

static bool SljitComplementarySumRHSFieldMatches(data_ptr_t row_pointer, const SljitComplementarySumRHSField &field,
                                                 const SljitStringSetComplementarySumDescriptor &classification) {
	if (field.compressed_size != 0) {
		auto source = row_pointer + field.source.layout_offset;
		return memcmp(source, field.compressed_constants[0].data(), field.compressed_size) == 0 ||
		       memcmp(source, field.compressed_constants[1].data(), field.compressed_size) == 0;
	}
	auto predicate = Load<string_t>(row_pointer + field.source.layout_offset);
	return SljitStringEqualsConstant(predicate, classification.constants[0], classification.signatures[0]) ||
	       SljitStringEqualsConstant(predicate, classification.constants[1], classification.signatures[1]);
}

template <class GROUP_TYPE>
static bool SljitTryPreaggregateJoinInputRowPointerComplementarySums(
    Vector &groups, Vector &row_pointers, idx_t count, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count) {
	compact_count = 0;
	if (count == 0 || compact_groups.ColumnCount() != 1 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    groups.GetType().InternalType() != GetTypeId<GROUP_TYPE>()) {
		return false;
	}

	UnifiedVectorFormat group_format;
	groups.ToUnifiedFormat(group_format);
	auto group_data = UnifiedVectorFormat::GetData<GROUP_TYPE>(group_format);
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	auto compact_group_data = PrepareFlatPreaggregatedGroupTarget<GROUP_TYPE>(compact_groups);

	static constexpr idx_t GROUP_LIMIT = SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS;
	static constexpr idx_t CACHE_CAPACITY = GROUP_LIMIT * 2;
	std::array<SljitJoinInputComplementaryGroupEntry<GROUP_TYPE>, CACHE_CAPACITY> entries {};
	std::array<bool, GROUP_LIMIT> group_validity {};
	std::array<int64_t, GROUP_LIMIT> matching_counts {};
	std::array<int64_t, GROUP_LIMIT> non_matching_counts {};
	std::array<idx_t, GROUP_LIMIT> represented_rows {};

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = group_format.sel->get_index(row_idx);
		const bool group_is_valid = group_format.validity.RowIsValid(source_idx);
		GROUP_TYPE key {};
		if (group_is_valid) {
			key = group_data[source_idx];
		}
		const auto hash = group_is_valid ? Hash(key) : hash_t(0);
		auto entry_idx = static_cast<idx_t>(hash) & (CACHE_CAPACITY - 1);
		idx_t group_idx = DConstants::INVALID_INDEX;
		for (idx_t probe_idx = 0; probe_idx < CACHE_CAPACITY; probe_idx++) {
			auto &entry = entries[entry_idx];
			if (!entry.occupied) {
				if (compact_count == GROUP_LIMIT) {
					return false;
				}
				entry.occupied = true;
				entry.valid = group_is_valid;
				entry.key = key;
				entry.group_idx = compact_count;
				group_idx = compact_count++;
				group_validity[group_idx] = group_is_valid;
				compact_group_data[group_idx] = key;
				break;
			}
			if (entry.valid == group_is_valid && (!group_is_valid || entry.key == key)) {
				group_idx = entry.group_idx;
				break;
			}
			entry_idx = (entry_idx + 1) & (CACHE_CAPACITY - 1);
		}
		if (group_idx == DConstants::INVALID_INDEX) {
			return false;
		}

		represented_rows[group_idx]++;
		auto row_pointer = row_pointer_data[row_idx];
		if (!SljitHashJoinRHSFixedColumnSourceIsValid(row_pointer, predicate_field.source)) {
			continue;
		}
		const bool matches = SljitComplementarySumRHSFieldMatches(row_pointer, predicate_field, classification);
		if (matches) {
			matching_counts[group_idx]++;
		} else {
			non_matching_counts[group_idx]++;
		}
	}

	auto &target_validity = FlatVector::ValidityMutable(compact_groups.data[0]);
	target_validity.Reset(compact_count);
	target_validity.SetAllValid(compact_count);
	for (idx_t group_idx = 0; group_idx < compact_count; group_idx++) {
		if (!group_validity[group_idx]) {
			target_validity.SetInvalid(group_idx);
		}
	}
	FlatVector::SetSize(compact_groups.data[0], count_t(compact_count));
	compact_groups.SetChildCardinality(compact_count);

	preaggregate_scratch.Prepare(payload_lanes, compact_count);
	for (idx_t group_idx = 0; group_idx < compact_count; group_idx++) {
		preaggregate_scratch.group_row_counts.push_back(represented_rows[group_idx]);
	}
	for (idx_t payload_idx = 0; payload_idx < preaggregate_scratch.payloads.size(); payload_idx++) {
		auto &payload = preaggregate_scratch.payloads[payload_idx];
		const bool matching_payload = payload_idx == classification.matching_payload_idx;
		for (idx_t group_idx = 0; group_idx < compact_count; group_idx++) {
			const auto delta = matching_payload ? matching_counts[group_idx] : non_matching_counts[group_idx];
			switch (payload.kind) {
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				payload.int64_values.push_back(delta);
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				payload.hugeint_values.emplace_back(delta);
				break;
			default:
				return false;
			}
			// CASE ... ELSE 0 is non-null even when its predicate is null.
			payload.value_is_set.push_back(1);
		}
	}
	return true;
}

static bool SljitTryPreaggregateJoinInputRowPointerComplementarySums(
    Vector &groups, Vector &row_pointers, idx_t count, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count) {
	switch (groups.GetType().InternalType()) {
	case PhysicalType::INT8:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<int8_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::INT16:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<int16_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::INT32:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<int32_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::INT64:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<int64_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::INT128:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<hugeint_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT8:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<uint8_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT16:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<uint16_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT32:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<uint32_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT64:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<uint64_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT128:
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<uhugeint_t>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	default:
		return false;
	}
}

static bool SljitTryExecuteJoinInputRowPointerComplementarySumUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t hash_join_idx, SljitExecutableRegionOp &aggregate_op, SljitDirectJoinOutputAggregateStrategy &strategy,
    DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers, idx_t count,
    optional_ptr<bool> deferred_grouped_finish, bool source_key0_int64_to_int32_unchecked,
    optional_ptr<string> failure_reason = nullptr) {
	auto block = [&](const char *reason) {
		strategy.join_input_complementary_sum_plan.build_state.Block(reason);
		if (failure_reason) {
			*failure_reason = reason;
		}
		return false;
	};
	auto &descriptor = strategy.descriptor;
	if (count == 0) {
		return false;
	}
	if (!scratch.HasOperatorBinding(hash_join_idx) || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return block("runtime_shape");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	auto &plan = strategy.join_input_complementary_sum_plan;
	if (!SljitTryBuildJoinInputRowPointerComplementarySumPlan(binding, aggregate_op, descriptor, plan)) {
		if (failure_reason) {
			*failure_reason = plan.build_state.blocker;
		}
		return false;
	}
	if (plan.join_input_group_column_idx >= join_input.ColumnCount() ||
	    join_input.data[plan.join_input_group_column_idx].GetType() != plan.join_input_group_type) {
		return block("group_input");
	}
	auto group_source = descriptor.group_sources[0];

	descriptor.EnsureInput(runtime.GetAllocator());
	auto &aggregate_input = descriptor.input.chunk;
	aggregate_input.Reset();
	aggregate_input.data[plan.group_input_vector_idx].Slice(join_input.data[plan.join_input_group_column_idx],
	                                                        match_selection, count);
	aggregate_input.SetChildCardinality(count);
	SljitApplyInputVectorGroupBatchCastProofs(aggregate_input, descriptor.group_sources, count);
	group_source = descriptor.group_sources[0];

	auto &payload_scratch = scratch.AggregatePayloadScratch(strategy.aggregate_idx);
	auto &materialized_groups =
	    payload_scratch.PrepareInputVectorGroups(runtime.GetAllocator(), descriptor.group_sources);
	if (!SljitTryMaterializeInputVectorGroupSource(aggregate_input, group_source, materialized_groups.data[0], count,
	                                               source_key0_int64_to_int32_unchecked)) {
		return block("group_materialize");
	}
	materialized_groups.SetChildCardinality(count);

	auto &sink_binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, strategy.aggregate_idx, aggregate_op.kind, aggregate_input,
	    aggregate_op.aggregate_update.plan.sink_info, "aggregate-update-runtime-binding-failed",
	    "SLJIT join-input row-pointer complementary aggregate update");
	if (!sink_binding.ready || !sink_binding.aggregate_update.ready || !sink_binding.aggregate_update.primitive.ready ||
	    !sink_binding.aggregate_update.grouped_state.ready || !sink_binding.aggregate_update.grouped_state.state) {
		return block("sink_binding");
	}
	auto &payload_lanes =
	    scratch.AggregatePayloadLanes(strategy.aggregate_idx, aggregate_op.aggregate_update.payload_descriptors,
	                                  sink_binding.aggregate_update.primitive);
	SljitStringSetComplementarySumUpdateState update_state;
	if (!SljitTryBindStringSetComplementarySumLanes(aggregate_op, payload_lanes, plan.classification, update_state)) {
		return block("payload_lanes");
	}

	auto &compact_groups = scratch.AggregatePreaggregatedGroups(strategy.aggregate_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(strategy.aggregate_idx);
	idx_t compact_count;
	auto preaggregate_start = SljitRegionStageStart(runtime);
	if (!SljitTryPreaggregateJoinInputRowPointerComplementarySums(
	        materialized_groups.data[0], row_pointers, count, plan.predicate_field, plan.classification, payload_lanes,
	        compact_groups, preaggregate_scratch, compact_count)) {
		return block("preaggregate");
	}
	RecordSljitRegionStageRuntime(runtime, strategy.aggregate_idx, aggregate_op.kind,
	                              "local_preaggregate_join_input_row_pointer_complementary_sum", preaggregate_start);

	SljitFlushPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, descriptor,
	                                          strategy.pending_batch);
	if (!strategy.pending_preaggregated_groups) {
		strategy.pending_preaggregated_groups = make_shared_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch>();
	}
	strategy.pending_preaggregated_scratch = &scratch;
	strategy.pending_preaggregated_deferred_grouped_finish = deferred_grouped_finish;
	if (!SljitBufferPreaggregatedPrimitiveGroups(
	        runtime, scratch, strategy.aggregate_idx, aggregate_op, compact_groups, preaggregate_scratch, payload_lanes,
	        sink_binding.aggregate_update.grouped_state, count, *strategy.pending_preaggregated_groups, false,
	        deferred_grouped_finish)) {
		throw InternalException("Validated SLJIT join-input row-pointer complementary preaggregation buffer failed");
	}
	RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
	                                            "join_input_row_pointer_preaggregated_complementary_sum_update", count);
	return true;
}

} // namespace duckdb
