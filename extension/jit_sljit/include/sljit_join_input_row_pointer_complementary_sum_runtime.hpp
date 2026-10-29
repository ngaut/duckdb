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
#include "sljit_join_input_complementary_sum_accumulator.hpp"
#include "sljit_selected_input_vector_group_key.hpp"
#include "sljit_typed_local_group_index.hpp"

namespace duckdb {

static bool
SljitTryInitializeCompressedComplementarySumRHSField(PhysicalType source_physical_type, idx_t compressed_size,
                                                     const SljitStringSetComplementarySumDescriptor &classification,
                                                     SljitComplementarySumRHSField &field, string &blocker) {
	field.compressed_size = compressed_size;
	if ((field.compressed_size != sizeof(uint8_t) && field.compressed_size != sizeof(uint16_t) &&
	     field.compressed_size != sizeof(uint32_t) && field.compressed_size != sizeof(uint64_t) &&
	     field.compressed_size != sizeof(uhugeint_t)) ||
	    GetTypeIdSize(source_physical_type) != field.compressed_size) {
		blocker = "compressed_size_" + to_string(field.compressed_size) + "_physical_" +
		          to_string(static_cast<int>(source_physical_type));
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
	if (!input_source.IsBuildInput() || !input_source.expression) {
		blocker = "source_kind";
		return false;
	}
	auto &predicate_projection = *input_source.expression;
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, input_source.input_idx, field.source)) {
		blocker = "rhs_source_" + to_string(input_source.input_idx);
		return false;
	}
	if (field.source.storage_kind != ExecutionHashJoinRHSFixedColumnStorageKind::ROW ||
	    field.source.layout_offset == DConstants::INVALID_INDEX) {
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
	return SljitTryInitializeCompressedComplementarySumRHSField(field.source.physical_type,
	                                                            predicate_projection.plan.string_decompress_source_size,
	                                                            classification, field, blocker);
}

static bool SljitTryResolveComplementarySumPerfectHashRHSOutput(
    const ExecutionHashJoinProbeBinding &binding, SljitJoinProjectionAggregateDescriptor &descriptor,
    const SljitStringSetComplementarySumDescriptor &classification, SljitComplementarySumRHSField &field,
    idx_t &rhs_output_idx, string &blocker) {
	if (classification.predicate_source_idx >= descriptor.input_sources.size()) {
		blocker = "source_bounds_" + to_string(classification.predicate_source_idx) + "_" +
		          to_string(descriptor.input_sources.size());
		return false;
	}
	auto &input_source = descriptor.input_sources[classification.predicate_source_idx];
	if (!input_source.IsBuildInput() || !input_source.expression ||
	    input_source.expression->plan.return_type.id() != LogicalTypeId::VARCHAR) {
		blocker = "source_kind";
		return false;
	}
	auto &predicate_projection = *input_source.expression;
	rhs_output_idx = input_source.input_idx;
	if (rhs_output_idx >= binding.perfect_layout.rhs_output_types.size()) {
		blocker = "rhs_type_bounds_" + to_string(rhs_output_idx) + "_" +
		          to_string(binding.perfect_layout.rhs_output_types.size());
		return false;
	}
	if (rhs_output_idx >= binding.perfect_layout.rhs_dictionary_buffers.size()) {
		blocker = "rhs_dictionary_bounds_" + to_string(rhs_output_idx) + "_" +
		          to_string(binding.perfect_layout.rhs_dictionary_buffers.size());
		return false;
	}
	if (!binding.perfect_layout.rhs_dictionary_buffers[rhs_output_idx]) {
		blocker = "rhs_dictionary_missing_" + to_string(rhs_output_idx);
		return false;
	}
	auto &rhs_type = binding.perfect_layout.rhs_output_types[rhs_output_idx];
	if (SljitProjectionIsSingleSourceReferenceLike(predicate_projection.plan)) {
		if (rhs_type.id() != LogicalTypeId::VARCHAR || rhs_type.InternalType() != PhysicalType::VARCHAR) {
			blocker = "rhs_reference_type_" + rhs_type.ToString();
			return false;
		}
		return true;
	}
	if (predicate_projection.plan.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
	    predicate_projection.plan.source_index != 0) {
		blocker = "projection_kind_" + to_string(static_cast<int>(predicate_projection.plan.kind));
		return false;
	}
	return SljitTryInitializeCompressedComplementarySumRHSField(rhs_type.InternalType(),
	                                                            predicate_projection.plan.string_decompress_source_size,
	                                                            classification, field, blocker);
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
	if (!binding.ready ||
	    (binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE &&
	     binding.layout_kind != ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) ||
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
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
		plan.predicate_storage = SljitComplementarySumPredicateStorage::REGULAR_ROW_POINTER;
		if (!SljitTryResolveComplementarySumRHSField(binding, descriptor, plan.classification, plan.predicate_field,
		                                             predicate_source_blocker)) {
			return block("predicate_rhs_field_" + predicate_source_blocker);
		}
	} else if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		plan.predicate_storage = SljitComplementarySumPredicateStorage::PERFECT_HASH_DICTIONARY;
		if (!SljitTryResolveComplementarySumPerfectHashRHSOutput(binding, descriptor, plan.classification,
		                                                         plan.predicate_field, plan.perfect_hash_rhs_output_idx,
		                                                         predicate_source_blocker)) {
			return block("predicate_rhs_dictionary_" + predicate_source_blocker);
		}
	} else {
		return block("predicate_storage");
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

//! Physical equality of fixed-width values. uhugeint_t's operator== is an out-of-line DuckDB API call; comparing the
//! bytes inlines at every width.
template <class T>
static inline bool SljitPhysicalEquals(const T &left, const T &right) {
	return memcmp(&left, &right, sizeof(T)) == 0;
}

template <class T>
static inline bool SljitCompressedEqualsEitherConstant(const_data_ptr_t value,
                                                       const SljitComplementarySumRHSField &field) {
	const auto compressed = Load<T>(value);
	return SljitPhysicalEquals(compressed, Load<T>(field.compressed_constants[0].data())) ||
	       SljitPhysicalEquals(compressed, Load<T>(field.compressed_constants[1].data()));
}

//! Whether a valid predicate value stored at value is one of the two classified constants. A compressed field stores
//! the packed physical value; an uncompressed one stores the string.
static inline bool SljitComplementarySumValueMatches(const_data_ptr_t value, const SljitComplementarySumRHSField &field,
                                                     const SljitStringSetComplementarySumDescriptor &classification) {
	switch (field.compressed_size) {
	case 0: {
		auto predicate = Load<string_t>(value);
		return SljitStringEqualsEitherConstant(predicate, classification.constants[0], classification.signatures[0],
		                                       classification.constants[1], classification.signatures[1]);
	}
	case sizeof(uint8_t):
		return SljitCompressedEqualsEitherConstant<uint8_t>(value, field);
	case sizeof(uint16_t):
		return SljitCompressedEqualsEitherConstant<uint16_t>(value, field);
	case sizeof(uint32_t):
		return SljitCompressedEqualsEitherConstant<uint32_t>(value, field);
	case sizeof(uint64_t):
		return SljitCompressedEqualsEitherConstant<uint64_t>(value, field);
	case sizeof(uhugeint_t):
		return SljitCompressedEqualsEitherConstant<uhugeint_t>(value, field);
	default:
		throw InternalException("SLJIT complementary sum has an unsupported compressed field width");
	}
}

struct SljitGenericComplementarySumRHSMatcher {
	bool Match(data_ptr_t row_pointer, bool &valid) const {
		valid = SljitHashJoinRHSFixedColumnSourceIsValid(row_pointer, field.source);
		return valid &&
		       SljitComplementarySumValueMatches(row_pointer + field.source.layout_offset, field, classification);
	}

	const SljitComplementarySumRHSField &field;
	const SljitStringSetComplementarySumDescriptor &classification;
};

struct SljitAllValidUhugeintComplementarySumRHSMatcher {
	bool Match(data_ptr_t row_pointer, bool &valid) const {
		valid = row_pointer != nullptr;
		if (!valid) {
			return false;
		}
		const auto value = Load<uhugeint_t>(row_pointer + layout_offset);
		return SljitPhysicalEquals(value, constant0) || SljitPhysicalEquals(value, constant1);
	}

	idx_t layout_offset;
	uhugeint_t constant0;
	uhugeint_t constant1;
};

template <class CONSUMER>
static bool SljitWithComplementarySumRHSMatcher(const SljitComplementarySumRHSField &field,
                                                const SljitStringSetComplementarySumDescriptor &classification,
                                                CONSUMER &&consumer) {
	if (field.source.all_valid && field.compressed_size == sizeof(uhugeint_t)) {
		SljitAllValidUhugeintComplementarySumRHSMatcher matcher {
		    field.source.layout_offset, Load<uhugeint_t>(field.compressed_constants[0].data()),
		    Load<uhugeint_t>(field.compressed_constants[1].data())};
		return consumer(matcher);
	}
	SljitGenericComplementarySumRHSMatcher matcher {field, classification};
	return consumer(matcher);
}

template <class GROUP_TYPE, class LOAD_GROUP>
static bool SljitTryPreaggregateJoinInputRowPointerComplementarySums(
    LOAD_GROUP &&load_group, Vector &row_pointers, idx_t count, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count) {
	compact_count = 0;
	if (count == 0 || compact_groups.ColumnCount() != 1 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}

	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	auto compact_group_data = PrepareFlatPreaggregatedGroupTarget<GROUP_TYPE>(compact_groups);

	static constexpr idx_t GROUP_LIMIT = SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS;
	// This path already has a fixed-width group key, so direct hash lookup is
	// cheaper than a branchy linear tier. Row-pointer reducers keep the hot tier
	// because extracting their build-side key is more expensive.
	SljitTypedLocalGroupIndex<GROUP_TYPE, GROUP_LIMIT, 0> group_index;
	std::array<int64_t, GROUP_LIMIT> matching_counts {};
	std::array<int64_t, GROUP_LIMIT> non_matching_counts {};
	std::array<idx_t, GROUP_LIMIT> represented_rows {};

	auto accumulate_rows = [&](auto &predicate_matcher) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			GROUP_TYPE key {};
			bool group_is_valid;
			if (!load_group(row_idx, key, group_is_valid)) {
				return false;
			}
			idx_t group_idx;
			bool created;
			if (!group_index.FindOrCreate(group_is_valid, key, group_idx, created)) {
				return false;
			}
			if (created) {
				compact_group_data[group_idx] = key;
			}

			represented_rows[group_idx]++;
			bool predicate_is_valid;
			const bool matches = predicate_matcher.Match(row_pointer_data[row_idx], predicate_is_valid);
			if (!predicate_is_valid) {
				continue;
			}
			if (matches) {
				matching_counts[group_idx]++;
			} else {
				non_matching_counts[group_idx]++;
			}
		}
		return true;
	};
	if (!SljitWithComplementarySumRHSMatcher(predicate_field, classification, accumulate_rows)) {
		return false;
	}
	compact_count = group_index.Count();

	auto &target_validity = FlatVector::ValidityMutable(compact_groups.data[0]);
	target_validity.Reset(compact_count);
	target_validity.SetAllValid(compact_count);
	for (idx_t group_idx = 0; group_idx < compact_count; group_idx++) {
		if (!group_index.IsValid(group_idx)) {
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

template <class GROUP_TYPE>
static bool SljitTryPreaggregateSelectedJoinInputRowPointerComplementarySums(
    Vector &group_input, const SelectionVector &match_selection, const ExecutionRowPointerGroupKeySource &group_source,
    Vector &row_pointers, idx_t count, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count,
    bool source_key0_int64_to_int32_unchecked) {
	auto consume = [&](auto &&load_group, auto &&, auto &&, auto &&, auto) {
		return SljitTryPreaggregateJoinInputRowPointerComplementarySums<GROUP_TYPE>(
		    std::forward<decltype(load_group)>(load_group), row_pointers, count, predicate_field, classification,
		    payload_lanes, compact_groups, preaggregate_scratch, compact_count);
	};
	return SljitDispatchSelectedInputVectorGroupKey<GROUP_TYPE>(group_input, match_selection, group_source,
	                                                            source_key0_int64_to_int32_unchecked, consume);
}

template <class GROUP_TYPE>
static bool SljitTryPreaggregateMaterializedJoinInputRowPointerComplementarySums(
    Vector &groups, Vector &row_pointers, idx_t count, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count) {
	if (groups.GetType().InternalType() != GetTypeId<GROUP_TYPE>()) {
		return false;
	}
	UnifiedVectorFormat group_format;
	groups.ToUnifiedFormat(group_format);
	auto group_data = UnifiedVectorFormat::GetData<GROUP_TYPE>(group_format);
	auto load_group = [&](idx_t row_idx, GROUP_TYPE &key, bool &valid) {
		const auto source_idx = group_format.sel->get_index(row_idx);
		valid = group_format.validity.RowIsValid(source_idx);
		key = valid ? group_data[source_idx] : GROUP_TYPE {};
		return true;
	};
	return SljitTryPreaggregateJoinInputRowPointerComplementarySums<GROUP_TYPE>(
	    load_group, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
	    preaggregate_scratch, compact_count);
}

struct SljitMaterializedJoinInputComplementaryPreaggregateDispatch {
	Vector &groups;
	Vector &row_pointers;
	idx_t count;
	const SljitComplementarySumRHSField &predicate_field;
	const SljitStringSetComplementarySumDescriptor &classification;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	DataChunk &compact_groups;
	SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch;
	idx_t &compact_count;

	template <class GROUP_TYPE>
	bool Execute() {
		return SljitTryPreaggregateMaterializedJoinInputRowPointerComplementarySums<GROUP_TYPE>(
		    groups, row_pointers, count, predicate_field, classification, payload_lanes, compact_groups,
		    preaggregate_scratch, compact_count);
	}
};

static bool SljitTryPreaggregateMaterializedJoinInputRowPointerComplementarySums(
    Vector &groups, Vector &row_pointers, idx_t count, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count) {
	SljitMaterializedJoinInputComplementaryPreaggregateDispatch dispatch {
	    groups,         row_pointers,         count,        predicate_field, classification, payload_lanes,
	    compact_groups, preaggregate_scratch, compact_count};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(groups.GetType().InternalType(), dispatch);
}

struct SljitSelectedJoinInputComplementaryPreaggregateDispatch {
	Vector &group_input;
	const SelectionVector &match_selection;
	const ExecutionRowPointerGroupKeySource &group_source;
	Vector &row_pointers;
	idx_t count;
	const SljitComplementarySumRHSField &predicate_field;
	const SljitStringSetComplementarySumDescriptor &classification;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	DataChunk &compact_groups;
	SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch;
	idx_t &compact_count;
	bool source_key0_int64_to_int32_unchecked;

	template <class GROUP_TYPE>
	bool Execute() {
		return SljitTryPreaggregateSelectedJoinInputRowPointerComplementarySums<GROUP_TYPE>(
		    group_input, match_selection, group_source, row_pointers, count, predicate_field, classification,
		    payload_lanes, compact_groups, preaggregate_scratch, compact_count, source_key0_int64_to_int32_unchecked);
	}
};

static bool SljitTryPreaggregateJoinInputRowPointerComplementarySums(
    Vector &group_input, const SelectionVector &match_selection, const ExecutionRowPointerGroupKeySource &group_source,
    Vector &row_pointers, idx_t count, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count,
    bool source_key0_int64_to_int32_unchecked) {
	SljitSelectedJoinInputComplementaryPreaggregateDispatch dispatch {
	    group_input,    match_selection,      group_source,   row_pointers,
	    count,          predicate_field,      classification, payload_lanes,
	    compact_groups, preaggregate_scratch, compact_count,  source_key0_int64_to_int32_unchecked};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(group_source.target_physical_type, dispatch);
}

template <class GROUP_TYPE, class LOAD_GROUP, class PREFLIGHT, class FLUSH_ACCUMULATOR>
static bool
SljitAccumulateJoinInputRowPointerComplementarySums(SljitDirectJoinOutputAggregateStrategy &strategy,
                                                    LOAD_GROUP &&load_group, Vector &row_pointers, idx_t count,
                                                    const SljitComplementarySumRHSField &predicate_field,
                                                    const SljitStringSetComplementarySumDescriptor &classification,
                                                    PREFLIGHT &&preflight, FLUSH_ACCUMULATOR &flush_accumulator) {
	if (count == 0 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	auto &accumulator = SljitGetJoinInputComplementarySumAccumulator<GROUP_TYPE>(strategy);
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	if (!preflight(count)) {
		return false;
	}
	auto accumulate_rows = [&](auto &predicate_matcher) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			GROUP_TYPE key {};
			bool group_is_valid;
			if (!load_group(row_idx, key, group_is_valid)) {
				throw InternalException("SLJIT selected group transform failed after successful preflight");
			}
			bool predicate_is_valid;
			const bool predicate_matches = predicate_matcher.Match(row_pointer_data[row_idx], predicate_is_valid);
			if (accumulator.Accumulate(group_is_valid, key, predicate_is_valid, predicate_matches)) {
				continue;
			}
			if (!flush_accumulator()) {
				throw InternalException("SLJIT join-input complementary accumulator overflow flush failed");
			}
			if (!accumulator.Accumulate(group_is_valid, key, predicate_is_valid, predicate_matches)) {
				throw InternalException("SLJIT join-input complementary accumulator remained full after flush");
			}
		}
		return true;
	};
	return SljitWithComplementarySumRHSMatcher(predicate_field, classification, accumulate_rows);
}

struct SljitPreparedJoinInputComplementarySumUpdate {
	ExecutionRowPointerGroupKeySource group_source;
	optional_ptr<ExecutionSinkBinding> sink_binding;
	optional_ptr<const vector<const ExecutionPrimitiveAggregateUpdateLane *>> payload_lanes;
	bool pipeline_accumulator_enabled = false;
};

static bool SljitTryPrepareJoinInputComplementarySumUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t hash_join_idx, SljitExecutableRegionOp &aggregate_op, SljitDirectJoinOutputAggregateStrategy &strategy,
    DataChunk &join_input, idx_t count, SljitPreparedJoinInputComplementarySumUpdate &prepared,
    optional_ptr<string> failure_reason = nullptr) {
	auto block = [&](const char *reason) {
		strategy.join_input_complementary_sum_plan.build_state.Block(reason);
		if (failure_reason) {
			*failure_reason = reason;
		}
		return false;
	};
	if (count == 0) {
		return false;
	}
	if (!scratch.HasOperatorBinding(hash_join_idx)) {
		return block("runtime_shape");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	auto &plan = strategy.join_input_complementary_sum_plan;
	auto &descriptor = strategy.descriptor;
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
	prepared.group_source = descriptor.group_sources[0];
	descriptor.EnsureInput(runtime.GetAllocator());
	auto &aggregate_input = descriptor.input.chunk;
	aggregate_input.Reset();
	aggregate_input.SetChildCardinality(count);
	auto &sink_binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, strategy.aggregate_idx, aggregate_op.Kind(), aggregate_input,
	    aggregate_op.AggregateUpdate().plan.sink_info, "aggregate-update-runtime-binding-failed",
	    "SLJIT join-input complementary aggregate update");
	if (!sink_binding.ready || !sink_binding.aggregate_update.ready || !sink_binding.aggregate_update.primitive.ready ||
	    !sink_binding.aggregate_update.grouped_state.ready || !sink_binding.aggregate_update.grouped_state.state) {
		return block("sink_binding");
	}
	auto &payload_lanes =
	    scratch.AggregatePayloadLanes(strategy.aggregate_idx, aggregate_op.AggregateUpdate().payload_descriptors,
	                                  sink_binding.aggregate_update.primitive);
	SljitStringSetComplementarySumUpdateState update_state;
	if (!SljitTryBindStringSetComplementarySumLanes(aggregate_op, payload_lanes, plan.classification, update_state)) {
		return block("payload_lanes");
	}
	SljitFlushPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, descriptor,
	                                          strategy.pending_batch);
	prepared.sink_binding = &sink_binding;
	prepared.payload_lanes = &payload_lanes;
	prepared.pipeline_accumulator_enabled = SljitJoinInputComplementarySumAccumulatorEnabled(strategy, plan);
	if (prepared.pipeline_accumulator_enabled) {
		strategy.join_input_complementary_sum_scratch = &scratch;
	}
	return true;
}

} // namespace duckdb
