//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_hash_join_runtime.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"

namespace duckdb {

struct SljitUnifiedHashJoinKeySourceDataDispatch {
	UnifiedVectorFormat &format;
	const_data_ptr_t data = nullptr;

	template <class T>
	void Execute() {
		data = reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<T>(format));
	}
};

const_data_ptr_t NativeHashJoinKeySourceData(UnifiedVectorFormat &format, SljitNativeHashJoinKeyKind kind) {
	SljitUnifiedHashJoinKeySourceDataDispatch dispatch {format};
	if (!SljitDispatchHashJoinKeyKind(kind, dispatch)) {
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
	return dispatch.data;
}

template <class T>
static inline bool SljitWidePerfectHashOffset(const T &value, const T &min_value, const T &max_value, idx_t &offset) {
	const bool below_min =
	    value.upper < min_value.upper || (value.upper == min_value.upper && value.lower < min_value.lower);
	if (below_min) {
		return false;
	}
	const bool above_max =
	    value.upper > max_value.upper || (value.upper == max_value.upper && value.lower > max_value.lower);
	if (above_max) {
		return false;
	}

	// Perfect-hash construction bounds the range to an idx_t-sized dense table. The
	// low-word subtraction is therefore the complete offset; the borrow is folded
	// into the high word to keep the calculation defined across a word boundary.
	const auto borrow = value.lower < min_value.lower ? 1U : 0U;
	const auto upper_delta = static_cast<uint64_t>(value.upper) - static_cast<uint64_t>(min_value.upper) - borrow;
	if (upper_delta != 0) {
		return false;
	}
	offset = NumericCast<idx_t>(value.lower - min_value.lower);
	return true;
}

template <class T, bool EMIT_MATCH_SELECTION, bool EMIT_BUILD_SELECTION>
static void SljitPopulateWidePerfectHashJoinSelectionsTyped(SljitNativePerfectHashJoinProbeInput &input) {
	const auto source = reinterpret_cast<const T *>(input.source_data);
	const auto min_value = [&]() {
		if constexpr (std::is_same<T, hugeint_t>::value) {
			return input.perfect_min_128;
		} else {
			return input.perfect_min_u128;
		}
	}();
	const auto max_value = [&]() {
		if constexpr (std::is_same<T, hugeint_t>::value) {
			return input.perfect_max_128;
		} else {
			return input.perfect_max_u128;
		}
	}();
	const auto source_selection = input.source_sel;
	const auto source_validity = input.source_validity;
	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < input.count; row_idx++) {
		const auto source_idx = source_selection ? source_selection[row_idx] : NumericCast<sel_t>(row_idx);
		if (source_validity && !ValidityMask::RowIsValid(source_validity[source_idx >> 6], source_idx & 63)) {
			continue;
		}
		idx_t build_idx;
		if (!SljitWidePerfectHashOffset(source[source_idx], min_value, max_value, build_idx)) {
			continue;
		}
		if (input.perfect_validity &&
		    !ValidityMask::RowIsValid(input.perfect_validity[build_idx / ValidityMask::BITS_PER_VALUE],
		                              build_idx % ValidityMask::BITS_PER_VALUE)) {
			continue;
		}
		if constexpr (EMIT_MATCH_SELECTION) {
			input.match_sel[selected_count] = NumericCast<sel_t>(row_idx);
		}
		if constexpr (EMIT_BUILD_SELECTION) {
			input.build_sel[selected_count] = NumericCast<sel_t>(build_idx);
		}
		selected_count++;
	}
	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.finished = true;
}

void SljitPopulateWidePerfectHashJoinSelections(SljitNativePerfectHashJoinProbeInput &input, bool emit_match_selection,
                                                bool emit_build_selection, bool unsigned_key) {
	auto dispatch = [&](auto type_tag) {
		using T = decltype(type_tag);
		if (emit_match_selection) {
			if (emit_build_selection) {
				SljitPopulateWidePerfectHashJoinSelectionsTyped<T, true, true>(input);
			} else {
				SljitPopulateWidePerfectHashJoinSelectionsTyped<T, true, false>(input);
			}
		} else if (emit_build_selection) {
			SljitPopulateWidePerfectHashJoinSelectionsTyped<T, false, true>(input);
		} else {
			SljitPopulateWidePerfectHashJoinSelectionsTyped<T, false, false>(input);
		}
	};
	if (unsigned_key) {
		dispatch(uhugeint_t());
	} else {
		dispatch(hugeint_t());
	}
}

SljitHashJoinProbeLayoutKind SljitHashJoinTableLayoutKind(const ExecutionHashJoinTableLayout &layout) {
	return SljitHashJoinProbeLayoutKindFromFlags(layout.use_salt, layout.chains_longer_than_one,
	                                             layout.dictionary_emission);
}

bool SljitHashJoinCanUseAllValidChainInput(const SljitNativeRegularHashJoinProbeInput &input) {
	return SljitHashJoinProbeLayoutChainsLongerThanOne(input.layout_kind) &&
	       (!SljitHashJoinProbeLayoutUsesDictionaryEmission(input.layout_kind) || input.aux_next_ptrs);
}

void SljitMarkHashJoinBuildMatchesAfterResidual(const SljitNativeHashJoinProbePlan &plan, Vector &row_pointers,
                                                idx_t count) {
	if (!plan.mark_build_match_after_residual) {
		return;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			throw InternalException("SLJIT native hash join residual build match has no row pointer");
		}
		row_pointer[plan.found_match_offset] = 1;
	}
}

const SljitNativeHashJoinProbeKeyPlan &
SljitValidatePerfectHashJoinProbeExecutionLayout(const SljitNativeHashJoinProbePlan &plan,
                                                 const ExecutionHashJoinProbeBinding &probe, DataChunk &input) {
	if (!plan.perfect_hash_probe) {
		throw InternalException("SLJIT native hash join probe received a perfect layout without perfect code");
	}
	if (plan.residual_predicate) {
		throw InternalException("SLJIT native perfect hash join probe does not support residual predicates");
	}
	if (plan.keys.size() != 1 || probe.probe_key_input_indices.size() != 1) {
		throw InternalException("SLJIT native perfect hash join probe requires one key");
	}
	auto &perfect_layout = probe.perfect_layout;
	if (!perfect_layout.ready || perfect_layout.build_capacity == 0) {
		throw InternalException("SLJIT native perfect hash join probe received an incomplete layout");
	}
	auto &key = plan.keys[0];
	if (probe.probe_key_input_indices[0] != key.key_input_index) {
		throw InternalException("SLJIT native perfect hash join probe key binding mismatch");
	}
	const auto source_physical_type = input.data[key.key_input_index].GetType().InternalType();
	const bool source_key0_int64_to_int32 =
	    SljitHashJoinKeyCanUseInt64SourceForInt32Key(0, key.key_kind, source_physical_type) &&
	    perfect_layout.key_physical_type == PhysicalType::INT32;
	if (!source_key0_int64_to_int32 && source_physical_type != perfect_layout.key_physical_type) {
		throw InternalException("SLJIT native perfect hash join probe key type mismatch");
	}
	return key;
}

void SljitPreparePerfectHashJoinProbeInput(const SljitNativeHashJoinProbeKeyPlan &key,
                                           const ExecutionPerfectHashJoinTableLayout &layout, DataChunk &input,
                                           SelectionVector &match_selection, SelectionVector &build_selection,
                                           SljitHashJoinProbeDrainState &state, bool allow_unchecked_int64_to_int32,
                                           SljitPreparedPerfectHashJoinProbeInput &result) {
	input.data[key.key_input_index].ToUnifiedFormat(result.source_format);
	const bool source_key0_int64_to_int32 = SljitHashJoinKeyCanUseInt64SourceForInt32Key(
	    0, key.key_kind, input.data[key.key_input_index].GetType().InternalType());

	auto &native_input = result.native_input;
	native_input.source_data =
	    source_key0_int64_to_int32
	        ? reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(result.source_format))
	        : NativeHashJoinKeySourceData(result.source_format, key.key_kind);
	native_input.source_sel = SljitNormalizedSourceSelectionData(result.source_format);
	native_input.source_validity = key.source_known_not_null || result.source_format.validity.CannotHaveNull()
	                                   ? nullptr
	                                   : result.source_format.validity.GetData();
	native_input.source_key0_int64_to_int32 = source_key0_int64_to_int32;
	native_input.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32 && allow_unchecked_int64_to_int32;
	native_input.count = input.size();
	native_input.match_sel = match_selection.data();
	native_input.build_sel = build_selection.data();
	native_input.perfect_min = layout.build_min;
	native_input.perfect_max = layout.build_max;
	native_input.perfect_validity = layout.build_validity;
	native_input.selected_count = 0;
	native_input.input_offset = state.input_offset;
	native_input.finished = false;
	native_input.perfect_min_128 = layout.build_min_128;
	native_input.perfect_max_128 = layout.build_max_128;
	native_input.perfect_min_u128 = layout.build_min_u128;
	native_input.perfect_max_u128 = layout.build_max_u128;
}

bool SljitCanDerivePerfectHashBuildSelectionFromIdentity(const SljitNativeHashJoinProbeKeyPlan &key,
                                                         const ExecutionPerfectHashJoinTableLayout &layout,
                                                         DataChunk &input) {
	if (key.key_input_index >= input.ColumnCount()) {
		return false;
	}
	const auto source_type = input.data[key.key_input_index].GetType().InternalType();
	if (source_type != PhysicalType::INT64) {
		return false;
	}
	// Keep this ABI at one normalized source width. Adding a template family for
	// every physical key width duplicates the complete direct aggregate consumer
	// graph, while the existing selection path remains the correct generic route
	// for those widths.
	if (key.key_kind == SljitNativeHashJoinKeyKind::INT64 && layout.key_physical_type == PhysicalType::INT64) {
		return true;
	}
	return key.key_kind == SljitNativeHashJoinKeyKind::INT32 && layout.key_physical_type == PhysicalType::INT32;
}

SljitHashJoinProbeLayoutKind
SljitValidateRegularHashJoinProbeExecutionLayout(const SljitNativeHashJoinProbePlan &plan,
                                                 const ExecutionHashJoinProbeBinding &probe) {
	auto &layout = probe.table_layout;
	if (!layout.Ready() || !layout.entries.words || layout.layout_offsets.empty()) {
		throw InternalException("SLJIT native hash join probe received an incomplete hash table layout");
	}
	if (layout.entries.capacity == 0 || layout.entries.bitmask != layout.entries.capacity - 1) {
		throw InternalException("SLJIT native hash join probe received an invalid entry-table capacity");
	}
	if (layout.entries.pointer_mask == 0 || (layout.entries.pointer_mask & layout.entries.salt_mask) != 0 ||
	    (layout.entries.pointer_mask | layout.entries.salt_mask) != NumericLimits<uint64_t>::Maximum()) {
		throw InternalException("SLJIT native hash join probe received incompatible entry-word masks");
	}
	switch (layout.entries.encoding) {
	case ExecutionHashJoinEntryEncoding::POINTER:
		if (layout.entries.pointer_mask != NumericLimits<uint64_t>::Maximum() || layout.entries.salt_mask != 0) {
			throw InternalException("SLJIT native hash join probe received an invalid pointer entry encoding");
		}
		break;
	case ExecutionHashJoinEntryEncoding::POINTER_LOWER_48_SALT_UPPER_16:
		if (layout.entries.pointer_mask != EXECUTION_HASH_JOIN_POINTER_LOWER_48_MASK ||
		    layout.entries.salt_mask != EXECUTION_HASH_JOIN_SALT_UPPER_16_MASK) {
			throw InternalException("SLJIT native hash join probe received an invalid 48-bit pointer entry encoding");
		}
		break;
	default:
		break;
	}
	if (layout.layout_offsets.size() < plan.keys.size()) {
		throw InternalException("SLJIT native hash join probe layout key count mismatch");
	}
	if (layout.pointer_offset != plan.pointer_offset) {
		throw InternalException("SLJIT native hash join probe pointer offset mismatch");
	}
	const auto table_layout_kind = SljitHashJoinTableLayoutKind(layout);
	if (plan.mark_build_match) {
		if (!layout.found_match_column_present) {
			throw InternalException("SLJIT native hash join probe expected a build-side found-match column");
		}
		if (layout.tuple_size != plan.found_match_offset) {
			throw InternalException("SLJIT native hash join probe found-match offset mismatch");
		}
		if (plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY &&
		    SljitHashJoinProbeLayoutUsesDictionaryEmission(table_layout_kind) &&
		    SljitHashJoinProbeLayoutChainsLongerThanOne(table_layout_kind) && !layout.entries.aux_next_ptrs) {
			throw InternalException("SLJIT native hash join mark-only probe requires dictionary chain pointers");
		}
	}
	for (idx_t key_idx = 0; key_idx < plan.keys.size(); key_idx++) {
		auto &key = plan.keys[key_idx];
		if (probe.probe_key_input_indices[key_idx] != key.key_input_index) {
			throw InternalException("SLJIT native hash join probe key binding mismatch");
		}
		if (layout.layout_offsets[key_idx] != key.key_layout_offset) {
			throw InternalException("SLJIT native hash join probe key layout offset mismatch");
		}
	}
	return table_layout_kind;
}

static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE = "regular_probe.generated";
static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_BLOOM_STAGE = "regular_probe.generated.bloom";
static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_MATCH_SELECTION_PROBE_STAGE =
    "regular_probe.generated.mark_match_selection";
static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_MATCH_SELECTION_BLOOM_PROBE_STAGE =
    "regular_probe.generated.mark_match_selection.bloom";
static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_NONMATCH_SELECTION_PROBE_STAGE =
    "regular_probe.generated.mark_nonmatch_selection";
static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_NONMATCH_SELECTION_BLOOM_PROBE_STAGE =
    "regular_probe.generated.mark_nonmatch_selection.bloom";
static constexpr const char *SLJIT_GENERATED_PERFECT_HASH_JOIN_PROBE_STAGE = "perfect_probe.generated";

const char *SljitGeneratedRegularHashJoinProbeStage() {
	return SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE;
}

const char *SljitGeneratedRegularHashJoinProbeStage(bool uses_bloom_filter) {
	return uses_bloom_filter ? SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_BLOOM_STAGE
	                         : SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE;
}

const char *SljitGeneratedRegularHashJoinProbeStage(bool uses_bloom_filter,
                                                    SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::NONE) {
		return SljitGeneratedRegularHashJoinProbeStage(uses_bloom_filter);
	}
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
		return uses_bloom_filter ? SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_MATCH_SELECTION_BLOOM_PROBE_STAGE
		                         : SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_MATCH_SELECTION_PROBE_STAGE;
	}
	return uses_bloom_filter ? SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_NONMATCH_SELECTION_BLOOM_PROBE_STAGE
	                         : SLJIT_GENERATED_REGULAR_HASH_JOIN_MARK_NONMATCH_SELECTION_PROBE_STAGE;
}

const char *SljitGeneratedPerfectHashJoinProbeStage() {
	return SLJIT_GENERATED_PERFECT_HASH_JOIN_PROBE_STAGE;
}

const char *SljitHashJoinProbeLayoutName(ExecutionHashJoinProbeLayoutKind kind) {
	switch (kind) {
	case ExecutionHashJoinProbeLayoutKind::NONE:
		return "none";
	case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
		return "regular_hash_table";
	case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
		return "perfect_hash_table";
	default:
		return "unknown";
	}
}

SinkResultType SljitExecuteNativeHashJoinBuildUpdate(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                                     SljitNativeRegionOpKind op_kind,
                                                     ExecutionHashJoinBuildBinding &build, DataChunk &input,
                                                     DataChunk &source_chunk, Vector &hash_values,
                                                     SelectionVector &build_sel) {
	auto stage_start = SljitRegionStageStart(runtime);
	ExecutionHashJoinBuildReferenceKeys(build, input);
	RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "reference_keys", stage_start);

	stage_start = SljitRegionStageStart(runtime);
	ExecutionHashJoinBuildApplyFilterPushdown(build);
	RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "filter_pushdown", stage_start);

	stage_start = SljitRegionStageStart(runtime);
	ExecutionHashJoinBuildReferencePayload(build, input);
	RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "reference_payload", stage_start);

	optional_ptr<const SelectionVector> build_selection;
	stage_start = SljitRegionStageStart(runtime);
	auto build_count = ExecutionHashJoinBuildPrepare(build, source_chunk, hash_values, build_sel, build_selection);
	RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "hash_table_prepare", stage_start);
	if (build_count == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (!build_selection) {
		throw InternalException("SLJIT hash join build prepare did not return a build selection");
	}

	stage_start = SljitRegionStageStart(runtime);
	ExecutionHashJoinBuildHash(build, source_chunk, hash_values, *build_selection, build_count);
	RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "hash_table_hash", stage_start);

	stage_start = SljitRegionStageStart(runtime);
	ExecutionHashJoinBuildAppend(build, source_chunk, *build_selection, build_count);
	RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "hash_table_append", stage_start);
	return SinkResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
