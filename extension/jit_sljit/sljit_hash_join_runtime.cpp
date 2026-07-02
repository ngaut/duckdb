//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_hash_join_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_runtime.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"

namespace duckdb {

const_data_ptr_t NativeHashJoinKeySourceData(UnifiedVectorFormat &format, SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int8_t>(format));
	case SljitNativeHashJoinKeyKind::INT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int16_t>(format));
	case SljitNativeHashJoinKeyKind::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeHashJoinKeyKind::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	case SljitNativeHashJoinKeyKind::INT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<hugeint_t>(format));
	case SljitNativeHashJoinKeyKind::UINT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint8_t>(format));
	case SljitNativeHashJoinKeyKind::UINT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint16_t>(format));
	case SljitNativeHashJoinKeyKind::UINT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint32_t>(format));
	case SljitNativeHashJoinKeyKind::UINT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint64_t>(format));
	case SljitNativeHashJoinKeyKind::UINT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uhugeint_t>(format));
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
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
	if (input.data[key.key_input_index].GetType().InternalType() != perfect_layout.key_physical_type) {
		throw InternalException("SLJIT native perfect hash join probe key type mismatch");
	}
	return key;
}

void SljitPreparePerfectHashJoinProbeInput(const SljitNativeHashJoinProbeKeyPlan &key,
                                           const ExecutionPerfectHashJoinTableLayout &layout, DataChunk &input,
                                           SelectionVector &match_selection, SelectionVector &build_selection,
                                           SljitHashJoinProbeDrainState &state,
                                           SljitPreparedPerfectHashJoinProbeInput &result) {
	input.data[key.key_input_index].ToUnifiedFormat(result.source_format);

	auto &native_input = result.native_input;
	native_input.source_data = NativeHashJoinKeySourceData(result.source_format, key.key_kind);
	native_input.source_sel = SljitNormalizedSourceSelectionData(result.source_format);
	native_input.source_validity =
	    result.source_format.validity.CannotHaveNull() ? nullptr : result.source_format.validity.GetData();
	native_input.count = input.size();
	native_input.match_sel = match_selection.data();
	native_input.build_sel = build_selection.data();
	native_input.perfect_min = layout.build_min;
	native_input.perfect_max = layout.build_max;
	native_input.perfect_validity = layout.build_validity;
	native_input.selected_count = 0;
	native_input.input_offset = state.input_offset;
	native_input.finished = false;
}

SljitHashJoinProbeLayoutKind
SljitValidateRegularHashJoinProbeExecutionLayout(const SljitNativeHashJoinProbePlan &plan,
                                                 const ExecutionHashJoinProbeBinding &probe) {
	auto &layout = probe.table_layout;
	if (!layout.ready || !layout.entries || layout.layout_offsets.empty()) {
		throw InternalException("SLJIT native hash join probe received an incomplete hash table layout");
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
		    SljitHashJoinProbeLayoutChainsLongerThanOne(table_layout_kind) && !layout.aux_next_ptrs) {
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

static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE = "generated_regular_probe_function";
static constexpr const char *SLJIT_GENERATED_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "generated_regular_probe_flat_all_valid_function";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_int64_pair_no_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_int64_pair_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_int64_pair_no_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_int64_pair_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_single_key_notequal_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_single_key_no_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_single_key_no_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_single_key_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_single_key_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_single_key_notequal_chain";
static constexpr const char *SLJIT_GENERATED_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "generated_regular_probe_selected_all_valid_function";
static constexpr const char *SLJIT_GENERATED_PERFECT_HASH_JOIN_PROBE_STAGE = "generated_perfect_probe_function";

template <bool SELECTED, bool CHAIN>
static bool ExecuteAllValidUint64PairProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                   SljitNativeRegularHashJoinProbeInput &input,
                                                   const SljitAllValidHashJoinProbeFacts &facts) {
	if constexpr (CHAIN) {
		return TryExecuteAllValidUint64PairProbe<SELECTED, true>(plan, input, facts.can_use_equality_chain_input);
	}
	return TryExecuteAllValidUint64PairProbe<SELECTED, false>(plan, input);
}

template <bool SELECTED>
static bool ExecuteAllValidSingleKeyNotEqualPredicateChainProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                                        SljitNativeRegularHashJoinProbeInput &input,
                                                                        const SljitAllValidHashJoinProbeFacts &) {
	return TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe<SELECTED>(plan, input);
}

template <bool SELECTED, bool CHAIN>
static bool ExecuteAllValidSingleKeyProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                  SljitNativeRegularHashJoinProbeInput &input,
                                                  const SljitAllValidHashJoinProbeFacts &facts) {
	if constexpr (CHAIN) {
		return TryExecuteAllValidSingleKeyProbe<SELECTED, true>(plan, input, facts.can_use_equality_chain_input);
	}
	return TryExecuteAllValidSingleKeyProbe<SELECTED, false>(plan, input);
}

template <bool SELECTED>
static const array<SljitAllValidHashJoinProbeFastPath, 5> &SljitAllValidHashJoinProbeFastPaths() {
	static const array<SljitAllValidHashJoinProbeFastPath, 5> fast_paths {{
	    {SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidUint64PairProbeFastPath<SELECTED, false>},
	    {SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidUint64PairProbeFastPath<SELECTED, true>},
	    {SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyNotEqualPredicateChainProbeFastPath<SELECTED>},
	    {SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyProbeFastPath<SELECTED, false>},
	    {SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyProbeFastPath<SELECTED, true>},
	}};
	return fast_paths;
}

const std::array<SljitAllValidHashJoinProbeFastPath, 5> &SljitAllValidHashJoinProbeFastPaths(bool selected) {
	return selected ? SljitAllValidHashJoinProbeFastPaths<true>() : SljitAllValidHashJoinProbeFastPaths<false>();
}

const char *SljitAllValidHashJoinProbeFastPathStage(const SljitAllValidHashJoinProbeFastPath &fast_path,
                                                    bool selected) {
	return selected ? fast_path.selected_stage : fast_path.flat_stage;
}

const char *SljitGeneratedRegularHashJoinProbeStage() {
	return SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE;
}

const char *SljitGeneratedAllValidRegularHashJoinProbeStage(bool selected) {
	return selected ? SLJIT_GENERATED_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE
	                : SLJIT_GENERATED_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE;
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
