//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_aggregate_consumer_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_dispatch_runtime.hpp"
#include "sljit_hash_join_consumer_result.hpp"
#include "sljit_hash_join_probe_execution_contract.hpp"
#include "sljit_hash_join_probe_input_filter_runtime.hpp"
#include "sljit_join_input_row_pointer_complementary_sum_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_exact_perfect_hash_join_runtime.hpp"

#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

template <bool SELECTED, class CONSUMER>
static bool TryExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainBufferedConsumer(
    ExecutionRegionRuntime &runtime, idx_t hash_join_idx, SljitNativeRegionOpKind hash_join_kind,
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input, CONSUMER &consumer,
    idx_t &matched_count) {
	auto probe_start = SljitRegionStageStart(runtime);
	if (!TryExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbe<SELECTED>(plan, input)) {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_kind, "direct_aggregate_consumer_probe",
	                              probe_start);
	matched_count = input.selected_count;
	auto consume_start = SljitRegionStageStart(runtime);
	consumer.Consume(input.match_sel, input.row_pointers, matched_count);
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_kind, "direct_aggregate_consumer_accumulate",
	                              consume_start);
	return true;
}

template <bool SELECTED, bool USE_SALT, bool HAS_BLOOM>
static void
ExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer(const SljitNativeHashJoinProbePlan &plan,
                                                                  SljitNativeRegularHashJoinProbeInput &input,
                                                                  SljitHashJoinMatchedRowBatchConsumer &consumer) {
	const auto count = input.count;
	const auto key_data = reinterpret_cast<const int64_t *__restrict>(input.source_data[0]);
	const sel_t *__restrict key_sel = nullptr;
	if constexpr (SELECTED) {
		key_sel = input.source_sel[0];
	}
	const auto entries = reinterpret_cast<const ht_entry_t *__restrict>(input.entries);
	const auto bitmask = input.bitmask;
	const auto key_offset = plan.keys[0].key_layout_offset;

	auto row_idx = input.input_offset;
	if (row_idx < count) {
		auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		auto key = UnsafeNumericCast<int32_t>(key_data[source_idx]);
		auto hash = Hash<int32_t>(key);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < count;
			int32_t next_key = 0;
			hash_t next_hash = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
				next_key = UnsafeNumericCast<int32_t>(key_data[next_source_idx]);
				next_hash = Hash<int32_t>(next_key);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			if (SljitBloomFilterMayContainTemplated<HAS_BLOOM>(input, hash)) {
				hash_t salt = 0;
				if constexpr (USE_SALT) {
					salt = hash & ht_entry_t::SALT_MASK;
				}
				while (true) {
					const auto entry_value = entries[ht_offset].GetValue();
					if (!entry_value) {
						break;
					}
					if constexpr (USE_SALT) {
						if ((entry_value & ht_entry_t::SALT_MASK) != salt) {
							ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
							continue;
						}
					}
					auto row_location = SljitHashJoinEntryPointer(entry_value);
					if (SljitHashJoinKeysEqual<int32_t>(row_location, key_offset, key)) {
						consumer.EmitNoChainMatch(row_idx, row_location);
						break;
					}
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
				}
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key = next_key;
			hash = next_hash;
			ht_offset = next_ht_offset;
		}
	}
	consumer.Finish();
}

template <bool SELECTED, bool USE_SALT>
static void
ExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer(const SljitNativeHashJoinProbePlan &plan,
                                                                  SljitNativeRegularHashJoinProbeInput &input,
                                                                  SljitHashJoinMatchedRowBatchConsumer &consumer) {
	if (input.bloom_filter_bits) {
		return ExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer<SELECTED, USE_SALT, true>(plan, input,
		                                                                                                   consumer);
	}
	return ExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer<SELECTED, USE_SALT, false>(plan, input,
	                                                                                                    consumer);
}

template <bool SELECTED>
static bool
TryExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer(const SljitNativeHashJoinProbePlan &plan,
                                                                     SljitNativeRegularHashJoinProbeInput &input,
                                                                     SljitHashJoinMatchedRowBatchConsumer &consumer) {
	if (!input.source_key0_int64_to_int32 || !input.source_key0_int64_to_int32_unchecked || plan.keys.size() != 1 ||
	    plan.keys[0].key_kind != SljitNativeHashJoinKeyKind::INT32 ||
	    !SljitHashJoinHasAllValidEqualityKey(plan.keys[0]) || plan.mark_build_match || plan.residual_predicate ||
	    plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    !SljitHashJoinCanUseAllValidNoChainProbe(plan, input, SELECTED)) {
		return false;
	}
	switch (input.layout_kind) {
	case SljitHashJoinProbeLayoutKind::NO_CHAIN:
		ExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer<SELECTED, false>(plan, input, consumer);
		return true;
	case SljitHashJoinProbeLayoutKind::NO_CHAIN_SALT:
		ExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer<SELECTED, true>(plan, input, consumer);
		return true;
	default:
		return false;
	}
}

template <class GROUP_TYPE, class LOAD_GROUP, class PREDICATE_MATCHER, class FLUSH_ACCUMULATOR>
struct SljitJoinInputComplementarySumProbeConsumer {
	SljitJoinInputComplementarySumProbeConsumer(
	    SljitTypedJoinInputComplementarySumAccumulator<GROUP_TYPE> &accumulator_p, LOAD_GROUP &load_group_p,
	    PREDICATE_MATCHER &predicate_matcher_p, FLUSH_ACCUMULATOR &flush_accumulator_p)
	    : accumulator(accumulator_p), load_group(load_group_p), predicate_matcher(predicate_matcher_p),
	      flush_accumulator(flush_accumulator_p) {
	}

	void Consume(const sel_t *row_indices, data_ptr_t const *row_pointers, idx_t count) {
		for (idx_t match_idx = 0; match_idx < count; match_idx++) {
			ConsumeOne(row_indices[match_idx], row_pointers[match_idx]);
		}
	}

private:
	inline void ConsumeOne(idx_t row_idx, data_ptr_t row_location) {
		GROUP_TYPE key {};
		bool group_is_valid;
		if (!load_group(row_idx, key, group_is_valid)) {
			throw InternalException("SLJIT direct probe consumer group transform failed after successful preflight");
		}
		bool predicate_is_valid;
		const bool predicate_matches = predicate_matcher.Match(row_location, predicate_is_valid);
		if (!accumulator.Accumulate(group_is_valid, key, predicate_is_valid, predicate_matches)) {
			if (!flush_accumulator()) {
				throw InternalException("SLJIT direct probe consumer accumulator overflow flush failed");
			}
			if (!accumulator.Accumulate(group_is_valid, key, predicate_is_valid, predicate_matches)) {
				throw InternalException("SLJIT direct probe consumer accumulator remained full after flush");
			}
		}
	}

	SljitTypedJoinInputComplementarySumAccumulator<GROUP_TYPE> &accumulator;
	LOAD_GROUP &load_group;
	PREDICATE_MATCHER &predicate_matcher;
	FLUSH_ACCUMULATOR &flush_accumulator;
};

template <bool SELECTED, class GROUP_TYPE, class LOAD_GROUP, class FLUSH_ACCUMULATOR>
static bool SljitTryExecuteComplementarySumProbeConsumerWithGroupLoader(
    ExecutionRegionRuntime &runtime, idx_t hash_join_idx, SljitNativeRegionOpKind hash_join_kind,
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    SljitDirectJoinOutputAggregateStrategy &strategy, LOAD_GROUP &load_group,
    const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification, FLUSH_ACCUMULATOR &flush_accumulator,
    idx_t &matched_count, bool &used_unchecked_narrowing) {
	auto &accumulator = SljitGetJoinInputComplementarySumAccumulator<GROUP_TYPE>(strategy);
	auto execute_with_predicate = [&](auto &predicate_matcher) {
		SljitJoinInputComplementarySumProbeConsumer<GROUP_TYPE, LOAD_GROUP, decltype(predicate_matcher),
		                                            FLUSH_ACCUMULATOR>
		    typed_consumer(accumulator, load_group, predicate_matcher, flush_accumulator);
		if (TryExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainBufferedConsumer<SELECTED>(
		        runtime, hash_join_idx, hash_join_kind, plan, input, typed_consumer, matched_count)) {
			used_unchecked_narrowing = true;
			return true;
		}
		SljitHashJoinMatchedRowBatchConsumer consumer(input, typed_consumer);
		used_unchecked_narrowing =
		    TryExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer<SELECTED>(plan, input, consumer);
		if (!used_unchecked_narrowing &&
		    !TryExecuteAllValidSingleKeyNoChainProbeWithConsumer<SELECTED>(plan, input, consumer)) {
			return false;
		}
		matched_count = consumer.MatchedCount();
		return true;
	};
	return SljitWithComplementarySumRHSMatcher(predicate_field, classification, execute_with_predicate);
}

template <bool SELECTED, class FLUSH_ACCUMULATOR>
struct SljitComplementarySumProbeConsumerGroupDispatch {
	ExecutionRegionRuntime &runtime;
	idx_t hash_join_idx;
	SljitNativeRegionOpKind hash_join_kind;
	const SljitNativeHashJoinProbePlan &plan;
	SljitNativeRegularHashJoinProbeInput &input;
	SljitDirectJoinOutputAggregateStrategy &strategy;
	Vector &group_input;
	const ExecutionRowPointerGroupKeySource &group_source;
	const SljitComplementarySumRHSField &predicate_field;
	const SljitStringSetComplementarySumDescriptor &classification;
	FLUSH_ACCUMULATOR &flush_accumulator;
	idx_t &matched_count;
	bool &used_unchecked_narrowing;

	template <class GROUP_TYPE>
	bool Execute() {
		auto consume_group_loader = [&](auto &&, auto &&preflight, auto &&preflighted_load_group, auto &&prepare,
		                                auto stage_prepared_keys) {
			if constexpr (decltype(stage_prepared_keys)::value) {
				auto &accumulator = SljitGetJoinInputComplementarySumAccumulator<GROUP_TYPE>(strategy);
				if (!prepare(input.count, accumulator.prepared_group_keys.data(),
				             accumulator.prepared_group_validity.data())) {
					return false;
				}
				auto prepared_load_group = [&](idx_t row_idx, GROUP_TYPE &key, bool &valid) {
					key = accumulator.prepared_group_keys[row_idx];
					valid = accumulator.prepared_group_validity[row_idx] != 0;
					return true;
				};
				return SljitTryExecuteComplementarySumProbeConsumerWithGroupLoader<SELECTED, GROUP_TYPE>(
				    runtime, hash_join_idx, hash_join_kind, plan, input, strategy, prepared_load_group, predicate_field,
				    classification, flush_accumulator, matched_count, used_unchecked_narrowing);
			}
			if (!preflight(input.count)) {
				return false;
			}
			return SljitTryExecuteComplementarySumProbeConsumerWithGroupLoader<SELECTED, GROUP_TYPE>(
			    runtime, hash_join_idx, hash_join_kind, plan, input, strategy, preflighted_load_group, predicate_field,
			    classification, flush_accumulator, matched_count, used_unchecked_narrowing);
		};
		return SljitDispatchSelectedInputVectorGroupKey<GROUP_TYPE>(
		    group_input, *FlatVector::IncrementalSelectionVector(), group_source, input.source_key0_int64_to_int32,
		    consume_group_loader);
	}
};

template <bool SELECTED, class FLUSH_ACCUMULATOR>
static bool SljitTryExecuteComplementarySumProbeConsumer(
    ExecutionRegionRuntime &runtime, idx_t hash_join_idx, SljitNativeRegionOpKind hash_join_kind,
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    SljitDirectJoinOutputAggregateStrategy &strategy, Vector &group_input,
    const ExecutionRowPointerGroupKeySource &group_source, const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification, FLUSH_ACCUMULATOR &flush_accumulator,
    idx_t &matched_count, bool &used_unchecked_narrowing) {
	SljitComplementarySumProbeConsumerGroupDispatch<SELECTED, FLUSH_ACCUMULATOR> dispatch {runtime,
	                                                                                       hash_join_idx,
	                                                                                       hash_join_kind,
	                                                                                       plan,
	                                                                                       input,
	                                                                                       strategy,
	                                                                                       group_input,
	                                                                                       group_source,
	                                                                                       predicate_field,
	                                                                                       classification,
	                                                                                       flush_accumulator,
	                                                                                       matched_count,
	                                                                                       used_unchecked_narrowing};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(group_source.target_physical_type, dispatch);
}

struct SljitPerfectHashDictionaryComplementarySumRHSMatcher {
	SljitPerfectHashDictionaryComplementarySumRHSMatcher(
	    Vector &dictionary, const SljitComplementarySumRHSField &field_p,
	    const SljitStringSetComplementarySumDescriptor &classification_p)
	    : field(field_p), classification(classification_p) {
		D_ASSERT(dictionary.GetVectorType() == VectorType::FLAT_VECTOR);
		data = FlatVector::GetData(dictionary);
		validity = &FlatVector::Validity(dictionary);
		all_valid = validity->CannotHaveNull();
	}

	bool AllValid() const {
		return all_valid;
	}

	bool Match(sel_t build_idx, bool &valid) const {
		const auto dictionary_idx = NumericCast<idx_t>(build_idx);
		valid = all_valid || validity->RowIsValid(dictionary_idx);
		if (!valid) {
			return false;
		}
		return MatchKnownValid(dictionary_idx);
	}

	bool MatchAllValid(sel_t build_idx) const {
		D_ASSERT(all_valid);
		return MatchKnownValid(NumericCast<idx_t>(build_idx));
	}

private:
	bool MatchKnownValid(idx_t dictionary_idx) const {
		if (field.compressed_size != 0) {
			const auto value = data + dictionary_idx * field.compressed_size;
			switch (field.compressed_size) {
			case sizeof(uint8_t): {
				const auto compressed = Load<uint8_t>(value);
				return compressed == Load<uint8_t>(field.compressed_constants[0].data()) ||
				       compressed == Load<uint8_t>(field.compressed_constants[1].data());
			}
			case sizeof(uint16_t): {
				const auto compressed = Load<uint16_t>(value);
				return compressed == Load<uint16_t>(field.compressed_constants[0].data()) ||
				       compressed == Load<uint16_t>(field.compressed_constants[1].data());
			}
			case sizeof(uint32_t): {
				const auto compressed = Load<uint32_t>(value);
				return compressed == Load<uint32_t>(field.compressed_constants[0].data()) ||
				       compressed == Load<uint32_t>(field.compressed_constants[1].data());
			}
			case sizeof(uint64_t): {
				const auto compressed = Load<uint64_t>(value);
				return compressed == Load<uint64_t>(field.compressed_constants[0].data()) ||
				       compressed == Load<uint64_t>(field.compressed_constants[1].data());
			}
			case sizeof(uhugeint_t): {
				const auto compressed = Load<uhugeint_t>(value);
				return compressed == Load<uhugeint_t>(field.compressed_constants[0].data()) ||
				       compressed == Load<uhugeint_t>(field.compressed_constants[1].data());
			}
			default:
				throw InternalException("SLJIT direct perfect probe has an unsupported compressed predicate width");
			}
		}
		const auto predicate = reinterpret_cast<const string_t *>(data)[dictionary_idx];
		return SljitStringEqualsEitherConstant(predicate, classification.constants[0], classification.signatures[0],
		                                       classification.constants[1], classification.signatures[1]);
	}

public:
	const SljitComplementarySumRHSField &field;
	const SljitStringSetComplementarySumDescriptor &classification;
	const_data_ptr_t data = nullptr;
	optional_ptr<const ValidityMask> validity;
	bool all_valid = false;
};

//! A compressed RHS has one immutable physical storage width for the lifetime
//! of the vector. Bind that width and its constants before entering the direct
//! consumer so its hot loop only loads and compares the physical value.
template <class STORAGE>
struct SljitPerfectHashAllValidCompressedRHSMatcher {
	SljitPerfectHashAllValidCompressedRHSMatcher(Vector &dictionary, const SljitComplementarySumRHSField &field) {
		D_ASSERT(dictionary.GetVectorType() == VectorType::FLAT_VECTOR);
		D_ASSERT(field.compressed_size == sizeof(STORAGE));
		D_ASSERT(FlatVector::Validity(dictionary).CannotHaveNull());
		data = FlatVector::GetData<STORAGE>(dictionary);
		first = Load<STORAGE>(field.compressed_constants[0].data());
		second = Load<STORAGE>(field.compressed_constants[1].data());
	}

	bool AllValid() const {
		return true;
	}

	bool Match(sel_t build_idx, bool &valid) const {
		valid = true;
		return MatchAllValid(build_idx);
	}

	bool MatchAllValid(sel_t build_idx) const {
		const auto value = data[NumericCast<idx_t>(build_idx)];
		return value == first || value == second;
	}

	const STORAGE *data = nullptr;
	STORAGE first {};
	STORAGE second {};
};

using SljitPerfectHashAllValidByteRHSMatcher = SljitPerfectHashAllValidCompressedRHSMatcher<uint8_t>;
using SljitPerfectHashAllValidUhugeintRHSMatcher = SljitPerfectHashAllValidCompressedRHSMatcher<uhugeint_t>;

static uint8_t SljitPerfectHashPredicateClassificationCode(bool valid, bool matches) {
	return static_cast<uint8_t>(valid ? (matches ? SljitPerfectHashPredicateClassification::MATCHING
	                                             : SljitPerfectHashPredicateClassification::NON_MATCHING)
	                                  : SljitPerfectHashPredicateClassification::NULL_VALUE);
}

static bool SljitPerfectHashPredicateNeedsDictionaryClassification(const SljitComplementarySumRHSField &field) {
	// The shared cache activates only after direct probe volume covers this RHS
	// dictionary. At that point a byte classification replaces every remaining
	// uncompressed string fetch and equality check, including the packed inline
	// case. Compressed fields are already narrower than the classifier value.
	return field.compressed_size == 0;
}

struct SljitSharedPerfectHashDictionaryComplementarySumRHSMatcher {
	explicit SljitSharedPerfectHashDictionaryComplementarySumRHSMatcher(
	    const SljitPerfectHashPredicateClassificationArtifact &artifact_p)
	    : artifact(artifact_p) {
	}

	bool AllValid() const {
		return artifact.all_valid;
	}

	bool Match(sel_t build_idx, bool &valid) const {
		const auto classification = artifact.classifications[NumericCast<idx_t>(build_idx)];
		valid = classification != static_cast<uint8_t>(SljitPerfectHashPredicateClassification::NULL_VALUE);
		return classification == static_cast<uint8_t>(SljitPerfectHashPredicateClassification::MATCHING);
	}

	bool MatchAllValid(sel_t build_idx) const {
		D_ASSERT(AllValid());
		return artifact.classifications[NumericCast<idx_t>(build_idx)] ==
		       static_cast<uint8_t>(SljitPerfectHashPredicateClassification::MATCHING);
	}

	const SljitPerfectHashPredicateClassificationArtifact &artifact;
};

template <class GROUP_TYPE, bool PREDICATE_ALL_VALID, class LOAD_GROUP, class PREDICATE_MATCHER,
          class FLUSH_ACCUMULATOR, class BUILD_INDEX>
struct SljitPerfectHashComplementarySumProbeConsumer {
	SljitPerfectHashComplementarySumProbeConsumer(
	    SljitTypedJoinInputComplementarySumAccumulator<GROUP_TYPE, PREDICATE_ALL_VALID> &accumulator_p,
	    LOAD_GROUP &load_group_p, const BUILD_INDEX &build_index_p, PREDICATE_MATCHER &predicate_matcher_p,
	    FLUSH_ACCUMULATOR &flush_accumulator_p)
	    : accumulator(accumulator_p), load_group(load_group_p), build_index(build_index_p),
	      predicate_matcher(predicate_matcher_p), flush_accumulator(flush_accumulator_p) {
	}

	bool Consume(idx_t count) {
		if constexpr (PREDICATE_ALL_VALID) {
			if (ConsumeOneOrTwoKnownGroups(count)) {
				return true;
			}
		}
		for (idx_t match_idx = 0; match_idx < count; match_idx++) {
			ConsumeOne(match_idx);
		}
		return false;
	}

private:
	bool ConsumeOneOrTwoKnownGroups(idx_t count) {
		if (!accumulator.HasOneOrTwoGroups()) {
			return false;
		}
		const auto group_count = accumulator.Count();
		D_ASSERT(group_count <= 2);
		int64_t matching_counts[2] = {0, 0};
		int64_t non_matching_counts[2] = {0, 0};
		auto commit = [&]() {
			for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
				accumulator.AddAllValidKnownGroup(group_idx, matching_counts[group_idx],
				                                  non_matching_counts[group_idx]);
			}
		};
		for (idx_t match_idx = 0; match_idx < count; match_idx++) {
			GROUP_TYPE key {};
			bool group_is_valid;
			if (!load_group(match_idx, key, group_is_valid)) {
				throw InternalException("SLJIT direct perfect probe group transform failed after successful preflight");
			}
			idx_t group_idx;
			if (accumulator.MatchesKnownGroup(0, group_is_valid, key)) {
				group_idx = 0;
			} else if (group_count == 2 && accumulator.MatchesKnownGroup(1, group_is_valid, key)) {
				group_idx = 1;
			} else {
				commit();
				for (idx_t remainder_idx = match_idx; remainder_idx < count; remainder_idx++) {
					ConsumeOne(remainder_idx);
				}
				return true;
			}
			const auto build_idx = build_index.Get(match_idx);
			if (predicate_matcher.MatchAllValid(build_idx)) {
				matching_counts[group_idx]++;
			} else {
				non_matching_counts[group_idx]++;
			}
		}
		commit();
		return true;
	}

	inline void ConsumeOne(idx_t match_idx) {
		GROUP_TYPE key {};
		bool group_is_valid;
		if (!load_group(match_idx, key, group_is_valid)) {
			throw InternalException("SLJIT direct perfect probe group transform failed after successful preflight");
		}
		const auto build_idx = build_index.Get(match_idx);
		bool predicate_matches;
		bool accumulated;
		if constexpr (PREDICATE_ALL_VALID) {
			predicate_matches = predicate_matcher.MatchAllValid(build_idx);
			accumulated = accumulator.AccumulateAllValid(group_is_valid, key, predicate_matches);
		} else {
			bool predicate_is_valid;
			predicate_matches = predicate_matcher.Match(build_idx, predicate_is_valid);
			accumulated = accumulator.Accumulate(group_is_valid, key, predicate_is_valid, predicate_matches);
		}
		if (!accumulated) {
			if (!flush_accumulator()) {
				throw InternalException("SLJIT direct perfect probe accumulator overflow flush failed");
			}
			if constexpr (PREDICATE_ALL_VALID) {
				accumulated = accumulator.AccumulateAllValid(group_is_valid, key, predicate_matches);
			} else {
				bool predicate_is_valid;
				predicate_matches = predicate_matcher.Match(build_idx, predicate_is_valid);
				accumulated = accumulator.Accumulate(group_is_valid, key, predicate_is_valid, predicate_matches);
			}
			if (!accumulated) {
				throw InternalException("SLJIT direct perfect probe accumulator remained full after flush");
			}
		}
	}

	SljitTypedJoinInputComplementarySumAccumulator<GROUP_TYPE, PREDICATE_ALL_VALID> &accumulator;
	LOAD_GROUP &load_group;
	const BUILD_INDEX &build_index;
	PREDICATE_MATCHER &predicate_matcher;
	FLUSH_ACCUMULATOR &flush_accumulator;
};

struct SljitPerfectHashMaterializedBuildIndex {
	explicit SljitPerfectHashMaterializedBuildIndex(const SelectionVector &selection_p) : selection(selection_p) {
	}

	inline sel_t Get(idx_t match_idx) const {
		return selection.get_index(match_idx);
	}

	const SelectionVector &selection;
};

struct SljitPerfectHashIdentityBuildIndex {
	SljitPerfectHashIdentityBuildIndex(const int64_t *source_p, const SelectionVector &source_selection_p,
	                                   int64_t minimum_p)
	    : source(source_p), source_selection(source_selection_p), minimum(minimum_p) {
	}

	inline sel_t Get(idx_t match_idx) const {
		const auto source_idx = source_selection.get_index(match_idx);
		return UnsafeNumericCast<sel_t>(source[source_idx] - minimum);
	}

	const int64_t *source;
	const SelectionVector &source_selection;
	int64_t minimum;
};

//! The direct-consumer output proof keeps probe rows contiguous. When its key
//! vector is contiguous too, the perfect-hash build index is just key-minus-min.
struct SljitPerfectHashContiguousIdentityBuildIndex {
	SljitPerfectHashContiguousIdentityBuildIndex(const int64_t *source_p, int64_t minimum_p)
	    : source(source_p), minimum(minimum_p) {
	}

	inline sel_t Get(idx_t match_idx) const {
		return UnsafeNumericCast<sel_t>(source[match_idx] - minimum);
	}

	const int64_t *source;
	int64_t minimum;
};

template <class EXECUTE>
static bool SljitWithPerfectHashIdentityBuildIndex(const SljitNativeHashJoinProbeKeyPlan &key,
                                                   const ExecutionPerfectHashJoinTableLayout &layout, DataChunk &input,
                                                   EXECUTE &execute, bool &used_contiguous_source) {
	D_ASSERT(SljitCanDerivePerfectHashBuildSelectionFromIdentity(key, layout, input));
	UnifiedVectorFormat source_format;
	input.data[key.key_input_index].ToUnifiedFormat(source_format);
	D_ASSERT(source_format.sel);
	const auto minimum = key.key_kind == SljitNativeHashJoinKeyKind::INT32
	                         ? NumericCast<int64_t>(SljitExactPerfectHashJoinMinimum<int32_t>(layout.build_min))
	                         : SljitExactPerfectHashJoinMinimum<int64_t>(layout.build_min);
	const auto source = UnifiedVectorFormat::GetData<int64_t>(source_format);
	if (source_format.sel == FlatVector::IncrementalSelectionVector()) {
		SljitPerfectHashContiguousIdentityBuildIndex build_index(source, minimum);
		used_contiguous_source = true;
		return execute(build_index);
	}
	SljitPerfectHashIdentityBuildIndex build_index(source, *source_format.sel, minimum);
	return execute(build_index);
}

template <class GROUP_TYPE, class PREDICATE_MATCHER, class BUILD_INDEX>
static bool SljitTryExecutePerfectHashInlineStringKnownGroupConsumer(
    SljitDirectJoinOutputAggregateStrategy &strategy, Vector &group_input, const SelectionVector &match_selection,
    const BUILD_INDEX &build_index, const ExecutionRowPointerGroupKeySource &group_source,
    PREDICATE_MATCHER &predicate_matcher, idx_t count, bool &used_identity_selection) {
	if (group_source.cast_kind != ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS ||
	    group_source.source_physical_type != PhysicalType::VARCHAR ||
	    group_input.GetType().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	auto &accumulator = SljitGetJoinInputComplementarySumAccumulator<GROUP_TYPE, true>(strategy);
	if (!accumulator.HasOneOrTwoGroups()) {
		return false;
	}
	const auto group_count = accumulator.Count();
	D_ASSERT(group_count <= 2);
	std::array<SljitInlineStringStorageSignature, 2> group_signatures {};
	std::array<bool, 2> group_validity {};
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		group_validity[group_idx] = accumulator.groups.IsValid(group_idx);
		if (group_validity[group_idx] &&
		    !SljitTryInlineStringStorageSignature(accumulator.groups.Key(group_idx), group_signatures[group_idx])) {
			return false;
		}
	}

	UnifiedVectorFormat group_format;
	group_input.ToUnifiedFormat(group_format);
	auto group_data = UnifiedVectorFormat::GetData<string_t>(group_format);
	// A compact probe result and a flat all-valid group vector share the same
	// ordinal space. Keep that normal case contiguous instead of reapplying two
	// identity selections and a null-mask lookup to every row.
	if (&match_selection == FlatVector::IncrementalSelectionVector() &&
	    group_format.sel == FlatVector::IncrementalSelectionVector() && group_format.validity.CannotHaveNull()) {
		int64_t matching_counts[2] = {0, 0};
		int64_t non_matching_counts[2] = {0, 0};
		for (idx_t match_idx = 0; match_idx < count; match_idx++) {
			idx_t group_idx = DConstants::INVALID_INDEX;
			if (!SljitTryClassifyOneOrTwoInlineStringStorageSignatures(group_data[match_idx], true, group_validity,
			                                                           group_signatures, group_count, group_idx)) {
				return false;
			}
			const auto build_idx = build_index.Get(match_idx);
			if (predicate_matcher.MatchAllValid(build_idx)) {
				matching_counts[group_idx]++;
			} else {
				non_matching_counts[group_idx]++;
			}
		}
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			accumulator.AddAllValidKnownGroup(group_idx, matching_counts[group_idx], non_matching_counts[group_idx]);
		}
		used_identity_selection = true;
		return true;
	}
	// These counters are local to the vector. An unseen key returns before the
	// commit below, so the generated predicate can run in the validation pass
	// without exposing any partial accumulator update or staging row-local keys.
	int64_t matching_counts[2] = {0, 0};
	int64_t non_matching_counts[2] = {0, 0};
	for (idx_t match_idx = 0; match_idx < count; match_idx++) {
		const auto source_idx = group_format.sel->get_index(match_selection.get_index(match_idx));
		idx_t group_idx = DConstants::INVALID_INDEX;
		if (!SljitTryClassifyOneOrTwoInlineStringStorageSignatures(
		        group_data[source_idx], group_format.validity.RowIsValid(source_idx), group_validity, group_signatures,
		        group_count, group_idx)) {
			return false;
		}
		const auto build_idx = build_index.Get(match_idx);
		if (predicate_matcher.MatchAllValid(build_idx)) {
			matching_counts[group_idx]++;
		} else {
			non_matching_counts[group_idx]++;
		}
	}
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		accumulator.AddAllValidKnownGroup(group_idx, matching_counts[group_idx], non_matching_counts[group_idx]);
	}
	return true;
}

template <bool PREDICATE_ALL_VALID, class PREDICATE_MATCHER, class FLUSH_ACCUMULATOR, class BUILD_INDEX>
struct SljitPerfectHashComplementarySumProbeConsumerGroupDispatch {
	SljitDirectJoinOutputAggregateStrategy &strategy;
	Vector &group_input;
	const SelectionVector &match_selection;
	const BUILD_INDEX &build_index;
	const ExecutionRowPointerGroupKeySource &group_source;
	PREDICATE_MATCHER &predicate_matcher;
	idx_t count;
	bool source_key0_int64_to_int32_unchecked;
	FLUSH_ACCUMULATOR &flush_accumulator;
	bool &used_one_or_two_known_groups;
	bool &used_inline_string_known_groups;
	bool &used_inline_string_identity_selection;

	template <class GROUP_TYPE>
	bool Execute() {
		if constexpr (PREDICATE_ALL_VALID && SljitInlineStringStorageSignatureSupported<GROUP_TYPE>::value) {
			if (SljitTryExecutePerfectHashInlineStringKnownGroupConsumer<GROUP_TYPE>(
			        strategy, group_input, match_selection, build_index, group_source, predicate_matcher, count,
			        used_inline_string_identity_selection)) {
				used_one_or_two_known_groups = true;
				used_inline_string_known_groups = true;
				return true;
			}
		}
		auto consume_group_loader = [&](auto &&, auto &&preflight, auto &&preflighted_load_group, auto &&prepare,
		                                auto stage_prepared_keys) {
			if constexpr (decltype(stage_prepared_keys)::value) {
				auto &accumulator =
				    SljitGetJoinInputComplementarySumAccumulator<GROUP_TYPE, PREDICATE_ALL_VALID>(strategy);
				if (!prepare(count, accumulator.prepared_group_keys.data(),
				             accumulator.prepared_group_validity.data())) {
					return false;
				}
				auto prepared_load_group = [&](idx_t match_idx, GROUP_TYPE &key, bool &valid) {
					key = accumulator.prepared_group_keys[match_idx];
					valid = accumulator.prepared_group_validity[match_idx] != 0;
					return true;
				};
				SljitPerfectHashComplementarySumProbeConsumer<GROUP_TYPE, PREDICATE_ALL_VALID,
				                                              decltype(prepared_load_group), PREDICATE_MATCHER,
				                                              FLUSH_ACCUMULATOR, BUILD_INDEX>
				    consumer(accumulator, prepared_load_group, build_index, predicate_matcher, flush_accumulator);
				used_one_or_two_known_groups |= consumer.Consume(count);
				return true;
			}
			if (!preflight(count)) {
				return false;
			}
			auto &accumulator = SljitGetJoinInputComplementarySumAccumulator<GROUP_TYPE, PREDICATE_ALL_VALID>(strategy);
			SljitPerfectHashComplementarySumProbeConsumer<GROUP_TYPE, PREDICATE_ALL_VALID,
			                                              decltype(preflighted_load_group), PREDICATE_MATCHER,
			                                              FLUSH_ACCUMULATOR, BUILD_INDEX>
			    consumer(accumulator, preflighted_load_group, build_index, predicate_matcher, flush_accumulator);
			used_one_or_two_known_groups |= consumer.Consume(count);
			return true;
		};
		return SljitDispatchSelectedInputVectorGroupKey<GROUP_TYPE>(
		    group_input, match_selection, group_source, source_key0_int64_to_int32_unchecked, consume_group_loader);
	}
};

template <bool PREDICATE_ALL_VALID, class PREDICATE_MATCHER, class FLUSH_ACCUMULATOR, class BUILD_INDEX>
static bool SljitExecutePerfectHashComplementarySumProbeConsumer(
    SljitDirectJoinOutputAggregateStrategy &strategy, Vector &group_input, const SelectionVector &match_selection,
    const BUILD_INDEX &build_index, const ExecutionRowPointerGroupKeySource &group_source,
    PREDICATE_MATCHER &predicate_matcher, idx_t count, bool source_key0_int64_to_int32_unchecked,
    FLUSH_ACCUMULATOR &flush_accumulator, bool &used_one_or_two_known_groups, bool &used_inline_string_known_groups,
    bool &used_inline_string_identity_selection) {
	SljitPerfectHashComplementarySumProbeConsumerGroupDispatch<PREDICATE_ALL_VALID, PREDICATE_MATCHER,
	                                                           FLUSH_ACCUMULATOR, BUILD_INDEX>
	    dispatch {strategy,
	              group_input,
	              match_selection,
	              build_index,
	              group_source,
	              predicate_matcher,
	              count,
	              source_key0_int64_to_int32_unchecked,
	              flush_accumulator,
	              used_one_or_two_known_groups,
	              used_inline_string_known_groups,
	              used_inline_string_identity_selection};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(group_source.target_physical_type, dispatch);
}

template <class BUILD_INDEX, class FLUSH_ACCUMULATOR>
static bool SljitTryExecutePerfectHashComplementarySumProbeConsumer(
    ExecutionRegionRuntime &runtime, SljitDirectJoinOutputAggregateStrategy &strategy, Vector &group_input,
    const SelectionVector &match_selection, const BUILD_INDEX &build_index,
    const ExecutionRowPointerGroupKeySource &group_source,
    const buffer_ptr<DictionaryEntry> &predicate_dictionary_entry, Vector &predicate_dictionary,
    SljitSharedPerfectHashPredicateClassificationCache &shared_predicate_classification,
    const SljitComplementarySumRHSField &predicate_field,
    const SljitStringSetComplementarySumDescriptor &classification, idx_t count,
    bool source_key0_int64_to_int32_unchecked, FLUSH_ACCUMULATOR &flush_accumulator) {
	if (predicate_dictionary.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	bool used_one_or_two_known_groups = false;
	bool used_inline_string_known_groups = false;
	bool used_inline_string_identity_selection = false;
	auto execute = [&](auto &active_predicate_matcher) {
		if (active_predicate_matcher.AllValid()) {
			runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.direct_aggregate_consumer.all_valid_rhs",
			                             count);
			const bool executed = SljitExecutePerfectHashComplementarySumProbeConsumer<true>(
			    strategy, group_input, match_selection, build_index, group_source, active_predicate_matcher, count,
			    source_key0_int64_to_int32_unchecked, flush_accumulator, used_one_or_two_known_groups,
			    used_inline_string_known_groups, used_inline_string_identity_selection);
			if (used_inline_string_known_groups) {
				runtime.RecordJitRuntimePath(
				    "hash_join_probe.perfect_probe.direct_aggregate_consumer.inline_string_known_groups", count);
			}
			if (used_inline_string_identity_selection) {
				runtime.RecordJitRuntimePath(
				    "hash_join_probe.perfect_probe.direct_aggregate_consumer.inline_string_identity_known_groups",
				    count);
			}
			if (used_one_or_two_known_groups) {
				runtime.RecordJitRuntimePath(
				    "hash_join_probe.perfect_probe.direct_aggregate_consumer.one_or_two_known_groups", count);
			}
			return executed;
		}
		return SljitExecutePerfectHashComplementarySumProbeConsumer<false>(
		    strategy, group_input, match_selection, build_index, group_source, active_predicate_matcher, count,
		    source_key0_int64_to_int32_unchecked, flush_accumulator, used_one_or_two_known_groups,
		    used_inline_string_known_groups, used_inline_string_identity_selection);
	};
	if (predicate_field.compressed_size == sizeof(uhugeint_t) &&
	    FlatVector::Validity(predicate_dictionary).CannotHaveNull()) {
		SljitPerfectHashAllValidUhugeintRHSMatcher packed_predicate_matcher(predicate_dictionary, predicate_field);
		runtime.RecordJitRuntimePath(
		    "hash_join_probe.perfect_probe.direct_aggregate_consumer.compressed_uhugeint_predicate", count);
		return execute(packed_predicate_matcher);
	}
	if (predicate_field.compressed_size == sizeof(uint8_t) &&
	    FlatVector::Validity(predicate_dictionary).CannotHaveNull()) {
		SljitPerfectHashAllValidByteRHSMatcher byte_predicate_matcher(predicate_dictionary, predicate_field);
		runtime.RecordJitRuntimePath(
		    "hash_join_probe.perfect_probe.direct_aggregate_consumer.compressed_byte_predicate", count);
		return execute(byte_predicate_matcher);
	}
	SljitPerfectHashDictionaryComplementarySumRHSMatcher predicate_matcher(predicate_dictionary, predicate_field,
	                                                                       classification);
	if (!SljitPerfectHashPredicateNeedsDictionaryClassification(predicate_field)) {
		return execute(predicate_matcher);
	}
	auto classification_observation = shared_predicate_classification.Observe(predicate_dictionary_entry, count, [&]() {
		SljitPerfectHashPredicateClassificationBuildResult result;
		result.all_valid = predicate_matcher.AllValid();
		result.classifications.reserve(predicate_dictionary_entry->data.size());
		for (idx_t dictionary_idx = 0; dictionary_idx < predicate_dictionary_entry->data.size(); dictionary_idx++) {
			bool valid;
			const bool matches = predicate_matcher.Match(NumericCast<sel_t>(dictionary_idx), valid);
			result.classifications.push_back(SljitPerfectHashPredicateClassificationCode(valid, matches));
		}
		return result;
	});
	if (classification_observation.started_dictionary_epoch) {
		runtime.RecordJitRuntimePath(
		    "hash_join_probe.perfect_probe.direct_aggregate_consumer.shared_predicate_classifier_dictionary_epoch");
	}
	if (classification_observation.activation_pending) {
		runtime.RecordJitRuntimePath(
		    "hash_join_probe.perfect_probe.direct_aggregate_consumer.shared_predicate_classifier_observing", count);
	}
	if (classification_observation.artifact) {
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.direct_aggregate_consumer.shared_predicate_cache",
		                             count);
		SljitSharedPerfectHashDictionaryComplementarySumRHSMatcher shared_predicate_matcher(
		    *classification_observation.artifact);
		return execute(shared_predicate_matcher);
	}
	return execute(predicate_matcher);
}

template <class EXECUTE_HASH_JOIN_PROBE>
static SljitHashJoinAggregateConsumerResult SljitTryExecuteHashJoinComplementarySumAggregateConsumer(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, const SljitHashJoinProbeSelectionPrimitive &probe_primitive,
    SljitExecutableRegionOp &hash_join_op, SljitDirectJoinOutputAggregateStrategy &strategy, DataChunk &join_input,
    SljitPostJoinProjectionStrategy &post_join_projection, optional_ptr<const vector<idx_t>> output_column_map,
    idx_t output_projection_idx, optional_ptr<bool> deferred_grouped_finish, idx_t probe_input_filter_idx,
    SljitHashJoinProbeInputFilterCache &probe_input_filter_cache, EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe) {
	SljitHashJoinAggregateConsumerResult result;
	const auto hash_join_idx = probe_primitive.hash_join_idx;
	if (hash_join_idx >= ops.size() || strategy.aggregate_idx >= ops.size() || join_input.size() == 0) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.input_shape";
		return result;
	}
	auto contract_view = SljitBuildHashJoinProbeExecutionContractView(
	    hash_join_op, optional_ptr<const SljitHashJoinProbeInputRemap>(&probe_primitive.input_remap),
	    SljitHashJoinProbeOutputContract::IDENTITY_PREFERRED_DIRECT_CONSUMER);
	auto &plan = *contract_view.plan;
	ExecutionOperatorBinding *binding_ptr = nullptr;
	auto bind_result = SljitBindRecordedNativeOperator(
	    runtime, native_runtime, scratch, hash_join_idx, hash_join_op, join_input, *contract_view.operator_info,
	    "native-operator-runtime-deferred", "SLJIT direct hash join aggregate consumer", binding_ptr,
	    result.deferred_reason);
	if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
		result.status = SljitHashJoinAggregateConsumerStatus::DEFERRED;
		return result;
	}
	if (!binding_ptr || !binding_ptr->ready || !binding_ptr->hash_join_probe.ready) {
		throw InternalException("SLJIT direct hash join aggregate consumer received an incomplete operator binding");
	}
	auto &probe = binding_ptr->hash_join_probe;
	const bool regular_hash_join = probe.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = probe.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if ((!regular_hash_join && !perfect_hash_join) || probe.empty_build_side ||
	    probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD || plan.residual_predicate ||
	    plan.mark_build_match || plan.mark_build_match_after_residual || plan.keys.size() != 1 ||
	    plan.equality_key_count != 1 || probe.probe_key_input_indices.size() != plan.keys.size() ||
	    (probe_input_filter_idx != DConstants::INVALID_INDEX && probe_primitive.HasOutputColumnMap()) ||
	    (regular_hash_join && plan.exact_source_filter_identity &&
	     plan.exact_source_filter_identity == probe.table_layout.runtime_filter_identity)) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.join_semantics";
		return result;
	}
	SljitHashJoinProbeLayoutKind table_layout_kind = SljitHashJoinProbeLayoutKind::NO_CHAIN;
	bool rhs_keys_all_valid = false;
	if (regular_hash_join) {
		auto &layout = probe.table_layout;
		table_layout_kind = SljitValidateRegularHashJoinProbeExecutionLayout(plan, probe);
		if (layout.needs_chain_matcher || SljitHashJoinProbeLayoutChainsLongerThanOne(table_layout_kind)) {
			result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.join_chain";
			return result;
		}
		rhs_keys_all_valid =
		    !layout.can_have_null || layout.null_keys_are_filtered || (probe.hash_table && !probe.hash_table->has_null);
	}
	auto filter_result =
	    SljitTryExecuteHashJoinProbeInputFilter(runtime, scratch, ops, hash_join_idx, probe_input_filter_idx, probe,
	                                            join_input.GetTypes(), join_input, probe_input_filter_cache);
	if (filter_result.status == SljitHashJoinProbeInputFilterStatus::NOT_APPLICABLE) {
		result.blocker = filter_result.blocker;
		return result;
	}
	if (filter_result.status == SljitHashJoinProbeInputFilterStatus::EMPTY) {
		result.status = SljitHashJoinAggregateConsumerStatus::EXECUTED;
		return result;
	}
	D_ASSERT(filter_result.input);
	auto &probe_input = *filter_result.input;
	if (!SljitTryPrepareDirectJoinOutputAggregateDescriptor(
	        runtime, ops, scratch, optional_ptr<SljitDirectJoinOutputAggregateStrategy>(&strategy),
	        post_join_projection, probe_input.size(), output_column_map, output_projection_idx)) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.aggregate_descriptor";
		return result;
	}
	auto &aggregate_op = ops[strategy.aggregate_idx];
	SljitPreparedJoinInputComplementarySumUpdate prepared_aggregate;
	string aggregate_failure;
	if (!SljitTryPrepareJoinInputComplementarySumUpdate(
	        runtime, native_runtime, scratch, hash_join_idx, aggregate_op, strategy, probe_input, probe_input.size(),
	        deferred_grouped_finish, prepared_aggregate, optional_ptr<string>(&aggregate_failure)) ||
	    !prepared_aggregate.pipeline_accumulator_enabled) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.aggregate_contract";
		return result;
	}
	auto &complementary_plan = strategy.join_input_complementary_sum_plan;
	const auto expected_predicate_storage = perfect_hash_join
	                                            ? SljitComplementarySumPredicateStorage::PERFECT_HASH_DICTIONARY
	                                            : SljitComplementarySumPredicateStorage::REGULAR_ROW_POINTER;
	if (complementary_plan.predicate_storage != expected_predicate_storage) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.predicate_storage";
		return result;
	}
	if (complementary_plan.join_input_group_column_idx >= probe_input.ColumnCount()) {
		throw InternalException("SLJIT direct hash join aggregate consumer group input is out of range");
	}
	auto flush_accumulator = [&]() {
		return SljitFlushJoinInputComplementarySumAccumulator(runtime, aggregate_op, strategy);
	};
	if (perfect_hash_join) {
		if (complementary_plan.perfect_hash_rhs_output_idx >= probe.perfect_layout.rhs_dictionary_buffers.size() ||
		    !probe.perfect_layout.rhs_dictionary_buffers[complementary_plan.perfect_hash_rhs_output_idx]) {
			result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.predicate_dictionary";
			return result;
		}
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		join_output.Reset();
		SljitHashJoinProbeDrainState state;
		string deferred_reason;
		auto probe_result =
		    execute_hash_join_probe(scratch, hash_join_idx, hash_join_op, probe_input, join_output, state,
		                            deferred_reason, probe_primitive.source_key0_int64_to_int32_unchecked,
		                            SljitHashJoinProbeOutputContract::IDENTITY_PREFERRED_DIRECT_CONSUMER,
		                            optional_ptr<const SljitHashJoinProbeInputRemap>(&probe_primitive.input_remap));
		if (probe_result == ExecutionOperatorBindResult::DEFERRED) {
			result.status = SljitHashJoinAggregateConsumerStatus::DEFERRED;
			result.deferred_reason = std::move(deferred_reason);
			return result;
		}
		if (!state.finished) {
			result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.perfect_probe_drain";
			return result;
		}
		if (join_output.size() == 0) {
			result.status = SljitHashJoinAggregateConsumerStatus::EXECUTED;
			return result;
		}
		const auto &predicate_dictionary_entry =
		    probe.perfect_layout.rhs_dictionary_buffers[complementary_plan.perfect_hash_rhs_output_idx];
		auto &predicate_dictionary = predicate_dictionary_entry->data;
		const auto &match_selection = state.output_proof.match_selection_is_identity
		                                  ? *FlatVector::IncrementalSelectionVector()
		                                  : scratch.FilterSelection(hash_join_idx);
		auto accumulate_start = SljitRegionStageStart(runtime);
		auto execute_consumer = [&](const auto &build_index) {
			return SljitTryExecutePerfectHashComplementarySumProbeConsumer(
			    runtime, strategy, probe_input.data[complementary_plan.join_input_group_column_idx], match_selection,
			    build_index, prepared_aggregate.group_source, predicate_dictionary_entry, predicate_dictionary,
			    hash_join_op.hash_join_probe.shared_predicate_classification, complementary_plan.predicate_field,
			    complementary_plan.classification, join_output.size(), state.output_proof.source_key0_int64_to_int32,
			    flush_accumulator);
		};
		bool executed;
		if (state.output_proof.perfect_build_selection_is_key_offset) {
			bool used_contiguous_source = false;
			executed = SljitWithPerfectHashIdentityBuildIndex(plan.keys[0], probe.perfect_layout, probe_input,
			                                                  execute_consumer, used_contiguous_source);
			if (executed) {
				runtime.RecordJitRuntimePath(
				    "hash_join_probe.perfect_probe.direct_aggregate_consumer.derived_build_index", join_output.size());
				if (used_contiguous_source) {
					runtime.RecordJitRuntimePath(
					    "hash_join_probe.perfect_probe.direct_aggregate_consumer.derived_build_index.contiguous_source",
					    join_output.size());
				}
			}
		} else {
			SljitPerfectHashMaterializedBuildIndex build_index(scratch.HashJoinBuildSelection(hash_join_idx));
			executed = execute_consumer(build_index);
		}
		RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind,
		                              "perfect_direct_aggregate_consumer_accumulate", accumulate_start);
		if (!executed) {
			result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.perfect_consumer_dispatch";
			return result;
		}
		result.matched_count = join_output.size();
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.direct_aggregate_consumer", result.matched_count);
		RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
		                                            "join_input_perfect_hash_probe_consumer_complementary_sum",
		                                            result.matched_count);
		result.status = SljitHashJoinAggregateConsumerStatus::EXECUTED;
		return result;
	}

	auto &layout = probe.table_layout;
	SljitHashJoinProbeDrainState state;
	auto prepared_input = SljitPrepareRegularHashJoinProbeInput(
	    runtime, hash_join_idx, hash_join_op.kind, plan, layout, probe_input, scratch.FilterSelection(hash_join_idx),
	    scratch.HashJoinRowPointers(hash_join_idx), scratch.HashJoinSources(hash_join_idx), state, table_layout_kind,
	    probe_primitive.source_key0_int64_to_int32_unchecked, rhs_keys_all_valid, probe.use_bloom_filter);
	if (prepared_input.input_kind == SljitHashJoinProbeInputKind::GENERIC) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.generic_probe_input";
		return result;
	}
	auto &native_input = prepared_input.native_input;
	auto probe_start = SljitRegionStageStart(runtime);
	const bool selected = prepared_input.input_kind == SljitHashJoinProbeInputKind::SELECTED_ALL_VALID;
	bool used_unchecked_narrowing = false;
	const bool executed =
	    selected
	        ? SljitTryExecuteComplementarySumProbeConsumer<true>(
	              runtime, hash_join_idx, hash_join_op.kind, plan, native_input, strategy,
	              probe_input.data[complementary_plan.join_input_group_column_idx], prepared_aggregate.group_source,
	              complementary_plan.predicate_field, complementary_plan.classification, flush_accumulator,
	              result.matched_count, used_unchecked_narrowing)
	        : SljitTryExecuteComplementarySumProbeConsumer<false>(
	              runtime, hash_join_idx, hash_join_op.kind, plan, native_input, strategy,
	              probe_input.data[complementary_plan.join_input_group_column_idx], prepared_aggregate.group_source,
	              complementary_plan.predicate_field, complementary_plan.classification, flush_accumulator,
	              result.matched_count, used_unchecked_narrowing);
	if (!executed) {
		result.blocker = "hash_join_probe.direct_aggregate_consumer_miss.consumer_dispatch";
		return result;
	}
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
	runtime.RecordHashJoinProbeLayout(SljitHashJoinProbeLayoutName(probe.layout_kind));
	RecordSljitRegionStageRuntimePath(
	    runtime, hash_join_idx, hash_join_op.kind,
	    selected
	        ? (used_unchecked_narrowing
	               ? "regular_probe.all_valid.selected.single_key.unchecked_int64_to_int32.no_chain.direct_aggregate_"
	                 "consumer"
	               : "regular_probe.all_valid.selected.single_key.no_chain.direct_aggregate_consumer")
	        : (used_unchecked_narrowing ? "regular_probe.all_valid.flat.single_key.unchecked_int64_to_int32.no_chain."
	                                      "direct_aggregate_consumer"
	                                    : "regular_probe.all_valid.flat.single_key.no_chain.direct_aggregate_consumer"),
	    probe_start);
	RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
	                                            "join_input_probe_consumer_complementary_sum", result.matched_count);
	RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
	                                            "join_input_row_pointer_preaggregated_complementary_sum_update",
	                                            result.matched_count);
	result.status = SljitHashJoinAggregateConsumerStatus::EXECUTED;
	return result;
}

} // namespace duckdb
