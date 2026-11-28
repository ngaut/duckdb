//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_adapter_scratch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_preaggregated_primitive_scratch.hpp"

namespace duckdb {

struct SljitPerfectHashDictionaryGroupCache {
	string dictionary_id;
	idx_t dictionary_size = 0;
	vector<idx_t> contributions;
	vector<sel_t> active_indices;
	idx_t active_count = 0;
	bool disabled = false;
};

struct SljitAggregatePayloadAdapterScratch {
	void PrepareUngrouped(idx_t payload_count) {
		payload_sources.Resize(payload_count);
		right_payload_sources.Resize(payload_count);
		aggregate_int64_values.assign(payload_count, nullptr);
		aggregate_hugeint_values.assign(payload_count, nullptr);
		aggregate_double_values.assign(payload_count, nullptr);
		aggregate_state_is_sets.assign(payload_count, nullptr);
		aggregate_row_counts.assign(payload_count, nullptr);
		constants.assign(payload_count, 0);
	}

	void PrepareExpressionSources(idx_t source_count) {
		payload_sources.Resize(source_count);
	}

	void PrepareFiltered(idx_t source_count, idx_t payload_count) {
		PrepareExpressionSources(source_count);
		string_matches.clear();
		aggregate_int64_values.assign(payload_count, nullptr);
		aggregate_hugeint_values.assign(payload_count, nullptr);
		aggregate_double_values.assign(payload_count, nullptr);
		aggregate_state_is_sets.assign(payload_count, nullptr);
		aggregate_row_counts.assign(payload_count, nullptr);
	}

	void PrepareGrouped(idx_t payload_count) {
		payload_sources.Resize(payload_count);
	}

	void PreparePerfectHash(idx_t payload_count, idx_t group_count) {
		PrepareGrouped(payload_count);
		group_sources.Resize(group_count);
		perfect_hash_dictionary_group_caches.resize(group_count);
		perfect_hash_dictionary_groups.assign(group_count, SljitPerfectHashDictionaryGroupRuntime());
	}

	DataChunk &PrepareInputVectorGroups(Allocator &allocator,
	                                    const vector<ExecutionRowPointerGroupKeySource> &sources) {
		bool reuse_groups = input_vector_group_types.size() == sources.size();
		if (reuse_groups) {
			for (idx_t source_idx = 0; source_idx < sources.size(); source_idx++) {
				if (input_vector_group_types[source_idx] != sources[source_idx].target_type) {
					reuse_groups = false;
					break;
				}
			}
		}
		if (!reuse_groups) {
			input_vector_group_types.clear();
			input_vector_group_types.reserve(sources.size());
			for (auto &source : sources) {
				input_vector_group_types.push_back(source.target_type);
			}
			if (!input_vector_groups) {
				input_vector_groups = make_uniq<DataChunk>();
			}
			input_vector_groups->Destroy();
			input_vector_groups->Initialize(allocator, input_vector_group_types);
		} else {
			D_ASSERT(input_vector_groups);
			input_vector_groups->Reset();
		}
		return *input_vector_groups;
	}

	SljitSourceVectorScratch payload_sources;
	SljitSourceVectorScratch right_payload_sources;
	SljitSourceVectorScratch group_sources;
	vector<SljitPerfectHashDictionaryGroupCache> perfect_hash_dictionary_group_caches;
	vector<SljitPerfectHashDictionaryGroupRuntime> perfect_hash_dictionary_groups;
	unique_ptr<DataChunk> input_vector_groups;
	vector<LogicalType> input_vector_group_types;
	//! The group keys of the entries of the storage dictionary dictionary_group_keys_id, and the rows they resolve to
	buffer_ptr<DictionaryEntry> dictionary_group_keys;
	string dictionary_group_keys_id;
	Vector dictionary_group_rows = Vector(LogicalType::POINTER);
	//! The rows of a batch whose dictionary entry had no row yet
	sel_t dictionary_deferred_rows[STANDARD_VECTOR_SIZE];
	vector<int64_t *> aggregate_int64_values;
	vector<hugeint_t *> aggregate_hugeint_values;
	vector<double *> aggregate_double_values;
	vector<bool *> aggregate_state_is_sets;
	vector<idx_t *> aggregate_row_counts;
	vector<int64_t> constants;
	//! per string comparison of a filtered kernel, its dictionary's results and the pointer the kernel reads
	vector<vector<uint8_t>> string_match_storage;
	vector<const uint8_t *> string_matches;
	//! per input source of a filtered kernel, whether the batch prepares it as valid
	vector<bool> string_compared_valid_sources;
};

struct SljitProjectionAdapterScratch {
	void Prepare(idx_t projection_count, bool track_fused) {
		sources.Resize(projection_count);
		right_sources.Resize(projection_count);
		result_data.assign(projection_count, nullptr);
		integer_constants.assign(projection_count, 0);
		if (track_fused) {
			fused.assign(projection_count, 0);
		} else {
			fused.clear();
		}
	}

	void SetSourceData(idx_t projection_idx, bool right_source, const_data_ptr_t source_data) {
		if (right_source) {
			right_sources.SetData(projection_idx, source_data);
		} else {
			sources.SetData(projection_idx, source_data);
		}
	}

	const_data_ptr_t *SourceDataArray() {
		return sources.DataArray();
	}

	const_data_ptr_t *RightSourceDataArray() {
		return right_sources.DataArray();
	}

	SljitSourceVectorScratch sources;
	SljitSourceVectorScratch right_sources;
	vector<data_ptr_t> result_data;
	vector<int64_t> integer_constants;
	vector<uint8_t> fused;
};

struct SljitExpressionAdapterScratch {
	void PrepareExpressionTree(DataChunk &input, const SljitExecutableRegionExpression &expr,
	                           SljitNativeVectorInput &native_input, const SelectionVector *execute_sel, idx_t count) {
		auto &plan = expr.plan;
		auto &source_indices =
		    expr.input_source_indices.empty() ? plan.expression_tree_source_indices : expr.input_source_indices;
		sources.Resize(source_indices.size());
		for (idx_t source_idx = 0; source_idx < source_indices.size(); source_idx++) {
			auto input_index = source_indices[source_idx];
			const auto known_not_null = SljitInputSourceKnownNotNull(expr.input_source_not_null, source_idx);
			sources.PrepareTypedExpressionSource(input, input_index, source_idx, execute_sel, count,
			                                     "SLJIT expression-tree source is out of range", known_not_null);
		}
		source_can_have_null = sources.SourceCanHaveNull();
		native_input.source_data_array = sources.DataArray();
		native_input.source_sel_array = sources.SelectionArray();
		native_input.source_validity_array = sources.ValidityArray();
		native_input.expression_tree_flat_no_selection = sources.FlatNoSelection(execute_sel);
		native_input.expression_tree_flat_all_valid = sources.FlatAllValid(execute_sel);
		native_input.expression_tree_sources_flat_all_valid =
		    sources.FlatAllValid(static_cast<const SelectionVector *>(nullptr));
		native_input.expression_tree_all_valid = sources.AllValid();
	}

	SljitNativePredicateSourceAdapter predicate_sources;
	SljitSourceVectorScratch sources;
	bool source_can_have_null = false;
	//! The expression's results over the entries of the storage dictionary named by dictionary_result_source_id
	buffer_ptr<DictionaryEntry> dictionary_result;
	string dictionary_result_source_id;
};

} // namespace duckdb
