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
		aggregate_state_is_sets.assign(payload_count, nullptr);
		aggregate_row_counts.assign(payload_count, nullptr);
		constants.assign(payload_count, 0);
	}

	void PrepareExpressionSources(idx_t source_count) {
		payload_sources.Resize(source_count);
	}

	void PrepareFiltered(idx_t source_count, idx_t payload_count) {
		PrepareExpressionSources(source_count);
		aggregate_int64_values.assign(payload_count, nullptr);
		aggregate_hugeint_values.assign(payload_count, nullptr);
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
	vector<int64_t *> aggregate_int64_values;
	vector<hugeint_t *> aggregate_hugeint_values;
	vector<bool *> aggregate_state_is_sets;
	vector<idx_t *> aggregate_row_counts;
	vector<int64_t> constants;
};

struct SljitProjectionAdapterScratch {
	struct PreparedInput {
		idx_t input_index = DConstants::INVALID_INDEX;
		const_data_ptr_t data = nullptr;
	};

	void Prepare(idx_t projection_count, bool track_fused) {
		sources.Resize(projection_count);
		right_sources.Resize(projection_count);
		result_data.assign(projection_count, nullptr);
		overflow_messages.assign(projection_count, nullptr);
		integer_constants.assign(projection_count, 0);
		float_constants.assign(projection_count, 0);
		double_constants.assign(projection_count, 0);
		collect_floating_stats = false;
		if (track_fused) {
			fused.assign(projection_count, 0);
		} else {
			fused.clear();
		}
		prepared_inputs.clear();
	}

	void PrepareFloatingStats(idx_t projection_count, bool single_precision) {
		collect_floating_stats = true;
		if (single_precision) {
			float_stats_min.assign(projection_count, 0);
			float_stats_max.assign(projection_count, 0);
			double_stats_min.clear();
			double_stats_max.clear();
		} else {
			double_stats_min.assign(projection_count, 0);
			double_stats_max.assign(projection_count, 0);
			float_stats_min.clear();
			float_stats_max.clear();
		}
		direct_append_stats.assign(projection_count, DirectAppendColumnStats());
	}

	void FinishFloatingStats(const vector<SljitExecutableRegionExpression> &projections, bool single_precision) {
		if (!collect_floating_stats) {
			direct_append_stats.clear();
			return;
		}
		direct_append_stats.resize(projections.size());
		for (idx_t projection_idx = 0; projection_idx < projections.size(); projection_idx++) {
			auto &stats = direct_append_stats[projection_idx];
			stats.has_stats = true;
			if (single_precision) {
				stats.physical_type = PhysicalType::FLOAT;
				stats.float_min = float_stats_min[projection_idx];
				stats.float_max = float_stats_max[projection_idx];
			} else {
				stats.physical_type = PhysicalType::DOUBLE;
				stats.double_min = double_stats_min[projection_idx];
				stats.double_max = double_stats_max[projection_idx];
			}
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

	bool TryGetPreparedInput(idx_t input_index, const_data_ptr_t &source_data) const {
		for (auto &prepared_input : prepared_inputs) {
			if (prepared_input.input_index == input_index) {
				source_data = prepared_input.data;
				return true;
			}
		}
		return false;
	}

	void AddPreparedInput(idx_t input_index, const_data_ptr_t source_data) {
		PreparedInput prepared_input;
		prepared_input.input_index = input_index;
		prepared_input.data = source_data;
		prepared_inputs.push_back(prepared_input);
	}

	SljitSourceVectorScratch sources;
	SljitSourceVectorScratch right_sources;
	vector<data_ptr_t> result_data;
	vector<const char *> overflow_messages;
	vector<int64_t> integer_constants;
	vector<float> float_constants;
	vector<double> double_constants;
	vector<float> float_stats_min;
	vector<float> float_stats_max;
	vector<double> double_stats_min;
	vector<double> double_stats_max;
	vector<DirectAppendColumnStats> direct_append_stats;
	vector<uint8_t> fused;
	vector<PreparedInput> prepared_inputs;
	bool collect_floating_stats = false;
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
			if (plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE) {
				sources.PrepareIntegerSource(input, input_index, source_idx, SljitNativeIntegerKind::DECIMAL64,
				                             execute_sel, count, "SLJIT expression-tree source is out of range",
				                             known_not_null);
			} else {
				sources.PrepareTypedExpressionSource(input, input_index, source_idx, execute_sel, count,
				                                     "SLJIT expression-tree source is out of range", known_not_null);
			}
		}
		source_can_have_null = sources.SourceCanHaveNull();
		native_input.source_data_array = sources.DataArray();
		native_input.source_sel_array = sources.SelectionArray();
		native_input.source_validity_array = sources.ValidityArray();
		native_input.expression_tree_flat_no_selection = sources.FlatNoSelection(execute_sel);
		native_input.expression_tree_flat_all_valid = sources.FlatAllValid(execute_sel);
		native_input.expression_tree_all_valid = sources.AllValid();
	}

	SljitNativePredicateSourceAdapter predicate_sources;
	SljitSourceVectorScratch sources;
	bool source_can_have_null = false;
};

} // namespace duckdb
