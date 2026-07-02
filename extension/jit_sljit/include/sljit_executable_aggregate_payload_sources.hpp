//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_payload_sources.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

static idx_t FindSljitInputSource(const vector<idx_t> &input_sources, idx_t input_source_index) {
	for (idx_t source_idx = 0; source_idx < input_sources.size(); source_idx++) {
		if (input_sources[source_idx] == input_source_index) {
			return source_idx;
		}
	}
	return DConstants::INVALID_INDEX;
}

static idx_t AddSljitCombinedInputSource(idx_t source_index, vector<idx_t> &combined_sources,
                                         vector<bool> *combined_source_not_null = nullptr,
                                         const vector<bool> *input_not_null = nullptr,
                                         vector<Value> *combined_source_min_values = nullptr,
                                         vector<Value> *combined_source_max_values = nullptr,
                                         const vector<Value> *input_min_values = nullptr,
                                         const vector<Value> *input_max_values = nullptr) {
	auto existing_idx = FindSljitInputSource(combined_sources, source_index);
	if (existing_idx != DConstants::INVALID_INDEX) {
		return existing_idx;
	}
	auto combined_idx = combined_sources.size();
	combined_sources.push_back(source_index);
	if (combined_source_not_null) {
		combined_source_not_null->push_back(
		    input_not_null && source_index < input_not_null->size() ? (*input_not_null)[source_index] : false);
	}
	if (combined_source_min_values) {
		combined_source_min_values->push_back(
		    input_min_values && source_index < input_min_values->size() ? (*input_min_values)[source_index] : Value());
	}
	if (combined_source_max_values) {
		combined_source_max_values->push_back(
		    input_max_values && source_index < input_max_values->size() ? (*input_max_values)[source_index] : Value());
	}
	return combined_idx;
}

static void RemapSljitExpressionTreeToCombinedInputs(
    ExecutionExpressionIR &node, const vector<idx_t> &local_sources, vector<idx_t> &combined_sources,
    vector<bool> *combined_source_not_null = nullptr, const vector<bool> *input_not_null = nullptr,
    vector<Value> *combined_source_min_values = nullptr, vector<Value> *combined_source_max_values = nullptr,
    const vector<Value> *input_min_values = nullptr, const vector<Value> *input_max_values = nullptr) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		if (node.ref_index >= local_sources.size()) {
			throw InternalException("SLJIT expression-tree reference source is out of range");
		}
		node.ref_index = AddSljitCombinedInputSource(
		    local_sources[node.ref_index], combined_sources, combined_source_not_null, input_not_null,
		    combined_source_min_values, combined_source_max_values, input_min_values, input_max_values);
		return;
	}
	if (node.left) {
		RemapSljitExpressionTreeToCombinedInputs(*node.left, local_sources, combined_sources, combined_source_not_null,
		                                         input_not_null, combined_source_min_values, combined_source_max_values,
		                                         input_min_values, input_max_values);
	}
	if (node.right) {
		RemapSljitExpressionTreeToCombinedInputs(*node.right, local_sources, combined_sources, combined_source_not_null,
		                                         input_not_null, combined_source_min_values, combined_source_max_values,
		                                         input_min_values, input_max_values);
	}
	if (node.else_node) {
		RemapSljitExpressionTreeToCombinedInputs(*node.else_node, local_sources, combined_sources,
		                                         combined_source_not_null, input_not_null, combined_source_min_values,
		                                         combined_source_max_values, input_min_values, input_max_values);
	}
	for (auto &child : node.children) {
		if (child) {
			RemapSljitExpressionTreeToCombinedInputs(*child, local_sources, combined_sources, combined_source_not_null,
			                                         input_not_null, combined_source_min_values,
			                                         combined_source_max_values, input_min_values, input_max_values);
		}
	}
}

} // namespace duckdb
