//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_filtered_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_aggregate_codegen.hpp"

#include "sljit_executable_aggregate_payload_sources.hpp"
#include "sljit_expression_selectivity.hpp"
#include "sljit_native_codegen.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

static constexpr idx_t SLJIT_FILTERED_PERFECT_HASH_MAX_SELECTIVITY_BASIS_POINTS = 7500;

static unique_ptr<ExecutionExpressionIR>
SljitPayloadReferenceExpressionTree(const SljitNativeRegionExpressionPlan &plan) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::REFERENCE;
	result->return_type = plan.return_type;
	result->physical_type = plan.return_type.InternalType();
	result->validity = ExecutionExpressionValidityKind::SOURCE;
	result->source = ExecutionExpressionSourceKind::VECTOR;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->ref_index = 0;
	return result;
}

static bool NormalizeSljitFilteredAggregatePayloadExpression(SljitExecutableRegionExpression &payload,
                                                             vector<idx_t> &local_sources) {
	if (payload.plan.expression_tree) {
		local_sources = payload.input_source_indices.empty() ? payload.plan.expression_tree_source_indices
		                                                     : payload.input_source_indices;
		return true;
	}
	if (payload.plan.kind != SljitNativeRegionExpressionKind::REFERENCE ||
	    payload.plan.source_index == DConstants::INVALID_INDEX) {
		return false;
	}
	payload.plan.expression_tree = SljitPayloadReferenceExpressionTree(payload.plan);
	local_sources.clear();
	local_sources.push_back(payload.plan.source_index);
	payload.plan.expression_tree_source_indices = local_sources;
	payload.input_source_indices = local_sources;
	return true;
}

static bool NormalizeSljitFilteredPerfectHashAggregatePayloadExpression(SljitExecutableRegionExpression &payload,
                                                                        vector<idx_t> &local_sources) {
	if (payload.plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE && payload.plan.expression_tree) {
		local_sources = payload.input_source_indices.empty() ? payload.plan.expression_tree_source_indices
		                                                     : payload.input_source_indices;
		return true;
	}
	if (payload.plan.kind != SljitNativeRegionExpressionKind::REFERENCE ||
	    payload.plan.source_index == DConstants::INVALID_INDEX) {
		return false;
	}
	const auto source_index = payload.input_source_indices.empty()
	                              ? payload.plan.source_index
	                              : (payload.plan.source_index < payload.input_source_indices.size()
	                                     ? payload.input_source_indices[payload.plan.source_index]
	                                     : DConstants::INVALID_INDEX);
	if (source_index == DConstants::INVALID_INDEX) {
		return false;
	}
	payload.plan.expression_tree = SljitPayloadReferenceExpressionTree(payload.plan);
	payload.plan.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
	local_sources.clear();
	local_sources.push_back(source_index);
	payload.plan.expression_tree_source_indices = local_sources;
	payload.input_source_indices = local_sources;
	return true;
}

bool SljitTryBuildFilteredAggregateUpdate(SljitExecutableRegionOp &filter_op, SljitExecutableRegionOp &aggregate_op,
                                          string &error, const vector<bool> &input_not_null,
                                          const vector<Value> &input_min_values,
                                          const vector<Value> &input_max_values) {
	if (filter_op.kind != SljitNativeRegionOpKind::FILTER ||
	    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return true;
	}
	D_ASSERT(filter_op.filter);
	auto &filter = filter_op.filter->expression;
	auto &aggregate_update = aggregate_op.aggregate_update;
	if (aggregate_update.filtered_update.IsExecutable() || !aggregate_update.plan.UsesPrimitivePayloads() ||
	    aggregate_update.payloads.empty() ||
	    aggregate_update.payloads.size() != aggregate_update.plan.sink_info.aggregates.size()) {
		return true;
	}
	if (!filter.plan.expression_tree) {
		return true;
	}

	if (aggregate_update.plan.use_perfect_hash_group_lookup &&
	    aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		SljitExecutableFilteredAggregateUpdate filtered_update;
		filtered_update.filter.plan = filter.plan.Copy(true, false);
		filtered_update.payloads.reserve(aggregate_update.payloads.size());
		for (auto &payload : aggregate_update.payloads) {
			SljitExecutableRegionExpression filtered_payload;
			filtered_payload.plan = payload.plan.Copy(true, false);
			filtered_payload.input_source_indices = payload.input_source_indices;
			filtered_payload.input_source_not_null = payload.input_source_not_null;
			filtered_update.payloads.push_back(std::move(filtered_payload));
		}
		if (!filtered_update.filter.plan.expression_tree) {
			return true;
		}

		vector<idx_t> combined_sources;
		vector<bool> combined_source_not_null;
		vector<Value> combined_source_min_values;
		vector<Value> combined_source_max_values;
		auto &filter_sources = filter.input_source_indices.empty() ? filter.plan.expression_tree_source_indices
		                                                           : filter.input_source_indices;
		RemapSljitExpressionTreeToCombinedInputs(*filtered_update.filter.plan.expression_tree, filter_sources,
		                                         combined_sources, &combined_source_not_null, &input_not_null,
		                                         &combined_source_min_values, &combined_source_max_values,
		                                         &input_min_values, &input_max_values);
		if (SljitEstimateExpressionSelectivityBasisPoints(*filtered_update.filter.plan.expression_tree,
		                                                  combined_source_min_values, combined_source_max_values) >
		    SLJIT_FILTERED_PERFECT_HASH_MAX_SELECTIVITY_BASIS_POINTS) {
			return true;
		}

		for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
			auto &aggregate = aggregate_update.plan.sink_info.aggregates[payload_idx];
			auto &payload = filtered_update.payloads[payload_idx];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			vector<idx_t> local_sources;
			if (!NormalizeSljitFilteredPerfectHashAggregatePayloadExpression(payload, local_sources)) {
				return true;
			}
			if (local_sources.empty()) {
				return true;
			}
			RemapSljitExpressionTreeToCombinedInputs(*payload.plan.expression_tree, local_sources, combined_sources,
			                                         &combined_source_not_null, &input_not_null,
			                                         &combined_source_min_values, &combined_source_max_values,
			                                         &input_min_values, &input_max_values);
		}

		filtered_update.input_source_indices = combined_sources;
		filtered_update.input_source_not_null = combined_source_not_null;
		filtered_update.filter.input_source_indices = combined_sources;
		filtered_update.filter.input_source_not_null = combined_source_not_null;
		filtered_update.filter.plan.expression_tree_source_indices = combined_sources;
		vector<SljitNativeRegionExpressionPlan> codegen_payloads;
		codegen_payloads.reserve(filtered_update.payloads.size());
		for (auto &payload : filtered_update.payloads) {
			payload.input_source_indices = combined_sources;
			payload.input_source_not_null = combined_source_not_null;
			payload.plan.expression_tree_source_indices = combined_sources;
			codegen_payloads.push_back(payload.plan.Copy(true, false));
		}

		SljitNativeAggregateUpdateFunction function = nullptr;
		string filtered_error;
		auto code = BuildSljitNativeFilteredPerfectHashGroupedFusedTypedExpressionAggregateUpdate(
		    *filtered_update.filter.plan.expression_tree, codegen_payloads, aggregate_update.plan.sink_info.aggregates,
		    aggregate_update.plan.sink_info.groups, aggregate_update.plan.group_expressions,
		    aggregate_update.plan.sink_info.aggregate_contract, combined_source_not_null, combined_source_min_values,
		    combined_source_max_values, function, filtered_error);
		auto compiled = SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(code), function);
		if (compiled.IsExecutable()) {
			filtered_update.compiled = std::move(compiled);
			filtered_update.kind = SljitFilteredAggregateKernelKind::PERFECT_HASH_GROUPED;
			aggregate_update.filtered_update = std::move(filtered_update);
			return true;
		}
		if (!filtered_error.empty() && filtered_error.rfind("unsupported", 0) != 0) {
			error = filtered_error;
			return false;
		}
	}

	if (aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return true;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateKindCanGenerate(primitive_kind)) {
			return true;
		}
	}

	SljitExecutableFilteredAggregateUpdate filtered_update;
	filtered_update.filter.plan = filter.plan.Copy(true, false);
	filtered_update.payloads.reserve(aggregate_update.payloads.size());
	for (auto &payload : aggregate_update.payloads) {
		SljitExecutableRegionExpression filtered_payload;
		filtered_payload.plan = payload.plan.Copy(true, false);
		filtered_update.payloads.push_back(std::move(filtered_payload));
	}
	if (!filtered_update.filter.plan.expression_tree) {
		return true;
	}

	vector<idx_t> combined_sources;
	vector<bool> combined_source_not_null;
	auto &filter_sources =
	    filter.input_source_indices.empty() ? filter.plan.expression_tree_source_indices : filter.input_source_indices;
	RemapSljitExpressionTreeToCombinedInputs(*filtered_update.filter.plan.expression_tree, filter_sources,
	                                         combined_sources, &combined_source_not_null, &input_not_null);
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateUsesPayloadExpression(primitive_kind)) {
			continue;
		}
		vector<idx_t> payload_sources;
		if (!NormalizeSljitFilteredAggregatePayloadExpression(filtered_update.payloads[payload_idx], payload_sources)) {
			return true;
		}
		RemapSljitExpressionTreeToCombinedInputs(*filtered_update.payloads[payload_idx].plan.expression_tree,
		                                         payload_sources, combined_sources, &combined_source_not_null,
		                                         &input_not_null);
	}
	filtered_update.input_source_indices = combined_sources;
	filtered_update.input_source_not_null = combined_source_not_null;
	filtered_update.filter.input_source_indices = combined_sources;
	filtered_update.filter.input_source_not_null = combined_source_not_null;
	filtered_update.filter.plan.expression_tree_source_indices = combined_sources;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	codegen_payloads.reserve(filtered_update.payloads.size());
	for (auto &payload : filtered_update.payloads) {
		payload.input_source_indices = combined_sources;
		payload.input_source_not_null = combined_source_not_null;
		payload.plan.expression_tree_source_indices = combined_sources;
		codegen_payloads.push_back(payload.plan.Copy(true, false));
	}

	SljitNativeAggregateUpdateFunction function = nullptr;
	string filtered_error;
	auto code = BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
	    *filtered_update.filter.plan.expression_tree, codegen_payloads, aggregate_update.plan.sink_info.aggregates,
	    function, filtered_error);
	auto compiled = SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(code), function);
	if (compiled.IsExecutable()) {
		filtered_update.compiled = std::move(compiled);
		filtered_update.kind = SljitFilteredAggregateKernelKind::UNGROUPED_PAYLOAD;
		aggregate_update.filtered_update = std::move(filtered_update);
		return true;
	}
	if (!filtered_error.empty() && filtered_error.rfind("unsupported", 0) != 0) {
		error = filtered_error;
		return false;
	}
	return true;
}

} // namespace duckdb
