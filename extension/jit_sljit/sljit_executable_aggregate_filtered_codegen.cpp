//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_filtered_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_aggregate_codegen.hpp"

#include "sljit_executable_aggregate_payload_sources.hpp"
#include "sljit_native_codegen.hpp"
#include "sljit_native_util.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

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
		payload.plan.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
		local_sources = payload.input_source_indices;
		return true;
	}
	if (payload.plan.kind != SljitNativeRegionExpressionKind::REFERENCE ||
	    payload.plan.source_index >= payload.input_source_indices.size()) {
		return false;
	}
	const auto source_column = payload.InputColumn(payload.plan.source_index);
	payload.plan.expression_tree = SljitPayloadReferenceExpressionTree(payload.plan);
	payload.plan.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
	local_sources.clear();
	local_sources.push_back(source_column);
	payload.plan.expression_tree_source_indices = local_sources;
	payload.input_source_indices = local_sources;
	return true;
}

static bool NormalizeSljitFilteredPerfectHashAggregatePayloadExpression(SljitExecutableRegionExpression &payload,
                                                                        vector<idx_t> &local_sources) {
	if (payload.plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE && payload.plan.expression_tree) {
		local_sources = payload.input_source_indices;
		return true;
	}
	if (payload.plan.kind != SljitNativeRegionExpressionKind::REFERENCE ||
	    payload.plan.source_index >= payload.input_source_indices.size()) {
		return false;
	}
	const auto source_index = payload.InputColumn(payload.plan.source_index);
	payload.plan.expression_tree = SljitPayloadReferenceExpressionTree(payload.plan);
	payload.plan.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
	local_sources.clear();
	local_sources.push_back(source_index);
	payload.plan.expression_tree_source_indices = local_sources;
	payload.input_source_indices = local_sources;
	return true;
}

//! Compiles the kernel that adds the payloads of each row `filter` accepts to an ungrouped aggregate's states, over the
//! input sources the filter and the payloads read. `kernel` holds no code for a shape the kernel does not compile.
static bool SljitTryBuildUngroupedFilteredAggregateKernel(const SljitExecutableRegionExpression &filter,
                                                          const SljitExecutableAggregateUpdate &aggregate_update,
                                                          const vector<bool> &input_not_null,
                                                          SljitExecutableFilteredAggregateUpdate &kernel,
                                                          string &error) {
	if (!aggregate_update.plan.UsesPrimitivePayloads() || aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.sink_info.Kind() != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    aggregate_update.payloads.empty() ||
	    aggregate_update.payloads.size() != aggregate_update.plan.sink_info.aggregates.size() ||
	    !filter.plan.expression_tree) {
		return true;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateKindCanGenerate(primitive_kind)) {
			return true;
		}
	}

	kernel.filter.plan = filter.plan.Copy(true, false);
	kernel.payloads.reserve(aggregate_update.payloads.size());
	for (auto &payload : aggregate_update.payloads) {
		SljitExecutableRegionExpression filtered_payload;
		filtered_payload.plan = payload.plan.Copy(true, false);
		filtered_payload.input_source_indices = payload.input_source_indices;
		filtered_payload.input_source_not_null = payload.input_source_not_null;
		kernel.payloads.push_back(std::move(filtered_payload));
	}

	vector<idx_t> combined_sources;
	vector<bool> combined_source_not_null;
	auto &filter_sources =
	    filter.input_source_indices.empty() ? filter.plan.expression_tree_source_indices : filter.input_source_indices;
	RemapSljitExpressionTreeToCombinedInputs(*kernel.filter.plan.expression_tree, filter_sources, combined_sources,
	                                         &combined_source_not_null, &input_not_null);
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateUsesPayloadExpression(primitive_kind)) {
			continue;
		}
		vector<idx_t> payload_sources;
		if (!NormalizeSljitFilteredAggregatePayloadExpression(kernel.payloads[payload_idx], payload_sources)) {
			return true;
		}
		RemapSljitExpressionTreeToCombinedInputs(*kernel.payloads[payload_idx].plan.expression_tree, payload_sources,
		                                         combined_sources, &combined_source_not_null, &input_not_null);
	}
	kernel.input_source_indices = combined_sources;
	kernel.input_source_not_null = combined_source_not_null;
	kernel.filter.input_source_indices = combined_sources;
	kernel.filter.input_source_not_null = combined_source_not_null;
	kernel.filter.plan.expression_tree_source_indices = combined_sources;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	codegen_payloads.reserve(kernel.payloads.size());
	for (auto &payload : kernel.payloads) {
		payload.input_source_indices = combined_sources;
		payload.input_source_not_null = combined_source_not_null;
		payload.plan.expression_tree_source_indices = combined_sources;
		codegen_payloads.push_back(payload.plan.Copy(true, false));
	}

	{
		vector<reference<const ExecutionExpressionIR>> expressions {*kernel.filter.plan.expression_tree};
		for (auto &payload : codegen_payloads) {
			if (payload.expression_tree) {
				expressions.push_back(*payload.expression_tree);
			}
		}
		for (auto &compare : CollectSljitTypedExpressionTreeStringCompares(expressions)) {
			SljitStringCompareMatches matches;
			TryReadSljitTypedExpressionTreeStringCompareConstant(compare.get(), matches.source, matches.constant,
			                                                     matches.compare_equal);
			kernel.string_compares.compares.push_back(std::move(matches));
		}
		kernel.string_compares.compare_only_sources.assign(combined_sources.size(), false);
		vector<reference<const ExecutionExpressionIR>> payload_expressions(expressions.begin() + 1, expressions.end());
		for (auto source_index : CollectSljitTypedExpressionTreeStringCompareOnlySources(
		         *kernel.filter.plan.expression_tree, payload_expressions)) {
			kernel.string_compares.compare_only_sources[source_index] = true;
		}
	}
	SljitNativeAggregateUpdateFunction function = nullptr;
	string filtered_error;
	auto code = BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
	    *kernel.filter.plan.expression_tree, codegen_payloads, aggregate_update.plan.sink_info.aggregates, function,
	    kernel.string_compares.fast_null_rows, filtered_error);
	auto compiled = SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(code), function);
	if (compiled.IsExecutable()) {
		kernel.compiled = std::move(compiled);
		kernel.kind = SljitFilteredAggregateKernelKind::UNGROUPED_PAYLOAD;
		kernel.payload_source_layout = SljitAggregatePayloadSourceLayout::FUSED_COMBINED;
		return true;
	}
	if (!filtered_error.empty() && filtered_error.rfind("unsupported", 0) != 0) {
		error = filtered_error;
		return false;
	}
	return true;
}

bool SljitTryBuildFilteredAggregateUpdate(SljitExecutableRegionOp &filter_op, SljitExecutableRegionOp &aggregate_op,
                                          string &error, const vector<bool> &input_not_null,
                                          const vector<Value> &input_min_values,
                                          const vector<Value> &input_max_values) {
	if (filter_op.Kind() != SljitNativeRegionOpKind::FILTER ||
	    aggregate_op.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return true;
	}
	D_ASSERT(filter_op.Filter());
	auto &filter = filter_op.Filter()->expression;
	auto &aggregate_update = aggregate_op.AggregateUpdate();
	if (aggregate_update.filtered_update.IsExecutable() || !aggregate_update.plan.UsesPrimitivePayloads() ||
	    aggregate_update.payloads.empty() ||
	    aggregate_update.payloads.size() != aggregate_update.plan.sink_info.aggregates.size()) {
		return true;
	}
	if (!filter.plan.expression_tree) {
		return true;
	}

	if (aggregate_update.plan.use_perfect_hash_group_lookup &&
	    aggregate_update.plan.sink_info.Kind() == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
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
		vector<SljitNativeRegionExpressionPlan> codegen_group_expressions;
		if (!SljitRemapGroupExpressionsToCombinedInputs(
		        aggregate_update.plan.group_expressions, combined_sources, codegen_group_expressions,
		        &combined_source_not_null, &input_not_null, &combined_source_min_values, &combined_source_max_values,
		        &input_min_values, &input_max_values)) {
			return true;
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
		for (auto &group_expression : codegen_group_expressions) {
			if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
				group_expression.expression_tree_source_indices = combined_sources;
			}
		}

		SljitNativeAggregateUpdateFunction function = nullptr;
		SljitPerfectHashGroupWitness group_witness;
		string filtered_error;
		auto code = BuildSljitNativeFilteredPerfectHashGroupedFusedAggregateUpdate(
		    *filtered_update.filter.plan.expression_tree, codegen_payloads, aggregate_update.plan.sink_info.aggregates,
		    aggregate_update.plan.sink_info.groups, codegen_group_expressions, aggregate_update.plan.sink_info.contract,
		    combined_source_not_null, combined_source_min_values, combined_source_max_values, function, group_witness,
		    filtered_error);
		auto compiled = SljitCompiledFunction<SljitNativeAggregateUpdateFunction>::TryCreate(std::move(code), function);
		if (compiled.IsExecutable()) {
			filtered_update.compiled = std::move(compiled);
			filtered_update.group_witness = group_witness;
			filtered_update.kind = SljitFilteredAggregateKernelKind::PERFECT_HASH_GROUPED;
			filtered_update.payload_source_layout = SljitAggregatePayloadSourceLayout::FUSED_COMBINED;
			aggregate_update.filtered_update = std::move(filtered_update);
			return true;
		}
		if (!filtered_error.empty() && filtered_error.rfind("unsupported", 0) != 0) {
			error = filtered_error;
			return false;
		}
	}

	SljitExecutableFilteredAggregateUpdate filtered_update;
	if (!SljitTryBuildUngroupedFilteredAggregateKernel(filter, aggregate_update, input_not_null, filtered_update,
	                                                   error)) {
		return false;
	}
	if (filtered_update.IsExecutable()) {
		aggregate_update.filtered_update = std::move(filtered_update);
	}
	return true;
}

bool SljitTryBuildUngroupedAggregateKernel(SljitExecutableAggregateUpdate &aggregate_update, string &error,
                                           const vector<bool> &input_not_null) {
	// the kernel of a filter every row passes
	SljitExecutableRegionExpression every_row;
	every_row.plan.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
	every_row.plan.return_type = LogicalType::BOOLEAN;
	every_row.plan.expression_tree = make_uniq<ExecutionExpressionIR>();
	auto &constant = *every_row.plan.expression_tree;
	constant.kind = ExecutionExpressionIRKind::CONSTANT;
	constant.return_type = LogicalType::BOOLEAN;
	constant.physical_type = PhysicalType::BOOL;
	constant.validity = ExecutionExpressionValidityKind::CONSTANT_VALID;
	constant.source = ExecutionExpressionSourceKind::CONSTANT;
	constant.exception_behavior = ExecutionExpressionExceptionKind::NONE;
	constant.constant = Value::BOOLEAN(true);

	SljitExecutableFilteredAggregateUpdate kernel;
	if (!SljitTryBuildUngroupedFilteredAggregateKernel(every_row, aggregate_update, input_not_null, kernel, error)) {
		return false;
	}
	if (!kernel.IsExecutable()) {
		return true;
	}
	aggregate_update.payloads = std::move(kernel.payloads);
	aggregate_update.fused_payload_update = std::move(kernel.compiled);
	aggregate_update.payload_source_layout = SljitAggregatePayloadSourceLayout::FUSED_COMBINED;
	aggregate_update.combined_payload_source_indices = std::move(kernel.input_source_indices);
	aggregate_update.combined_payload_source_not_null = std::move(kernel.input_source_not_null);
	aggregate_update.combined_payload_string_compares = std::move(kernel.string_compares);
	return true;
}

} // namespace duckdb
