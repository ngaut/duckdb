//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_aggregate_codegen.hpp"

#include "sljit_aggregate_fused_payload_sources.hpp"
#include "sljit_executable_aggregate_payload_sources.hpp"
#include "sljit_native_codegen.hpp"

namespace duckdb {

struct SljitFusedTypedAggregatePayloads {
	vector<SljitExecutableRegionExpression> executable_payloads;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	vector<SljitNativeRegionExpressionPlan> codegen_group_expressions;
	vector<idx_t> combined_sources;
	vector<bool> combined_source_not_null;
	vector<Value> combined_source_min_values;
	vector<Value> combined_source_max_values;
};

static bool TryBuildFusedTypedAggregatePayloads(const SljitNativeAggregateUpdatePlan &op,
                                                SljitFusedTypedAggregatePayloads &payloads,
                                                const vector<bool> *input_not_null = nullptr,
                                                const vector<Value> *input_min_values = nullptr,
                                                const vector<Value> *input_max_values = nullptr,
                                                const vector<SljitNativeRegionExpressionPlan> *group_expressions =
                                                    nullptr) {
	payloads = SljitFusedTypedAggregatePayloads();
	payloads.executable_payloads.reserve(op.payloads.size());
	payloads.codegen_payloads.reserve(op.payloads.size());
	auto combined_source_not_null = input_not_null ? &payloads.combined_source_not_null : nullptr;
	auto combined_source_min_values = input_min_values ? &payloads.combined_source_min_values : nullptr;
	auto combined_source_max_values = input_max_values ? &payloads.combined_source_max_values : nullptr;

	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &aggregate = op.sink_info.aggregates[payload_idx];
		SljitExecutableRegionExpression executable_payload;
		executable_payload.plan = op.payloads[payload_idx].Copy(true, false);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			payloads.executable_payloads.push_back(std::move(executable_payload));
			continue;
		}
		if (executable_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			executable_payload.plan.source_index =
			    AddSljitCombinedInputSource(executable_payload.plan.source_index, payloads.combined_sources,
			                                combined_source_not_null, input_not_null, combined_source_min_values,
			                                combined_source_max_values, input_min_values, input_max_values);
			payloads.executable_payloads.push_back(std::move(executable_payload));
			continue;
		}
		if (executable_payload.plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		    !executable_payload.plan.expression_tree) {
			return false;
		}
		has_typed_payload = true;
		vector<idx_t> local_sources = executable_payload.plan.expression_tree_source_indices;
		if (local_sources.empty()) {
			return false;
		}
		RemapSljitExpressionTreeToCombinedInputs(*executable_payload.plan.expression_tree, local_sources,
		                                         payloads.combined_sources, combined_source_not_null, input_not_null,
		                                         combined_source_min_values, combined_source_max_values,
		                                         input_min_values, input_max_values);
		payloads.executable_payloads.push_back(std::move(executable_payload));
	}
	if (group_expressions) {
		payloads.codegen_group_expressions.reserve(group_expressions->size());
		for (auto &group_expression : *group_expressions) {
			auto codegen_group = group_expression.Copy(true, false);
			if (codegen_group.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
				if (!codegen_group.expression_tree || codegen_group.expression_tree_source_indices.empty()) {
					return false;
				}
				has_typed_payload = true;
				vector<idx_t> local_sources = codegen_group.expression_tree_source_indices;
				RemapSljitExpressionTreeToCombinedInputs(
				    *codegen_group.expression_tree, local_sources, payloads.combined_sources, combined_source_not_null,
				    input_not_null, combined_source_min_values, combined_source_max_values, input_min_values,
				    input_max_values);
			}
			payloads.codegen_group_expressions.push_back(std::move(codegen_group));
		}
	}
	if (!has_typed_payload) {
		return false;
	}

	for (auto &payload : payloads.executable_payloads) {
		payload.input_source_indices = payloads.combined_sources;
		payload.input_source_not_null = payloads.combined_source_not_null;
		payload.plan.expression_tree_source_indices = payloads.combined_sources;
		payloads.codegen_payloads.push_back(payload.plan.Copy(true, false));
	}
	for (auto &group_expression : payloads.codegen_group_expressions) {
		if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			group_expression.expression_tree_source_indices = payloads.combined_sources;
		}
	}
	return true;
}

static bool TryBuildUngroupedFusedTypedExpressionAggregateUpdate(const SljitNativeAggregateUpdatePlan &op,
                                                                 SljitExecutableAggregateUpdate &executable,
                                                                 string &error, const vector<bool> &input_not_null) {
	if (!op.use_primitive_payloads || op.use_grouped_state_addresses ||
	    op.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	SljitFusedTypedAggregatePayloads payloads;
	if (!TryBuildFusedTypedAggregatePayloads(op, payloads, &input_not_null)) {
		return true;
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativeUngroupedFusedTypedExpressionAggregateUpdate(
	    payloads.codegen_payloads, op.sink_info.aggregates, fused_function, fused_error);
	if (fused_code && fused_function) {
		executable.payloads = std::move(payloads.executable_payloads);
		executable.fused_payload_update_code = std::move(fused_code);
		executable.fused_payload_update_function = fused_function;
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static bool TryBuildPerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const SljitNativeAggregateUpdatePlan &op, SljitExecutableAggregateUpdate &executable, string &error,
    const vector<bool> &input_not_null, const vector<Value> &input_min_values, const vector<Value> &input_max_values) {
	if (!op.use_primitive_payloads || !op.use_perfect_hash_group_lookup || op.payloads.empty() ||
	    op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	SljitFusedTypedAggregatePayloads payloads;
	if (!TryBuildFusedTypedAggregatePayloads(op, payloads, &input_not_null, &input_min_values, &input_max_values,
	                                        &op.group_expressions)) {
		return true;
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdate(
	    payloads.codegen_payloads, op.sink_info.aggregates, op.sink_info.groups, payloads.codegen_group_expressions,
	    op.sink_info.aggregate_contract, payloads.combined_source_not_null, payloads.combined_source_min_values,
	    payloads.combined_source_max_values, fused_function, fused_error);
	if (fused_code && fused_function) {
		executable.payloads = std::move(payloads.executable_payloads);
		executable.fused_payload_update_code = std::move(fused_code);
		executable.fused_payload_update_function = fused_function;
		executable.fused_payload_update_owns_group_lookup = true;
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static bool TryBuildGroupedFusedTypedExpressionAggregateUpdate(const SljitNativeAggregateUpdatePlan &op,
                                                               SljitExecutableAggregateUpdate &executable,
                                                               string &error, const vector<bool> &input_not_null) {
	if (!op.use_primitive_payloads || !op.use_grouped_state_addresses || op.use_perfect_hash_group_lookup ||
	    op.payloads.empty() || op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	SljitFusedTypedAggregatePayloads payloads;
	if (!TryBuildFusedTypedAggregatePayloads(op, payloads, &input_not_null)) {
		return true;
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativeGroupedFusedTypedExpressionAggregateUpdate(
	    payloads.codegen_payloads, op.sink_info.aggregates, op.sink_info.aggregate_contract, fused_function,
	    fused_error);
	if (fused_code && fused_function) {
		executable.payloads = std::move(payloads.executable_payloads);
		executable.fused_payload_update_code = std::move(fused_code);
		executable.fused_payload_update_function = fused_function;
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static void PopulateSljitExecutableAggregatePayloadSourceFacts(SljitExecutableRegionExpression &payload,
                                                               const vector<bool> &input_not_null) {
	auto &plan = payload.plan;
	payload.input_source_indices.clear();
	payload.input_source_not_null.clear();
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		payload.input_source_indices.push_back(plan.source_index);
		payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, plan.source_index));
		return;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		payload.input_source_indices.push_back(plan.source_index);
		payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, plan.source_index));
		payload.input_source_indices.push_back(plan.right_source_index);
		payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, plan.right_source_index));
		return;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		payload.input_source_indices = plan.expression_tree_source_indices;
		payload.input_source_not_null.reserve(payload.input_source_indices.size());
		for (auto source_idx : payload.input_source_indices) {
			payload.input_source_not_null.push_back(SljitSourceKnownNotNull(&input_not_null, source_idx));
		}
		return;
	default:
		return;
	}
}

void SljitBuildExecutableAggregateUpdateMetadata(const SljitNativeAggregateUpdatePlan &op,
                                                 SljitExecutableAggregateUpdate &executable,
                                                 const vector<bool> &input_not_null) {
	executable.plan.sink_info = op.sink_info;
	executable.plan.input_types = op.input_types;
	executable.plan.estimated_input_count = op.estimated_input_count;
	executable.plan.group_reserve = op.group_reserve;
	executable.plan.use_primitive_payloads = op.use_primitive_payloads;
	executable.plan.use_grouped_state_addresses = op.use_grouped_state_addresses;
	executable.plan.use_perfect_hash_group_lookup = op.use_perfect_hash_group_lookup;
	executable.plan.group_expressions.reserve(op.group_expressions.size());
	for (auto &group_expression : op.group_expressions) {
		executable.plan.group_expressions.push_back(group_expression.Copy(true, false));
	}
	executable.payloads.reserve(op.payloads.size());
	for (auto &payload : op.payloads) {
		SljitExecutableRegionExpression executable_payload;
		executable_payload.plan = payload.Copy(true, false);
		PopulateSljitExecutableAggregatePayloadSourceFacts(executable_payload, input_not_null);
		executable.payloads.push_back(std::move(executable_payload));
	}
}

static bool SljitAggregateUpdateRequiresTypedGroupedBackend(const SljitNativeAggregateUpdatePlan &op) {
	if (!op.use_primitive_payloads || !op.use_grouped_state_addresses) {
		return false;
	}
	for (auto &payload : op.payloads) {
		if (payload.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return true;
		}
	}
	for (auto &group_expression : op.group_expressions) {
		if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return true;
		}
	}
	return false;
}

bool SljitBuildExecutableAggregateUpdatePayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                    SljitExecutableAggregateUpdate &executable, string &error,
                                                    const vector<bool> &input_not_null,
                                                    const vector<Value> &input_min_values,
                                                    const vector<Value> &input_max_values) {
	if (op.use_primitive_payloads && !op.use_grouped_state_addresses && op.payloads.size() > 1) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(op.payloads, op.sink_info.aggregates,
		                                                                         fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	if (!TryBuildUngroupedFusedTypedExpressionAggregateUpdate(op, executable, error, input_not_null)) {
		return false;
	}
	if (executable.fused_payload_update_function) {
		return true;
	}
	if (!TryBuildPerfectHashGroupedFusedTypedExpressionAggregateUpdate(op, executable, error, input_not_null,
	                                                                   input_min_values, input_max_values)) {
		return false;
	}
	if (executable.fused_payload_update_function) {
		return true;
	}
	if (!TryBuildGroupedFusedTypedExpressionAggregateUpdate(op, executable, error, input_not_null)) {
		return false;
	}
	if (executable.fused_payload_update_function) {
		return true;
	}
	if (SljitAggregateUpdateRequiresTypedGroupedBackend(op)) {
		error = "SLJIT grouped typed aggregate update has no generated typed payload backend";
		return false;
	}
	if (op.use_primitive_payloads && op.use_perfect_hash_group_lookup && !op.payloads.empty()) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.groups, op.group_expressions,
		    op.sink_info.aggregate_contract, fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			executable.fused_payload_update_owns_group_lookup = true;
			return true;
		}
		if (!fused_error.empty()) {
			error = fused_error;
			return false;
		}
	}
	if (op.use_primitive_payloads && op.use_grouped_state_addresses && !op.payloads.empty()) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.aggregate_contract, fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	return SljitBuildExecutableAggregateUpdateFallbackPayloadCode(op, executable, error);
}

void SljitSelectExecutableAggregateUpdateStrategy(SljitExecutableAggregateUpdate &executable) {
	auto &strategy = executable.grouped_update_strategy;
	strategy.Clear();
	auto &plan = executable.plan;
	if (plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || !plan.use_primitive_payloads ||
	    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    executable.fused_payload_update_owns_group_lookup) {
		return;
	}
	if (executable.fused_payload_update_function &&
	    SljitFusedAggregatePayloadsUseTypedExpressionTrees(executable.payloads, plan.sink_info.aggregates)) {
		strategy.Add(SljitGroupedAggregateUpdateStrategy::DIRECT_STATE_ADDRESS_PAYLOAD_UPDATE);
		return;
	}
	strategy.Add(SljitGroupedAggregateUpdateStrategy::PREAGGREGATED_PRIMITIVE_GROUPS);
	strategy.Add(SljitGroupedAggregateUpdateStrategy::DIRECT_APPEND_NEW_GROUPS);
	strategy.Add(SljitGroupedAggregateUpdateStrategy::DIRECT_NEW_GROUPS);
	if (executable.fused_payload_update_function) {
		strategy.Add(SljitGroupedAggregateUpdateStrategy::DIRECT_STATE_ADDRESS_PAYLOAD_UPDATE);
	}
}

} // namespace duckdb
