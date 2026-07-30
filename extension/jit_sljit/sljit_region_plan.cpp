//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan.hpp"
#include "sljit_executable_stats.hpp"
#include "sljit_region_plan_internal.hpp"

#include "duckdb/common/constants.hpp"

namespace duckdb {

static constexpr idx_t SLJIT_FLAT_NULLABLE_FAST_PATH_MIN_CARDINALITY = STANDARD_VECTOR_SIZE * 4;
static bool SljitPredicateHasLowCardinalityStringLike(const SljitNativePredicate &predicate,
                                                      const vector<idx_t> &input_distinct_counts,
                                                      idx_t &distinct_count) {
	if (predicate.kind == SljitNativePredicateKind::STRING_LIKE_CONSTANT) {
		if (predicate.source_index >= input_distinct_counts.size()) {
			return false;
		}
		distinct_count = input_distinct_counts[predicate.source_index];
		return distinct_count > 0 && distinct_count <= EXECUTION_REGION_LOW_CARDINALITY_STRING_SEARCH_LIMIT;
	}
	if (predicate.child &&
	    SljitPredicateHasLowCardinalityStringLike(*predicate.child, input_distinct_counts, distinct_count)) {
		return true;
	}
	for (auto &child : predicate.children) {
		if (child && SljitPredicateHasLowCardinalityStringLike(*child, input_distinct_counts, distinct_count)) {
			return true;
		}
	}
	return false;
}

static void AddSljitLowCardinalityStringPredicatePreferences(const SljitNativeRegionPlan &region,
                                                             ExecutionRegionLoweringPlan &lowering_plan) {
	auto current_distinct_counts = region.source_distinct_counts;
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (auto &op : region.ops) {
		idx_t distinct_count;
		if (op.kind == SljitNativeRegionOpKind::FILTER && op.filter.predicate &&
		    SljitPredicateHasLowCardinalityStringLike(*op.filter.predicate, current_distinct_counts, distinct_count)) {
			lowering_plan.AddBackendLowCardinalityStringPredicatePreference(distinct_count);
		}
		SljitUpdateExecutableCurrentDistinctCounts(op, current_distinct_counts, current_min_values, current_max_values);
		SljitUpdateExecutableCurrentRanges(op, current_min_values, current_max_values);
	}
}

static bool IsIdentityProjection(const SljitNativeRegionOpPlan &op, const vector<LogicalType> &input_types) {
	if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.size() != input_types.size() ||
	    op.output_types.size() != input_types.size()) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < input_types.size(); col_idx++) {
		auto &projection = op.projections[col_idx];
		if (projection.kind != SljitNativeRegionExpressionKind::REFERENCE || projection.source_index != col_idx ||
		    projection.return_type != input_types[col_idx] || op.output_types[col_idx] != input_types[col_idx]) {
			return false;
		}
	}
	return true;
}

static vector<bool> BuildSljitSourceOutputNotNull(const ExecutionRegionSourceInfo &source) {
	vector<bool> result;
	if (!source.table_scan_contract.present) {
		return result;
	}
	auto &contract = source.table_scan_contract;
	result.reserve(contract.source_contract_output_projection_map.size());
	for (auto input_idx : contract.source_contract_output_projection_map) {
		result.push_back(input_idx < contract.source_contract_input_not_null.size()
		                     ? contract.source_contract_input_not_null[input_idx]
		                     : false);
	}
	return result;
}

template <class T>
static vector<T> BuildSljitSourceOutputStats(const ExecutionRegionSourceInfo &source, const vector<T> &input_stats) {
	vector<T> result;
	if (!source.table_scan_contract.present) {
		return result;
	}
	auto &contract = source.table_scan_contract;
	result.reserve(contract.source_contract_output_projection_map.size());
	for (auto input_idx : contract.source_contract_output_projection_map) {
		result.push_back(input_idx < input_stats.size() ? input_stats[input_idx] : T());
	}
	return result;
}

static void SelectSljitSourceStages(const ExecutionRegionCandidate &candidate, idx_t node_idx,
                                    const SljitSourceContractPlan &contract_plan,
                                    ExecutionRegionLoweringPlan &lowering_plan) {
	ExecutionRegionSelectedSourceRecipe source_recipe;
	source_recipe.execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	source_recipe.scan_filter_mode = contract_plan.scan_filter_mode;
	source_recipe.input_types = contract_plan.source_contract_input_types;
	lowering_plan.SetSourceRecipe(std::move(source_recipe));
	for (idx_t stage_idx = 0; stage_idx < candidate.stage_plan.stages.size(); stage_idx++) {
		auto &stage = candidate.stage_plan.stages[stage_idx];
		if (stage.node_index != node_idx) {
			continue;
		}
		auto execution = stage.execution;
		if (stage.kind == ExecutionRegionStageKind::SOURCE) {
			execution = ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
		} else if (stage.kind == ExecutionRegionStageKind::SOURCE_FILTER) {
			execution = contract_plan.scan_filter_mode == ExecutionRegionScanFilterMode::ALL
			                ? ExecutionRegionStageExecutionKind::NATIVE_CONTRACT
			                : ExecutionRegionStageExecutionKind::GENERATED_IR;
		}
		lowering_plan.SelectStage(stage_idx, execution);
	}
	if (contract_plan.low_cardinality_string_like_max_distinct_count > 0) {
		lowering_plan.AddBackendLowCardinalityStringPredicatePreference(
		    contract_plan.low_cardinality_string_like_max_distinct_count);
	}
}

static void SelectSljitNodeStages(const ExecutionRegionCandidate &candidate, idx_t node_idx,
                                  ExecutionRegionLoweringPlan &lowering_plan) {
	for (idx_t stage_idx = 0; stage_idx < candidate.stage_plan.stages.size(); stage_idx++) {
		auto &stage = candidate.stage_plan.stages[stage_idx];
		if (stage.node_index == node_idx) {
			lowering_plan.SelectStage(stage_idx, stage.execution);
		}
	}
}

static vector<bool> BuildSljitSourceNotNullForContractPlan(const ExecutionRegionSourceInfo &source,
                                                           const SljitSourceContractPlan &contract_plan) {
	if (contract_plan.UsesSourceContractInputLayout()) {
		return source.table_scan_contract.source_contract_input_not_null;
	}
	return BuildSljitSourceOutputNotNull(source);
}

static vector<idx_t>
BuildSljitSourceDistinctReserveCountsForContractPlan(const ExecutionRegionSourceInfo &source,
                                                     const SljitSourceContractPlan &contract_plan) {
	auto &counts = source.table_scan_contract.source_contract_input_distinct_reserve_counts;
	if (contract_plan.UsesSourceContractInputLayout()) {
		return counts;
	}
	return BuildSljitSourceOutputStats(source, counts);
}

static vector<idx_t> BuildSljitSourceDistinctCountsForContractPlan(const ExecutionRegionSourceInfo &source,
                                                                   const SljitSourceContractPlan &contract_plan) {
	auto &counts = source.table_scan_contract.source_contract_input_distinct_counts;
	if (contract_plan.UsesSourceContractInputLayout()) {
		return counts;
	}
	return BuildSljitSourceOutputStats(source, counts);
}

static vector<Value> BuildSljitSourceMinValuesForContractPlan(const ExecutionRegionSourceInfo &source,
                                                              const SljitSourceContractPlan &contract_plan) {
	auto &values = source.table_scan_contract.source_contract_input_min_values;
	if (contract_plan.UsesSourceContractInputLayout()) {
		return values;
	}
	return BuildSljitSourceOutputStats(source, values);
}

static vector<Value> BuildSljitSourceMaxValuesForContractPlan(const ExecutionRegionSourceInfo &source,
                                                              const SljitSourceContractPlan &contract_plan) {
	auto &values = source.table_scan_contract.source_contract_input_max_values;
	if (contract_plan.UsesSourceContractInputLayout()) {
		return values;
	}
	return BuildSljitSourceOutputStats(source, values);
}

static SljitRegionNodePlan PlanSljitRegionNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                               const vector<bool> &input_not_null, bool render_diagnostics) {
	switch (node.kind) {
	case ExecutionRegionNodeKind::FILTER:
		return PlanSljitFilterNode(node, render_diagnostics);
	case ExecutionRegionNodeKind::PROJECTION:
		return PlanSljitProjectionNode(node, input_types, render_diagnostics);
	case ExecutionRegionNodeKind::OPERATOR:
		if (node.operator_info && node.operator_info->kind == ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
			return PlanSljitHashJoinProbeOperatorNode(node, input_types, input_not_null, render_diagnostics);
		}
		if (node.operator_info &&
		    node.operator_info->kind == ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE) {
			return PlanSljitNestedLoopJoinProbeOperatorNode(node, input_types, render_diagnostics);
		}
		return SljitNodeBlockerBoundary(node, "operator contract boundary has no SLJIT native contract");
	default:
		return SljitNodeBlockerBoundary(node, "region IR node is outside SLJIT native region lowering");
	}
}

static idx_t SljitEstimatedNodeInputCardinality(const ExecutionRegionIR &region_ir,
                                                const ExecutionRegionCandidate &candidate, idx_t node_idx) {
	if (node_idx > 0 && node_idx - 1 < region_ir.nodes.size()) {
		return region_ir.nodes[node_idx - 1].estimated_cardinality;
	}
	if (node_idx == 0 && !region_ir.nodes.empty() && region_ir.nodes[0].kind == ExecutionRegionNodeKind::SOURCE) {
		return region_ir.nodes[node_idx].estimated_cardinality;
	}
	return candidate.estimated_cardinality;
}

static void ApplySljitAggregateUpdateInputEstimate(SljitRegionNodePlan &node_plan, idx_t estimated_input_count) {
	if (estimated_input_count == 0) {
		return;
	}
	for (auto &op : node_plan.native_ops) {
		if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		    op.aggregate_update.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			op.aggregate_update.estimated_input_count =
			    MaxValue(op.aggregate_update.estimated_input_count, estimated_input_count);
		}
	}
}

static void ApplySljitAggregateUpdateInputEstimate(SljitNativeRegionPlan &region, idx_t estimated_input_count) {
	if (estimated_input_count == 0) {
		return;
	}
	for (auto &op : region.ops) {
		if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		    op.aggregate_update.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			op.aggregate_update.estimated_input_count =
			    MaxValue(op.aggregate_update.estimated_input_count, estimated_input_count);
		}
	}
}

static void SpecializeSljitNativeRegionAggregatePayloadRanges(SljitNativeRegionPlan &region) {
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (auto &op : region.ops) {
		if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			SljitSpecializeAggregatePayloadRanges(op.aggregate_update, current_min_values, current_max_values);
		}
		SljitUpdateExecutableCurrentRanges(op, current_min_values, current_max_values);
	}
}

static void AddSljitLoweredNode(ExecutionRegionLoweringPlan &lowering_plan, const ExecutionRegionNode &node,
                                const SljitRegionNodePlan &node_plan) {
	if (node_plan.kind == ExecutionRegionLoweringKind::NATIVE) {
		lowering_plan.AddBackendDataShapeCapability(node.input_format, node.output_format, node.vector_source,
		                                            node.selection_source);
	}
	if (!node_plan.fusion_blocker.empty()) {
		lowering_plan.AddFusionBlocker(node_plan.fusion_blocker);
	}
	if (lowering_plan.RecordDetailedNodes()) {
		lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind, node_plan.kind, node_plan.reason);
		return;
	}
	lowering_plan.AddCompactNode(node.operator_kind, node_plan.kind, node_plan.reason);
}

struct SljitRegionLoweringCursor {
	explicit SljitRegionLoweringCursor(SljitNativeRegionPlan &native_region) : native_region(native_region) {
	}

	const vector<LogicalType> &InputTypes() const {
		return current_types;
	}

	const vector<bool> &InputNotNull() const {
		return current_not_null;
	}

	void ApplyExactFilterProof(SljitRegionNodePlan &node_plan) const {
		for (auto &op : node_plan.native_ops) {
			if (op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE || op.hash_join_probe.keys.size() != 1) {
				continue;
			}
			auto source_index = op.hash_join_probe.keys[0].key_input_index;
			if (source_index < current_exact_filter_identities.size()) {
				op.hash_join_probe.exact_source_filter_identity = current_exact_filter_identities[source_index];
			}
		}
	}

	bool CanFuse() const {
		return can_fuse;
	}

	void BreakAtBoundary(const vector<LogicalType> &boundary_output_types) {
		can_fuse = false;
		current_types = boundary_output_types;
		current_not_null.assign(current_types.size(), false);
		current_exact_filter_identities.assign(current_types.size(), nullptr);
	}

	void AcceptSource(const ExecutionRegionNode &node, SljitRegionNodePlan &node_plan, vector<bool> source_not_null) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		auto source_output_types = node_plan.source_contract.source_output_types.empty()
		                               ? node.output_types
		                               : node_plan.source_contract.source_output_types;
		current_exact_filter_identities.assign(source_output_types.size(), nullptr);
		if (node.source) {
			auto &contract = node.source->table_scan_contract;
			for (auto &proof : node.source->exact_filter_proofs) {
				if (!proof.identity) {
					continue;
				}
				if (node_plan.source_contract.UsesSourceContractInputLayout()) {
					if (proof.source_input_index < current_exact_filter_identities.size()) {
						current_exact_filter_identities[proof.source_input_index] = proof.identity;
					}
					continue;
				}
				for (idx_t output_idx = 0; output_idx < contract.source_contract_output_projection_map.size();
				     output_idx++) {
					if (contract.source_contract_output_projection_map[output_idx] == proof.source_input_index &&
					    output_idx < current_exact_filter_identities.size()) {
						current_exact_filter_identities[output_idx] = proof.identity;
					}
				}
			}
		}
		native_region.source_output_types = source_output_types;
		if (node.source) {
			native_region.aggregate_state_source.function_name = node.source->function_name;
			native_region.aggregate_state_source.state_scan_contract = node.source->native_state_scan_contract;
		}
		if (source_not_null.size() != source_output_types.size()) {
			source_not_null.assign(source_output_types.size(), false);
		}
		for (auto &op : node_plan.native_ops) {
			SljitUpdateExecutableCurrentNotNull(op, source_not_null);
			UpdateExactFilterProofs(op);
		}
		auto output_types = SljitRegionNodeHasNativeOps(node_plan) ? SljitRegionNodeLastNativeOp(node_plan).output_types
		                                                           : std::move(source_output_types);
		AppendIfFusing(node_plan);
		current_types = std::move(output_types);
		current_not_null = std::move(source_not_null);
	}

	void AcceptSink(SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		D_ASSERT(SljitRegionNodeHasNativeOps(node_plan));
		AppendIfFusing(node_plan);
	}

	void AcceptOperator(const ExecutionRegionNode &node, SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		D_ASSERT(SljitRegionNodeHasNativeOps(node_plan));
		if (current_types.empty()) {
			current_types = node.output_types;
		}
		if (can_fuse && node_plan.native_ops.size() == 1 &&
		    IsIdentityProjection(node_plan.native_ops[0], current_types)) {
			return;
		}
		for (auto &op : node_plan.native_ops) {
			SljitUpdateExecutableCurrentNotNull(op, current_not_null);
			UpdateExactFilterProofs(op);
		}
		auto output_types = SljitRegionNodeLastNativeOp(node_plan).output_types;
		AppendIfFusing(node_plan);
		current_types = std::move(output_types);
	}

private:
	void UpdateExactFilterProofs(const SljitNativeRegionOpPlan &op) {
		if (op.kind == SljitNativeRegionOpKind::FILTER) {
			return;
		}
		if (op.kind == SljitNativeRegionOpKind::PROJECTION) {
			vector<shared_ptr<ExecutionRuntimeFilterIdentity>> projected(op.projections.size());
			for (idx_t output_idx = 0; output_idx < op.projections.size(); output_idx++) {
				auto &projection = op.projections[output_idx];
				if (projection.kind == SljitNativeRegionExpressionKind::REFERENCE &&
				    projection.source_index < current_exact_filter_identities.size()) {
					projected[output_idx] = current_exact_filter_identities[projection.source_index];
				}
			}
			current_exact_filter_identities = std::move(projected);
			return;
		}
		current_exact_filter_identities.assign(op.output_types.size(), nullptr);
	}

	void AppendIfFusing(SljitRegionNodePlan &node_plan) {
		if (can_fuse) {
			for (auto &op : node_plan.native_ops) {
				native_region.ops.push_back(std::move(op));
			}
		}
	}

	vector<LogicalType> current_types;
	vector<bool> current_not_null;
	vector<shared_ptr<ExecutionRuntimeFilterIdentity>> current_exact_filter_identities;
	SljitNativeRegionPlan &native_region;
	bool can_fuse = true;
};

ExecutionRegionLoweringPlan BuildSljitRegionPlan(const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate, bool render_diagnostics) {
	ExecutionRegionLoweringPlan lowering_plan;
	lowering_plan.SetRecordDetailedNodes(render_diagnostics);
	lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::UNSUPPORTED);
	if (candidate.stage_plan.HasStages()) {
		lowering_plan.SetOperatorStageIR(candidate.stage_plan.ir);
	}
	SljitNativeRegionPlan native_region;
	SljitRegionLoweringCursor cursor(native_region);
	for (idx_t node_idx = 0; node_idx < region_ir.nodes.size(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (node.kind == ExecutionRegionNodeKind::SOURCE) {
			auto node_plan = PlanSljitSourceNode(node, candidate.traits.sink_kind, render_diagnostics);
			AddSljitLoweredNode(lowering_plan, node, node_plan);
			if (node_plan.kind == ExecutionRegionLoweringKind::NATIVE) {
				SelectSljitSourceStages(candidate, node_idx, node_plan.source_contract, lowering_plan);
				native_region.scan_filter_mode = node_plan.source_contract.scan_filter_mode;
				for (auto &scan_filter : node_plan.scan_filters) {
					native_region.scan_filters.push_back(std::move(scan_filter));
				}
				vector<bool> source_not_null;
				if (node.source && node.source->table_scan_contract.present) {
					native_region.source_distinct_counts =
					    BuildSljitSourceDistinctCountsForContractPlan(*node.source, node_plan.source_contract);
					native_region.source_distinct_reserve_counts =
					    BuildSljitSourceDistinctReserveCountsForContractPlan(*node.source, node_plan.source_contract);
					native_region.source_min_values =
					    BuildSljitSourceMinValuesForContractPlan(*node.source, node_plan.source_contract);
					native_region.source_max_values =
					    BuildSljitSourceMaxValuesForContractPlan(*node.source, node_plan.source_contract);
					source_not_null = BuildSljitSourceNotNullForContractPlan(*node.source, node_plan.source_contract);
					native_region.source_not_null = source_not_null;
				}
				cursor.AcceptSource(node, node_plan, std::move(source_not_null));
			} else {
				cursor.BreakAtBoundary(node.output_types);
			}
			continue;
		}
		if (node.kind == ExecutionRegionNodeKind::SINK) {
			auto node_plan = PlanSljitFullPipelineSinkNode(node, cursor.InputTypes(), render_diagnostics);
			ApplySljitAggregateUpdateInputEstimate(node_plan,
			                                       SljitEstimatedNodeInputCardinality(region_ir, candidate, node_idx));
			AddSljitLoweredNode(lowering_plan, node, node_plan);
			if (node_plan.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(node_plan)) {
				SelectSljitNodeStages(candidate, node_idx, lowering_plan);
				cursor.AcceptSink(node_plan);
			} else {
				cursor.BreakAtBoundary(node.output_types);
			}
			continue;
		}
		SljitRegionNodePlan node_plan;
		node_plan = PlanSljitRegionNode(node, cursor.InputTypes(), cursor.InputNotNull(), render_diagnostics);
		if (node_plan.kind == ExecutionRegionLoweringKind::NATIVE) {
			cursor.ApplyExactFilterProof(node_plan);
		}
		AddSljitLoweredNode(lowering_plan, node, node_plan);
		if (node_plan.kind != ExecutionRegionLoweringKind::NATIVE || !SljitRegionNodeHasNativeOps(node_plan)) {
			cursor.BreakAtBoundary(node.output_types);
			continue;
		}
		SelectSljitNodeStages(candidate, node_idx, lowering_plan);
		cursor.AcceptOperator(node, node_plan);
	}
	if (cursor.CanFuse() && !native_region.ops.empty()) {
		FuseAdjacentNativeProjections(native_region, render_diagnostics);
		FusePrimitiveAggregateUpdates(native_region, native_region.source_output_types, render_diagnostics);
		ApplySljitAggregateUpdateInputEstimate(native_region, candidate.estimated_cardinality);
		SpecializeSljitNativeRegionAggregatePayloadRanges(native_region);
		if (candidate.estimated_cardinality < SLJIT_FLAT_NULLABLE_FAST_PATH_MIN_CARDINALITY) {
			DisableSljitRegionFlatNullableFastPath(native_region);
		}
		AddSljitNativeRegionCapabilityFacts(lowering_plan, native_region);
		AddSljitLowCardinalityStringPredicatePreferences(native_region, lowering_plan);
		string codegen_blocker;
		if (SljitNativeRegionHasExecutableBodyGap(native_region, codegen_blocker)) {
			auto fusion_blocker =
			    "operator-contract-blocker:native-operator-executable-body-missing;" + codegen_blocker;
			if (!candidate.stage_plan.ir.empty()) {
				fusion_blocker += ";" + candidate.stage_plan.ir;
			}
			lowering_plan.AddFusionBlocker(std::move(fusion_blocker));
			return lowering_plan;
		}
		if (candidate.stage_plan.SourceBoundaryCount() == 0 && candidate.stage_plan.MissingContractCount() == 0) {
			if (lowering_plan.SelectedStageCount() != candidate.stage_plan.stages.size()) {
				lowering_plan.AddFusionBlocker("candidate-stage-plan-blocker:incomplete-backend-stage-selection;" +
				                               candidate.stage_plan.ir);
				return lowering_plan;
			}
			for (idx_t op_idx = 0; op_idx < native_region.ops.size(); op_idx++) {
				auto &op = native_region.ops[op_idx];
				if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
				    op.hash_join_probe.exact_source_filter_identity) {
					op.hash_join_probe.exact_source_filter_binding = op_idx;
				}
			}
			auto backend_plan = make_shared_ptr<SljitRegionBackendPlan>();
			backend_plan->native_region = make_uniq<SljitNativeRegionPlan>(std::move(native_region));
			backend_plan->artifact_semantic_key =
			    BuildSljitRegionArtifactSemanticKey(candidate, *backend_plan->native_region);
			backend_plan->artifact_binding_key = BuildSljitRegionArtifactBindingKey(*backend_plan->native_region);
			lowering_plan.backend_plan = std::move(backend_plan);
			lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::NATIVE);
		}
		if (lowering_plan.SelectedStageCount() != candidate.stage_plan.stages.size()) {
			if (candidate.stage_plan.SourceBoundaryCount() > 0) {
				lowering_plan.AddFusionBlocker("candidate-stage-plan-blocker:source-boundary;" +
				                               candidate.stage_plan.ir);
			}
			if (candidate.stage_plan.MissingContractCount() > 0) {
				lowering_plan.AddFusionBlocker("candidate-stage-plan-blocker:missing-contract;" +
				                               candidate.stage_plan.ir);
			}
		}
	}
	return lowering_plan;
}

} // namespace duckdb
