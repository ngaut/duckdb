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
#include "sljit_native_util.hpp"
#include "sljit_typed_expression_plan.hpp"

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
		if (op.Kind() == SljitNativeRegionOpKind::FILTER && op.Filter().predicate &&
		    SljitPredicateHasLowCardinalityStringLike(*op.Filter().predicate, current_distinct_counts,
		                                              distinct_count)) {
			lowering_plan.AddBackendLowCardinalityStringPredicatePreference(distinct_count);
		}
		SljitUpdateExecutableCurrentDistinctCounts(op, current_distinct_counts, current_min_values, current_max_values);
		SljitUpdateExecutableCurrentRanges(op, current_min_values, current_max_values);
	}
}

static bool IsIdentityProjection(const SljitNativeRegionOpPlan &op, const vector<LogicalType> &input_types) {
	if (op.Kind() != SljitNativeRegionOpKind::PROJECTION || op.Projections().size() != input_types.size() ||
	    op.output_types.size() != input_types.size()) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < input_types.size(); col_idx++) {
		auto &projection = op.Projections()[col_idx];
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
	source_recipe.deferrable_columns = contract_plan.deferrable_columns;
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
		if (node.operator_info && node.operator_info->Kind() == ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
			return PlanSljitHashJoinProbeOperatorNode(node, input_types, input_not_null, render_diagnostics);
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

static void ApplySljitAggregateUpdateInputEstimate(vector<SljitNativeRegionOpPlan> &ops, idx_t estimated_input_count) {
	if (estimated_input_count == 0) {
		return;
	}
	for (auto &op : ops) {
		if (op.Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		    op.AggregateUpdate().sink_info.Kind() == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
			op.estimated_input_count = MaxValue(op.estimated_input_count, estimated_input_count);
		}
	}
}

static void SpecializeSljitNativeRegionAggregatePayloadRanges(SljitNativeRegionPlan &region) {
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (auto &op : region.ops) {
		if (op.Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			SljitSpecializeAggregatePayloadRanges(op.AggregateUpdate(), current_min_values, current_max_values);
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
			if (op.Kind() != SljitNativeRegionOpKind::HASH_JOIN_PROBE || op.HashJoinProbe().keys.size() != 1) {
				continue;
			}
			auto source_index = op.HashJoinProbe().keys[0].key_input_index;
			if (source_index < current_exact_filter_identities.size()) {
				op.HashJoinProbe().exact_source_filter_identity = current_exact_filter_identities[source_index];
			}
		}
	}

	bool CanFuse() const {
		return can_fuse;
	}

	void BreakAtBoundary(const vector<LogicalType> &boundary_output_types) {
		can_fuse = false;
		operator_layout = false;
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
		// The source lowering repairs the fetched layout whenever its projection map is not the identity, so the source
		// produces the scan's output exactly when its types are the scan's
		operator_layout = output_types == node.output_types;
		AppendIfFusing(node_plan);
		current_types = std::move(output_types);
		current_not_null = std::move(source_not_null);
	}

	void AcceptSink(SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		D_ASSERT(SljitRegionNodeHasNativeOps(node_plan));
		node_plan.native_ops[0].input_is_operator_input = operator_layout;
		AppendIfFusing(node_plan);
	}

	void AcceptOperator(const ExecutionRegionNode &node, SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		D_ASSERT(SljitRegionNodeHasNativeOps(node_plan));
		// every op lowered from the operator carries its index: where DuckDB's operators can take over
		for (auto &op : node_plan.native_ops) {
			op.operator_index = node.operator_index;
		}
		if (current_types.empty()) {
			current_types = node.output_types;
		}
		if (can_fuse && node_plan.native_ops.size() == 1 &&
		    IsIdentityProjection(node_plan.native_ops[0], current_types)) {
			return;
		}
		// An operator lowers to ops whose last one declares the operator's output
		if (SljitRegionNodeLastNativeOp(node_plan).output_types != node.output_types) {
			throw InternalException("SLJIT lowered an operator to ops that do not produce its output");
		}
		node_plan.native_ops[0].input_is_operator_input = operator_layout;
		// A projection computes its operator's output from whatever layout it reads; a filter or a join computes its
		// operator's output only from its operator's input
		operator_layout = operator_layout || node.kind == ExecutionRegionNodeKind::PROJECTION;
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
		if (op.Kind() == SljitNativeRegionOpKind::FILTER) {
			return;
		}
		if (op.Kind() == SljitNativeRegionOpKind::PROJECTION) {
			vector<shared_ptr<ExecutionRuntimeFilterIdentity>> projected(op.Projections().size());
			for (idx_t output_idx = 0; output_idx < op.Projections().size(); output_idx++) {
				auto &projection = op.Projections()[output_idx];
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
	//! The ops so far produce exactly what DuckDB's last accepted operator produces
	bool operator_layout = false;
};

//! A source node lowers to filters and a layout repair only
static SljitNativeRegionOpPlan CopySljitSourceOpPlan(const SljitNativeRegionOpPlan &op) {
	SljitNativeRegionOpPlan result(op.Kind());
	result.operator_index = op.operator_index;
	result.input_types = op.input_types;
	result.output_types = op.output_types;
	switch (op.Kind()) {
	case SljitNativeRegionOpKind::FILTER:
		result.Filter() = op.Filter().Copy();
		return result;
	case SljitNativeRegionOpKind::PROJECTION:
		for (auto &projection : op.Projections()) {
			result.Projections().push_back(projection.Copy());
		}
		return result;
	default:
		throw InternalException("SLJIT source node lowered to an operator other than a filter or projection");
	}
}

//! The tree's references, back from the plan's source slots to the columns of its input
static void SljitRestoreTreeInputReferences(ExecutionExpressionIR &node, const vector<idx_t> &source_indices) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		D_ASSERT(node.ref_index < source_indices.size());
		node.ref_index = source_indices[node.ref_index];
		return;
	}
	for (auto child : {node.left.get(), node.right.get(), node.else_node.get()}) {
		if (child) {
			SljitRestoreTreeInputReferences(*child, source_indices);
		}
	}
	for (auto &child : node.children) {
		SljitRestoreTreeInputReferences(*child, source_indices);
	}
}

//! Whether the aggregate's update kernel can take the filter before it: an ungrouped or perfect-hash aggregate whose
//! every aggregate has a filtered kernel
static bool SljitAggregateTakesFilterInKernel(const SljitNativeAggregateUpdatePlan &aggregate) {
	auto sink_kind = aggregate.sink_info.Kind();
	if (sink_kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	    sink_kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		return false;
	}
	for (auto &update : aggregate.sink_info.aggregates) {
		if (!SljitFilteredAggregateKindCanGenerate(update.primitive_update_kind)) {
			return false;
		}
	}
	return true;
}

//! A filter right after the source's generated filters (past the source's layout repair, which only picks columns)
//! that the region's aggregate takes in its kernel joins them in one predicate: the aggregate updates in one pass over
//! every row instead of after a selection. The joined filter is no longer the table filters alone, so it is not the
//! source's filter: a chunk whose zone maps hold the table filters still runs it, and the scan reads every column in
//! full instead of the later filter's columns for the rows the table filters keep, which pays only for that one pass.
//! A filter that may raise an error stays apart, as the joined predicate may evaluate it on rows the table filters
//! reject
static void SljitJoinSourceFilterWithNextFilter(SljitNativeRegionPlan &region, bool render_diagnostics) {
	auto &ops = region.ops;
	if ((region.scan_filter_mode != ExecutionRegionScanFilterMode::STATIC_PRUNING_ONLY &&
	     region.scan_filter_mode != ExecutionRegionScanFilterMode::DYNAMIC_FILTERS_WITH_STATIC_PRUNING) ||
	    ops.size() < 2 || ops[0].Kind() != SljitNativeRegionOpKind::FILTER) {
		return;
	}
	// the source's layout repair: the input column each of its outputs is
	vector<idx_t> repair_columns;
	idx_t next_idx = 1;
	if (ops[1].Kind() == SljitNativeRegionOpKind::PROJECTION) {
		for (auto &projection : ops[1].Projections()) {
			if (projection.kind != SljitNativeRegionExpressionKind::REFERENCE) {
				return;
			}
			repair_columns.push_back(projection.source_index);
		}
		next_idx = 2;
	}
	if (ops.size() <= next_idx + 1 || ops[next_idx].Kind() != SljitNativeRegionOpKind::FILTER ||
	    ops[next_idx].output_types != ops[next_idx - 1].output_types) {
		return;
	}
	// the aggregate composes a projection before it into its payloads
	auto aggregate_idx = next_idx + 1;
	if (ops[aggregate_idx].Kind() == SljitNativeRegionOpKind::PROJECTION) {
		aggregate_idx++;
	}
	if (aggregate_idx + 1 != ops.size() || ops[aggregate_idx].Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    !SljitAggregateTakesFilterInKernel(ops[aggregate_idx].AggregateUpdate())) {
		return;
	}
	auto &table_filter = ops[0].Filter();
	auto &next_filter = ops[next_idx].Filter();
	if (!table_filter.expression_tree || !next_filter.expression_tree ||
	    !SljitExpressionIRCannotThrow(*next_filter.expression_tree)) {
		return;
	}
	auto root = make_uniq<ExecutionExpressionIR>();
	root->kind = ExecutionExpressionIRKind::CONJUNCTION;
	root->return_type = LogicalType::BOOLEAN;
	root->physical_type = PhysicalType::BOOL;
	root->validity = ExecutionExpressionValidityKind::THREE_VALUED_BOOLEAN;
	root->source = ExecutionExpressionSourceKind::DERIVED;
	root->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	root->conjunction_op = ExecutionExpressionConjunctionOp::AND;
	for (auto plan : {&table_filter, &next_filter}) {
		auto predicate = plan->expression_tree->Copy();
		SljitRestoreTreeInputReferences(*predicate, plan->expression_tree_source_indices);
		if (plan == &next_filter && !repair_columns.empty()) {
			SljitRestoreTreeInputReferences(*predicate, repair_columns);
		}
		if (predicate->kind == ExecutionExpressionIRKind::CONJUNCTION &&
		    predicate->conjunction_op == ExecutionExpressionConjunctionOp::AND) {
			for (auto &child : predicate->children) {
				root->children.push_back(std::move(child));
			}
		} else {
			root->children.push_back(std::move(predicate));
		}
	}
	ExecutionExpressionFragment fragment;
	fragment.expression_index = 0;
	fragment.return_type = LogicalType::BOOLEAN;
	fragment.root = std::move(root);
	if (render_diagnostics) {
		fragment.ir = "duckdb.expr typed-vector-ir;" + DescribeExecutionExpressionIR(*fragment.root);
	}
	SljitNativeRegionExpressionPlan joined;
	string error;
	if (!TryLowerNativeRegionExpression(fragment, true, joined, error, render_diagnostics)) {
		return;
	}
	ops[0].Filter() = std::move(joined);
	ops.erase(ops.begin() + NumericCast<int64_t>(next_idx));
	region.source_filter_reads_beyond_table_filters = true;
}

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
				for (auto &op : node_plan.native_ops) {
					native_region.native_continuation_source_ops.push_back(CopySljitSourceOpPlan(op));
				}
				cursor.AcceptSource(node, node_plan, std::move(source_not_null));
			} else {
				cursor.BreakAtBoundary(node.output_types);
			}
			continue;
		}
		if (node.kind == ExecutionRegionNodeKind::SINK) {
			auto node_plan = PlanSljitFullPipelineSinkNode(node, cursor.InputTypes(), render_diagnostics);
			ApplySljitAggregateUpdateInputEstimate(node_plan.native_ops,
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
		SljitJoinSourceFilterWithNextFilter(native_region, render_diagnostics);
		FuseAdjacentNativeProjections(native_region, render_diagnostics);
		FusePrimitiveAggregateUpdates(native_region, native_region.source_output_types, render_diagnostics);
		ApplySljitAggregateUpdateInputEstimate(native_region.ops, candidate.estimated_cardinality);
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
				if (op.Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
				    op.HashJoinProbe().exact_source_filter_identity) {
					op.HashJoinProbe().exact_source_filter_binding = op_idx;
				}
			}
			auto backend_plan = make_shared_ptr<SljitRegionBackendPlan>();
			BindSljitRegionInstanceSlots(native_region);
			backend_plan->native_region = make_uniq<SljitNativeRegionPlan>(std::move(native_region));
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
