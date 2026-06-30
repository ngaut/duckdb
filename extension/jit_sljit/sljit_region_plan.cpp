//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan.hpp"
#include "sljit_region_plan_internal.hpp"

#include "duckdb/common/constants.hpp"

namespace duckdb {

static constexpr idx_t SLJIT_FLAT_NULLABLE_FAST_PATH_MIN_CARDINALITY = STANDARD_VECTOR_SIZE * 4;

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

static bool SljitNativeRegionExpressionGeneratesCode(const SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::CONSTANT:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		return false;
	default:
		return true;
	}
}

static bool SljitNativeRegionExpressionsGenerateCode(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	for (auto &expr : expressions) {
		if (SljitNativeRegionExpressionGeneratesCode(expr)) {
			return true;
		}
	}
	return false;
}

static bool SljitNativeRegionOpGeneratesMachineCode(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return SljitNativeRegionExpressionGeneratesCode(op.filter);
	case SljitNativeRegionOpKind::PROJECTION:
		return SljitNativeRegionExpressionsGenerateCode(op.projections);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return !op.hash_join_probe.keys.empty() && op.hash_join_probe.equality_key_count > 0 &&
		       op.hash_join_probe.equality_key_count <= op.hash_join_probe.keys.size() &&
		       op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::NONE;
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return false;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return op.nested_loop_join_probe.join_type == ExecutionRegionJoinType::INNER &&
		       op.nested_loop_join_probe.conditions.size() == 1;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return SljitNativeRegionExpressionsGenerateCode(op.nested_loop_join_build.rhs_conditions);
	case SljitNativeRegionOpKind::ORDER_SINK:
		return SljitNativeRegionExpressionsGenerateCode(op.order_sink.order_keys);
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return false;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return op.aggregate_update.use_primitive_payloads;
	default:
		return false;
	}
}

static bool SljitNativeRegionOpIsNativeSink(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return op.hash_join_build.sink_info.kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD &&
		       op.hash_join_build.sink_info.hash_join_contract.native_build_contract.status ==
		           ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return op.nested_loop_join_build.sink_info.kind == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD &&
		       op.nested_loop_join_build.sink_info.nested_loop_join_contract.native_build_contract.status ==
		           ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::APPEND_SINK:
		return (op.append_sink.sink_info.kind == ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK ||
		        op.append_sink.sink_info.kind == ExecutionRegionSinkKind::MATERIALIZATION) &&
		       op.append_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::ORDER_SINK:
		return op.order_sink.sink_info.kind == ExecutionRegionSinkKind::SORT &&
		       op.order_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return op.delim_join_sink.sink_info.kind == ExecutionRegionSinkKind::DELIM_JOIN_SINK &&
		       op.delim_join_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
		auto &sink = op.aggregate_update.sink_info;
		return (sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		        sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
		        sink.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) &&
		       sink.aggregate_contract.native_state_update_contract.status == ExecutionRegionStateContractStatus::READY;
	}
	default:
		return false;
	}
}

static string SljitNativeRegionOpBoundaryBlocker(const SljitNativeRegionOpPlan &op) {
	if (op.kind == SljitNativeRegionOpKind::FILTER || op.kind == SljitNativeRegionOpKind::PROJECTION) {
		return string();
	}
	if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return string();
	}
	if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		if (op.aggregate_update.use_primitive_payloads) {
			return string();
		}
		return "operator-contract-blocker:aggregate-update-generated-payload-missing";
	}
	if (SljitNativeRegionOpIsNativeSink(op)) {
		return string();
	}
	return "operator-contract-blocker:whole-vectorized-operator-boundary;stage=" +
	       string(SljitNativeRegionOpKindName(op.kind));
}

static void DisableSljitRegionFlatNullableFastPath(SljitNativeRegionPlan &region) {
	auto set_expression = [&](SljitNativeRegionExpressionPlan &expr) {
		expr.emit_flat_nullable_fast_path = false;
	};
	auto set_expressions = [&](vector<SljitNativeRegionExpressionPlan> &expressions) {
		for (auto &expr : expressions) {
			set_expression(expr);
		}
	};
	for (auto &op : region.ops) {
		set_expression(op.filter);
		set_expression(op.hash_join_probe.residual_filter);
		for (auto &condition : op.nested_loop_join_probe.conditions) {
			set_expression(condition.lhs_condition);
		}
		set_expressions(op.nested_loop_join_build.rhs_conditions);
		set_expressions(op.order_sink.order_keys);
		set_expressions(op.aggregate_update.payloads);
		set_expressions(op.aggregate_update.group_expressions);
		set_expressions(op.projections);
	}
}

static bool SljitNativeRegionHasExecutableBodyGap(const SljitNativeRegionPlan &region, string &blocker) {
	bool generates_machine_code = false;
	for (auto &op : region.ops) {
		auto op_blocker = SljitNativeRegionOpBoundaryBlocker(op);
		if (!op_blocker.empty()) {
			blocker = std::move(op_blocker);
			return true;
		}
		generates_machine_code = generates_machine_code || SljitNativeRegionOpGeneratesMachineCode(op);
	}
	if (!generates_machine_code) {
		blocker = "SLJIT native region emits no generated machine code";
		return true;
	}
	return false;
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

static SljitRegionNodePlan PlanSljitRegionNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                               string &error, bool render_diagnostics) {
	switch (node.kind) {
	case ExecutionRegionNodeKind::FILTER:
		return PlanSljitFilterNode(node, error, render_diagnostics);
	case ExecutionRegionNodeKind::PROJECTION:
		return PlanSljitProjectionNode(node, input_types, error, render_diagnostics);
	case ExecutionRegionNodeKind::SOURCE:
		return SljitRegionBoundaryNode(SljitSourceBoundaryReason(node, render_diagnostics));
	case ExecutionRegionNodeKind::SINK: {
		return PlanSljitSinkNode(node, input_types, render_diagnostics);
	}
	case ExecutionRegionNodeKind::OPERATOR:
		if (node.operator_info && node.operator_info->kind == ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
			return PlanSljitHashJoinProbeOperatorNode(node, input_types, render_diagnostics);
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

static void AddSljitLoweredNode(ExecutionRegionLoweringPlan &lowering_plan, const ExecutionRegionNode &node,
                                const SljitRegionNodePlan &node_plan) {
	if (node_plan.kind == ExecutionRegionLoweringKind::NATIVE) {
		lowering_plan.AddBackendDataShapeCapability(node.input_format, node.output_format, node.vector_source,
		                                            node.selection_source);
	}
	if (lowering_plan.RecordDetailedNodes()) {
		lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind, node_plan.kind, node_plan.reason);
		return;
	}
	lowering_plan.AddCompactNode(node.operator_kind, node_plan.kind, node_plan.reason);
}

static void AddSljitNativeRegionCapabilityFacts(ExecutionRegionLoweringPlan &lowering_plan,
                                                const SljitNativeRegionPlan &native_region) {
	for (auto not_null : native_region.source_not_null) {
		lowering_plan.AddBackendSourceValidityCapability(not_null);
	}
	for (auto &op : native_region.ops) {
		switch (op.kind) {
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			lowering_plan.AddBackendHashJoinProbeCapability(
			    op.hash_join_probe.perfect_hash_probe, op.hash_join_probe.residual_predicate,
			    op.hash_join_probe.equality_key_count, op.hash_join_probe.keys.size());
			for (auto &key : op.hash_join_probe.keys) {
				lowering_plan.AddBackendJoinKeyTypeCapability(key.key_type);
			}
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			for (auto &key : op.hash_join_build.sink_info.hash_join_keys) {
				lowering_plan.AddBackendJoinKeyTypeCapability(key.type);
			}
			lowering_plan.AddBackendHashJoinBuildCapability();
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
			for (auto &condition : op.nested_loop_join_probe.conditions) {
				lowering_plan.AddBackendJoinKeyTypeCapability(condition.type);
			}
			lowering_plan.AddBackendNestedLoopJoinProbeCapability();
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			for (auto &condition_type : op.nested_loop_join_build.condition_types) {
				lowering_plan.AddBackendJoinKeyTypeCapability(condition_type);
			}
			lowering_plan.AddBackendNestedLoopJoinBuildCapability();
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
			auto &aggregate_update = op.aggregate_update;
			for (auto &group : aggregate_update.sink_info.groups) {
				lowering_plan.AddBackendGroupKeyTypeCapability(group.type);
			}
			for (auto &payload : aggregate_update.payloads) {
				lowering_plan.AddBackendPayloadTypeCapability(payload.return_type);
			}
			lowering_plan.AddBackendAggregateUpdateCapability(
			    aggregate_update.sink_info.aggregate_contract.kind, aggregate_update.use_primitive_payloads,
			    aggregate_update.use_grouped_state_addresses, aggregate_update.use_perfect_hash_group_lookup);
			break;
		}
		default:
			break;
		}
	}
}

static bool SljitNativeContractReady(bool present, const ExecutionRegionNativeOperatorContract &native_contract) {
	return present && native_contract.status == ExecutionRegionStateContractStatus::READY;
}

static void AddSljitContractBlocker(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                    bool contract_ready, const char *ready_error, const char *missing_error,
                                    const char *ready_blocker, const char *missing_blocker,
                                    const string &ready_reason = string()) {
	backend_error = contract_ready ? ready_error : missing_error;
	if (!contract_ready) {
		lowering_plan.AddFusionBlocker(missing_blocker);
		return;
	}
	string blocker = ready_blocker;
	if (!ready_reason.empty()) {
		blocker += ";";
		blocker += ready_reason;
	}
	lowering_plan.AddFusionBlocker(std::move(blocker));
}

static void AddSljitFullPipelineSinkBlockers(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                             const ExecutionRegionNode &node, const SljitRegionNodePlan &node_plan,
                                             const ExecutionRegionContract &contract) {
	if (node_plan.kind != ExecutionRegionLoweringKind::BOUNDARY || !ExecutionRegionABIIsFullPipeline(contract.abi) ||
	    !node.sink) {
		return;
	}
	switch (node.sink->kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD: {
		auto build_contract_ready = SljitNativeContractReady(node.sink->hash_join_contract.present,
		                                                     node.sink->hash_join_contract.native_build_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, build_contract_ready,
		                        "SLJIT full-pipeline hash join build sink rejected by native hash join build lowering",
		                        "SLJIT full-pipeline hash join build sink requires a native hash join build contract",
		                        "sink-contract-blocker:hash-join-build-native-lowering",
		                        "sink-contract-blocker:hash-join-build-contract-missing");
		break;
	}
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD: {
		auto build_contract_ready = SljitNativeContractReady(
		    node.sink->nested_loop_join_contract.present, node.sink->nested_loop_join_contract.native_build_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, build_contract_ready,
		                        "SLJIT full-pipeline nested loop join build sink rejected by native lowering",
		                        "SLJIT full-pipeline nested loop join build sink requires a native build contract",
		                        "sink-contract-blocker:nested-loop-join-build-native-lowering-missing",
		                        "sink-contract-blocker:nested-loop-join-build-contract-missing", node_plan.reason);
		break;
	}
	default:
		break;
	}
}

static void AddSljitOperatorContractBlockers(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                             const ExecutionRegionNode &node, const SljitRegionNodePlan &node_plan) {
	if (node_plan.kind != ExecutionRegionLoweringKind::BOUNDARY || !node.operator_info) {
		return;
	}
	switch (node.operator_info->kind) {
	case ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE: {
		auto probe_contract_ready =
		    SljitNativeContractReady(node.operator_info->hash_join_contract.present,
		                             node.operator_info->hash_join_contract.native_probe_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, probe_contract_ready,
		                        "SLJIT hash join probe rejected by native hash join lowering",
		                        "SLJIT hash join probe requires a native hash join probe contract",
		                        "operator-contract-blocker:hash-join-probe-native-lowering-missing",
		                        "operator-contract-blocker:hash-join-probe-contract-missing", node_plan.reason);
		break;
	}
	case ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE: {
		auto probe_contract_ready =
		    SljitNativeContractReady(node.operator_info->nested_loop_join_contract.present,
		                             node.operator_info->nested_loop_join_contract.native_probe_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, probe_contract_ready,
		                        "SLJIT nested loop join probe rejected by native lowering",
		                        "SLJIT nested loop join probe requires a native probe contract",
		                        "operator-contract-blocker:nested-loop-join-probe-native-lowering-missing",
		                        "operator-contract-blocker:nested-loop-join-probe-contract-missing", node_plan.reason);
		break;
	}
	default:
		break;
	}
}

struct SljitRegionLoweringCursor {
	SljitRegionLoweringCursor(vector<LogicalType> input_types, SljitNativeRegionPlan &native_region)
	    : current_types(std::move(input_types)), native_region(native_region) {
	}

	const vector<LogicalType> &InputTypes() const {
		return current_types;
	}

	bool CanFuse() const {
		return can_fuse;
	}

	void BreakAtBoundary(const vector<LogicalType> &boundary_output_types) {
		can_fuse = false;
		current_types = boundary_output_types;
	}

	void AcceptSource(const ExecutionRegionNode &node, SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		auto source_output_types = SljitRegionNodeHasNativeOps(node_plan)
		                               ? SljitRegionNodeLastNativeOp(node_plan).output_types
		                               : node.output_types;
		AppendIfFusing(node_plan);
		current_types = std::move(source_output_types);
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
		auto output_types = SljitRegionNodeLastNativeOp(node_plan).output_types;
		AppendIfFusing(node_plan);
		current_types = std::move(output_types);
	}

private:
	void AppendIfFusing(SljitRegionNodePlan &node_plan) {
		if (can_fuse) {
			for (auto &op : node_plan.native_ops) {
				native_region.ops.push_back(std::move(op));
			}
		}
	}

	vector<LogicalType> current_types;
	SljitNativeRegionPlan &native_region;
	bool can_fuse = true;
};

ExecutionRegionLoweringPlan BuildSljitRegionPlan(const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate, bool render_diagnostics) {
	ExecutionRegionLoweringPlan lowering_plan;
	lowering_plan.SetRecordDetailedNodes(render_diagnostics);
	string backend_error;
	lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::UNSUPPORTED);
	if (candidate.stage_plan.HasStages()) {
		lowering_plan.SetOperatorStageIR(candidate.stage_plan.ir);
	}
	SljitNativeRegionPlan native_region;
	SljitRegionLoweringCursor cursor(candidate.input_types, native_region);
	if (candidate.EndNode() > region_ir.nodes.size()) {
		backend_error = "SLJIT region candidate references nodes outside the region IR";
		return lowering_plan;
	}
	auto &contract = candidate.contract;
	bool selected_uses_scan_filters = false;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (node.kind == ExecutionRegionNodeKind::SOURCE) {
			auto executable_source = SljitCanExecuteSourceNode(node, contract);
			auto source_execution =
			    candidate.source_execution != ExecutionRegionSourceExecutionKind::NONE
			        ? candidate.source_execution
			        : (node.source ? node.source->execution : ExecutionRegionSourceExecutionKind::NONE);
			auto node_plan =
			    executable_source
			        ? PlanSljitSourceNode(node, contract, source_execution, render_diagnostics)
			        : PlanSljitRegionNode(node, cursor.InputTypes(), backend_error, render_diagnostics);
			const bool source_requires_native = executable_source &&
			                                    node_plan.kind == ExecutionRegionLoweringKind::BOUNDARY &&
			                                    node_plan.requires_source_contract;
			AddSljitLoweredNode(lowering_plan, node, node_plan);
			if (source_requires_native) {
				lowering_plan.AddFusionBlocker(
				    "source-contract-blocker:requires-source-contract;source_execution=duckdb-source-boundary");
			}
			if (executable_source && node_plan.kind != ExecutionRegionLoweringKind::BOUNDARY) {
				auto selected_source_execution = node_plan.source_execution != ExecutionRegionSourceExecutionKind::NONE
				                                     ? node_plan.source_execution
				                                     : node.source->execution;
				lowering_plan.SetSelectedSourceExecution(selected_source_execution);
				native_region.source_execution = selected_source_execution;
			}
			if (executable_source && node_plan.kind == ExecutionRegionLoweringKind::NATIVE) {
				const bool generated_source_filters = node.source && !node.source->filters.empty() &&
				                                      !node_plan.uses_scan_filters &&
				                                      SljitRegionNodeHasNativeOps(node_plan);
				selected_uses_scan_filters = selected_uses_scan_filters || node_plan.uses_scan_filters;
				cursor.AcceptSource(node, node_plan);
				if (node.source && node.source->table_scan_contract.present) {
					native_region.source_distinct_counts =
					    node.source->table_scan_contract.source_contract_input_distinct_counts;
					native_region.source_min_values = node.source->table_scan_contract.source_contract_input_min_values;
					native_region.source_max_values = node.source->table_scan_contract.source_contract_input_max_values;
					native_region.source_not_null =
					    generated_source_filters ? node.source->table_scan_contract.source_contract_input_not_null
					                             : BuildSljitSourceOutputNotNull(*node.source);
				}
			} else {
				cursor.BreakAtBoundary(node.output_types);
			}
			continue;
		}
		if (node.kind == ExecutionRegionNodeKind::SINK) {
			auto node_plan =
			    ExecutionRegionABIIsFullPipeline(contract.abi)
			        ? PlanSljitFullPipelineSinkNode(node, cursor.InputTypes(), render_diagnostics)
			    : candidate.context_has_missing_operator_contract
			        ? SljitRegionBoundaryNode("sink region requires upstream operators with native contracts")
			        : PlanSljitRegionNode(node, cursor.InputTypes(), backend_error, render_diagnostics);
			AddSljitLoweredNode(lowering_plan, node, node_plan);
			AddSljitFullPipelineSinkBlockers(lowering_plan, backend_error, node, node_plan, contract);
			if (node_plan.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(node_plan)) {
				cursor.AcceptSink(node_plan);
			} else {
				cursor.BreakAtBoundary(node.output_types);
			}
			continue;
		}
		SljitRegionNodePlan node_plan;
		if (node.kind == ExecutionRegionNodeKind::OPERATOR &&
		    node.boundary == ExecutionRegionBoundaryKind::OPERATOR_NATIVE &&
		    !ExecutionRegionABIIsFullPipeline(contract.abi)) {
			node_plan = SljitRegionBoundaryNode("native operator contract requires full-pipeline region ABI");
		} else if (node.kind == ExecutionRegionNodeKind::OPERATOR &&
		           node.boundary == ExecutionRegionBoundaryKind::OPERATOR_CONTRACT_BOUNDARY &&
		           !ExecutionRegionABIIsFullPipeline(contract.abi)) {
			node_plan =
			    SljitRegionBoundaryNode("native operator contract boundary requires full-pipeline region ownership");
		} else {
			node_plan = PlanSljitRegionNode(node, cursor.InputTypes(), backend_error, render_diagnostics);
		}
		AddSljitLoweredNode(lowering_plan, node, node_plan);
		AddSljitOperatorContractBlockers(lowering_plan, backend_error, node, node_plan);
		if (node_plan.kind != ExecutionRegionLoweringKind::NATIVE || !SljitRegionNodeHasNativeOps(node_plan)) {
			cursor.BreakAtBoundary(node.output_types);
			continue;
		}
		cursor.AcceptOperator(node, node_plan);
	}
	if (cursor.CanFuse() && !native_region.ops.empty()) {
		FuseAdjacentNativeProjections(native_region, render_diagnostics);
		FusePrimitiveAggregateUpdates(native_region, candidate.input_types, render_diagnostics);
		if (candidate.estimated_cardinality < SLJIT_FLAT_NULLABLE_FAST_PATH_MIN_CARDINALITY) {
			DisableSljitRegionFlatNullableFastPath(native_region);
		}
		AddSljitNativeRegionCapabilityFacts(lowering_plan, native_region);
		lowering_plan.SetUsesScanFilters(selected_uses_scan_filters);
		string codegen_blocker;
		if (SljitNativeRegionHasExecutableBodyGap(native_region, codegen_blocker)) {
			backend_error = codegen_blocker;
			auto fusion_blocker =
			    "operator-contract-blocker:native-operator-executable-body-missing;" + codegen_blocker;
			if (!candidate.contract.ir.empty()) {
				fusion_blocker += ";" + candidate.contract.ir;
			}
			lowering_plan.AddFusionBlocker(std::move(fusion_blocker));
			return lowering_plan;
		}
		if (contract.source_boundary_count == 0 && contract.missing_contract_count == 0) {
			lowering_plan.SetFullyFused(true);
			auto backend_plan = make_shared_ptr<SljitRegionBackendPlan>();
			backend_plan->error = std::move(backend_error);
			backend_plan->native_region = make_uniq<SljitNativeRegionPlan>(std::move(native_region));
			lowering_plan.backend_plan = std::move(backend_plan);
			lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::NATIVE);
		}
		if (!lowering_plan.IsFullyFused()) {
			if (contract.source_boundary_count > 0) {
				lowering_plan.AddFusionBlocker("candidate-contract-blocker:source-boundary;" + contract.ir);
			}
			if (contract.missing_contract_count > 0) {
				lowering_plan.AddFusionBlocker("candidate-contract-blocker:missing-contract;" + contract.ir);
			}
		}
	}
	return lowering_plan;
}

} // namespace duckdb
