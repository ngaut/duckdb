#include "execution_region_stage_plan.hpp"

#include "execution_region_description.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

static ExecutionRegionStageExecutionKind
ExecutionRegionSourceStageExecution(const ExecutionRegionNode &node,
                                    ExecutionRegionSourceExecutionKind source_execution) {
	if (!node.source) {
		return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	}
	auto &source = *node.source;
	if (source.kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR &&
	    source.native_state_scan_contract.status != ExecutionRegionStateContractStatus::NONE) {
		return source.native_state_scan_contract.status == ExecutionRegionStateContractStatus::READY
		           ? ExecutionRegionStageExecutionKind::NATIVE_CONTRACT
		           : ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	    source.source_contract.status == ExecutionRegionSourceContractStatus::READY) {
		return ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		return ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
	}
	return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
}

static bool ExecutionRegionProjectionHasExecutableWork(const ExecutionRegionNode &node) {
	for (auto &projection : node.projections) {
		if (!projection || !projection->root) {
			continue;
		}
		if (projection->root->kind != ExecutionExpressionIRKind::REFERENCE) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionSourceFilterHasGeneratedExpression(const ExecutionRegionSourceFilter &filter) {
	return filter.generated_source_stage_candidate;
}

static bool ExecutionRegionSourceFilterHasExecutableWork(const ExecutionRegionSourceFilter &filter) {
	if (!filter.expression || !filter.expression->root) {
		return false;
	}
	auto &root = *filter.expression->root;
	return root.kind != ExecutionExpressionIRKind::CONSTANT || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	       root.constant.IsNull() || !BooleanValue::Get(root.constant);
}

static bool ExecutionRegionSourceHasGeneratedFilterLayout(const ExecutionRegionNode &node) {
	if (!node.source || !node.source->table_scan_contract.present) {
		return false;
	}
	auto &contract = node.source->table_scan_contract;
	auto &projection_map = contract.source_contract_output_projection_map;
	if (projection_map.size() != node.output_types.size()) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		auto input_idx = projection_map[output_idx];
		if (input_idx >= contract.source_contract_input_types.size() ||
		    contract.source_contract_input_types[input_idx] != node.output_types[output_idx]) {
			return false;
		}
	}
	return true;
}

static ExecutionRegionStageExecutionKind ExecutionRegionSourceFilterExecution(const ExecutionRegionNode &node,
                                                                              const ExecutionRegionSourceFilter &filter,
                                                                              bool has_generated_filter_layout) {
	if (has_generated_filter_layout && ExecutionRegionSourceFilterHasGeneratedExpression(filter) &&
	    filter.scan_column_index < node.source->table_scan_contract.source_contract_input_types.size()) {
		return ExecutionRegionStageExecutionKind::GENERATED_IR;
	}
	if (node.source->table_scan_contract.present && node.source->table_scan_contract.filter_pushdown) {
		return ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
	}
	return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
}

static void AddExecutionRegionStage(ExecutionRegionStagePlan &plan, ExecutionRegionStageKind kind,
                                    ExecutionRegionStageExecutionKind execution, idx_t node_index,
                                    const ExecutionRegionNode &node, idx_t filter_index = DConstants::INVALID_INDEX,
                                    string reason = string(),
                                    ExecutionCompiledContractKind operation = ExecutionCompiledContractKind::NONE,
                                    ExecutionCompiledDrainKind drain = ExecutionCompiledDrainKind::NONE,
                                    string required_capability = string(), bool executable_work = false,
                                    idx_t expression_cost = 0) {
	ExecutionRegionStage stage;
	stage.kind = kind;
	stage.execution = execution;
	stage.operation = operation;
	stage.drain = drain;
	stage.executable_work = executable_work;
	stage.expression_cost = expression_cost;
	stage.node_index = node_index;
	stage.operator_index = node.operator_index;
	stage.filter_index = filter_index;
	stage.operator_name = node.operator_name;
	stage.required_capability = std::move(required_capability);
	stage.reason = std::move(reason);
	plan.stages.push_back(std::move(stage));
}

static string BuildExecutionRegionCompiledStageReason(const ExecutionCompiledStageContract &compiled_stage,
                                                      const ExecutionRegionNode &node) {
	string reason = compiled_stage.ir.empty() ? node.blocker_reason : compiled_stage.ir;
	if (!compiled_stage.blocker.empty() && !StringUtil::Contains(reason, compiled_stage.blocker)) {
		if (!reason.empty()) {
			reason += ";";
		}
		reason += "blocker=" + compiled_stage.blocker;
	}
	return reason;
}

static void AddExecutionRegionCompiledStage(ExecutionRegionStagePlan &plan,
                                            const ExecutionCompiledStageContract &compiled_stage, idx_t node_index,
                                            const ExecutionRegionNode &node,
                                            ExecutionRegionStageExecutionKind execution) {
	AddExecutionRegionStage(plan, compiled_stage.stage, execution, node_index, node, DConstants::INVALID_INDEX,
	                        BuildExecutionRegionCompiledStageReason(compiled_stage, node), compiled_stage.operation,
	                        compiled_stage.drain, compiled_stage.required_capability, compiled_stage.executable_work);
}

static bool AddExecutionRegionCompiledStages(ExecutionRegionStagePlan &plan, const ExecutionRegionNode &node,
                                             idx_t node_index) {
	if (!node.compiled_contract.Present()) {
		return false;
	}
	for (auto &compiled_stage : node.compiled_contract.stages) {
		AddExecutionRegionCompiledStage(plan, compiled_stage, node_index, node, compiled_stage.execution);
	}
	return true;
}

static string BuildExecutionRegionStagePlanShape(const string &candidate_shape) {
	string result = "full-pipeline";
	result += ":";
	result += candidate_shape;
	return result;
}

ExecutionRegionStagePlan BuildExecutionRegionStagePlan(const ExecutionRegionIR &region_ir,
                                                       const string &candidate_shape, ExecutionRegionIRMode mode) {
	ExecutionRegionStagePlan plan;
	plan.shape = BuildExecutionRegionStagePlanShape(candidate_shape);
	for (idx_t node_idx = 0; node_idx < region_ir.nodes.size(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE: {
			auto source_execution = node.source ? node.source->execution : ExecutionRegionSourceExecutionKind::NONE;
			bool added_source_stage = false;
			if (node.compiled_contract.Present()) {
				for (auto &compiled_stage : node.compiled_contract.stages) {
					if (compiled_stage.stage != ExecutionRegionStageKind::SOURCE) {
						continue;
					}
					auto execution = ExecutionRegionSourceStageExecution(node, source_execution);
					AddExecutionRegionCompiledStage(plan, compiled_stage, node_idx, node, execution);
					added_source_stage = true;
				}
			}
			if (!added_source_stage) {
				auto execution = ExecutionRegionSourceStageExecution(node, source_execution);
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::SOURCE, execution, node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			if (node.source) {
				const auto has_generated_filter_layout = ExecutionRegionSourceHasGeneratedFilterLayout(node);
				for (idx_t filter_idx = 0; filter_idx < node.source->filters.size(); filter_idx++) {
					auto &filter = node.source->filters[filter_idx];
					auto selected_filter_execution =
					    ExecutionRegionSourceFilterExecution(node, filter, has_generated_filter_layout);
					auto filter_execution =
					    source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY
					        ? ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY
					        : selected_filter_execution;
					AddExecutionRegionStage(plan, ExecutionRegionStageKind::SOURCE_FILTER, filter_execution, node_idx,
					                        node, filter_idx, filter.reason,
					                        ExecutionCompiledContractKind::FILTER_STAGE,
					                        ExecutionCompiledDrainKind::ZERO_OR_ONE_OUTPUT, string(),
					                        filter_execution == ExecutionRegionStageExecutionKind::GENERATED_IR &&
					                            ExecutionRegionSourceFilterHasExecutableWork(filter),
					                        filter.expression ? filter.expression->traits.expression_cost : 0);
				}
			}
			break;
		}
		case ExecutionRegionNodeKind::FILTER:
			AddExecutionRegionStage(plan, ExecutionRegionStageKind::FILTER,
			                        node.blocker_reason.empty() ? ExecutionRegionStageExecutionKind::GENERATED_IR
			                                                    : ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
			                        node_idx, node, DConstants::INVALID_INDEX, node.blocker_reason,
			                        ExecutionCompiledContractKind::NONE, ExecutionCompiledDrainKind::NONE, string(),
			                        node.blocker_reason.empty());
			break;
		case ExecutionRegionNodeKind::PROJECTION:
			AddExecutionRegionStage(plan, ExecutionRegionStageKind::PROJECTION,
			                        node.blocker_reason.empty() ? ExecutionRegionStageExecutionKind::GENERATED_IR
			                                                    : ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
			                        node_idx, node, DConstants::INVALID_INDEX, node.blocker_reason,
			                        ExecutionCompiledContractKind::NONE, ExecutionCompiledDrainKind::NONE, string(),
			                        node.blocker_reason.empty() && ExecutionRegionProjectionHasExecutableWork(node));
			break;
		case ExecutionRegionNodeKind::OPERATOR: {
			if (!AddExecutionRegionCompiledStages(plan, node, node_idx)) {
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::OPERATOR_BOUNDARY,
				                        ExecutionRegionStageExecutionKind::MISSING_CONTRACT, node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			break;
		}
		case ExecutionRegionNodeKind::SINK:
			if (!AddExecutionRegionCompiledStages(plan, node, node_idx)) {
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::SINK_BOUNDARY,
				                        ExecutionRegionStageExecutionKind::MISSING_CONTRACT, node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			break;
		default:
			break;
		}
	}
	FinalizeExecutionRegionStagePlan(plan, mode);
	return plan;
}

} // namespace duckdb
