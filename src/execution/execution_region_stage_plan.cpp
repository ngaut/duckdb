#include "execution_region_stage_plan.hpp"

#include "execution_region_contract.hpp"
#include "execution_region_description.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

static ExecutionRegionStageExecutionKind
ExecutionRegionStageExecutionFromOwnership(ExecutionRegionOwnershipKind ownership) {
	switch (ownership) {
	case ExecutionRegionOwnershipKind::GENERATED_IR:
		return ExecutionRegionStageExecutionKind::GENERATED_IR;
	case ExecutionRegionOwnershipKind::NATIVE_CONTRACT:
		return ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
	case ExecutionRegionOwnershipKind::SOURCE_BOUNDARY:
		return ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
	case ExecutionRegionOwnershipKind::MISSING_CONTRACT:
		return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	case ExecutionRegionOwnershipKind::NONE:
	default:
		return ExecutionRegionStageExecutionKind::NONE;
	}
}

static ExecutionRegionOwnershipKind
ExecutionRegionOwnershipFromStageExecution(ExecutionRegionStageExecutionKind execution) {
	switch (execution) {
	case ExecutionRegionStageExecutionKind::GENERATED_IR:
		return ExecutionRegionOwnershipKind::GENERATED_IR;
	case ExecutionRegionStageExecutionKind::NATIVE_CONTRACT:
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	case ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY:
		return ExecutionRegionOwnershipKind::SOURCE_BOUNDARY;
	case ExecutionRegionStageExecutionKind::MISSING_CONTRACT:
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	default:
		return ExecutionRegionOwnershipKind::NONE;
	}
}

static ExecutionRegionStageExecutionKind
ExecutionRegionSourceStageExecution(const ExecutionRegionNode &node, const ExecutionRegionContract &contract,
                                    ExecutionRegionSourceExecutionKind source_execution) {
	if (contract.source_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT) {
		return ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		return ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
	}
	if (node.source && node.source->source_contract.status == ExecutionRegionSourceContractStatus::BLOCKED) {
		return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	}
	return ExecutionRegionStageExecutionFromOwnership(contract.source_ownership);
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

static bool ExecutionRegionStageHasExecutableWork(ExecutionRegionStageExecutionKind execution, bool executable_work) {
	return executable_work && execution != ExecutionRegionStageExecutionKind::MISSING_CONTRACT &&
	       execution != ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
}

static void AddExecutionRegionStage(ExecutionRegionStagePlan &plan, ExecutionRegionStageKind kind,
                                    ExecutionRegionStageExecutionKind execution, ExecutionRegionOwnershipKind ownership,
                                    idx_t node_index, const ExecutionRegionNode &node,
                                    idx_t filter_index = DConstants::INVALID_INDEX, string reason = string(),
                                    ExecutionCompiledContractKind operation = ExecutionCompiledContractKind::NONE,
                                    ExecutionCompiledDrainKind drain = ExecutionCompiledDrainKind::NONE,
                                    string required_capability = string(), bool executable_work = false) {
	ExecutionRegionStage stage;
	stage.kind = kind;
	stage.execution = execution;
	stage.ownership = ownership;
	stage.operation = operation;
	stage.drain = drain;
	stage.executable_work = executable_work;
	stage.node_index = node_index;
	stage.operator_index = node.operator_index;
	stage.filter_index = filter_index;
	stage.operator_name = node.operator_name;
	stage.required_capability = std::move(required_capability);
	stage.reason = std::move(reason);
	plan.has_executable_work =
	    plan.has_executable_work || ExecutionRegionStageHasExecutableWork(execution, executable_work);
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
                                            ExecutionRegionStageExecutionKind execution,
                                            ExecutionRegionOwnershipKind ownership) {
	AddExecutionRegionStage(plan, compiled_stage.stage, execution, ownership, node_index, node,
	                        DConstants::INVALID_INDEX, BuildExecutionRegionCompiledStageReason(compiled_stage, node),
	                        compiled_stage.operation, compiled_stage.drain, compiled_stage.required_capability,
	                        compiled_stage.executable_work);
}

static bool AddExecutionRegionCompiledStages(ExecutionRegionStagePlan &plan, const ExecutionRegionNode &node,
                                             idx_t node_index) {
	if (!node.compiled_contract.Present()) {
		return false;
	}
	for (auto &compiled_stage : node.compiled_contract.stages) {
		auto execution = compiled_stage.execution;
		auto ownership = ExecutionRegionOwnershipFromStageExecution(execution);
		AddExecutionRegionCompiledStage(plan, compiled_stage, node_index, node, execution, ownership);
	}
	return true;
}

static string BuildExecutionRegionStagePlanShape(const ExecutionRegionContract &contract,
                                                 const string &candidate_shape) {
	string result = ExecutionRegionABIIsFullPipeline(contract.abi) ? "full-pipeline" : "unknown";
	result += ":";
	result += candidate_shape;
	return result;
}

ExecutionRegionStagePlan BuildExecutionRegionStagePlan(const ExecutionRegionIR &region_ir,
                                                       const ExecutionRegionCandidate &candidate,
                                                       ExecutionRegionIRMode mode) {
	ExecutionRegionStagePlan plan;
	plan.shape = BuildExecutionRegionStagePlanShape(candidate.contract, candidate.shape);
	const auto end_node = MinValue(candidate.EndNode(), NumericCast<idx_t>(region_ir.nodes.size()));
	for (idx_t node_idx = candidate.first_node; node_idx < end_node; node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE: {
			auto source_execution = GetExecutionRegionCandidateSourceExecution(candidate, node);
			bool added_source_stage = false;
			if (node.compiled_contract.Present()) {
				for (auto &compiled_stage : node.compiled_contract.stages) {
					if (compiled_stage.stage != ExecutionRegionStageKind::SOURCE) {
						continue;
					}
					auto execution = ExecutionRegionSourceStageExecution(node, candidate.contract, source_execution);
					auto ownership = ExecutionRegionOwnershipFromStageExecution(execution);
					AddExecutionRegionCompiledStage(plan, compiled_stage, node_idx, node, execution, ownership);
					added_source_stage = true;
				}
			}
			if (!added_source_stage) {
				auto execution = ExecutionRegionSourceStageExecution(node, candidate.contract, source_execution);
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::SOURCE, execution,
				                        ExecutionRegionOwnershipFromStageExecution(execution), node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			if (node.source) {
				for (idx_t filter_idx = 0; filter_idx < node.source->filters.size(); filter_idx++) {
					auto &filter = node.source->filters[filter_idx];
					ExecutionRegionOwnershipKind filter_ownership;
					if (ExecutionRegionCandidateUsesScanFilters(candidate, node)) {
						filter_ownership = ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
					} else if (ExecutionRegionSourceFilterHasGeneratedExpression(filter)) {
						filter_ownership = ExecutionRegionOwnershipKind::GENERATED_IR;
					} else {
						filter_ownership = ExecutionRegionOwnershipKind::MISSING_CONTRACT;
					}
					auto filter_execution =
					    source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY
					        ? ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY
					        : ExecutionRegionStageExecutionFromOwnership(filter_ownership);
					AddExecutionRegionStage(plan, ExecutionRegionStageKind::SOURCE_FILTER, filter_execution,
					                        filter_ownership, node_idx, node, filter_idx, filter.reason,
					                        ExecutionCompiledContractKind::FILTER_STAGE,
					                        ExecutionCompiledDrainKind::ZERO_OR_ONE_OUTPUT, string(),
					                        filter_execution == ExecutionRegionStageExecutionKind::GENERATED_IR);
				}
			}
			break;
		}
		case ExecutionRegionNodeKind::FILTER:
			AddExecutionRegionStage(plan, ExecutionRegionStageKind::FILTER,
			                        node.blocker_reason.empty() ? ExecutionRegionStageExecutionKind::GENERATED_IR
			                                                    : ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
			                        node.blocker_reason.empty() ? ExecutionRegionOwnershipKind::GENERATED_IR
			                                                    : ExecutionRegionOwnershipKind::MISSING_CONTRACT,
			                        node_idx, node, DConstants::INVALID_INDEX, node.blocker_reason,
			                        ExecutionCompiledContractKind::NONE, ExecutionCompiledDrainKind::NONE, string(),
			                        node.blocker_reason.empty());
			break;
		case ExecutionRegionNodeKind::PROJECTION:
			AddExecutionRegionStage(plan, ExecutionRegionStageKind::PROJECTION,
			                        node.blocker_reason.empty() ? ExecutionRegionStageExecutionKind::GENERATED_IR
			                                                    : ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
			                        node.blocker_reason.empty() ? ExecutionRegionOwnershipKind::GENERATED_IR
			                                                    : ExecutionRegionOwnershipKind::MISSING_CONTRACT,
			                        node_idx, node, DConstants::INVALID_INDEX, node.blocker_reason,
			                        ExecutionCompiledContractKind::NONE, ExecutionCompiledDrainKind::NONE, string(),
			                        node.blocker_reason.empty() && ExecutionRegionProjectionHasExecutableWork(node));
			break;
		case ExecutionRegionNodeKind::OPERATOR: {
			if (!AddExecutionRegionCompiledStages(plan, node, node_idx)) {
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::OPERATOR_BOUNDARY,
				                        ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
				                        ExecutionRegionOwnershipKind::MISSING_CONTRACT, node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			break;
		}
		case ExecutionRegionNodeKind::SINK:
			if (!AddExecutionRegionCompiledStages(plan, node, node_idx)) {
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::SINK_BOUNDARY,
				                        ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
				                        ExecutionRegionOwnershipKind::MISSING_CONTRACT, node_idx, node,
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
