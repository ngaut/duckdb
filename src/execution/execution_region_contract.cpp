#include "execution_region_contract.hpp"

namespace duckdb {

// Storage filtering avoids materializing a generated selection for moderate aggregate inputs. For very large inputs,
// keep the filter in the generated SIMD loop so the scan and reduction remain one hot path.
static constexpr idx_t EXECUTION_REGION_STORAGE_FILTER_MIN_BATCHES = 256;
static constexpr idx_t EXECUTION_REGION_STORAGE_FILTER_MAX_BATCHES = 4096;

static string ExecutionRegionContractBool(bool value) {
	return value ? "true" : "false";
}

static string BuildExecutionRegionContractStringList(const vector<string> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += values[value_idx];
	}
	result += "]";
	return result;
}

static void AddExecutionRegionContractUniqueString(vector<string> &values, string value) {
	if (value.empty()) {
		return;
	}
	for (auto &entry : values) {
		if (entry == value) {
			return;
		}
	}
	values.push_back(std::move(value));
}

static bool ExecutionRegionContractShouldRenderDiagnostics(ExecutionRegionIRMode mode) {
	return mode == ExecutionRegionIRMode::TRACE;
}

static void RecordExecutionRegionContractOwnership(ExecutionRegionContract &contract,
                                                   ExecutionRegionOwnershipKind ownership) {
	switch (ownership) {
	case ExecutionRegionOwnershipKind::GENERATED_IR:
		contract.generated_operator_count++;
		break;
	case ExecutionRegionOwnershipKind::SOURCE_BOUNDARY:
		contract.source_boundary_count++;
		break;
	default:
		break;
	}
}

static void RecordExecutionRegionMissingContract(ExecutionRegionContract &contract, const string &required_capability,
                                                 const string &blocker) {
	contract.missing_contract_count++;
	AddExecutionRegionContractUniqueString(contract.required_capabilities, required_capability);
	AddExecutionRegionContractUniqueString(contract.blockers, blocker);
}

ExecutionRegionSourceExecutionKind GetExecutionRegionCandidateSourceExecution(const ExecutionRegionCandidate &candidate,
                                                                              const ExecutionRegionNode &node) {
	if (candidate.source_execution != ExecutionRegionSourceExecutionKind::NONE) {
		return candidate.source_execution;
	}
	return node.source ? node.source->execution : ExecutionRegionSourceExecutionKind::NONE;
}

static bool ExecutionRegionSourceFiltersCanUseGeneratedOutput(const ExecutionRegionNode &node) {
	if (!node.source || node.source->filters.empty() || !node.source->table_scan_contract.present) {
		return false;
	}
	auto &contract = node.source->table_scan_contract;
	auto &projection_map = contract.source_contract_output_projection_map;
	if (projection_map.size() != node.output_types.size()) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		auto input_idx = projection_map[output_idx];
		if (input_idx >= contract.source_contract_input_types.size()) {
			return false;
		}
		if (contract.source_contract_input_types[input_idx] != node.output_types[output_idx]) {
			return false;
		}
	}
	for (auto &filter : node.source->filters) {
		if (!filter.generated_source_stage_candidate) {
			return false;
		}
		if (filter.scan_column_index >= contract.source_contract_input_types.size()) {
			return false;
		}
	}
	return true;
}

static bool ExecutionRegionExpressionHasStorageSensitiveStringMatch(const ExecutionExpressionIR &expression) {
	if (expression.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    (expression.intrinsic == ExecutionExpressionIntrinsicKind::STRING_CONTAINS ||
	     expression.intrinsic == ExecutionExpressionIntrinsicKind::STRING_LIKE)) {
		return true;
	}
	if (expression.left && ExecutionRegionExpressionHasStorageSensitiveStringMatch(*expression.left)) {
		return true;
	}
	if (expression.right && ExecutionRegionExpressionHasStorageSensitiveStringMatch(*expression.right)) {
		return true;
	}
	if (expression.else_node && ExecutionRegionExpressionHasStorageSensitiveStringMatch(*expression.else_node)) {
		return true;
	}
	for (auto &child : expression.children) {
		if (child && ExecutionRegionExpressionHasStorageSensitiveStringMatch(*child)) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionSourceFiltersAreStorageCompatible(const ExecutionRegionNode &node) {
	if (!node.source) {
		return false;
	}
	for (auto &filter : node.source->filters) {
		if (!filter.expression || !filter.expression->root ||
		    ExecutionRegionExpressionHasStorageSensitiveStringMatch(*filter.expression->root)) {
			return false;
		}
	}
	return true;
}

bool ExecutionRegionCandidateUsesScanFilters(const ExecutionRegionCandidate &candidate,
                                             const ExecutionRegionNode &node) {
	if (!candidate.contract.OwnsSource() || !node.source || !node.source->table_scan_contract.present) {
		return false;
	}
	if (node.source->table_scan_contract.dynamic_filters) {
		return node.source->table_scan_contract.filter_pushdown;
	}
	if (node.source->filters.empty()) {
		return false;
	}
	if (ExecutionRegionSourceFiltersCanUseGeneratedOutput(node)) {
		const auto source_cardinality = MaxValue(node.source->table_scan_contract.estimated_source_cardinality,
		                                         candidate.traits.source_contract_input_cardinality);
		const auto sink_kind = candidate.traits.sink_kind;
		const bool aggregate_sink = sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		                            sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
		const bool moderate_source =
		    source_cardinality >= STANDARD_VECTOR_SIZE * EXECUTION_REGION_STORAGE_FILTER_MIN_BATCHES &&
		    source_cardinality <= STANDARD_VECTOR_SIZE * EXECUTION_REGION_STORAGE_FILTER_MAX_BATCHES;
		const bool storage_compatible_filters = ExecutionRegionSourceFiltersAreStorageCompatible(node);
		const bool moderate_single_filter_aggregate = aggregate_sink && storage_compatible_filters && moderate_source &&
		                                              candidate.traits.source_filter_count == 1 &&
		                                              sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
		const bool highly_selective_aggregate = aggregate_sink && storage_compatible_filters &&
		                                        candidate.estimated_cardinality > 0 &&
		                                        candidate.estimated_cardinality * 16 < source_cardinality;
		if (!moderate_single_filter_aggregate && !highly_selective_aggregate) {
			return false;
		}
	}
	return node.source->table_scan_contract.filter_pushdown;
}

static ExecutionRegionOwnershipKind
ClassifyExecutionSourceProtocolContractOwnership(const ExecutionRegionSourceInfo &source,
                                                 ExecutionRegionContract &region_contract,
                                                 ExecutionRegionSourceExecutionKind execution) {
	auto &source_contract = source.source_contract;
	if (execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	    source_contract.status == ExecutionRegionSourceContractStatus::READY) {
		AddExecutionRegionContractUniqueString(region_contract.required_capabilities,
		                                       source_contract.required_capability);
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	}
	if (execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		AddExecutionRegionContractUniqueString(region_contract.blockers, source_contract.blocker);
		return ExecutionRegionOwnershipKind::SOURCE_BOUNDARY;
	}
	if (!source_contract.required_capability.empty() || !source_contract.blocker.empty()) {
		RecordExecutionRegionMissingContract(region_contract, source_contract.required_capability,
		                                     source_contract.blocker);
	}
	return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
}

static ExecutionRegionOwnershipKind
ClassifyExecutionRegionStateScanOwnership(const ExecutionRegionSourceInfo &source,
                                          ExecutionRegionContract &region_contract) {
	auto &state_contract = source.native_state_scan_contract;
	if (state_contract.status == ExecutionRegionStateContractStatus::NONE) {
		return ExecutionRegionOwnershipKind::NONE;
	}
	if (state_contract.status == ExecutionRegionStateContractStatus::READY) {
		AddExecutionRegionContractUniqueString(region_contract.required_capabilities,
		                                       state_contract.required_capability);
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	}
	RecordExecutionRegionMissingContract(region_contract, state_contract.required_capability, state_contract.blocker);
	return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
}

static void RecordExecutionRegionGroupedStateContract(ExecutionRegionContract &region_contract,
                                                      const ExecutionRegionAggregateContract &contract);

static ExecutionRegionOwnershipKind
ClassifyExecutionCompiledContractOwnership(const ExecutionCompiledOperatorContract &compiled_contract,
                                           ExecutionRegionContract &region_contract, const string &blocker_reason) {
	if (!compiled_contract.Present()) {
		RecordExecutionRegionMissingContract(region_contract, "native-operator-contract", blocker_reason);
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	bool saw_native = false;
	bool saw_generated = false;
	bool saw_source_boundary = false;
	bool saw_missing = false;
	for (auto &stage : compiled_contract.stages) {
		switch (stage.execution) {
		case ExecutionRegionStageExecutionKind::NATIVE_CONTRACT:
			saw_native = true;
			AddExecutionRegionContractUniqueString(region_contract.required_capabilities, stage.required_capability);
			break;
		case ExecutionRegionStageExecutionKind::GENERATED_IR:
			saw_generated = true;
			break;
		case ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY:
			saw_source_boundary = true;
			AddExecutionRegionContractUniqueString(region_contract.blockers,
			                                       stage.blocker.empty() ? blocker_reason : stage.blocker);
			break;
		case ExecutionRegionStageExecutionKind::MISSING_CONTRACT:
			saw_missing = true;
			RecordExecutionRegionMissingContract(region_contract, stage.required_capability,
			                                     stage.blocker.empty() ? blocker_reason : stage.blocker);
			break;
		default:
			break;
		}
	}
	if (saw_missing) {
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	if (saw_source_boundary) {
		return ExecutionRegionOwnershipKind::SOURCE_BOUNDARY;
	}
	if (saw_native) {
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	}
	if (saw_generated) {
		return ExecutionRegionOwnershipKind::GENERATED_IR;
	}
	return ExecutionRegionOwnershipKind::NONE;
}

static ExecutionRegionOwnershipKind
ClassifyExecutionRegionSourceOwnership(const ExecutionRegionNode &node, ExecutionRegionContract &region_contract,
                                       ExecutionRegionSourceExecutionKind execution) {
	if (!node.source) {
		RecordExecutionRegionMissingContract(region_contract, "source-contract", node.blocker_reason);
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	auto &source = *node.source;
	if (source.kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
		region_contract.state_scan_ownership = ClassifyExecutionRegionStateScanOwnership(source, region_contract);
		if (region_contract.state_scan_ownership != ExecutionRegionOwnershipKind::NONE) {
			return region_contract.state_scan_ownership;
		}
		return ClassifyExecutionSourceProtocolContractOwnership(source, region_contract, execution);
	}
	region_contract.state_scan_ownership = ExecutionRegionOwnershipKind::NONE;
	return ClassifyExecutionSourceProtocolContractOwnership(source, region_contract, execution);
}

static void RecordExecutionRegionGroupedStateContract(ExecutionRegionContract &region_contract,
                                                      const ExecutionRegionAggregateContract &contract) {
	auto &state_contract = contract.native_grouped_state_contract;
	if (state_contract.status == ExecutionRegionStateContractStatus::NONE ||
	    state_contract.status == ExecutionRegionStateContractStatus::READY) {
		if (state_contract.status == ExecutionRegionStateContractStatus::READY) {
			AddExecutionRegionContractUniqueString(region_contract.required_capabilities,
			                                       state_contract.required_capability);
		}
		return;
	}
	RecordExecutionRegionMissingContract(region_contract, state_contract.required_capability, state_contract.blocker);
}

static bool ExecutionRegionHashAggregateSinkUsesRegularDistinctNativeUpdate(const ExecutionRegionSinkInfo &sink) {
	auto &contract = sink.aggregate_contract;
	return sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	       (contract.distinct_aggregate_count != 0 || contract.distinct_table_count != 0 ||
	        contract.distinct_child_count != 0 || contract.distinct_filter_count != 0);
}

static ExecutionRegionOwnershipKind ClassifyExecutionRegionSinkOwnership(const ExecutionRegionNode &node,
                                                                         ExecutionRegionContract &region_contract) {
	if (!node.sink) {
		RecordExecutionRegionMissingContract(region_contract, "native-sink-contract", node.blocker_reason);
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	auto &sink = *node.sink;
	if ((sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	     sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) &&
	    !ExecutionRegionHashAggregateSinkUsesRegularDistinctNativeUpdate(sink)) {
		RecordExecutionRegionGroupedStateContract(region_contract, sink.aggregate_contract);
	}
	return ClassifyExecutionCompiledContractOwnership(node.compiled_contract, region_contract, node.blocker_reason);
}

static ExecutionRegionOwnershipKind CombineExecutionRegionTransformOwnership(ExecutionRegionOwnershipKind current,
                                                                             ExecutionRegionOwnershipKind next) {
	if (current == ExecutionRegionOwnershipKind::MISSING_CONTRACT ||
	    next == ExecutionRegionOwnershipKind::MISSING_CONTRACT) {
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	if (current == ExecutionRegionOwnershipKind::GENERATED_IR || next == ExecutionRegionOwnershipKind::GENERATED_IR) {
		return ExecutionRegionOwnershipKind::GENERATED_IR;
	}
	return current == ExecutionRegionOwnershipKind::NONE ? next : current;
}

static string DescribeExecutionRegionContract(const ExecutionRegionContract &contract) {
	string result = "contract<abi=" + string(ExecutionRegionABIToString(contract.abi));
	result += ",owns_source=" + ExecutionRegionContractBool(contract.OwnsSource());
	result += ",owns_transform=" + ExecutionRegionContractBool(contract.OwnsTransform());
	result += ",owns_sink=" + ExecutionRegionContractBool(contract.OwnsSink());
	result += ",owns_state_scan=" + ExecutionRegionContractBool(contract.OwnsStateScan());
	result += ",source=" + string(ExecutionRegionOwnershipKindToString(contract.source_ownership));
	result += ",state_scan=" + string(ExecutionRegionOwnershipKindToString(contract.state_scan_ownership));
	result += ",transform=" + string(ExecutionRegionOwnershipKindToString(contract.transform_ownership));
	result += ",sink=" + string(ExecutionRegionOwnershipKindToString(contract.sink_ownership));
	result += ",generated_ops=" + std::to_string(contract.generated_operator_count);
	result += ",source_boundaries=" + std::to_string(contract.source_boundary_count);
	result += ",missing_contracts=" + std::to_string(contract.missing_contract_count);
	result += ",required_capabilities=" + BuildExecutionRegionContractStringList(contract.required_capabilities);
	result += ",blockers=" + BuildExecutionRegionContractStringList(contract.blockers);
	result += ">";
	return result;
}

static ExecutionRegionABI DetermineExecutionRegionContractABI(const ExecutionRegionContract &contract) {
	if (contract.OwnsSource() && contract.OwnsSink()) {
		return ExecutionRegionABI::FULL_PIPELINE;
	}
	return ExecutionRegionABI::NONE;
}

ExecutionRegionContract BuildExecutionRegionContract(const ExecutionRegionIR &region_ir,
                                                     const ExecutionRegionCandidate &candidate,
                                                     ExecutionRegionIRMode mode) {
	ExecutionRegionContract contract;
	bool has_transform = false;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode() && node_idx < region_ir.nodes.size();
	     node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE:
			contract.source_ownership = ClassifyExecutionRegionSourceOwnership(
			    node, contract, GetExecutionRegionCandidateSourceExecution(candidate, node));
			RecordExecutionRegionContractOwnership(contract, contract.source_ownership);
			break;
		case ExecutionRegionNodeKind::FILTER:
		case ExecutionRegionNodeKind::PROJECTION: {
			auto ownership = node.blocker_reason.empty() ? ExecutionRegionOwnershipKind::GENERATED_IR
			                                             : ExecutionRegionOwnershipKind::MISSING_CONTRACT;
			if (ownership == ExecutionRegionOwnershipKind::MISSING_CONTRACT) {
				RecordExecutionRegionMissingContract(contract, "native-expression-ir", node.blocker_reason);
			}
			contract.transform_ownership =
			    CombineExecutionRegionTransformOwnership(contract.transform_ownership, ownership);
			RecordExecutionRegionContractOwnership(contract, ownership);
			has_transform = true;
			break;
		}
		case ExecutionRegionNodeKind::SINK:
			contract.sink_ownership = ClassifyExecutionRegionSinkOwnership(node, contract);
			RecordExecutionRegionContractOwnership(contract, contract.sink_ownership);
			break;
		case ExecutionRegionNodeKind::OPERATOR: {
			auto ownership =
			    ClassifyExecutionCompiledContractOwnership(node.compiled_contract, contract, node.blocker_reason);
			contract.transform_ownership =
			    CombineExecutionRegionTransformOwnership(contract.transform_ownership, ownership);
			RecordExecutionRegionContractOwnership(contract, ownership);
			has_transform = true;
			break;
		}
		default:
			contract.transform_ownership = CombineExecutionRegionTransformOwnership(
			    contract.transform_ownership, ExecutionRegionOwnershipKind::MISSING_CONTRACT);
			RecordExecutionRegionMissingContract(contract, "native-region-node", node.blocker_reason);
			RecordExecutionRegionContractOwnership(contract, ExecutionRegionOwnershipKind::MISSING_CONTRACT);
			has_transform = true;
			break;
		}
	}
	if (!has_transform) {
		contract.transform_ownership = ExecutionRegionOwnershipKind::NONE;
	}
	contract.abi = DetermineExecutionRegionContractABI(contract);
	if (ExecutionRegionContractShouldRenderDiagnostics(mode)) {
		contract.ir = DescribeExecutionRegionContract(contract);
	}
	return contract;
}

static bool ExecutionRegionStageIsOperatorSlot(const ExecutionRegionStage &stage) {
	return stage.kind == ExecutionRegionStageKind::HASH_JOIN_PROBE ||
	       stage.kind == ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE ||
	       stage.kind == ExecutionRegionStageKind::OPERATOR_BOUNDARY;
}

static bool ExecutionRegionStageRequiresMissingContract(ExecutionRegionStageExecutionKind execution) {
	return execution == ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
}

static bool ExecutionRegionStageHasMissingOperatorContract(const ExecutionRegionStage &stage) {
	if (ExecutionRegionStageIsOperatorSlot(stage)) {
		if (ExecutionRegionStageRequiresMissingContract(stage.execution)) {
			return true;
		}
	}
	return false;
}

idx_t CountExecutionRegionMissingOperatorContracts(const ExecutionRegionStagePlan &stage_plan) {
	idx_t result = 0;
	for (auto &stage : stage_plan.stages) {
		if (ExecutionRegionStageHasMissingOperatorContract(stage)) {
			result++;
		}
	}
	return result;
}

} // namespace duckdb
